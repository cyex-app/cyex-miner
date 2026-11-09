//! Badge drops: drop campaigns that give a chat badge for watch time. Live channels that can earn a
//! badge picked in the web UI get a watch slot through the BADGES rule. Channels can be quick-added
//! for a badge; those are removed again once it's earned or the campaign ends.

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, RwLock,
    },
};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use super::Miner;
use crate::{
    entities::{drops::Campaign, streamer::Source},
    priority::Priority,
    store::Store,
    utils::now,
};

/// `meta` key for the campaign ids of the picked badges.
const PICKED_KEY: &str = "badges";
/// `meta` key for the quick-added channels, see `AddedChannel`.
const ADDED_KEY: &str = "badge_channels";
/// Channels added per quick add. Only 2 are watched at a time; the rest are backups.
const QUICK_ADD: usize = 5;
/// `meta` key for the "Auto select badges" switch ("1" when on).
const AUTO_KEY: &str = "badges_auto";
/// How often auto select tries to add channels for a badge none of the list can earn.
const AUTO_ADD_RETRY_SECS: f64 = 30.0 * 60.0;

/// A badge drop as the web UI shows it.
#[derive(Debug, Clone, Serialize)]
pub struct BadgeView {
    pub campaign_id: String,
    pub name: String,
    pub image: String,
    pub game: String,
    pub minutes_required: i64,
    pub minutes_watched: i64,
    pub ends_at: f64,
    pub earned: bool,
    pub picked: bool,
    /// Channels the campaign is limited to; 0 means any channel streaming the game with drops on.
    pub allowed_channels: usize,
    /// Quick-added channels still in the list.
    pub added_channels: usize,
}

/// A badge earned while the miner runs, for the web UI.
#[derive(Debug, Clone, Serialize)]
pub struct EarnedBadge {
    pub campaign_id: String,
    pub name: String,
    pub image: String,
    pub game: String,
    pub minutes_required: i64,
    pub earned_at: f64,
    /// The id of the channel being watched for it when it was earned.
    pub channel_id: Option<String>,
    /// That channel's login, current as long as the channel is in the list.
    pub channel: Option<String>,
}

#[derive(Debug, Clone)]
struct Badge {
    campaign_id: String,
    drop_id: String,
    benefit_id: String,
    name: String,
    image: String,
    game_id: String,
    game: String,
    game_slug: String,
    allowed: Vec<(String, String)>,
    minutes_required: i64,
    minutes_watched: i64,
    ends_at: f64,
    earned: bool,
    /// Seen not earned in the inventory, so earning it now happened this session.
    seen_unearned: bool,
    /// Found in the drops inventory but not in the campaign list; kept until it ends.
    from_inventory: bool,
}

impl Badge {
    /// Every badge for watch time in the campaign, fewest minutes first.
    /// `events`: Twitch's free event badges, lowercase name -> image URL.
    fn from_campaign(c: &Campaign, events: &HashMap<String, String>) -> Vec<Self> {
        let names: HashSet<String> = events.keys().cloned().collect();
        c.watch_badge_drops(&names)
            .into_iter()
            .map(|(drop, reward)| {
                let image = match reward.image.as_str() {
                    "" => events.get(&reward.name.to_lowercase()).cloned().unwrap_or_default(),
                    image => image.to_string(),
                };
                Self {
                    campaign_id: c.id.clone(),
                    drop_id: drop.id.clone(),
                    benefit_id: reward.benefit_id.clone(),
                    name: if reward.name.is_empty() { drop.name.clone() } else { reward.name.clone() },
                    image,
                    game_id: c.game_id.clone(),
                    game: c.game_name.clone(),
                    game_slug: c.game_slug.clone(),
                    allowed: c.allowed_channels.clone(),
                    minutes_required: drop.minutes_required,
                    minutes_watched: 0,
                    ends_at: drop.end_at.min(c.end_at).timestamp() as f64,
                    earned: false,
                    seen_unearned: false,
                    from_inventory: false,
                }
            })
            .collect()
    }
}

/// The badge of a campaign to show and work on: the quickest one not earned yet, else the longest.
fn current<'a>(badges: &'a [Badge], campaign_id: &str) -> Option<&'a Badge> {
    let of_campaign = badges.iter().filter(|b| b.campaign_id == campaign_id);
    of_campaign
        .clone()
        .filter(|b| !b.earned)
        .min_by_key(|b| b.minutes_required)
        .or_else(|| of_campaign.max_by_key(|b| b.minutes_required))
}

/// Campaigns with a badge left to earn.
fn open_campaigns(badges: &[Badge]) -> HashSet<String> {
    badges.iter().filter(|b| !b.earned).map(|b| b.campaign_id.clone()).collect()
}

/// A channel the quick add put in the list.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddedChannel {
    campaign_id: String,
    /// When the campaign ends; the channel goes then even if the campaign list can't be loaded.
    ends_at: f64,
}

pub(super) struct BadgeState {
    badges: RwLock<Vec<Badge>>,
    /// Campaign ids.
    picked: RwLock<HashSet<String>>,
    /// Quick-added channels by channel id.
    added: Mutex<HashMap<String, AddedChannel>>,
    /// Twitch's free event badges from `EVENT_BADGES_URL`: lowercase name -> image URL.
    events: RwLock<HashMap<String, String>>,
    /// Auto select badges: pick every badge, and add channels for it when needed.
    auto: AtomicBool,
    /// When auto select last tried to add channels, by campaign id.
    auto_tried: Mutex<HashMap<String, f64>>,
    /// Badges earned since the miner started, oldest first.
    earned: Mutex<Vec<EarnedBadge>>,
}

impl BadgeState {
    pub(super) fn load(store: &Store) -> Self {
        let read = |key: &str| store.meta(key).ok().flatten().unwrap_or_default();
        Self {
            badges: RwLock::new(Vec::new()),
            picked: RwLock::new(serde_json::from_str(&read(PICKED_KEY)).unwrap_or_default()),
            added: Mutex::new(serde_json::from_str(&read(ADDED_KEY)).unwrap_or_default()),
            events: RwLock::new(HashMap::new()),
            auto: AtomicBool::new(read(AUTO_KEY) == "1"),
            auto_tried: Mutex::new(HashMap::new()),
            earned: Mutex::new(Vec::new()),
        }
    }
}

/// Twitch's free event badges (names, images, time windows), kept by catquery. Twitch doesn't
/// always mark a drop's badge as one; a reward with one of these names is treated as a badge.
const EVENT_BADGES_URL: &str = "https://api.catquery.com/eventBadges";

/// The running, free event badges in a `EVENT_BADGES_URL` response: lowercase name -> image URL.
pub(crate) fn parse_event_badges(resp: &Value, now: f64) -> HashMap<String, String> {
    resp["badges"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["free"].as_bool() == Some(true))
        .filter(|b| {
            let ends = b["endAt"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
            ends.map_or(true, |e| e.timestamp() as f64 > now)
        })
        .filter_map(|b| {
            let name = b["name"].as_str()?.trim().to_lowercase();
            (!name.is_empty()).then(|| (name, b["imageUrl"].as_str().unwrap_or_default().to_string()))
        })
        .collect()
}

impl Miner {
    /// The badges that can still be earned, one per campaign: the next one of a campaign with
    /// several. Earned ones are in `earned_badges`.
    pub fn badges(&self) -> Vec<BadgeView> {
        let picked = self.badges.picked.read().unwrap().clone();
        let added = self.badges.added.lock().unwrap().clone();
        let badges = self.badges.badges.read().unwrap();
        let view = badges
            .iter()
            .filter(|b| !b.earned && current(&badges, &b.campaign_id).is_some_and(|c| c.drop_id == b.drop_id))
            .map(|b| BadgeView {
                campaign_id: b.campaign_id.clone(),
                name: b.name.clone(),
                image: b.image.clone(),
                game: b.game.clone(),
                minutes_required: b.minutes_required,
                minutes_watched: b.minutes_watched,
                ends_at: b.ends_at,
                earned: b.earned,
                picked: picked.contains(&b.campaign_id),
                allowed_channels: b.allowed.len(),
                added_channels: added.values().filter(|a| a.campaign_id == b.campaign_id).count(),
            })
            .collect();
        view
    }

    pub fn earned_badges(&self) -> Vec<EarnedBadge> {
        let mut earned = self.badges.earned.lock().unwrap().clone();
        // Looked up by id, so a renamed channel shows (and links to) its new name.
        for e in &mut earned {
            if let Some(s) = e.channel_id.as_deref().and_then(|id| self.find_by_channel(id)) {
                e.channel = Some(s.username.clone());
            }
        }
        earned
    }

    /// The campaign's next badge to earn (its last one once all are earned).
    fn find_badge(&self, campaign_id: &str) -> Result<Badge> {
        current(&self.badges.badges.read().unwrap(), campaign_id)
            .cloned()
            .ok_or_else(|| anyhow!("No running badge drop {campaign_id}"))
    }

    /// Whether any badge is picked, which keeps the campaigns loop running.
    pub(super) fn has_picked_badges(&self) -> bool {
        !self.badges.picked.read().unwrap().is_empty()
    }

    /// Replaces the badge list from freshly loaded campaigns, keeping known progress. Picks of
    /// campaigns that are no longer running are dropped.
    pub(super) fn set_badge_campaigns(&self, campaigns: &[Campaign]) {
        let events = self.badges.events.read().unwrap().clone();
        let now = now();
        let running: HashSet<String> = {
            let mut badges = self.badges.badges.write().unwrap();
            let mut fresh: Vec<Badge> = campaigns
                .iter()
                .flat_map(|c| Badge::from_campaign(c, &events))
                .map(|mut b| {
                    // By drop: a campaign's badges each have their own progress.
                    if let Some(old) = badges.iter().find(|o| o.drop_id == b.drop_id) {
                        b.minutes_watched = old.minutes_watched;
                        b.earned = old.earned;
                        b.seen_unearned = old.seen_unearned;
                        if b.game.is_empty() {
                            b.game = old.game.clone();
                        }
                    }
                    b
                })
                .collect();
            let listed = fresh.iter().map(|b| &b.campaign_id).collect::<HashSet<_>>().len();
            // Badges only the inventory knows stay until they end.
            let from_inventory: Vec<Badge> = badges
                .iter()
                .filter(|b| b.from_inventory && b.ends_at > now && !fresh.iter().any(|f| f.drop_id == b.drop_id))
                .cloned()
                .collect();
            fresh.extend(from_inventory);
            *badges = fresh;
            // Says where an empty Badge drops list comes from: no campaigns, or none giving a badge.
            info!(
                "{} drop campaigns running, {listed} with a badge for watch time ({} event badges known)",
                campaigns.len(),
                events.len()
            );
            badges.iter().map(|b| b.campaign_id.clone()).collect()
        };
        let saved = {
            let mut picked = self.badges.picked.write().unwrap();
            let before = picked.len();
            picked.retain(|id| running.contains(id));
            (picked.len() != before).then(|| serde_json::to_string(&*picked).unwrap_or_default())
        };
        if let Some(json) = saved {
            if let Err(e) = self.store.set_meta(PICKED_KEY, &json) {
                warn!("Could not save the picked badges: {e:#}");
            }
        }
    }

    /// Loads Twitch's free event badges, used to recognise badge rewards by name. A failed load
    /// keeps the last list.
    pub(super) async fn refresh_event_badges(&self) {
        let loaded = async {
            let resp = self.twitch.http().get(EVENT_BADGES_URL).send().await?.error_for_status()?;
            Ok::<_, anyhow::Error>(parse_event_badges(&resp.json::<Value>().await?, now()))
        };
        match loaded.await {
            Ok(events) => *self.badges.events.write().unwrap() = events,
            Err(e) => warn!("Could not load the event badge list: {e:#}"),
        }
    }

    /// Adds badge campaigns the drops inventory has in progress but the campaign list doesn't.
    fn add_inventory_badges(&self, inventory: &Value) {
        let events = self.badges.events.read().unwrap().clone();
        let now = now();
        let mut badges = self.badges.badges.write().unwrap();
        for json in inventory["dropCampaignsInProgress"].as_array().into_iter().flatten() {
            let Some(campaign) = Campaign::from_json(json) else { continue };
            for mut badge in Badge::from_campaign(&campaign, &events) {
                if badge.ends_at <= now || badges.iter().any(|b| b.drop_id == badge.drop_id) {
                    continue;
                }
                info!("Found the {} badge drop in the drops inventory", badge.name);
                badge.from_inventory = true;
                badges.push(badge);
            }
        }
    }

    /// Applies watch progress and earned badges from the drops inventory. A badge seen not earned
    /// before that is earned now goes in the session's earned list.
    pub(super) fn update_badge_progress(&self, inventory: &Value) {
        self.add_inventory_badges(inventory);
        // Which watched channel each campaign is earned on, for the earned list.
        // (channel id, login, game, campaign ids)
        let watched: Vec<(String, String, String, HashSet<String>)> = self
            .streamers()
            .iter()
            .filter_map(|s| {
                let st = s.state();
                let ids = st.stream.campaigns_ids.iter().cloned().collect();
                let game = st.stream.game.as_ref().map(|g| g.display_name.clone()).unwrap_or_default();
                (st.watching && st.badge_drop).then(|| (s.channel_id.clone(), s.username.clone(), game, ids))
            })
            .collect();
        let mut newly = Vec::new();
        let earned: HashSet<&str> = inventory["gameEventDrops"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["id"].as_str())
            .collect();
        let in_progress = inventory["dropCampaignsInProgress"].as_array();
        for b in self.badges.badges.write().unwrap().iter_mut() {
            let (campaign_id, drop_id) = (b.campaign_id.as_str(), b.drop_id.as_str());
            let progress = in_progress
                .into_iter()
                .flatten()
                .filter(|c| c["id"].as_str() == Some(campaign_id))
                .flat_map(|c| c["timeBasedDrops"].as_array().into_iter().flatten())
                .find(|d| d["id"].as_str() == Some(drop_id))
                .map(|d| &d["self"]);
            let mut done = earned.contains(b.benefit_id.as_str());
            if let Some(p) = progress {
                b.minutes_watched = p["currentMinutesWatched"].as_i64().unwrap_or(b.minutes_watched);
                done |= p["isClaimed"].as_bool() == Some(true);
            }
            if done && !b.earned && b.seen_unearned {
                let channel = watched.iter().find(|(_, _, _, ids)| ids.contains(campaign_id));
                // The stream's game where the campaign didn't name one.
                let game = match (b.game.is_empty(), channel) {
                    (true, Some((_, _, game, _))) => game.clone(),
                    _ => b.game.clone(),
                };
                newly.push(EarnedBadge {
                    campaign_id: b.campaign_id.clone(),
                    name: b.name.clone(),
                    image: b.image.clone(),
                    game,
                    minutes_required: b.minutes_required,
                    earned_at: now(),
                    channel_id: channel.map(|(id, _, _, _)| id.clone()),
                    channel: channel.map(|(_, login, _, _)| login.clone()),
                });
            }
            b.earned |= done;
            b.seen_unearned |= !done;
        }
        for e in &newly {
            match &e.channel {
                Some(channel) => self.event(format!("Earned the {} badge watching {channel}", e.name)),
                None => self.event(format!("Earned the {} badge", e.name)),
            }
        }
        self.badges.earned.lock().unwrap().extend(newly);
    }

    /// Marks the live channels that can earn a picked badge right now, for the BADGES rule.
    pub(super) fn update_badge_targets(&self) {
        let targets: Vec<(String, String)> = {
            let picked = self.badges.picked.read().unwrap().clone();
            self.badges
                .badges
                .read()
                .unwrap()
                .iter()
                .filter(|b| !b.earned && picked.contains(&b.campaign_id))
                .map(|b| (b.campaign_id.clone(), b.game_id.clone()))
                .collect()
        };
        for s in self.streamers() {
            let mut st = s.state();
            let game = st.stream.game.as_ref().map(|g| g.id.clone());
            let eligible = st.is_online
                && targets.iter().any(|(campaign_id, game_id)| {
                    game.as_ref() == Some(game_id) && st.stream.campaigns_ids.contains(campaign_id)
                });
            st.badge_drop = eligible;
        }
    }

    /// Picks or unpicks a badge. Picking one turns the BADGES rule on if it was off; unpicking one
    /// removes the channels quick-added for it.
    pub fn set_badge_picked(&self, campaign_id: &str, picked: bool) -> Result<()> {
        let badge = self.find_badge(campaign_id)?;
        let saved = {
            let mut set = self.badges.picked.write().unwrap();
            if picked {
                set.insert(campaign_id.to_string());
            } else {
                set.remove(campaign_id);
            }
            serde_json::to_string(&*set)?
        };
        self.store.set_meta(PICKED_KEY, &saved).context("could not save the picked badges")?;
        if picked {
            let mut rules = self.priority();
            if !rules.contains(&Priority::Badges) {
                rules.insert(0, Priority::Badges);
                self.set_priority(rules)?;
            }
            self.event(format!("Picked the {} badge: live channels that can earn it get a watch slot", badge.name));
        } else {
            let added: Vec<String> = {
                let added = self.badges.added.lock().unwrap();
                added.iter().filter(|(_, a)| a.campaign_id == campaign_id).map(|(id, _)| id.clone()).collect()
            };
            for id in added {
                self.drop_badge_channel(&id, "badge unpicked");
            }
            self.event(format!("Unpicked the {} badge", badge.name));
        }
        self.update_badge_targets();
        Ok(())
    }

    pub fn auto_badges(&self) -> bool {
        self.badges.auto.load(Ordering::Relaxed)
    }

    /// Turns auto select on or off; saved in the database. Turning it on picks every badge right
    /// away; channels are added by the campaigns loop (`run_auto_badges`).
    pub fn set_auto_badges(&self, on: bool) -> Result<()> {
        self.store
            .set_meta(AUTO_KEY, if on { "1" } else { "0" })
            .context("could not save the auto select setting")?;
        if self.badges.auto.swap(on, Ordering::Relaxed) == on {
            return Ok(());
        }
        self.event(if on {
            "Auto selecting badges: every badge drop is picked, and channels are added when needed"
        } else {
            "Stopped auto selecting badges"
        });
        if on {
            self.badges.auto_tried.lock().unwrap().clear();
            self.pick_all_badges();
        }
        Ok(())
    }

    /// Picks every badge not earned yet that isn't picked.
    fn pick_all_badges(&self) {
        let unpicked: Vec<String> = {
            let picked = self.badges.picked.read().unwrap().clone();
            let open = open_campaigns(&self.badges.badges.read().unwrap());
            open.into_iter().filter(|id| !picked.contains(id)).collect()
        };
        for id in unpicked {
            if let Err(e) = self.set_badge_picked(&id, true) {
                warn!("Could not pick badge {id}: {e:#}");
            }
        }
    }

    /// Auto select, run every minute: picks new badges, and for a badge no channel in the list
    /// can earn right now, adds live channels for it (at most every 30 minutes per badge).
    pub(super) async fn run_auto_badges(&self) {
        if !self.auto_badges() || self.paused() {
            return;
        }
        self.pick_all_badges();
        let now = now();
        let needs_channels: Vec<(String, String)> = {
            // Campaigns some live channel in the list can earn right now.
            let covered: HashSet<String> = self
                .streamers()
                .iter()
                .filter_map(|s| {
                    let st = s.state();
                    st.is_online.then(|| st.stream.campaigns_ids.clone())
                })
                .flatten()
                .collect();
            // Channels already added per campaign; past `2 * QUICK_ADD` no more are added.
            let mut added: HashMap<String, usize> = HashMap::new();
            for a in self.badges.added.lock().unwrap().values() {
                *added.entry(a.campaign_id.clone()).or_default() += 1;
            }
            let tried = self.badges.auto_tried.lock().unwrap();
            let badges = self.badges.badges.read().unwrap();
            open_campaigns(&badges)
                .into_iter()
                .filter(|id| !covered.contains(id))
                .filter(|id| added.get(id).copied().unwrap_or(0) < 2 * QUICK_ADD)
                .filter(|id| tried.get(id).map_or(true, |t| now - t > AUTO_ADD_RETRY_SECS))
                .filter_map(|id| current(&badges, &id).map(|b| (id.clone(), b.name.clone())))
                .collect()
        };
        for (id, name) in needs_channels {
            self.badges.auto_tried.lock().unwrap().insert(id.clone(), now);
            // Each added channel is logged as it's added.
            if let Err(e) = self.quick_add_badge(&id).await {
                info!("No channels added for the {name} badge yet: {e:#}");
            }
        }
    }

    /// Adds up to `QUICK_ADD` live channels that can earn the badge, most viewers first, and picks it.
    /// Returns the added logins.
    pub async fn quick_add_badge(&self, campaign_id: &str) -> Result<Vec<String>> {
        let badge = self.find_badge(campaign_id)?;
        if badge.earned {
            bail!("The {} badge is already earned", badge.name);
        }
        // Live streams of the game: from the directory, or the campaign's own channel list.
        let streaming: Vec<(String, String, i64)> = if badge.allowed.is_empty() {
            if badge.game_slug.is_empty() {
                bail!("Twitch didn't say which game the {} badge is for", badge.name);
            }
            self.twitch.live_in_game(&badge.game_slug).await?
        } else {
            let logins: Vec<String> = badge.allowed.iter().map(|(_, login)| login.clone()).collect();
            let overview = self.twitch.channels_overview(&logins).await;
            badge
                .allowed
                .iter()
                .zip(overview)
                .filter_map(|((id, login), (_, info))| {
                    let info = info.ok().flatten()?;
                    (info.game.as_ref()?.id == badge.game_id).then(|| (id.clone(), login.clone(), info.viewers))
                })
                .collect()
        };
        // Drops have to be on for this campaign on the stream itself.
        let ids: Vec<String> = streaming.iter().map(|(id, _, _)| id.clone()).collect();
        let campaigns = self.twitch.campaign_ids_for_channels(&ids).await;
        let mut live: Vec<(String, String, i64)> = streaming
            .into_iter()
            .zip(campaigns)
            .filter(|(_, ids)| ids.contains(&badge.campaign_id))
            .map(|(channel, _)| channel)
            .collect();

        // Leave channels already in the database (followed ones may not be loaded) and removed ones alone.
        let known: HashSet<String> = self.store.channels()?.into_iter().map(|r| r.id).collect();
        let blacklist = self.store.blacklist().unwrap_or_default();
        live.retain(|(id, _, _)| !known.contains(id) && !blacklist.contains(id));
        live.sort_by_key(|(_, _, viewers)| Reverse(*viewers));
        live.truncate(QUICK_ADD);
        if live.is_empty() {
            bail!("No eligible channel that isn't in your list is live for {} right now", badge.name);
        }

        let mut added = Vec::new();
        for (id, login, _) in live {
            let reserved = self.adding.lock().unwrap().insert(login.clone());
            if !reserved {
                continue;
            }
            // Recorded first, so a crash mid-add can't leave a badge channel that's never removed.
            let entry = AddedChannel { campaign_id: badge.campaign_id.clone(), ends_at: badge.ends_at };
            self.badges.added.lock().unwrap().insert(id.clone(), entry);
            self.save_badge_channels();
            let result = self.add_channel(id.clone(), &login, Source::Badge).await;
            self.adding.lock().unwrap().remove(&login);
            match result {
                Ok(_) => added.push(login),
                Err(e) => {
                    self.forget_badge_channel(&id);
                    warn!("Could not add {login} for the {} badge: {e:#}", badge.name);
                }
            }
        }
        if added.is_empty() {
            bail!("Could not add any channel for {}, see the log", badge.name);
        }

        let picked = self.badges.picked.read().unwrap().contains(campaign_id);
        if picked {
            self.update_badge_targets();
        } else {
            self.set_badge_picked(campaign_id, true)?;
        }
        Ok(added)
    }

    /// Removes quick-added channels whose campaign has every badge earned, or has ended.
    pub(super) fn prune_badge_channels(&self) {
        let earned: HashSet<String> = {
            let badges = self.badges.badges.read().unwrap();
            let open = open_campaigns(&badges);
            badges
                .iter()
                .filter(|b| !open.contains(&b.campaign_id))
                .map(|b| b.campaign_id.clone())
                .collect()
        };
        let now = now();
        let done: Vec<(String, &str)> = self
            .badges
            .added
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, a)| {
                if earned.contains(&a.campaign_id) {
                    Some((id.clone(), "badge earned"))
                } else {
                    (now > a.ends_at).then(|| (id.clone(), "badge drop ended"))
                }
            })
            .collect();
        for (id, reason) in done {
            self.drop_badge_channel(&id, reason);
        }
    }

    /// Removes a quick-added channel, without blacklisting it.
    fn drop_badge_channel(&self, channel_id: &str, reason: &str) {
        self.forget_badge_channel(channel_id);
        // A channel added again by hand is Manual by now and stays.
        if !self.find_by_channel(channel_id).is_some_and(|s| s.source == Source::Badge) {
            return;
        }
        let Ok(streamer) = self.unregister(channel_id) else { return };
        if let Err(e) = self.store.remove_channel(channel_id) {
            warn!("Could not remove {} from the database: {e:#}", streamer.username);
        }
        self.event(format!("Removed {streamer}: {reason}"));
    }

    /// Stops tracking a channel as quick-added, e.g. because it was removed or added by hand.
    pub(super) fn forget_badge_channel(&self, channel_id: &str) {
        let removed = self.badges.added.lock().unwrap().remove(channel_id).is_some();
        if removed {
            self.save_badge_channels();
        }
    }

    fn save_badge_channels(&self) {
        let json = serde_json::to_string(&*self.badges.added.lock().unwrap()).unwrap_or_default();
        if let Err(e) = self.store.set_meta(ADDED_KEY, &json) {
            warn!("Could not save the quick-added badge channels: {e:#}");
        }
    }
}
