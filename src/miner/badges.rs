//! Badge drops: drop campaigns that give a chat badge for watch time. Live channels that can earn a
//! badge picked in the web UI get a watch slot through the BADGES rule. Channels can be quick-added
//! for a badge; those are removed again once it's earned or the campaign ends.

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    sync::{Mutex, RwLock},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

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
}

impl Badge {
    fn from_campaign(c: &Campaign) -> Option<Self> {
        let drop = c.watch_badge_drop()?;
        let reward = drop.badge.as_ref()?;
        Some(Self {
            campaign_id: c.id.clone(),
            drop_id: drop.id.clone(),
            benefit_id: reward.benefit_id.clone(),
            name: if reward.name.is_empty() { drop.name.clone() } else { reward.name.clone() },
            image: reward.image.clone(),
            game_id: c.game_id.clone(),
            game: c.game_name.clone(),
            game_slug: c.game_slug.clone(),
            allowed: c.allowed_channels.clone(),
            minutes_required: drop.minutes_required,
            minutes_watched: 0,
            ends_at: drop.end_at.min(c.end_at).timestamp() as f64,
            earned: false,
        })
    }
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
}

impl BadgeState {
    pub(super) fn load(store: &Store) -> Self {
        let read = |key: &str| store.meta(key).ok().flatten().unwrap_or_default();
        Self {
            badges: RwLock::new(Vec::new()),
            picked: RwLock::new(serde_json::from_str(&read(PICKED_KEY)).unwrap_or_default()),
            added: Mutex::new(serde_json::from_str(&read(ADDED_KEY)).unwrap_or_default()),
        }
    }
}

impl Miner {
    pub fn badges(&self) -> Vec<BadgeView> {
        let picked = self.badges.picked.read().unwrap().clone();
        let added = self.badges.added.lock().unwrap().clone();
        self.badges
            .badges
            .read()
            .unwrap()
            .iter()
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
            .collect()
    }

    fn find_badge(&self, campaign_id: &str) -> Result<Badge> {
        self.badges
            .badges
            .read()
            .unwrap()
            .iter()
            .find(|b| b.campaign_id == campaign_id)
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
        let running: HashSet<String> = {
            let mut badges = self.badges.badges.write().unwrap();
            let fresh: Vec<Badge> = campaigns
                .iter()
                .filter_map(Badge::from_campaign)
                .map(|mut b| {
                    if let Some(old) = badges.iter().find(|o| o.campaign_id == b.campaign_id) {
                        b.minutes_watched = old.minutes_watched;
                        b.earned = old.earned;
                    }
                    b
                })
                .collect();
            *badges = fresh;
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

    /// Applies watch progress and earned badges from the drops inventory.
    pub(super) fn update_badge_progress(&self, inventory: &Value) {
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
            b.earned |= done;
        }
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

    /// Removes quick-added channels whose badge is earned or whose campaign has ended.
    pub(super) fn prune_badge_channels(&self) {
        let earned: HashSet<String> = self
            .badges
            .badges
            .read()
            .unwrap()
            .iter()
            .filter(|b| b.earned)
            .map(|b| b.campaign_id.clone())
            .collect();
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
