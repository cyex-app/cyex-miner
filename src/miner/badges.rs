//! Badges for watch time, from catquery's event badge list (`EVENT_BADGES_URL`) and
//! streamdatabase's global badge list (`STREAMDATABASE_LIST_URL`), merged so each badge is there
//! once: each free, running badge's streamdatabase page says where to watch (a category, or named
//! channels) and for how long. Live channels that can earn a picked badge are watched alone, ahead
//! of every other rule, apart from short breaks for pending watch streaks. Channels can be
//! quick-added for a badge; those are removed again once it's earned or it ends.

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
use serde_json::{json, Value};
use tracing::{info, warn};

use super::Miner;
use crate::{
    entities::{
        drops::Campaign,
        streamer::{Source, Streamer},
    },
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
/// A quick-added channel offline this long is removed, making room for live ones.
const BADGE_CHANNEL_OFFLINE_SECS: f64 = 10.0 * 60.0;
/// `meta` key for the reward badges (by drop id) the miner stopped watching for: watched well
/// past their goal without Twitch confirming them, so they aren't watched again after a restart.
const REWARD_DONE_KEY: &str = "reward_badges_done";
/// Minutes past twice its goal a badge Twitch reports no progress for is watched before the miner
/// gives up on it. It isn't counted as earned then; only Twitch's word counts.
const REWARD_MARGIN_MINUTES: f64 = 5.0;
/// `meta` key for the days watched for badges that ask for several (GTA VI's chains: 60 minutes
/// on 2 to 6 days) that Twitch reports nothing for, by drop id, see `DayCount`.
const REWARD_DAYS_KEY: &str = "reward_badge_days";
/// `meta` key for when the badges of a series were given, by drop id (see `Badge::awarded_at`).
/// Twitch doesn't list some (GTA VI's chains) in the inventory, so a restart would forget when
/// the last day was earned, and with it when the next one opens.
const REWARD_AWARDED_KEY: &str = "reward_badge_awarded";
/// A badge's next day opens this long after the previous day's minutes were earned (Twitch's
/// daily reset), plus a few minutes so none are watched before it does.
const BADGE_DAY_SECS: f64 = 24.0 * 60.0 * 60.0 + 5.0 * 60.0;
/// Twitch's "Special Events" categories: a campaign filed under one is earned on its channels
/// whatever they stream (TwitchDropsMiner treats them the same way).
const SPECIAL_GAME_IDS: [&str; 2] = ["509663", "509672"];

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
    /// Watched well past its goal without Twitch confirming it; no longer watched for until
    /// picked again or channels are added for it.
    pub unconfirmed: bool,
    /// Days to watch `minutes_required` on; 1 for most badges.
    pub days_required: i64,
    /// Days the miner counted for a badge Twitch reports nothing for; 0 otherwise.
    pub days_done: i64,
    /// Today's minutes are in (a badge watched on several days): watched for again from
    /// `next_day_at`.
    pub done_today: bool,
    /// When the next day of a badge on several days opens (24 hours after the last one was
    /// earned); 0 when it isn't waiting.
    pub next_day_at: f64,
    /// Twitch says the drop isn't unlocked yet (an earlier one first, or a later day).
    pub locked: bool,
}

/// The days counted for a badge watched on several days, see `REWARD_DAYS_KEY`. A day's
/// minutes count once its goal is met; the next day opens 24 hours after that, as on Twitch.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct DayCount {
    /// Minutes watched for the day going on.
    minutes: f64,
    /// Days whose minutes were watched.
    days_done: i64,
    /// When the last day's minutes were in; 0 before the first.
    last_done_at: f64,
}

impl DayCount {
    /// When the next day opens, while it hasn't yet.
    fn waiting_until(&self, now: f64) -> Option<f64> {
        let next = self.last_done_at + BADGE_DAY_SECS;
        (self.last_done_at > 0.0 && now < next).then_some(next)
    }
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
    /// An event badge: earned on live channels in its game (or the channels it names),
    /// whatever drops they list.
    reward: bool,
    /// Minutes watched for a reward badge, counted by the miner while Twitch reports none.
    reward_minutes: f64,
    /// A reward badge whose progress Twitch reports in the drops inventory (a drop giving a
    /// reward of the same name): Twitch's minutes count, not the miner's.
    twitch_tracked: bool,
    /// A reward badge watched well past its goal without Twitch confirming it: not watched for
    /// anymore, but not earned either.
    stopped: bool,
    /// Days to watch `minutes_required` on (each day); 1 for most badges.
    days_required: i64,
    /// The miner's own day count, for a badge on several days Twitch reports nothing for.
    days: DayCount,
    /// Twitch says its drop isn't unlocked yet (`hasPreconditionsMet` false): not watched for.
    locked: bool,
    /// When its window starts, for grouping a series (see `update_series`).
    starts_at: f64,
    /// When Twitch gave it (the inventory's `lastAwardedAt`), or when the miner saw it earned; 0
    /// when not known. The next badge of its series waits 24 hours after it.
    awarded_at: f64,
    /// A later badge of a series (GTA VI's chains): not shown or watched for until the ones
    /// before it are earned, see `update_series`.
    queued: bool,
    /// The first badge of a series waits until 24 hours after the one before it was given, as
    /// Twitch's daily reset does; 0 when it isn't waiting.
    series_wait_until: f64,
}

/// Groups badges that are one series: the same game and window, asking for a different number
/// of days each (GTA VI's chains: Purple on 1 day, Bronze on 2, ... Miami on 6). Only the first
/// not earned yet is shown and watched for (`queued` marks the rest), and it waits 24 hours
/// after the one before it was given (`series_wait_until`).
fn update_series(badges: &mut [Badge]) {
    let key = |b: &Badge| {
        (b.reward && !b.game_id.is_empty()).then(|| format!("{}|{}|{}", b.game_id, b.starts_at as i64, b.ends_at as i64))
    };
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, b) in badges.iter().enumerate() {
        if let Some(k) = key(b) {
            groups.entry(k).or_default().push(i);
        }
    }
    for b in badges.iter_mut() {
        b.queued = false;
        b.series_wait_until = 0.0;
    }
    for mut members in groups.into_values() {
        // A series asks for a different number of days in each badge; badges that just share a
        // game and window (two 30-minute badges) aren't one.
        let days: HashSet<i64> = members.iter().map(|&i| badges[i].days_required).collect();
        if members.len() < 2 || days.len() != members.len() {
            continue;
        }
        members.sort_by_key(|&i| badges[i].days_required);
        let Some(head) = members.iter().position(|&i| !badges[i].done()) else { continue };
        for &i in &members[head + 1..] {
            badges[i].queued = !badges[i].done();
        }
        if head > 0 {
            let (before, days_before) = {
                let b = &badges[members[head - 1]];
                (b.awarded_at, b.days_required)
            };
            let next = &mut badges[members[head]];
            if before > 0.0 {
                next.series_wait_until = before + BADGE_DAY_SECS;
            }
            // The days of the badges before it count toward it on Twitch (Bronze's 2 days are
            // Purple's day and one more), so the miner's own count starts there.
            if next.days.days_done < days_before {
                next.days = DayCount { minutes: 0.0, days_done: days_before, last_done_at: before };
            }
        }
    }
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
                    reward: c.reward,
                    reward_minutes: 0.0,
                    twitch_tracked: false,
                    stopped: false,
                    days_required: drop.days_required.max(1),
                    days: DayCount::default(),
                    locked: false,
                    starts_at: drop.start_at.max(c.start_at).timestamp() as f64,
                    awarded_at: 0.0,
                    queued: false,
                    series_wait_until: 0.0,
                }
            })
            .collect()
    }

    /// Whether the account has the badge already: by Twitch set id (an event badge's id, a drop's
    /// benefit id) or by title.
    fn owned_in(&self, owned: &HashSet<String>) -> bool {
        let set_id = self.campaign_id.strip_prefix("event:").unwrap_or(&self.benefit_id);
        [set_id, self.benefit_id.as_str(), self.name.as_str()]
            .iter()
            .any(|k| !k.trim().is_empty() && owned.contains(&k.trim().to_lowercase()))
    }

    /// Earned, or given up on: nothing left to watch for.
    fn done(&self) -> bool {
        self.earned || self.stopped
    }

    /// A badge on several days whose minutes for the day the miner counted already: it waits
    /// until the next day opens, 24 hours after. Twitch's own count, where it has one, decides
    /// instead (a drop for the next day is locked until then).
    fn done_today(&self, now: f64) -> bool {
        (self.days_required > 1 && !self.twitch_tracked && self.days.waiting_until(now).is_some())
            || now < self.series_wait_until
    }

    /// When it's watched for again, while it waits for the next day; 0 otherwise.
    fn next_day_at(&self, now: f64) -> f64 {
        let own = if self.twitch_tracked { None } else { self.days.waiting_until(now) };
        let series = (now < self.series_wait_until).then_some(self.series_wait_until);
        own.into_iter().chain(series).fold(0.0, f64::max)
    }

    /// Whether it's worth watching for right now: not earned or given up on, unlocked on Twitch,
    /// next in its series, and the day's minutes not in yet.
    fn wanted(&self, now: f64) -> bool {
        !self.done() && !self.locked && !self.queued && !self.done_today(now)
    }

    /// Whether a channel streaming `game_id` counts for the badge's game. A campaign without a
    /// game, or in a Special Events category, counts on any game.
    fn game_matches(&self, game_id: Option<&str>) -> bool {
        self.game_id.is_empty()
            || SPECIAL_GAME_IDS.contains(&self.game_id.as_str())
            || game_id == Some(self.game_id.as_str())
    }

    /// Whether a live channel can earn the badge right now. Drop campaigns need the channel to
    /// list the campaign among its drops; reward campaigns aren't listed there, so they go by
    /// game and by the channels they name.
    fn earnable_on(&self, channel_id: &str, game_id: Option<&str>, campaign_ids: &[String]) -> bool {
        let channel_ok = if self.reward {
            self.allowed.is_empty() || self.allowed.iter().any(|(id, _)| id == channel_id)
        } else {
            campaign_ids.contains(&self.campaign_id)
        };
        channel_ok && self.game_matches(game_id)
    }
}

/// The badge of a campaign to show and work on: the quickest one not earned yet, else the longest.
fn current<'a>(badges: &'a [Badge], campaign_id: &str) -> Option<&'a Badge> {
    let of_campaign = badges.iter().filter(|b| b.campaign_id == campaign_id);
    of_campaign
        .clone()
        .filter(|b| !b.done())
        .min_by_key(|b| b.minutes_required)
        .or_else(|| of_campaign.max_by_key(|b| b.minutes_required))
}

/// Campaigns with a badge left to earn.
fn open_campaigns(badges: &[Badge]) -> HashSet<String> {
    badges.iter().filter(|b| !b.done()).map(|b| b.campaign_id.clone()).collect()
}

/// Twitch's own progress toward a badge, from the drops inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TwitchProgress {
    pub minutes: i64,
    pub required: i64,
    pub claimed: bool,
    /// `hasPreconditionsMet` false: the drop waits for an earlier one, or a later day.
    pub locked: bool,
}

/// Twitch's own progress toward a badge named `name`, from the drops inventory's campaigns in
/// progress: the drop giving a reward of that name. Event and reward badges carry no Twitch drop
/// id, so they're matched by name.
pub(crate) fn inventory_progress(inventory: &Value, name: &str) -> Option<TwitchProgress> {
    let name = name.trim().to_lowercase();
    inventory["dropCampaignsInProgress"]
        .as_array()?
        .iter()
        .flat_map(|c| c["timeBasedDrops"].as_array().into_iter().flatten())
        .find(|d| {
            d["benefitEdges"].as_array().into_iter().flatten().any(|e| {
                e["benefit"]["name"].as_str().is_some_and(|n| n.trim().to_lowercase() == name)
            })
        })
        .map(|d| {
            let p = &d["self"];
            TwitchProgress {
                minutes: p["currentMinutesWatched"].as_i64().unwrap_or(0),
                required: d["requiredMinutesWatched"].as_i64().unwrap_or(0),
                claimed: p["isClaimed"].as_bool() == Some(true),
                locked: p["hasPreconditionsMet"].as_bool() == Some(false),
            }
        })
}

/// A channel watched for a badge: (channel id, login, game name, game id, campaign ids).
type WatchedChannel = (String, String, String, Option<String>, Vec<String>);

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
    /// Reward badges watched to their goal, by drop id, see `REWARD_DONE_KEY`.
    reward_done: Mutex<HashSet<String>>,
    /// Day counts of badges on several days, see `REWARD_DAYS_KEY`.
    reward_days: Mutex<HashMap<String, DayCount>>,
    /// When badges were given, see `REWARD_AWARDED_KEY`.
    awarded: Mutex<HashMap<String, f64>>,
    /// Running event badges for watch time, see `load_event_campaigns`.
    event_campaigns: RwLock<Vec<Campaign>>,
    /// The account's badges (lowercase set ids and titles), see `Twitch::owned_badges`. They
    /// count as earned, so they're never shown or watched for. The last read stays on a failure.
    owned: RwLock<HashSet<String>>,
    /// Streamdatabase pages that gave a watch goal, with when they were read, by badge id.
    event_pages: Mutex<HashMap<String, (f64, Value)>>,
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
            reward_done: Mutex::new(serde_json::from_str(&read(REWARD_DONE_KEY)).unwrap_or_default()),
            reward_days: Mutex::new(serde_json::from_str(&read(REWARD_DAYS_KEY)).unwrap_or_default()),
            awarded: Mutex::new(serde_json::from_str(&read(REWARD_AWARDED_KEY)).unwrap_or_default()),
            event_campaigns: RwLock::new(Vec::new()),
            owned: RwLock::new(HashSet::new()),
            event_pages: Mutex::new(HashMap::new()),
        }
    }
}

/// Twitch's free event badges (names, images, time windows, streamdatabase page), kept by
/// catquery: the list of badges the miner works on.
const EVENT_BADGES_URL: &str = "https://api.catquery.com/eventBadges";
/// How long a badge's streamdatabase page is used before it's read again: new runs of a badge,
/// on other channels, get added to it.
/// Short, as a run can last as little as an hour and a half.
const EVENT_PAGE_TTL_SECS: f64 = 20.0 * 60.0;

/// Streamdatabase's list of every Twitch global badge, the second source next to catquery: a
/// badge either one lists is found, so one being late or down doesn't hide a badge. It has each
/// badge's cost and current window; what to watch is on the badge's own page, as for catquery.
const STREAMDATABASE_LIST_URL: &str = "https://www.streamdatabase.com/twitch/global-badges";
const STREAMDATABASE_BADGE_URL: &str = "https://www.streamdatabase.com/twitch/global-badges";

/// The free, time-limited badges on streamdatabase's list page (its `__NEXT_DATA__`), in
/// catquery's shape (`id`, `name`, `imageUrl`, `startAt`, `endAt`, `free`, `streamdatabaseUrl`)
/// so both lists are read the same way. Badges without a window are left out: whether they run
/// can't be told.
pub(crate) fn parse_streamdatabase_list(page: &Value) -> Vec<Value> {
    let at = |b: &Value, date: &str, time: &str, default: &str| {
        let date = b[date].as_str()?;
        let time = b[time].as_str().unwrap_or(default);
        let t = chrono::NaiveDateTime::parse_from_str(&format!("{date} {time}"), "%Y-%m-%d %H:%M").ok()?;
        Some(t.and_utc())
    };
    page["props"]["pageProps"]["initialTwitchGlobalBadges"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["cost"].as_str() == Some("free") && b["removed"].as_bool() != Some(true))
        .filter_map(|b| {
            let current = &b["current"];
            let set_id = current["set_id"].as_str().filter(|s| !s.is_empty())?;
            let version = current["version"]["id"].as_str().unwrap_or("1");
            let start = at(b, "start_at_date", "start_at_time", "00:00")?;
            // An end date without a time runs to the end of that day.
            let end = match b["end_at_time"].as_str() {
                Some(_) => at(b, "end_at_date", "end_at_time", "23:59")? + chrono::Duration::seconds(59),
                None => at(b, "end_at_date", "end_at_time", "00:00")? + chrono::Duration::days(1),
            };
            Some(json!({
                "id": set_id,
                "name": current["version"]["title"].as_str().unwrap_or(set_id),
                "imageUrl": current["version"]["image_url_4x"].as_str().unwrap_or_default(),
                "startAt": start.to_rfc3339(),
                "endAt": end.to_rfc3339(),
                "free": true,
                "streamdatabaseUrl": format!("{STREAMDATABASE_BADGE_URL}/{set_id}/{version}"),
            }))
        })
        .collect()
}

/// Catquery's badges, then streamdatabase's that catquery doesn't have, as one
/// `{"badges": [...]}` list. The same badge is the same Twitch set id on both (catquery's `id`);
/// the title catches one listed under another id. Catquery's entry wins, with gaps filled in
/// from streamdatabase's.
pub(crate) fn merge_badge_lists(catquery: &[Value], streamdatabase: &[Value]) -> Value {
    let key = |b: &Value, k: &str| b[k].as_str().map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
    let mut merged: Vec<Value> = Vec::new();
    for badge in catquery.iter().chain(streamdatabase) {
        let (id, name) = (key(badge, "id"), key(badge, "name"));
        let same = merged.iter_mut().find(|m| {
            (id.is_some() && key(m, "id") == id) || (name.is_some() && key(m, "name") == name)
        });
        match same {
            Some(m) => {
                for field in ["imageUrl", "startAt", "endAt", "streamdatabaseUrl"] {
                    if m[field].as_str().is_none_or(str::is_empty) && !badge[field].is_null() {
                        m[field] = badge[field].clone();
                    }
                }
            }
            None => merged.push(badge.clone()),
        }
    }
    json!({ "badges": merged })
}

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
            .filter(|b| !b.earned && !b.queued && current(&badges, &b.campaign_id).is_some_and(|c| c.drop_id == b.drop_id))
            .map(|b| BadgeView {
                campaign_id: b.campaign_id.clone(),
                name: b.name.clone(),
                image: b.image.clone(),
                game: b.game.clone(),
                minutes_required: b.minutes_required,
                // A badge on several days the miner counts: the day's minutes, 0 once a new day opens.
                minutes_watched: if b.days_required > 1
                    && !b.twitch_tracked
                    && b.days.last_done_at > 0.0
                    && !b.done_today(now())
                    && b.days.minutes >= b.minutes_required as f64 + REWARD_MARGIN_MINUTES
                {
                    0
                } else {
                    b.minutes_watched
                },
                ends_at: b.ends_at,
                earned: b.earned,
                picked: picked.contains(&b.campaign_id),
                allowed_channels: b.allowed.len(),
                added_channels: added.values().filter(|a| a.campaign_id == b.campaign_id).count(),
                unconfirmed: b.stopped,
                days_required: b.days_required,
                days_done: b.days.days_done,
                done_today: b.done_today(now()),
                next_day_at: b.next_day_at(now()),
                locked: b.locked,
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
        let done = self.badges.reward_done.lock().unwrap().clone();
        let days = self.badges.reward_days.lock().unwrap().clone();
        let awarded = self.badges.awarded.lock().unwrap().clone();
        let owned = self.badges.owned.read().unwrap().clone();
        let running: HashSet<String> = {
            let mut badges = self.badges.badges.write().unwrap();
            let fresh: Vec<Badge> = campaigns
                .iter()
                .flat_map(|c| Badge::from_campaign(c, &events))
                .map(|mut b| {
                    // By drop: a campaign's badges each have their own progress.
                    if let Some(old) = badges.iter().find(|o| o.drop_id == b.drop_id) {
                        b.minutes_watched = old.minutes_watched;
                        b.earned = old.earned;
                        b.seen_unearned = old.seen_unearned;
                        b.reward_minutes = old.reward_minutes;
                        b.twitch_tracked = old.twitch_tracked;
                        b.stopped = old.stopped;
                        b.days = old.days.clone();
                        b.awarded_at = old.awarded_at;
                        b.locked = old.locked;
                        if b.twitch_tracked {
                            b.minutes_required = old.minutes_required;
                        }
                        if b.game.is_empty() {
                            b.game = old.game.clone();
                        }
                    }
                    // Owned per Twitch: earned. Given up on before a restart: stopped, not
                    // earned, as Twitch never confirmed it; its progress in the inventory, once
                    // seen, takes over again.
                    b.earned |= b.owned_in(&owned);
                    b.stopped |= b.reward && !b.twitch_tracked && done.contains(&b.drop_id);
                    if b.awarded_at == 0.0 {
                        b.awarded_at = awarded.get(&b.drop_id).copied().unwrap_or(0.0);
                    }
                    // Days counted before a restart.
                    if b.days == DayCount::default() {
                        if let Some(d) = days.get(&b.drop_id) {
                            b.days = d.clone();
                        }
                    }
                    b
                })
                .collect();
            *badges = fresh;
            update_series(&mut badges);
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
        let catquery = async {
            let resp = self.twitch.http().get(EVENT_BADGES_URL).send().await?.error_for_status()?;
            let json = resp.json::<Value>().await?;
            let badges = json["badges"].as_array().cloned().ok_or_else(|| anyhow!("no badge list: {json}"))?;
            Ok::<_, anyhow::Error>(badges)
        };
        let streamdatabase = async {
            let resp = self.twitch.http().get(STREAMDATABASE_LIST_URL).send().await?.error_for_status()?;
            let page = crate::entities::drops::next_data(&resp.text().await?).ok_or_else(|| anyhow!("no page data"))?;
            Ok::<_, anyhow::Error>(parse_streamdatabase_list(&page))
        };
        let (owned, catquery, streamdatabase) = tokio::join!(self.twitch.owned_badges(), catquery, streamdatabase);
        match owned {
            Ok(owned) => *self.badges.owned.write().unwrap() = owned,
            Err(e) => warn!("Could not load the badges you already have: {e:#}"),
        }
        // Either list alone is enough; with neither, the last list stays.
        let (catquery, streamdatabase) = match (catquery, streamdatabase) {
            (Err(a), Err(b)) => {
                return warn!("Could not load either badge list: catquery: {a:#}; streamdatabase: {b:#}");
            }
            (a, b) => {
                let a = a.unwrap_or_else(|e| {
                    warn!("Could not load catquery's badge list, using streamdatabase's alone: {e:#}");
                    Vec::new()
                });
                let b = b.unwrap_or_else(|e| {
                    warn!("Could not load streamdatabase's badge list, using catquery's alone: {e:#}");
                    Vec::new()
                });
                (a, b)
            }
        };
        let resp = merge_badge_lists(&catquery, &streamdatabase);
        *self.badges.events.write().unwrap() = parse_event_badges(&resp, now());
        self.load_event_campaigns(&resp).await;
    }

    /// Turns the running free event badges into campaigns, from their streamdatabase pages (what
    /// to watch, where). This is where the miner's badges come from: Twitch sends some global
    /// badges (e.g. "watch 30 minutes of League of Legends") as neither drops nor reward
    /// campaigns, and the page names the channels each run of a badge is on. Pages that gave a
    /// watch goal are kept for `EVENT_PAGE_TTL_SECS`, as new runs (channels) get added to them;
    /// the others are read again next time.
    async fn load_event_campaigns(&self, resp: &Value) {
        let now = now();
        let running: Vec<&Value> = resp["badges"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|b| b["free"].as_bool() == Some(true))
            .filter(|b| {
                let at = |k: &str| b[k].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
                at("startAt").is_none_or(|s| s.timestamp() as f64 <= now)
                    && at("endAt").is_some_and(|e| e.timestamp() as f64 > now)
            })
            .collect();
        let mut campaigns = Vec::new();
        let mut upcoming = Vec::new();
        let mut unknown = Vec::new();
        for badge in running {
            let (Some(id), Some(url)) = (badge["id"].as_str(), badge["streamdatabaseUrl"].as_str()) else {
                continue;
            };
            let cached = self
                .badges
                .event_pages
                .lock()
                .unwrap()
                .get(id)
                .filter(|(at, _)| now - at < EVENT_PAGE_TTL_SECS)
                .map(|(_, page)| page.clone());
            // (page, freshly loaded)
            let (page, fresh) = match cached {
                Some(page) => (Some(page), false),
                None => match self.load_event_page(url).await {
                    Ok(page) => (page, true),
                    Err(e) => {
                        warn!("Could not read how to earn the {} badge: {e:#}", badge["name"]);
                        // The last copy, if any, still says where to watch.
                        (self.badges.event_pages.lock().unwrap().get(id).map(|(_, page)| page.clone()), false)
                    }
                },
            };
            match page.as_ref().and_then(|p| Campaign::from_event_badge(badge, p)) {
                Some(c) => {
                    if fresh {
                        self.badges.event_pages.lock().unwrap().insert(id.to_string(), (now, page.unwrap_or_default()));
                    }
                    // Only the run going on now counts; a later one shows once it starts. A badge
                    // both lists have is merged already; this keeps it once even if not.
                    let known = campaigns
                        .iter()
                        .any(|k: &Campaign| k.id == c.id || k.name.trim().eq_ignore_ascii_case(c.name.trim()));
                    if c.dt_match() {
                        if !known {
                            campaigns.push(c);
                        }
                    } else if c.start_at.timestamp() as f64 > now {
                        upcoming.push(format!("{} from {}", c.name, c.start_at.format("%Y-%m-%d %H:%M UTC")));
                    }
                }
                None => unknown.push(badge["name"].as_str().unwrap_or(id).to_string()),
            }
        }
        let found: Vec<String> = campaigns
            .iter()
            .map(|c| {
                let place = match (c.allowed_channels.first(), c.game_name.as_str()) {
                    (Some((_, login)), _) => login.clone(),
                    (None, "") => "any channel".to_string(),
                    (None, game) => game.to_string(),
                };
                format!("{} ({} min, {place})", c.name, c.drops[0].minutes_required)
            })
            .collect();
        info!(
            "Event badges for watch time: {}{}{}",
            if found.is_empty() { "none".to_string() } else { found.join(", ") },
            if upcoming.is_empty() { String::new() } else { format!("; later: {}", upcoming.join(", ")) },
            if unknown.is_empty() { String::new() } else { format!("; not for watch time or not described yet: {}", unknown.join(", ")) }
        );
        *self.badges.event_campaigns.write().unwrap() = campaigns;
    }

    async fn load_event_page(&self, url: &str) -> Result<Option<Value>> {
        let html = self.twitch.http().get(url).send().await?.error_for_status()?.text().await?;
        Ok(crate::entities::drops::next_data(&html))
    }

    /// The event badges for watch time found by `refresh_event_badges`.
    pub(super) fn event_campaigns(&self) -> Vec<Campaign> {
        self.badges.event_campaigns.read().unwrap().clone()
    }

    /// Applies watch progress and earned badges: owned per Twitch, claimed in the drops inventory,
    /// or a drop there giving a reward of the same name (Twitch's own minutes). A badge seen not
    /// earned before that is earned now goes in the session's earned list.
    pub(super) fn update_badge_progress(&self, inventory: &Value) {
        // Which watched channel each badge is earned on, for the earned list.
        let watched: Vec<WatchedChannel> = self
            .streamers()
            .iter()
            .filter_map(|s| {
                let st = s.state();
                let game = st.stream.game.as_ref();
                let name = game.map(|g| g.display_name.clone()).unwrap_or_default();
                let id = game.map(|g| g.id.clone());
                (st.watching && st.badge_drop)
                    .then(|| (s.channel_id.clone(), s.username.clone(), name, id, st.stream.campaigns_ids.clone()))
            })
            .collect();
        let mut newly = Vec::new();
        let earned: HashSet<&str> = inventory["gameEventDrops"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["id"].as_str())
            .collect();
        // Event badges (from streamdatabase) have no Twitch id: one the inventory has a reward
        // of the same name for is already owned, e.g. from a drop campaign.
        let earned_names: HashSet<String> = inventory["gameEventDrops"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["name"].as_str().map(|n| n.trim().to_lowercase()))
            .collect();
        // When Twitch gave each of them, for the 24 hours the next badge of a series waits.
        let awarded: HashMap<String, f64> = inventory["gameEventDrops"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| {
                let at = chrono::DateTime::parse_from_rfc3339(d["lastAwardedAt"].as_str()?).ok()?;
                Some((d["name"].as_str()?.trim().to_lowercase(), at.timestamp() as f64))
            })
            .collect();
        let in_progress = inventory["dropCampaignsInProgress"].as_array();
        let owned = self.badges.owned.read().unwrap().clone();
        // A given up badge Twitch shows unfinished: saved, so a restart doesn't give up on it again.
        let mut revived = false;
        let mut badges = self.badges.badges.write().unwrap();
        let awarded_before: Vec<f64> = badges.iter().map(|b| b.awarded_at).collect();
        for b in badges.iter_mut() {
            if let Some(&at) = awarded.get(&b.name.trim().to_lowercase()) {
                b.awarded_at = at;
            }
            let (campaign_id, drop_id) = (b.campaign_id.as_str(), b.drop_id.as_str());
            let progress = in_progress
                .into_iter()
                .flatten()
                .filter(|c| c["id"].as_str() == Some(campaign_id))
                .flat_map(|c| c["timeBasedDrops"].as_array().into_iter().flatten())
                .find(|d| d["id"].as_str() == Some(drop_id))
                .map(|d| &d["self"]);
            let mut done = earned.contains(b.benefit_id.as_str())
                || (b.reward && earned_names.contains(&b.name.trim().to_lowercase()))
                || b.owned_in(&owned);
            if let Some(p) = progress {
                b.minutes_watched = p["currentMinutesWatched"].as_i64().unwrap_or(b.minutes_watched);
                done |= p["isClaimed"].as_bool() == Some(true);
            }
            // An event or reward badge a Twitch drop gives: Twitch's minutes and claim count,
            // also for one the miner had given up on.
            if b.reward {
                if let Some(TwitchProgress { minutes, required, claimed, locked }) = inventory_progress(inventory, &b.name) {
                    if !b.twitch_tracked {
                        info!("Using Twitch's progress for the {} badge: {minutes}/{required} min", b.name);
                    }
                    if locked && !b.locked {
                        info!("The {} badge isn't unlocked on Twitch yet; watching for it once it is", b.name);
                    }
                    b.twitch_tracked = true;
                    b.locked = locked && !claimed;
                    b.minutes_watched = minutes;
                    if required > 0 {
                        b.minutes_required = required;
                    }
                    if b.stopped && !claimed {
                        revived = true;
                        info!("Watching for the {} badge again: Twitch shows it unfinished", b.name);
                    }
                    b.stopped &= claimed;
                    done |= claimed;
                }
            }
            if done && !b.earned && b.seen_unearned {
                let channel = watched
                    .iter()
                    .find(|(id, _, _, game, ids)| b.earnable_on(id, game.as_deref(), ids));
                // The stream's game where the campaign didn't name one.
                let game = match (b.game.is_empty(), channel) {
                    (true, Some((_, _, game, _, _))) => game.clone(),
                    _ => b.game.clone(),
                };
                newly.push(EarnedBadge {
                    campaign_id: b.campaign_id.clone(),
                    name: b.name.clone(),
                    image: b.image.clone(),
                    game,
                    minutes_required: b.minutes_required,
                    earned_at: now(),
                    channel_id: channel.map(|(id, _, _, _, _)| id.clone()),
                    channel: channel.map(|(_, login, _, _, _)| login.clone()),
                });
            }
            // Seen earned this session without Twitch saying when: from now. (One earned before the
            // miner started has no time; the next of its series doesn't wait then, rather than
            // losing a day.)
            if done && !b.earned && b.seen_unearned && b.awarded_at == 0.0 {
                b.awarded_at = now();
            }
            b.earned |= done;
            b.seen_unearned |= !done;
        }
        update_series(&mut badges);
        let awarded_changed = badges.iter().map(|b| b.awarded_at).ne(awarded_before);
        drop(badges);
        if awarded_changed {
            self.save_reward_awarded();
        }
        for e in &newly {
            match &e.channel {
                Some(channel) => self.event(format!("Earned the {} badge watching {channel}", e.name)),
                None => self.event(format!("Earned the {} badge", e.name)),
            }
        }
        self.badges.earned.lock().unwrap().extend(newly);
        if revived {
            self.save_reward_done();
        }
    }

    /// Marks the live channels that can earn a picked badge right now, for the BADGES rule.
    pub(super) fn update_badge_targets(&self) {
        let targets: Vec<Badge> = {
            let picked = self.badges.picked.read().unwrap().clone();
            self.badges
                .badges
                .read()
                .unwrap()
                .iter()
                .filter(|b| b.wanted(now()) && picked.contains(&b.campaign_id))
                .cloned()
                .collect()
        };
        for s in self.streamers() {
            let mut st = s.state();
            let game = st.stream.game.as_ref().map(|g| g.id.clone());
            let eligible = st.is_online
                && targets
                    .iter()
                    .any(|b| b.earnable_on(&s.channel_id, game.as_deref(), &st.stream.campaigns_ids));
            st.badge_drop = eligible;
        }
    }

    /// Counts minutes watched on a channel toward the picked reward badges it can earn that
    /// Twitch reports no progress for. That count is never proof: Twitch may count less (it
    /// counts drop progress on one stream at a time). So a badge is only earned once Twitch says
    /// so (`update_badge_progress`); one watched to twice its goal plus a margin without that is
    /// given up on, not earned, until it's picked again or channels are added for it.
    pub(super) fn count_reward_minutes(&self, s: &Streamer, minutes: f64) {
        if minutes <= 0.0 {
            return;
        }
        let (game, campaign_ids) = {
            let st = s.state();
            if !st.badge_drop {
                return;
            }
            (st.stream.game.as_ref().map(|g| g.id.clone()), st.stream.campaigns_ids.clone())
        };
        let picked = self.badges.picked.read().unwrap().clone();
        let now = now();
        let mut stopped = Vec::new();
        let mut days_in = Vec::new();
        let mut days_changed = false;
        for b in self.badges.badges.write().unwrap().iter_mut() {
            if !b.reward || b.twitch_tracked || !b.wanted(now) || !picked.contains(&b.campaign_id) {
                continue;
            }
            if !b.earnable_on(&s.channel_id, game.as_deref(), &campaign_ids) {
                continue;
            }
            if b.days_required > 1 {
                // Each day's minutes on their own: a day that opened after the last one was in
                // starts from 0.
                if b.days.last_done_at > 0.0 && b.days.minutes >= b.minutes_required as f64 + REWARD_MARGIN_MINUTES {
                    b.days.minutes = 0.0;
                }
                let before = b.days.minutes.floor();
                b.days.minutes += minutes;
                days_changed |= b.days.minutes.floor() > before;
                b.minutes_watched = (b.days.minutes.floor() as i64).min(b.minutes_required);
                if b.days.minutes >= b.minutes_required as f64 + REWARD_MARGIN_MINUTES {
                    b.days.days_done += 1;
                    b.days.last_done_at = now;
                    days_changed = true;
                    // Two days past the days asked for without Twitch confirming it: given up on.
                    // One spare day, for a day the miner counted that Twitch didn't (one earned
                    // before the miner knew, so it couldn't tell when the next day opened).
                    if b.days.days_done > b.days_required + 1 {
                        b.stopped = true;
                        stopped.push((b.name.clone(), b.days.days_done * b.minutes_required));
                    } else {
                        days_in.push((b.name.clone(), b.days.days_done, b.days_required));
                    }
                }
                continue;
            }
            b.reward_minutes += minutes;
            b.minutes_watched = (b.reward_minutes.floor() as i64).min(b.minutes_required);
            if b.reward_minutes >= 2.0 * b.minutes_required as f64 + REWARD_MARGIN_MINUTES {
                b.stopped = true;
                stopped.push((b.name.clone(), b.reward_minutes.round() as i64));
            }
        }
        if days_changed {
            self.save_reward_days();
        }
        for (name, done, days) in &days_in {
            self.event(format!(
                "Watched the day's minutes for the {name} badge (day {done} of {days}); watching for it again in 24 hours"
            ));
        }
        if !days_in.is_empty() && stopped.is_empty() {
            self.update_badge_targets();
        }
        if stopped.is_empty() {
            return;
        }
        self.save_reward_done();
        for (name, watched) in &stopped {
            self.event(format!(
                "Watched {watched} minutes for the {name} badge, but Twitch hasn't confirmed it: stopped watching \
                 for it. Pick it again or add channels to try once more"
            ));
        }
        self.update_badge_targets();
    }

    /// Saves the day counts of badges on several days, see `REWARD_DAYS_KEY`.
    fn save_reward_days(&self) {
        let json = {
            let badges = self.badges.badges.read().unwrap();
            let mut days = self.badges.reward_days.lock().unwrap();
            *days = badges
                .iter()
                .filter(|b| b.days != DayCount::default())
                .map(|b| (b.drop_id.clone(), b.days.clone()))
                .collect();
            serde_json::to_string(&*days).unwrap_or_default()
        };
        if let Err(e) = self.store.set_meta(REWARD_DAYS_KEY, &json) {
            warn!("Could not save the badge day counts: {e:#}");
        }
    }

    /// Saves when badges were given, see `REWARD_AWARDED_KEY`.
    fn save_reward_awarded(&self) {
        let json = {
            let badges = self.badges.badges.read().unwrap();
            let mut awarded = self.badges.awarded.lock().unwrap();
            // Badges no longer listed keep theirs, in case they come back (a source down).
            awarded.extend(badges.iter().filter(|b| b.awarded_at > 0.0).map(|b| (b.drop_id.clone(), b.awarded_at)));
            serde_json::to_string(&*awarded).unwrap_or_default()
        };
        if let Err(e) = self.store.set_meta(REWARD_AWARDED_KEY, &json) {
            warn!("Could not save when the badges were given: {e:#}");
        }
    }

    /// Saves which reward badges were given up on, see `REWARD_DONE_KEY`.
    fn save_reward_done(&self) {
        let json = {
            let badges = self.badges.badges.read().unwrap();
            let mut done = self.badges.reward_done.lock().unwrap();
            *done = badges.iter().filter(|b| b.reward && b.stopped).map(|b| b.drop_id.clone()).collect();
            serde_json::to_string(&*done).unwrap_or_default()
        };
        if let Err(e) = self.store.set_meta(REWARD_DONE_KEY, &json) {
            warn!("Could not save the given up reward badges: {e:#}");
        }
    }

    /// Gives a badge that was given up on another go: its own count starts over.
    fn retry_badge(&self, campaign_id: &str) {
        let retried = {
            let mut badges = self.badges.badges.write().unwrap();
            let mut retried = false;
            for b in badges.iter_mut().filter(|b| b.campaign_id == campaign_id && b.stopped) {
                b.stopped = false;
                b.reward_minutes = 0.0;
                b.minutes_watched = 0;
                b.days = DayCount::default();
                retried = true;
            }
            retried
        };
        if retried {
            self.save_reward_done();
            self.save_reward_days();
        }
    }

    /// Picks or unpicks a badge. Picking one turns the BADGES rule on if it was off; unpicking one
    /// removes the channels quick-added for it.
    pub fn set_badge_picked(&self, campaign_id: &str, picked: bool) -> Result<()> {
        if picked {
            self.retry_badge(campaign_id);
        }
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
        let open: Vec<Badge> = {
            let badges = self.badges.badges.read().unwrap();
            badges.iter().filter(|b| b.wanted(now)).cloned().collect()
        };
        let needs_channels: Vec<(String, String)> = {
            // Campaigns some live channel in the list can earn right now, like `update_badge_targets`.
            let covered: HashSet<String> = self
                .streamers()
                .iter()
                .filter_map(|s| {
                    let st = s.state();
                    let game = st.stream.game.as_ref().map(|g| g.id.clone());
                    st.is_online.then(|| {
                        open.iter()
                            .filter(|b| b.earnable_on(&s.channel_id, game.as_deref(), &st.stream.campaigns_ids))
                            .map(|b| b.campaign_id.clone())
                            .collect::<Vec<_>>()
                    })
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
            // Locked ones, and ones whose minutes for today are in, need no channels now.
            let wanted: HashSet<String> = open.iter().map(|b| b.campaign_id.clone()).collect();
            open_campaigns(&badges)
                .into_iter()
                .filter(|id| wanted.contains(id))
                .filter(|id| !covered.contains(id))
                .filter(|id| added.get(id).copied().unwrap_or(0) < 2 * QUICK_ADD)
                .filter(|id| tried.get(id).map_or(true, |t| now - t > AUTO_ADD_RETRY_SECS))
                .filter_map(|id| current(&badges, &id).map(|b| (id.clone(), b.name.clone())))
                .collect()
        };
        for (id, name) in needs_channels {
            // Each added channel is logged as it's added. Only a failed try waits before the next
            // one; after a successful add, the badge is looked after again as soon as those
            // channels can't earn it anymore.
            match self.quick_add_badge(&id).await {
                Ok(_) => {
                    self.badges.auto_tried.lock().unwrap().remove(&id);
                }
                Err(e) => {
                    self.badges.auto_tried.lock().unwrap().insert(id.clone(), now);
                    info!("No channels added for the {name} badge yet: {e:#}");
                }
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
        // Adding channels for one that was given up on gives it another go.
        self.retry_badge(campaign_id);
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
                    let game = info.game.as_ref().map(|g| g.id.as_str());
                    badge.game_matches(game).then(|| (id.clone(), login.clone(), info.viewers))
                })
                .collect()
        };
        // Drops have to be on for this campaign on the stream itself. Reward campaigns aren't
        // listed among a channel's drops; streaming the game is enough.
        let mut live: Vec<(String, String, i64)> = if badge.reward {
            streaming
        } else {
            let ids: Vec<String> = streaming.iter().map(|(id, _, _)| id.clone()).collect();
            let campaigns = self.twitch.campaign_ids_for_channels(&ids).await;
            streaming
                .into_iter()
                .zip(campaigns)
                .filter(|(_, ids)| ids.contains(&badge.campaign_id))
                .map(|(channel, _)| channel)
                .collect()
        };

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

    /// Removes quick-added channels whose campaign has every badge earned or has ended, and ones
    /// that can't earn it anymore (offline for a while, or playing another game), so they don't
    /// count toward the limit and fresh channels can be added in their place.
    pub(super) fn prune_badge_channels(&self) {
        let (earned, games): (HashSet<String>, HashMap<String, Badge>) = {
            let badges = self.badges.badges.read().unwrap();
            let open = open_campaigns(&badges);
            let earned = badges
                .iter()
                .filter(|b| !open.contains(&b.campaign_id))
                .map(|b| b.campaign_id.clone())
                .collect();
            let games = badges.iter().map(|b| (b.campaign_id.clone(), b.clone())).collect();
            (earned, games)
        };
        let now = now();
        let gone = |channel_id: &str, campaign_id: &str| {
            let Some(s) = self.find_by_channel(channel_id) else { return false };
            let st = s.state();
            if !st.checked {
                return false;
            }
            if !st.is_online {
                return now - st.offline_at > BADGE_CHANNEL_OFFLINE_SECS;
            }
            let game = st.stream.game.as_ref().map(|g| g.id.as_str());
            game.is_some() && games.get(campaign_id).is_some_and(|b| !b.game_matches(game))
        };
        // A copy, so no channel state is locked while the list is.
        let added = self.badges.added.lock().unwrap().clone();
        let done: Vec<(String, &str)> = added
            .iter()
            .filter_map(|(id, a)| {
                if earned.contains(&a.campaign_id) {
                    Some((id.clone(), "badge earned"))
                } else if now > a.ends_at || (!games.is_empty() && !games.contains_key(&a.campaign_id)) {
                    // Not listed anymore: ended early, or not a badge after all.
                    Some((id.clone(), "badge drop ended"))
                } else {
                    gone(id, &a.campaign_id).then(|| (id.clone(), "it can't earn the badge right now"))
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

#[cfg(test)]
mod scenarios;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn drop_campaign(game_id: &str) -> Campaign {
        Campaign::from_json(&json!({
            "id": "c1",
            "game": { "id": game_id, "displayName": "Special Events" },
            "startAt": "2020-01-01T00:00:00Z",
            "endAt": "2099-01-01T00:00:00Z",
            "timeBasedDrops": [{
                "id": "d1", "requiredMinutesWatched": 15, "requiredSubs": 0,
                "startAt": "2020-01-01T00:00:00Z", "endAt": "2099-01-01T00:00:00Z",
                "benefitEdges": [{ "benefit": { "id": "b1", "name": "Jordan", "distributionType": "BADGE" } }]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn special_events_campaigns_count_on_any_game() {
        let on = vec!["c1".to_string()];
        let special = &Badge::from_campaign(&drop_campaign("509663"), &HashMap::new())[0];
        // The channel streams IRL; the campaign is filed under Special Events.
        assert!(special.earnable_on("ch", Some("509672x"), &on));
        assert!(special.earnable_on("ch", Some("509660"), &on));
        // It still has to list the campaign among its drops.
        assert!(!special.earnable_on("ch", Some("509660"), &[]));

        let game = &Badge::from_campaign(&drop_campaign("21779"), &HashMap::new())[0];
        assert!(game.earnable_on("ch", Some("21779"), &on));
        assert!(!game.earnable_on("ch", Some("509660"), &on));
    }

    #[test]
    fn owned_badges_count_by_set_id_or_title() {
        let resp = json!({ "data": { "currentUser": { "availableBadges": [
            { "setID": "qsmp2", "title": "QSMP2" },
            { "setID": "something-else", "title": "K/DA Fan " }
        ] } } });
        let owned = crate::twitch::parse_owned_badges(&resp).unwrap();
        let event = |id: &str, name: &str| {
            let badge = json!({ "id": id, "name": name, "startAt": "2020-01-01T00:00:00.000Z", "endAt": "2099-01-01T00:00:00.000Z" });
            let page = json!({ "props": { "pageProps": { "availabilities": [{
                "objectives": [[{ "type": "twitch_watch", "minutes": 60, "days": null }]], "categories": [], "channels": null
            }] } } });
            Badge::from_campaign(&Campaign::from_event_badge(&badge, &page).unwrap(), &HashMap::new()).remove(0)
        };
        assert!(event("qsmp2", "Renamed").owned_in(&owned));
        assert!(event("kda-fan", "K/DA Fan").owned_in(&owned));
        assert!(!event("clover", "Clover").owned_in(&owned));
        // A failed read isn't "owns nothing".
        assert!(crate::twitch::parse_owned_badges(&json!({ "data": { "currentUser": null } })).is_err());
        assert!(crate::twitch::parse_owned_badges(&json!({ "errors": [{ "message": "x" }] })).is_err());
    }

    #[test]
    fn event_badges_go_by_game_and_named_channels() {
        let event = |channels: Value| {
            let badge = json!({
                "id": "kda-fan", "name": "K/DA Fan",
                "startAt": "2020-01-01T00:00:00.000Z", "endAt": "2099-01-01T00:00:00.000Z", "free": true
            });
            let page = json!({ "props": { "pageProps": { "availabilities": [{
                "objectives": [[{ "type": "twitch_watch", "minutes": 30, "days": null }]],
                "categories": [{ "game": { "id": "21779", "name": "League of Legends" }, "slug": null }],
                "channels": channels
            }] } } });
            let c = Campaign::from_event_badge(&badge, &page).unwrap();
            Badge::from_campaign(&c, &HashMap::new()).remove(0)
        };
        // Not listed among the channel's drops, which is fine for an event badge.
        let any = event(Value::Null);
        assert!(any.earnable_on("ch", Some("21779"), &[]));
        assert!(!any.earnable_on("ch", Some("1"), &[]));
        let named = event(json!([{ "user": { "id": "42", "login": "samham" } }]));
        assert!(named.earnable_on("42", Some("21779"), &[]));
        assert!(!named.earnable_on("ch", Some("21779"), &[]));
    }
}

#[cfg(test)]
mod given_up_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn given_up_badges_are_done_but_not_earned() {
        let badge = json!({ "id": "kda-fan", "name": "K/DA Fan", "startAt": "2020-01-01T00:00:00Z", "endAt": "2099-01-01T00:00:00Z" });
        let page = json!({ "props": { "pageProps": { "availabilities": [{
            "objectives": [[{ "type": "twitch_watch", "minutes": 30, "days": null }]], "categories": [], "channels": null
        }] } } });
        let mut b = Badge::from_campaign(&Campaign::from_event_badge(&badge, &page).unwrap(), &HashMap::new()).remove(0);
        assert!(!b.done());
        b.stopped = true;
        assert!(b.done() && !b.earned);
        assert!(open_campaigns(&[b]).is_empty());
    }
}
