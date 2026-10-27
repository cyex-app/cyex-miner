use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};

use super::stream::Stream;
use crate::utils::{millify, now};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChatPresence {
    Always,
    Never,
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamerSettings {
    pub follow_raid: bool,
    pub claim_drops: bool,
    pub claim_moments: bool,
    pub watch_streak: bool,
    pub chat: ChatPresence,
    /// Always take a watch slot when live, ahead of the `priority` rules (e.g. for watch-time badges).
    pub prioritize: bool,
}

impl Default for StreamerSettings {
    fn default() -> Self {
        Self {
            follow_raid: true,
            claim_drops: true,
            claim_moments: true,
            watch_streak: true,
            chat: ChatPresence::Online,
            prioritize: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Added by hand (web UI, or imported from the old `streamers` list in the config).
    Manual,
    /// Loaded from the account's follow list.
    Followers,
    /// Quick-added for a picked badge drop; removed again once it's earned or the campaign ends.
    Badge,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct HistoryEntry {
    pub counter: i64,
    pub amount: i64,
}

#[derive(Debug, Default)]
pub struct StreamerState {
    /// False until the first online/offline check completed.
    pub checked: bool,
    pub is_online: bool,
    pub stream_up: f64,
    pub online_at: f64,
    pub offline_at: f64,
    pub channel_points: i64,
    pub starting_points: Option<i64>,
    pub multipliers: Vec<f64>,
    pub stream: Stream,
    pub raid_id: Option<String>,
    pub history: BTreeMap<String, HistoryEntry>,
    pub watching: bool,
    /// Starts as `settings.prioritize`, can be toggled from the web UI.
    pub prioritized: bool,
    /// The current watch streak on this channel, `None` until Twitch reported one.
    pub watch_streak: Option<u32>,
    /// Live with a badge drop the user picked and hasn't earned yet, see `Miner::update_badge_targets`.
    pub badge_drop: bool,
}

impl StreamerState {
    pub fn update_history(&mut self, reason: &str, earned: i64, counter: i64) {
        let entry = self.history.entry(reason.to_string()).or_default();
        entry.counter += counter;
        entry.amount += earned;
        if reason == "WATCH_STREAK" {
            self.stream.watch_streak_missing = false;
        }
    }

    pub fn set_points(&mut self, points: i64) {
        self.channel_points = points;
        self.starting_points.get_or_insert(points);
    }

    pub fn stream_up_elapsed(&self) -> bool {
        self.stream_up == 0.0 || now() - self.stream_up > 120.0
    }

    pub fn total_multiplier(&self) -> f64 {
        self.multipliers.iter().sum()
    }

    pub fn print_history(&self) -> String {
        self.history
            .iter()
            .filter(|(_, h)| h.counter != 0)
            .map(|(k, h)| format!("{k}({} times, {} gained)", h.counter, millify(h.amount)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug)]
pub struct Streamer {
    pub username: String,
    pub channel_id: String,
    pub settings: StreamerSettings,
    pub source: Source,
    state: Mutex<StreamerState>,
}

impl Streamer {
    pub fn new(username: String, channel_id: String, settings: StreamerSettings, source: Source) -> Self {
        Self {
            state: Mutex::new(StreamerState {
                prioritized: settings.prioritize,
                ..Default::default()
            }),
            username,
            channel_id,
            settings,
            source,
        }
    }

    /// Never hold this guard across an `.await`.
    pub fn state(&self) -> MutexGuard<'_, StreamerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Live on a game with drop campaigns available on this channel, finished or not.
    pub fn drops_condition(&self) -> bool {
        let state = self.state();
        self.settings.claim_drops && state.is_online && !state.stream.campaigns_ids.is_empty()
    }

    /// Live with drops left to earn: a campaign for the current game with unclaimed drops.
    pub fn has_unclaimed_drops(&self) -> bool {
        self.drops_condition() && !self.state().stream.campaigns.is_empty()
    }

    /// Whether this stream still needs watching to earn its watch streak: the streak isn't
    /// claimed, the stream started more than 30 minutes after the previous one ended, and less
    /// than ~7 minutes of it were watched.
    pub fn streak_missing(&self) -> bool {
        let st = self.state();
        self.settings.watch_streak
            && st.stream.watch_streak_missing
            && (st.offline_at == 0.0 || (now() - st.offline_at) / 60.0 > 30.0)
            && st.stream.minute_watched < 7.0
    }
}

impl std::fmt::Display for Streamer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let points = self.state().channel_points;
        write!(f, "{} ({} points)", self.username, millify(points))
    }
}
