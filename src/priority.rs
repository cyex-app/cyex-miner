//! Picks which live channels get the watch slots, following an ordered list of rules.

use std::{cmp::Reverse, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::entities::streamer::Streamer;

/// Twitch only counts minutes watched for 2 streams at a time.
pub const MAX_WATCHING: usize = 2;

/// A rule for filling the watch slots. Rules are tried in order until every slot is taken; ties
/// within a rule go to the channel higher in the list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Priority {
    /// Live channels eligible for a badge drop picked in the web UI.
    Badges,
    /// Starred channels.
    Prioritized,
    /// Streams whose watch streak isn't earned yet.
    Streak,
    /// Channels with the longest current watch streak.
    LongestStreak,
    /// Streams with unclaimed drops for the game being played.
    Drops,
    /// Channels with an active points multiplier (e.g. subscribed), highest first.
    Subscribed,
    PointsAscending,
    PointsDescending,
    /// The list order.
    Order,
}

impl Priority {
    /// The candidates this rule picks, best first.
    fn rank(self, candidates: &[Arc<Streamer>]) -> Vec<&Arc<Streamer>> {
        let mut ranked: Vec<&Arc<Streamer>> = candidates.iter().filter(|s| self.applies(s)).collect();
        // Stable sorts, so ties keep the list order.
        match self {
            Self::LongestStreak => ranked.sort_by_key(|s| Reverse(s.state().watch_streak)),
            Self::Subscribed => ranked.sort_by(|a, b| {
                // One state lock at a time.
                let a = a.state().total_multiplier();
                let b = b.state().total_multiplier();
                b.total_cmp(&a)
            }),
            Self::PointsAscending => ranked.sort_by_key(|s| s.state().channel_points),
            Self::PointsDescending => ranked.sort_by_key(|s| Reverse(s.state().channel_points)),
            // Streams already partly watched first: finishing a streak beats starting another
            // and risking both ending before either is in.
            Self::Streak => ranked.sort_by_key(|s| s.stream_minutes() == 0.0),
            Self::Badges | Self::Prioritized | Self::Drops | Self::Order => {}
        }
        ranked
    }

    fn applies(self, s: &Streamer) -> bool {
        match self {
            Self::Badges => s.state().badge_drop,
            Self::Prioritized => s.state().prioritized,
            Self::Streak => s.streak_missing(),
            Self::LongestStreak => s.state().watch_streak.is_some_and(|n| n > 0),
            Self::Drops => s.has_unclaimed_drops(),
            Self::Subscribed => s.state().total_multiplier() > 0.0,
            Self::PointsAscending | Self::PointsDescending | Self::Order => true,
        }
    }
}

/// Up to `MAX_WATCHING` of the live `candidates` (in list order), chosen by `rules`.
pub fn select_watching(candidates: &[Arc<Streamer>], rules: &[Priority]) -> Vec<Arc<Streamer>> {
    let mut selected: Vec<Arc<Streamer>> = Vec::with_capacity(MAX_WATCHING);
    for rule in rules {
        for s in rule.rank(candidates) {
            if selected.len() == MAX_WATCHING {
                return selected;
            }
            if !selected.iter().any(|x| Arc::ptr_eq(x, s)) {
                selected.push(s.clone());
            }
        }
    }
    selected
}

/// What the watch loop watches in one pass.
pub struct WatchPlan {
    pub watching: Vec<Arc<Streamer>>,
    /// The channel watched alone for a picked badge, kept through a streak break.
    pub badge_channel: Option<Arc<Streamer>>,
    /// `watching` is a break for pending watch streaks, with the badge waiting.
    pub streak_break: bool,
}

/// The channels to watch this pass. `focus` is the badge channel of the last pass.
///
/// Twitch counts badge progress on one stream at a time; switching between two every few
/// seconds keeps restarting it. So while a picked badge can be earned, only its channel is
/// watched (like TwitchDropsMiner), whatever the other rules say, and the second slot waits until
/// it's earned. The channel already focused stays, so progress isn't split. Without `BADGES` in
/// the rules, this is off.
///
/// A watch streak is lost with its stream while a badge can wait, so a pending streak (with
/// `STREAK` in the rules) gets a short break of its own: both slots on streaks until Twitch
/// confirms them or they give up, then the badge channel alone again. Only channels with a
/// streak to lose get one (Twitch's count above 0): with many channels live, breaks for every
/// stream would hold a badge up for hours. The badge channel's own streak needs no break: it's
/// earned while the badge is.
pub fn plan_watching(candidates: &[Arc<Streamer>], rules: &[Priority], focus: Option<&str>) -> WatchPlan {
    let badge_channel = rules
        .contains(&Priority::Badges)
        .then(|| {
            let eligible = || candidates.iter().filter(|s| s.state().badge_drop);
            eligible()
                .find(|s| focus == Some(s.channel_id.as_str()))
                .or_else(|| eligible().next())
                .cloned()
        })
        .flatten();
    let streaks = match &badge_channel {
        Some(b) if rules.contains(&Priority::Streak) && !b.streak_missing() => {
            let at_stake: Vec<Arc<Streamer>> = candidates
                .iter()
                .filter(|s| s.state().watch_streak.is_some_and(|n| n > 0))
                .cloned()
                .collect();
            select_watching(&at_stake, &[Priority::Streak])
        }
        _ => Vec::new(),
    };
    let watching = match &badge_channel {
        Some(_) if !streaks.is_empty() => streaks.clone(),
        Some(s) => vec![s.clone()],
        None => select_watching(candidates, rules),
    };
    WatchPlan { streak_break: badge_channel.is_some() && !streaks.is_empty(), watching, badge_channel }
}

/// Drops repeated rules, keeping the first of each.
pub fn dedup(rules: Vec<Priority>) -> Vec<Priority> {
    let mut unique = Vec::with_capacity(rules.len());
    for rule in rules {
        if !unique.contains(&rule) {
            unique.push(rule);
        }
    }
    unique
}
