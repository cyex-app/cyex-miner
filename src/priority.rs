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
            Self::Prioritized | Self::Streak | Self::Drops | Self::Order => {}
        }
        ranked
    }

    fn applies(self, s: &Streamer) -> bool {
        match self {
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
