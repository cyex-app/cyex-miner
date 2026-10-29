use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::utils::percentage;

fn parse_dt(v: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(v.as_str()?).ok().map(|d| d.with_timezone(&Utc))
}

/// A Twitch chat badge given by a drop.
#[derive(Debug, Clone)]
pub struct BadgeReward {
    /// The benefit id, which the inventory lists under `gameEventDrops` once earned.
    pub benefit_id: String,
    pub name: String,
    pub image: String,
}

#[derive(Debug, Clone)]
pub struct Drop {
    pub id: String,
    pub name: String,
    pub benefit: String,
    /// The reward Twitch marks as a chat badge (`distributionType` BADGE), if any.
    pub badge: Option<BadgeReward>,
    /// Every reward, for matching badges Twitch doesn't mark as one by name.
    pub rewards: Vec<BadgeReward>,
    /// Subscriptions needed instead of (or on top of) watching.
    pub required_subs: i64,
    pub minutes_required: i64,
    pub has_preconditions_met: Option<bool>,
    pub current_minutes_watched: i64,
    pub drop_instance_id: Option<String>,
    pub is_claimed: bool,
    pub is_claimable: bool,
    pub is_printable: bool,
    pub percentage_progress: i64,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
}

impl Drop {
    pub fn from_json(v: &Value) -> Option<Self> {
        let mut benefits: Vec<String> = v["benefitEdges"]
            .as_array()
            .map(|edges| {
                edges
                    .iter()
                    .filter_map(|e| e["benefit"]["name"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        benefits.sort();
        benefits.dedup();
        let reward = |b: &Value| BadgeReward {
            benefit_id: b["id"].as_str().unwrap_or_default().to_string(),
            name: b["name"].as_str().unwrap_or_default().to_string(),
            image: b["imageAssetURL"].as_str().unwrap_or_default().to_string(),
        };
        let edges = v["benefitEdges"].as_array().map(Vec::as_slice).unwrap_or_default();
        let badge = edges
            .iter()
            .map(|e| &e["benefit"])
            .find(|b| b["distributionType"].as_str() == Some("BADGE"))
            .map(reward);
        Some(Self {
            id: v["id"].as_str()?.to_string(),
            name: v["name"].as_str().unwrap_or_default().to_string(),
            benefit: benefits.join(", "),
            badge,
            rewards: edges.iter().map(|e| reward(&e["benefit"])).collect(),
            required_subs: v["requiredSubs"].as_i64().unwrap_or(0),
            minutes_required: v["requiredMinutesWatched"].as_i64().unwrap_or(0),
            has_preconditions_met: None,
            current_minutes_watched: 0,
            drop_instance_id: None,
            is_claimed: false,
            is_claimable: false,
            is_printable: false,
            percentage_progress: 0,
            start_at: parse_dt(&v["startAt"])?,
            end_at: parse_dt(&v["endAt"])?,
        })
    }

    pub fn dt_match(&self) -> bool {
        let now = Utc::now();
        self.start_at < now && now < self.end_at
    }

    /// Applies the `self` progress object from the inventory.
    pub fn update(&mut self, progress: &Value) {
        let watched = progress["currentMinutesWatched"].as_i64().unwrap_or(0);
        self.has_preconditions_met = progress["hasPreconditionsMet"].as_bool();

        let updated_percentage = percentage(watched, self.minutes_required);
        let quarter = updated_percentage % 25 == 0;
        self.is_printable = watched > self.current_minutes_watched
            && ((updated_percentage > self.percentage_progress
                && quarter
                && self.current_minutes_watched != 0)
                || (watched == 1 && self.current_minutes_watched == 0));

        self.current_minutes_watched = watched;
        self.drop_instance_id = progress["dropInstanceID"].as_str().map(str::to_string);
        self.is_claimed = progress["isClaimed"].as_bool().unwrap_or(false);
        self.is_claimable = !self.is_claimed && self.drop_instance_id.is_some();
        self.percentage_progress = updated_percentage;
    }

    pub fn progress_bar(&self) -> String {
        let progress = (self.percentage_progress / 2).clamp(0, 50) as usize;
        format!(
            "|{}{}|  {}% [{}/{}]",
            "█".repeat(progress),
            " ".repeat(50 - progress),
            self.percentage_progress,
            self.current_minutes_watched,
            self.minutes_required
        )
    }
}

impl std::fmt::Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}) {}/{} ({}%)",
            self.name,
            self.benefit,
            self.current_minutes_watched,
            self.minutes_required,
            self.percentage_progress
        )
    }
}

#[derive(Debug, Clone)]
pub struct Campaign {
    pub id: String,
    pub game_id: String,
    pub game_name: String,
    pub game_slug: String,
    pub name: String,
    /// Channels the campaign is limited to, as (id, login); empty means any channel in the game.
    pub allowed_channels: Vec<(String, String)>,
    pub in_inventory: bool,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub drops: Vec<Drop>,
}

impl Campaign {
    pub fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            id: v["id"].as_str()?.to_string(),
            game_id: v["game"]["id"].as_str().unwrap_or_default().to_string(),
            game_name: v["game"]["displayName"].as_str().unwrap_or_default().to_string(),
            game_slug: v["game"]["slug"].as_str().unwrap_or_default().to_string(),
            name: v["name"].as_str().unwrap_or_default().to_string(),
            allowed_channels: if v["allow"]["isEnabled"].as_bool() == Some(false) {
                Vec::new()
            } else {
                v["allow"]["channels"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|c| Some((c["id"].as_str()?.to_string(), c["name"].as_str()?.to_lowercase())))
                    .collect()
            },
            in_inventory: false,
            start_at: parse_dt(&v["startAt"])?,
            end_at: parse_dt(&v["endAt"])?,
            drops: v["timeBasedDrops"]
                .as_array()
                .map(|drops| drops.iter().filter_map(Drop::from_json).collect())
                .unwrap_or_default(),
        })
    }

    pub fn dt_match(&self) -> bool {
        let now = Utc::now();
        self.start_at < now && now < self.end_at
    }

    /// The drop that gives a badge for watching alone, and that badge. A reward counts as a badge
    /// when Twitch marks it as one, or when its name is in `event_badges` (lowercase names of
    /// Twitch's event badges), for badges Twitch delivers some other way.
    pub fn watch_badge_drop(&self, event_badges: &HashSet<String>) -> Option<(&Drop, &BadgeReward)> {
        self.drops
            .iter()
            .filter(|d| d.minutes_required > 0 && d.required_subs == 0)
            .find_map(|d| {
                let reward = d
                    .badge
                    .as_ref()
                    .or_else(|| d.rewards.iter().find(|r| event_badges.contains(&r.name.to_lowercase())))?;
                Some((d, reward))
            })
    }

    /// Removes drops that are already claimed or outside their time window.
    pub fn clear_drops(&mut self) {
        self.drops.retain(|d| d.dt_match() && !d.is_claimed);
    }
}

impl std::fmt::Display for Campaign {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}, Game: {} - Drops: {} pcs. - In inventory: {}",
            self.name,
            self.game_name,
            self.drops.len(),
            self.in_inventory
        )
    }
}
