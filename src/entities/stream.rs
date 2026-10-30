use base64::Engine;
use serde_json::Value;

use super::drops::Campaign;
use crate::utils::now;

#[derive(Debug, Clone, Default)]
pub struct Game {
    pub id: String,
    pub name: String,
    pub display_name: String,
}

impl Game {
    pub fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            id: v["id"].as_str()?.to_string(),
            name: v["name"].as_str().unwrap_or_default().to_string(),
            display_name: v["displayName"].as_str().unwrap_or_default().to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub broadcast_id: Option<String>,
    pub title: String,
    pub game: Option<Game>,
    pub viewers_count: i64,
    pub campaigns: Vec<Campaign>,
    pub campaigns_ids: Vec<String>,
    pub payload: Option<Value>,
    pub watch_streak_missing: bool,
    pub minute_watched: f64,
    last_update: f64,
    minute_watched_timestamp: f64,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            broadcast_id: None,
            title: String::new(),
            game: None,
            viewers_count: 0,
            campaigns: Vec::new(),
            campaigns_ids: Vec::new(),
            payload: None,
            watch_streak_missing: true,
            minute_watched: 0.0,
            last_update: 0.0,
            minute_watched_timestamp: 0.0,
        }
    }
}

impl Stream {
    pub fn update(&mut self, broadcast_id: String, title: &str, game: Option<Game>, viewers: i64) {
        self.broadcast_id = Some(broadcast_id);
        self.title = title.trim().to_string();
        self.game = game;
        self.viewers_count = viewers;
        self.last_update = now();
    }

    pub fn encode_payload(&self) -> Option<String> {
        let json = serde_json::to_string(self.payload.as_ref()?).ok()?;
        Some(base64::engine::general_purpose::STANDARD.encode(json))
    }

    pub fn update_required(&self) -> bool {
        self.last_update == 0.0 || self.update_elapsed() >= 120.0
    }

    pub fn update_elapsed(&self) -> f64 {
        if self.last_update == 0.0 {
            0.0
        } else {
            now() - self.last_update
        }
    }

    /// Forces the next online check to fetch fresh stream info.
    pub fn invalidate(&mut self) {
        self.last_update = 0.0;
    }

    pub fn init_watch_streak(&mut self) {
        self.watch_streak_missing = true;
        self.minute_watched = 0.0;
        self.minute_watched_timestamp = 0.0;
    }

    /// Counts the time since the last watch; returns the minutes added.
    pub fn update_minute_watched(&mut self) -> f64 {
        let now = now();
        let mut added = 0.0;
        if self.minute_watched_timestamp != 0.0 {
            // Capped at a minute: watches are ~20s apart, and a longer gap means the stream lost its
            // watch slot for a while, which mustn't count towards the watch streak.
            added = ((now - self.minute_watched_timestamp) / 60.0).min(1.0);
            self.minute_watched += added;
        }
        self.minute_watched_timestamp = now;
        added
    }
}

impl std::fmt::Display for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.game {
            Some(game) => write!(f, "{} ({})", self.title, game.display_name),
            None => write!(f, "{}", self.title),
        }
    }
}
