use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{entities::streamer::StreamerSettings, priority::Priority};

pub const EXAMPLE_CONFIG: &str = include_str!("../config.example.toml");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FollowersOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".into(),
            port: 5000,
        }
    }
}

/// A streamer in the config: either a bare name or a table with per-streamer overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StreamerEntry {
    Name(String),
    Full {
        username: String,
        #[serde(flatten)]
        overrides: toml::Table,
    },
}

impl StreamerEntry {
    pub fn username(&self) -> String {
        match self {
            Self::Name(name) => name,
            Self::Full { username, .. } => username,
        }
        .trim()
        .to_lowercase()
    }

    /// This entry's per-channel settings, as a JSON object (without `username`).
    pub fn overrides(&self) -> Result<serde_json::Value> {
        Ok(match self {
            Self::Name(_) => serde_json::Value::Object(Default::default()),
            Self::Full { overrides, .. } => serde_json::to_value(overrides)?,
        })
    }

    /// Applies this entry's overrides on top of the default streamer settings.
    pub fn settings(&self, defaults: &StreamerSettings) -> Result<StreamerSettings> {
        merge_settings(defaults, &self.overrides()?)
            .with_context(|| format!("invalid settings for streamer '{}'", self.username()))
    }
}

/// Per-channel overrides (a JSON object) applied on top of the default streamer settings.
pub fn merge_settings(defaults: &StreamerSettings, overrides: &serde_json::Value) -> Result<StreamerSettings> {
    let mut base = serde_json::to_value(defaults)?;
    merge(&mut base, overrides.clone());
    Ok(serde_json::from_value(base)?)
}

fn merge(base: &mut serde_json::Value, patch: serde_json::Value) {
    match (base, patch) {
        (serde_json::Value::Object(base), serde_json::Value::Object(patch)) => {
            for (key, value) in patch {
                merge(base.entry(key).or_insert(serde_json::Value::Null), value);
            }
        }
        (base, patch) => *base = patch,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub username: String,
    pub claim_drops_startup: bool,
    /// Only read until the watch priority is first saved in the web UI.
    pub priority: Vec<Priority>,
    pub followers: bool,
    pub followers_order: FollowersOrder,
    /// Only read once, to import into the database.
    pub blacklist: Vec<String>,
    pub web: WebConfig,
    pub streamer_defaults: StreamerSettings,
    /// Only read once, to import into the database.
    pub streamers: Vec<StreamerEntry>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            username: String::new(),
            claim_drops_startup: false,
            priority: vec![Priority::Badges, Priority::Prioritized, Priority::Streak, Priority::Drops, Priority::Order],
            followers: false,
            followers_order: FollowersOrder::Asc,
            blacklist: Vec::new(),
            web: WebConfig::default(),
            streamer_defaults: StreamerSettings::default(),
            streamers: Vec::new(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut config: Config =
            toml::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))?;
        config.username = config.username.trim().to_lowercase();
        if config.username.is_empty() {
            bail!("'username' must be set in {}", path.display());
        }
        config.blacklist = config.blacklist.iter().map(|s| s.trim().to_lowercase()).collect();
        Ok(config)
    }
}
