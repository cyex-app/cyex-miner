//! Predictions: bets channel points on the predictions of channels in the list, when turned on in
//! the web UI. A bet goes in shortly before the prediction closes, on the outcome the strategy
//! picks from the odds at that moment.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use super::Miner;
use crate::{entities::streamer::Streamer, store::Store, utils::now};

/// `meta` key for the prediction settings (JSON).
const SETTINGS_KEY: &str = "predictions";
/// Seconds before a prediction closes that the bet goes in: late enough for the odds to settle,
/// early enough for Twitch to accept it.
const BET_BEFORE_CLOSE: f64 = 6.0;
/// Twitch's limits for one bet.
const MIN_BET: i64 = 10;
const MAX_BET: i64 = 250_000;
/// Bets kept for the web UI.
const RECENT: usize = 20;

/// How the outcome to bet on is picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Strategy {
    /// The outcome most people picked. Wins most often, pays least.
    Crowd,
    /// The outcome with the most points on it.
    SmartMoney,
    /// The outcome with the fewest points on it. Wins least often, pays most.
    Underdog,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PredictionSettings {
    pub enabled: bool,
    pub strategy: Strategy,
    /// Share of the channel's points to bet, in percent.
    pub percent: u8,
    /// The most to bet at once.
    pub max_points: i64,
}

impl Default for PredictionSettings {
    fn default() -> Self {
        Self { enabled: false, strategy: Strategy::Crowd, percent: 5, max_points: 1000 }
    }
}

impl PredictionSettings {
    /// The bet for a channel balance, or `None` when it's below Twitch's minimum.
    pub fn bet(&self, balance: i64) -> Option<i64> {
        let amount = (balance * i64::from(self.percent) / 100).min(self.max_points).min(MAX_BET);
        (amount >= MIN_BET).then_some(amount)
    }
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub id: String,
    pub title: String,
    pub users: i64,
    pub points: i64,
}

impl Outcome {
    fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            id: v["id"].as_str()?.to_string(),
            title: v["title"].as_str().unwrap_or_default().to_string(),
            users: v["total_users"].as_i64().unwrap_or(0),
            points: v["total_points"].as_i64().unwrap_or(0),
        })
    }
}

/// The outcome a strategy bets on, as an index into `outcomes`. `None` when nobody has voted yet.
pub fn pick(outcomes: &[Outcome], strategy: Strategy) -> Option<usize> {
    let voted = outcomes.iter().enumerate().filter(|(_, o)| o.users > 0);
    let best = match strategy {
        // Ties go to the first outcome.
        Strategy::Crowd => voted.rev().max_by_key(|(_, o)| o.users),
        Strategy::SmartMoney => voted.rev().max_by_key(|(_, o)| o.points),
        Strategy::Underdog => voted.min_by_key(|(_, o)| o.points),
    };
    best.map(|(i, _)| i)
}

#[derive(Debug, Clone)]
struct Prediction {
    title: String,
    status: String,
    outcomes: Vec<Outcome>,
    closes_at: f64,
}

/// A bet as the web UI shows it.
#[derive(Debug, Clone, Serialize)]
pub struct BetView {
    pub event_id: String,
    pub channel: String,
    pub title: String,
    pub outcome: String,
    pub points: i64,
    /// "pending", "won", "lost" or "refunded".
    pub result: String,
    /// Points gained or lost once resolved: the payout minus the bet, or minus the bet.
    pub net: i64,
    pub at: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PredictionsView {
    pub settings: PredictionSettings,
    pub bets: Vec<BetView>,
}

pub(super) struct PredictionState {
    settings: RwLock<PredictionSettings>,
    /// Open predictions by event id.
    open: Mutex<HashMap<String, Prediction>>,
    /// Newest first.
    bets: Mutex<VecDeque<BetView>>,
}

impl PredictionState {
    pub(super) fn load(store: &Store) -> Self {
        let settings: PredictionSettings = store
            .meta(SETTINGS_KEY)
            .ok()
            .flatten()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self { settings: RwLock::new(settings), open: Mutex::new(HashMap::new()), bets: Mutex::new(VecDeque::new()) }
    }
}

/// The PubSub topic with a channel's predictions.
pub(super) fn channel_topic(channel_id: &str) -> String {
    format!("predictions-channel-v1.{channel_id}")
}

impl Miner {
    pub fn predictions(&self) -> PredictionsView {
        PredictionsView {
            settings: self.prediction_settings(),
            bets: self.predictions.bets.lock().unwrap().iter().cloned().collect(),
        }
    }

    pub(super) fn prediction_settings(&self) -> PredictionSettings {
        self.predictions.settings.read().unwrap().clone()
    }

    fn user_topic(&self) -> String {
        format!("predictions-user-v1.{}", self.twitch.creds.user_id)
    }

    /// Saves new settings. Turning predictions on or off takes effect right away.
    pub fn set_prediction_settings(&self, settings: PredictionSettings) -> Result<()> {
        if !(1..=100).contains(&settings.percent) {
            bail!("the share to bet must be between 1 and 100%");
        }
        if settings.max_points < MIN_BET {
            bail!("the most to bet must be at least {MIN_BET} points");
        }
        self.store
            .set_meta(SETTINGS_KEY, &serde_json::to_string(&settings)?)
            .context("could not save the prediction settings")?;
        let was_enabled = std::mem::replace(&mut *self.predictions.settings.write().unwrap(), settings.clone()).enabled;
        if settings.enabled != was_enabled {
            self.subscribe_predictions(settings.enabled);
            self.event(if settings.enabled { "Predictions turned on" } else { "Predictions turned off" });
        }
        Ok(())
    }

    /// Listens to (or stops listening to) the predictions of every channel. The bet results topic
    /// stays once added, so bets already placed still get their result after turning it off.
    /// Scheduled bets are kept too; they check the setting when they're due.
    pub(super) fn subscribe_predictions(&self, on: bool) {
        if on {
            self.pubsub.submit(self.user_topic());
        }
        for s in self.streamers() {
            let topic = channel_topic(&s.channel_id);
            if on {
                self.pubsub.submit(topic);
            } else {
                self.pubsub.remove(&topic);
            }
        }
    }

    /// A channel started a prediction: note it and schedule the bet.
    pub(super) fn prediction_created(self: &Arc<Self>, streamer: Arc<Streamer>, data: &Value) {
        let settings = self.prediction_settings();
        let event = &data["event"];
        let (Some(id), Some("ACTIVE")) = (event["id"].as_str(), event["status"].as_str()) else { return };
        if !settings.enabled || self.paused() {
            return;
        }
        let created = event["created_at"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map_or_else(now, |t| t.timestamp_millis() as f64 / 1000.0);
        let window = event["prediction_window_seconds"].as_f64().unwrap_or(0.0);
        let prediction = Prediction {
            title: event["title"].as_str().unwrap_or_default().to_string(),
            status: "ACTIVE".to_string(),
            outcomes: event["outcomes"].as_array().into_iter().flatten().filter_map(Outcome::from_json).collect(),
            closes_at: created + window,
        };
        let wait = prediction.closes_at - BET_BEFORE_CLOSE - now();
        if prediction.closes_at - now() < 2.0 {
            return; // Too late to bet.
        }
        self.event(format!("{streamer} started a prediction: {}", prediction.title));
        {
            let mut open = self.predictions.open.lock().unwrap();
            // Predictions whose updates stopped coming (e.g. the channel was removed) are dropped
            // an hour after they closed.
            let cutoff = now() - 3600.0;
            open.retain(|_, p| p.closes_at > cutoff);
            open.insert(id.to_string(), prediction);
        }
        let (miner, id) = (self.clone(), id.to_string());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(wait.max(0.0))).await;
            miner.place_bet(&streamer, &id).await;
        });
    }

    /// New odds, or the prediction was locked, resolved or cancelled.
    pub(super) fn prediction_updated(&self, data: &Value) {
        let event = &data["event"];
        let Some(id) = event["id"].as_str() else { return };
        let mut open = self.predictions.open.lock().unwrap();
        let Some(prediction) = open.get_mut(id) else { return };
        prediction.status = event["status"].as_str().unwrap_or_default().to_string();
        let outcomes: Vec<Outcome> =
            event["outcomes"].as_array().into_iter().flatten().filter_map(Outcome::from_json).collect();
        if !outcomes.is_empty() {
            prediction.outcomes = outcomes;
        }
        // Without a bet there's no result to wait for.
        let ended = matches!(prediction.status.as_str(), "RESOLVED" | "CANCELED");
        let bet = self.predictions.bets.lock().unwrap().iter().any(|b| b.event_id == id);
        if ended && !bet {
            open.remove(id);
        }
    }

    async fn place_bet(&self, streamer: &Streamer, event_id: &str) {
        let settings = self.prediction_settings();
        if !settings.enabled || self.paused() || !streamer.state().is_online {
            return;
        }
        let balance = streamer.state().channel_points;
        let choice = {
            let open = self.predictions.open.lock().unwrap();
            let Some(p) = open.get(event_id).filter(|p| p.status == "ACTIVE") else { return };
            pick(&p.outcomes, settings.strategy).map(|i| (p.outcomes[i].clone(), p.title.clone()))
        };
        let Some((outcome, title)) = choice else {
            self.event(format!("Skipped the prediction on {streamer}: nobody had voted yet"));
            return;
        };
        let Some(points) = settings.bet(balance) else {
            self.event(format!("Skipped the prediction on {streamer}: not enough points to bet"));
            return;
        };
        if let Err(e) = self.twitch.make_prediction(event_id, &outcome.id, points).await {
            warn!("Bet on {} failed: {e:#}", streamer.username);
            self.event(format!("Could not bet on {streamer}'s prediction: {e:#}"));
            return;
        }
        self.event(format!("Bet {points} on \"{}\" in {streamer}'s prediction: {title}", outcome.title));
        let mut bets = self.predictions.bets.lock().unwrap();
        bets.push_front(BetView {
            event_id: event_id.to_string(),
            channel: streamer.username.clone(),
            title,
            outcome: outcome.title,
            points,
            result: "pending".to_string(),
            net: 0,
            at: now(),
        });
        bets.truncate(RECENT);
    }

    /// The result of a bet this account made.
    pub(super) fn prediction_result(&self, streamer: &Streamer, data: &Value) {
        let prediction = &data["prediction"];
        let Some(id) = prediction["event_id"].as_str() else { return };
        self.predictions.open.lock().unwrap().remove(id);
        let placed = prediction["points"].as_i64().unwrap_or(0);
        let won = prediction["result"]["points_won"].as_i64().unwrap_or(0);
        let (result, net) = match prediction["result"]["type"].as_str() {
            Some("WIN") => ("won", won - placed),
            Some("LOSE") => ("lost", -placed),
            Some("REFUND") => ("refunded", 0),
            _ => return,
        };
        {
            let mut bets = self.predictions.bets.lock().unwrap();
            let Some(bet) = bets.iter_mut().find(|b| b.event_id == id) else { return };
            bet.result = result.to_string();
            bet.net = net;
        }
        self.event(match result {
            "won" => format!("Won the prediction on {streamer}: +{net}"),
            "lost" => format!("Lost the prediction on {streamer}: {net}"),
            _ => format!("The prediction on {streamer} was cancelled, the bet was refunded"),
        });
    }
}
