//! Handles PubSub messages: points, bonus chests, live status, raids, moments and predictions.

use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Miner;
use crate::{entities::streamer::Streamer, pubsub::PubSubMessage, utils::now};

impl Miner {
    pub(super) async fn dispatch_pubsub(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<String>) {
        let mut last: Option<(String, String)> = None;
        while let Some(raw) = rx.recv().await {
            let Some(msg) = PubSubMessage::parse(&raw) else {
                debug!("Unparseable PubSub message: {raw}");
                continue;
            };
            // With several connections the same message may arrive twice.
            let key = (msg.timestamp_raw.clone(), msg.identifier());
            if last.as_ref() == Some(&key) {
                continue;
            }
            last = Some(key);

            let Some(streamer) = self.find_by_channel(&msg.channel_id) else {
                continue;
            };
            self.handle_message(streamer, msg);
        }
    }

    fn handle_message(self: &Arc<Self>, streamer: Arc<Streamer>, msg: PubSubMessage) {
        let data = &msg.data;
        match (msg.topic.as_str(), msg.kind.as_str()) {
            ("community-points-user-v1", kind @ ("points-earned" | "points-spent")) => {
                if let Some(balance) = data["balance"]["balance"].as_i64() {
                    streamer.state().set_points(balance);
                }
                if kind == "points-earned" {
                    let earned = data["point_gain"]["total_points"].as_i64().unwrap_or(0);
                    let reason = data["point_gain"]["reason_code"].as_str().unwrap_or("UNKNOWN");
                    streamer.state().update_history(reason, earned, 1);
                    self.event(format!("+{earned} → {streamer} - Reason: {reason}."));
                    if reason == "WATCH_STREAK" {
                        let miner = self.clone();
                        tokio::spawn(async move {
                            // Give Twitch a moment to count the new streak.
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            miner.refresh_watch_streaks(&[streamer]).await;
                        });
                    }
                }
            }
            ("community-points-user-v1", "claim-available") => {
                if let Some(claim_id) = data["claim"]["id"].as_str().map(str::to_string) {
                    let miner = self.clone();
                    tokio::spawn(async move { miner.claim_bonus(&streamer, &claim_id).await });
                }
            }
            ("video-playback-by-id", "stream-up") => streamer.state().stream_up = now(),
            ("video-playback-by-id", "stream-down") => {
                if streamer.state().is_online {
                    self.set_offline(&streamer);
                }
            }
            ("video-playback-by-id", "viewcount") => {
                if streamer.state().stream_up_elapsed() {
                    let miner = self.clone();
                    tokio::spawn(async move { miner.check_online(&streamer).await });
                }
            }
            ("raid", "raid_update_v2") => {
                let raid = &msg.message["raid"];
                let (Some(raid_id), Some(target)) = (raid["id"].as_str(), raid["target_login"].as_str()) else {
                    return;
                };
                if !streamer.settings.follow_raid || self.paused() {
                    return;
                }
                {
                    let mut st = streamer.state();
                    if st.raid_id.as_deref() == Some(raid_id) {
                        return;
                    }
                    st.raid_id = Some(raid_id.to_string());
                }
                self.event(format!("Joining raid from {streamer} to {target}!"));
                let (miner, raid_id) = (self.clone(), raid_id.to_string());
                tokio::spawn(async move {
                    if let Err(e) = miner.twitch.join_raid(&raid_id).await {
                        warn!("Joining raid failed: {e:#}");
                    }
                });
            }
            ("predictions-channel-v1", "event-created") => self.prediction_created(streamer, data),
            ("predictions-channel-v1", "event-updated") => self.prediction_updated(data),
            ("predictions-user-v1", "prediction-result") => self.prediction_result(&streamer, data),
            ("community-moments-channel-v1", "active") => {
                let claim = streamer.settings.claim_moments && !self.paused();
                if let (true, Some(moment_id)) = (claim, data["moment_id"].as_str()) {
                    self.event(format!("Claiming the moment for {streamer}!"));
                    let (miner, moment_id) = (self.clone(), moment_id.to_string());
                    tokio::spawn(async move {
                        if let Err(e) = miner.twitch.claim_moment(&moment_id).await {
                            warn!("Moment claim failed: {e:#}");
                        }
                    });
                }
            }
            _ => {}
        }
    }
}
