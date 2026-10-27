//! Drop campaigns: tracks progress on the live channels and claims finished drops.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use serde_json::Value;
use tracing::warn;

use super::Miner;
use crate::{
    entities::drops::{Campaign, Drop},
    utils::{now, random_between},
};

impl Miner {
    async fn claim_drop(&self, drop: &Drop) -> bool {
        if self.paused() {
            return false;
        }
        let Some(instance_id) = &drop.drop_instance_id else {
            return false;
        };
        self.event(format!("Claim {drop}"));
        self.twitch.claim_drop(instance_id).await
    }

    pub async fn claim_all_drops_from_inventory(&self) {
        if self.paused() {
            return;
        }
        let inventory = match self.twitch.inventory().await {
            Ok(inv) => inv,
            Err(e) => return warn!("Could not load the drops inventory: {e:#}"),
        };
        for campaign in inventory["dropCampaignsInProgress"].as_array().into_iter().flatten() {
            for drop_json in campaign["timeBasedDrops"].as_array().into_iter().flatten() {
                let Some(mut drop) = Drop::from_json(drop_json) else { continue };
                drop.update(&drop_json["self"]);
                if drop.is_claimable {
                    drop.is_claimed = self.claim_drop(&drop).await;
                    tokio::time::sleep(random_between(5.0, 10.0)).await;
                }
            }
        }
    }

    async fn fetch_campaigns(&self) -> Result<Vec<Campaign>> {
        let ids: Vec<String> = self
            .twitch
            .active_drop_campaigns()
            .await?
            .iter()
            .filter_map(|c| c["id"].as_str().map(str::to_string))
            .collect();
        let details = self.twitch.campaign_details(&ids).await?;
        Ok(details
            .iter()
            .filter_map(Campaign::from_json)
            .filter(Campaign::dt_match)
            .filter_map(|mut c| {
                c.clear_drops();
                (!c.drops.is_empty()).then_some(c)
            })
            .collect())
    }

    /// Updates drop progress from the inventory and claims whatever is ready.
    async fn sync_campaigns(&self, campaigns: &mut [Campaign], inventory: &Value) {
        let Some(in_progress) = inventory["dropCampaignsInProgress"].as_array() else {
            return;
        };
        for campaign in campaigns.iter_mut() {
            campaign.clear_drops();
            let Some(progress) = in_progress
                .iter()
                .find(|p| p["id"].as_str() == Some(campaign.id.as_str()))
            else {
                continue;
            };
            campaign.in_inventory = true;
            for drop_json in progress["timeBasedDrops"].as_array().into_iter().flatten() {
                let Some(drop) = campaign
                    .drops
                    .iter_mut()
                    .find(|d| drop_json["id"].as_str() == Some(d.id.as_str()))
                else {
                    continue;
                };
                drop.update(&drop_json["self"]);
                if drop.is_claimable {
                    drop.is_claimed = self.claim_drop(drop).await;
                }
            }
            campaign.clear_drops();
        }
    }

    pub(super) async fn campaigns_loop(self: Arc<Self>) {
        let mut campaigns: Vec<Campaign> = Vec::new();
        let mut last_refresh = 0.0;
        loop {
            let streamers = self.streamers();
            // The defaults count too: badge drops are listed before any channel is added for them.
            let claims_drops =
                self.config.streamer_defaults.claim_drops || streamers.iter().any(|s| s.settings.claim_drops);
            if !self.paused() && claims_drops {
                if now() - last_refresh > 30.0 * 60.0 {
                    last_refresh = now();
                    self.claim_all_drops_from_inventory().await;
                    match self.fetch_campaigns().await {
                        Ok(c) => {
                            self.set_badge_campaigns(&c);
                            campaigns = c;
                        }
                        Err(e) => warn!("Could not load drop campaigns: {e:#}"),
                    }
                }
                match self.twitch.inventory().await {
                    Ok(inventory) => {
                        self.sync_campaigns(&mut campaigns, &inventory).await;
                        self.update_badge_progress(&inventory);
                    }
                    Err(e) => warn!("Error while syncing inventory: {e:#}"),
                }
                for s in streamers.iter().filter(|s| s.drops_condition()) {
                    let mut st = s.state();
                    let game_id = st.stream.game.as_ref().map(|g| g.id.clone()).unwrap_or_default();
                    let matching = campaigns
                        .iter()
                        .filter(|c| {
                            !c.drops.is_empty()
                                && c.game_id == game_id
                                && st.stream.campaigns_ids.contains(&c.id)
                        })
                        .cloned()
                        .collect();
                    st.stream.campaigns = matching;
                }
                self.update_badge_targets();
                self.prune_badge_channels();
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}
