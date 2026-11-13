//! Drop campaigns: tracks progress on the live channels and claims finished drops.

use std::{
    collections::HashSet,
    fs,
    io::Write,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use anyhow::Result;
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::Miner;
use crate::{
    entities::drops::{Campaign, Drop},
    twitch::merge_campaign_ids,
    utils::{now, random_between},
};

/// Seconds between campaign list reads, and the shorter wait when no campaign id was found.
/// Short enough for badge events that only run an hour or two.
const CAMPAIGNS_EVERY: f64 = 10.0 * 60.0;
const CAMPAIGNS_RETRY: f64 = 5.0 * 60.0;

/// The badge debug file, next to the config.
const BADGE_DEBUG_FILE: &str = "badge-debug.jsonl";
/// Past this size the file is moved to `badge-debug.jsonl.old` and a new one started.
const BADGE_DEBUG_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// The drop campaigns followed by the reward and event badge campaigns. An event badge
/// (`event:` id, from streamdatabase) that a Twitch campaign also gives is left out: Twitch's
/// own campaign reports progress.
pub(crate) fn with_event_badges(drops: &[Campaign], rewards: &[Campaign]) -> Vec<Campaign> {
    let from_twitch: HashSet<String> = drops
        .iter()
        .chain(rewards.iter().filter(|c| !c.id.starts_with("event:")))
        .flat_map(|c| &c.drops)
        .flat_map(|d| &d.rewards)
        .map(|r| r.name.trim().to_lowercase())
        .collect();
    drops
        .iter()
        .chain(rewards.iter().filter(|c| !(c.id.starts_with("event:") && from_twitch.contains(&c.name.trim().to_lowercase()))))
        .cloned()
        .collect()
}

impl Miner {
    /// Appends what Twitch sends about drops and reward campaigns to `BADGE_DEBUG_FILE` when it
    /// changed, to see how badge campaigns the miner doesn't handle yet look (watch goals on
    /// several days, badges to pick from). `last` is the previous snapshot.
    async fn write_badge_debug(&self, last: &mut String) {
        let mut snapshot = match self.twitch.badge_debug_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(e) => return debug!("Could not load the badge debug data: {e:#}"),
        };
        // The regular campaign list is long and already handled; a summary is enough.
        if let Some(campaigns) = snapshot["dashboard"]["currentUser"]["dropCampaigns"].as_array_mut() {
            for c in campaigns.iter_mut() {
                *c = json!({ "id": c["id"], "name": c["name"], "status": c["status"], "game": c["game"]["displayName"] });
            }
        }
        let text = snapshot.to_string();
        if text == *last {
            return;
        }
        let path = self.config.data_dir.join(BADGE_DEBUG_FILE);
        if fs::metadata(&path).is_ok_and(|m| m.len() > BADGE_DEBUG_MAX_BYTES) {
            let _ = fs::rename(&path, path.with_extension("jsonl.old"));
        }
        let line = json!({ "at": chrono::Utc::now().to_rfc3339(), "data": snapshot });
        let written = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| writeln!(f, "{line}"));
        match written {
            Ok(()) => *last = text,
            Err(e) => warn!("Could not write {}: {e}", path.display()),
        }
    }

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

    /// The campaigns to track, and whether no campaign id was found at all (worth trying again soon).
    async fn fetch_campaigns(&self) -> Result<(Vec<Campaign>, bool)> {
        let mut ids: Vec<String> = self
            .twitch
            .active_drop_campaigns()
            .await?
            .iter()
            .filter_map(|c| c["id"].as_str().map(str::to_string))
            .collect();
        let dashboard = ids.len();
        // Some accounts get no dashboard list: use the ids the live channels' reads found, and
        // the inventory's campaigns in progress.
        if ids.is_empty() {
            let channels: Vec<Vec<String>> = self
                .streamers()
                .iter()
                .map(|s| s.state().stream.campaigns_ids.clone())
                .collect();
            let inventory: Vec<String> = match self.twitch.inventory().await {
                Ok(inv) => inv["dropCampaignsInProgress"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c["id"].as_str().map(str::to_string))
                    .collect(),
                Err(e) => {
                    debug!("Could not load the inventory for the campaign fallback: {e:#}");
                    Vec::new()
                }
            };
            ids = merge_campaign_ids(&channels, &inventory);
        }
        self.note_campaign_counts(dashboard, ids.len());
        let details = self.twitch.campaign_details(&ids).await?;
        // What each campaign gives, to see why a badge does or doesn't show (RUST_LOG=points_miner=debug).
        for c in &details {
            let rewards: Vec<String> = c["timeBasedDrops"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|d| d["benefitEdges"].as_array().into_iter().flatten().map(move |e| (d, e)))
                .map(|(d, e)| {
                    format!(
                        "{} ({}, {} min, {} subs)",
                        e["benefit"]["name"].as_str().unwrap_or("?"),
                        e["benefit"]["distributionType"].as_str().unwrap_or("?"),
                        d["requiredMinutesWatched"],
                        d["requiredSubs"]
                    )
                })
                .collect();
            debug!("Drop campaign {}: {}", c["name"].as_str().unwrap_or("?"), rewards.join(", "));
        }
        let retry_soon = ids.is_empty();
        let campaigns = details
            .iter()
            .filter_map(Campaign::from_json)
            .filter(Campaign::dt_match)
            .filter_map(|mut c| {
                c.clear_drops();
                (!c.drops.is_empty()).then_some(c)
            })
            .collect();
        Ok((campaigns, retry_soon))
    }

    /// Running reward campaigns with a watch goal (Twitch's global badges), with the ids of the
    /// channels they name looked up. A failed read gives none; the drop campaigns don't depend on it.
    async fn fetch_reward_campaigns(&self) -> Vec<Campaign> {
        let json = match self.twitch.reward_campaigns().await {
            Ok(json) => json,
            Err(e) => {
                warn!("Could not load reward campaigns (global badges): {e:#}");
                return Vec::new();
            }
        };
        let mut campaigns: Vec<Campaign> = json
            .iter()
            .filter_map(Campaign::from_reward_json)
            .filter(Campaign::dt_match)
            .filter_map(|mut c| {
                c.clear_drops();
                (!c.drops.is_empty()).then_some(c)
            })
            .collect();
        for c in &mut campaigns {
            if c.allowed_channels.is_empty() {
                continue;
            }
            let logins: Vec<String> = c.allowed_channels.iter().map(|(_, l)| l.clone()).collect();
            let ids = self.twitch.channel_ids(&logins).await;
            c.allowed_channels = logins
                .into_iter()
                .zip(ids)
                .filter_map(|(login, id)| Some((id.ok().flatten()?, login)))
                .collect();
        }
        let watch: Vec<String> = campaigns
            .iter()
            .flat_map(|c| {
                c.drops.iter().filter(|d| d.minutes_required > 0 && d.required_subs == 0).map(move |d| {
                    let place = match (c.allowed_channels.first(), c.game_name.as_str()) {
                        (Some((_, login)), _) => login.clone(),
                        (None, "") => "any channel".to_string(),
                        (None, game) => game.to_string(),
                    };
                    format!("{} ({} min, {place})", d.name, d.minutes_required)
                })
            })
            .collect();
        info!("{} reward campaigns running, with a watch goal (badges among them count): {}", json.len(), if watch.is_empty() { "none".into() } else { watch.join(", ") });
        campaigns
    }

    /// One line when the campaign counts change: dashboard ids, ids tracked after the fallback.
    fn note_campaign_counts(&self, dashboard: usize, tracked: usize) {
        static LAST: Mutex<Option<(usize, usize)>> = Mutex::new(None);
        let mut last = LAST.lock().unwrap_or_else(PoisonError::into_inner);
        if *last != Some((dashboard, tracked)) {
            *last = Some((dashboard, tracked));
            info!("Drop campaigns: {dashboard} on the dashboard, {tracked} tracked.");
        }
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
        let mut last_badge_debug = String::new();
        loop {
            let streamers = self.streamers();
            // The defaults count too: badge drops are listed before any channel is added for them.
            let claims_drops = self.config.streamer_defaults.claim_drops
                || self.has_picked_badges()
                || self.auto_badges()
                || streamers.iter().any(|s| s.settings.claim_drops);
            if !self.paused() && claims_drops {
                if now() - last_refresh > CAMPAIGNS_EVERY {
                    last_refresh = now();
                    self.claim_all_drops_from_inventory().await;
                    self.refresh_event_badges().await;
                    let mut rewards = self.fetch_reward_campaigns().await;
                    rewards.extend(self.event_campaigns());
                    match self.fetch_campaigns().await {
                        Ok((c, retry_soon)) => {
                            self.set_badge_campaigns(&with_event_badges(&c, &rewards));
                            campaigns = c;
                            // No ids anywhere: the channel reads may not have run yet, look again soon.
                            if retry_soon {
                                last_refresh = now() - (CAMPAIGNS_EVERY - CAMPAIGNS_RETRY);
                            }
                        }
                        Err(e) => {
                            warn!("Could not load drop campaigns: {e:#}");
                            // Reward badges don't depend on the drop list; keep the known drop
                            // campaigns' badges by passing the last list along.
                            self.set_badge_campaigns(&with_event_badges(&campaigns, &rewards));
                        }
                    }
                    if self.config.badge_debug {
                        self.write_badge_debug(&mut last_badge_debug).await;
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
                self.run_auto_badges().await;
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}
