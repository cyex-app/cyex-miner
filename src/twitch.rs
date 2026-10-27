//! Twitch GraphQL / HTTP client.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use reqwest::Client;
use serde_json::{json, Value};
use tracing::debug;

use crate::{
    auth::Credentials,
    config::FollowersOrder,
    constants::*,
    entities::stream::Game,
    utils::{now, random_between, random_hex},
};

/// Queries per batched GQL request (Twitch rejects batches much larger than this).
const GQL_BATCH: usize = 30;

pub struct PointsContext {
    pub balance: i64,
    pub multipliers: Vec<f64>,
    pub claim_id: Option<String>,
}

pub struct StreamInfo {
    pub broadcast_id: String,
    pub title: String,
    pub game: Option<Game>,
    pub viewers: i64,
}

pub struct Twitch {
    http: Client,
    pub creds: Credentials,
    client_session: String,
    /// (Client-Version, fetched at)
    client_version: Mutex<(String, f64)>,
    /// The analytics endpoint is the same for every channel, so it is scraped once and cached.
    spade_url: Mutex<Option<String>>,
    build_id_re: Regex,
    settings_re: Regex,
    spade_re: Regex,
}

// A failed query comes back as `{"errors": [...], "data": null}`. That must be an error, not
// "no such channel" or "offline": callers act on those (followers sync deletes channels).
pub(crate) fn parse_channel_id(resp: &Value) -> Result<Option<String>> {
    if resp["data"].is_null() {
        bail!("unexpected response: {resp}");
    }
    Ok(resp["data"]["user"]["id"].as_str().map(str::to_string))
}

/// `Ok(None)` means the channel is offline.
pub(crate) fn parse_stream_info(resp: &Value) -> Result<Option<StreamInfo>> {
    if resp["data"].is_null() {
        bail!("unexpected response: {resp}");
    }
    let user = &resp["data"]["user"];
    let stream = &user["stream"];
    if stream.is_null() {
        return Ok(None);
    }
    Ok(Some(StreamInfo {
        broadcast_id: stream["id"].as_str().unwrap_or_default().to_string(),
        title: user["broadcastSettings"]["title"].as_str().unwrap_or_default().to_string(),
        game: Game::from_json(&user["broadcastSettings"]["game"]),
        viewers: stream["viewersCount"].as_i64().unwrap_or(0),
    }))
}

/// `Ok(None)` means the channel does not exist.
fn parse_points_context(resp: &Value) -> Result<Option<PointsContext>> {
    let data = &resp["data"];
    if data.is_null() {
        bail!("unexpected response: {resp}");
    }
    if data["community"].is_null() {
        return Ok(None);
    }
    let points = &data["community"]["channel"]["self"]["communityPoints"];
    Ok(Some(PointsContext {
        balance: points["balance"].as_i64().unwrap_or(0),
        multipliers: points["activeMultipliers"]
            .as_array()
            .map(|m| m.iter().filter_map(|x| x["factor"].as_f64()).collect())
            .unwrap_or_default(),
        claim_id: points["availableClaim"]["id"].as_str().map(str::to_string),
    }))
}

fn parse_campaign_ids(resp: &Value) -> Vec<String> {
    resp["data"]["channel"]["viewerDropCampaigns"]
        .as_array()
        .map(|c| c.iter().filter_map(|x| x["id"].as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

/// The current watch streak from a `RewardList` response. Twitch sends the count as a string.
/// Live channels from a game directory page, as (id, login, viewers).
pub(crate) fn parse_directory(resp: &Value) -> Vec<(String, String, i64)> {
    resp["data"]["game"]["streams"]["edges"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let node = &e["node"];
            let b = &node["broadcaster"];
            Some((
                b["id"].as_str()?.to_string(),
                b["login"].as_str()?.to_string(),
                node["viewersCount"].as_i64().unwrap_or(0),
            ))
        })
        .collect()
}

pub fn parse_watch_streak(resp: &Value) -> Option<u32> {
    let value = &resp["data"]["channel"]["self"]["watchStreakMilestone"]["watchStreakMilestone"]["value"];
    match value {
        Value::String(s) => s.parse().ok(),
        other => other.as_u64().and_then(|n| u32::try_from(n).ok()),
    }
}

fn last_url_line(text: &str) -> Option<&str> {
    text.lines()
        .map(str::trim)
        .filter(|l| l.starts_with("http"))
        .next_back()
}

impl Twitch {
    pub fn new(http: Client, creds: Credentials) -> Self {
        Self {
            http,
            creds,
            client_session: random_hex(16),
            client_version: Mutex::new((CLIENT_VERSION.to_string(), 0.0)),
            spade_url: Mutex::new(None),
            build_id_re: Regex::new(
                r#"window\.__twilightBuildID\s*=\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})""#,
            )
            .unwrap(),
            settings_re: Regex::new(
                r"(https://static\.twitchcdn\.net/config/settings.*?js|https://assets\.twitch\.tv/config/settings.*?\.js)",
            )
            .unwrap(),
            spade_re: Regex::new(r#""spade_url":"(.*?)""#).unwrap(),
        }
    }

    /// The web client's build id, refreshed hourly.
    async fn client_version(&self) -> String {
        {
            let cached = self.client_version.lock().unwrap();
            if now() - cached.1 < 3600.0 {
                return cached.0.clone();
            }
        }
        let fetched = async {
            let page = self
                .http
                .get(TWITCH_URL)
                .header("User-Agent", CHROME_USER_AGENT)
                .send()
                .await
                .ok()?
                .text()
                .await
                .ok()?;
            Some(self.build_id_re.captures(&page)?[1].to_string())
        }
        .await;
        let mut cached = self.client_version.lock().unwrap();
        if let Some(version) = fetched {
            cached.0 = version;
        }
        cached.1 = now();
        cached.0.clone()
    }

    pub async fn gql(&self, body: &Value) -> Result<Value> {
        let resp = self
            .http
            .post(GQL_URL)
            .json(body)
            .header("Authorization", format!("OAuth {}", self.creds.auth_token))
            .header("Client-Id", CLIENT_ID)
            .header("Client-Session-Id", &self.client_session)
            .header("Client-Version", self.client_version().await)
            .header("User-Agent", CHROME_USER_AGENT)
            .header("X-Device-Id", &self.creds.device_id)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        debug!("GQL {} -> {status}: {text}", body["operationName"]);
        serde_json::from_str(&text).with_context(|| format!("GQL returned {status}: {text}"))
    }

    /// Sends many queries in as few requests as possible. Results are in the same order as
    /// `bodies`; a failed query only fails its own result.
    pub async fn gql_batch(&self, bodies: Vec<Value>) -> Vec<Result<Value>> {
        let mut results = Vec::with_capacity(bodies.len());
        for (i, chunk) in bodies.chunks(GQL_BATCH).enumerate() {
            if i > 0 {
                tokio::time::sleep(random_between(0.5, 1.0)).await;
            }
            match self.gql(&Value::Array(chunk.to_vec())).await {
                Ok(Value::Array(items)) if items.len() == chunk.len() => {
                    results.extend(items.into_iter().map(Ok));
                }
                Ok(other) => {
                    let got = other.as_array().map_or(0, Vec::len);
                    results.extend(chunk.iter().map(|_| {
                        Err(anyhow!("batch returned {got} results for {} queries", chunk.len()))
                    }));
                }
                Err(e) => results.extend(chunk.iter().map(|_| Err(anyhow!("{e:#}")))),
            }
        }
        results
    }

    pub async fn get_channel_id(&self, login: &str) -> Result<Option<String>> {
        parse_channel_id(&self.gql(&GET_ID_FROM_LOGIN.with(json!({ "login": login }))).await?)
    }

    pub async fn channel_ids(&self, logins: &[String]) -> Vec<Result<Option<String>>> {
        let bodies = logins
            .iter()
            .map(|login| GET_ID_FROM_LOGIN.with(json!({ "login": login })))
            .collect();
        self.gql_batch(bodies)
            .await
            .into_iter()
            .map(|r| r.and_then(|resp| parse_channel_id(&resp)))
            .collect()
    }

    /// The current login of each channel id, 100 ids per request. Deleted or banned channels
    /// are missing from the result.
    pub async fn logins_by_id(&self, ids: &[String]) -> Result<HashMap<String, String>> {
        let mut logins = HashMap::new();
        for chunk in ids.chunks(100) {
            let resp = self
                .gql(&json!({
                    "query": "query($ids: [ID!]) { users(ids: $ids) { id login } }",
                    "variables": { "ids": chunk },
                }))
                .await?;
            let users = resp["data"]["users"]
                .as_array()
                .ok_or_else(|| anyhow!("unexpected response: {resp}"))?;
            for user in users {
                if let (Some(id), Some(login)) = (user["id"].as_str(), user["login"].as_str()) {
                    logins.insert(id.to_string(), login.to_lowercase());
                }
            }
        }
        Ok(logins)
    }

    /// `Ok(None)` means the channel is offline.
    pub async fn stream_info(&self, login: &str) -> Result<Option<StreamInfo>> {
        parse_stream_info(&self.gql(&STREAM_INFO.with(json!({ "channel": login }))).await?)
    }

    /// Points context and stream info for many channels, batched together.
    pub async fn channels_overview(
        &self,
        logins: &[String],
    ) -> Vec<(Result<Option<PointsContext>>, Result<Option<StreamInfo>>)> {
        let bodies = logins
            .iter()
            .flat_map(|login| {
                [
                    CHANNEL_POINTS_CONTEXT.with(json!({ "channelLogin": login })),
                    STREAM_INFO.with(json!({ "channel": login })),
                ]
            })
            .collect();
        let mut results = self.gql_batch(bodies).await.into_iter();
        let mut overview = Vec::with_capacity(logins.len());
        while let (Some(points), Some(info)) = (results.next(), results.next()) {
            overview.push((
                points.and_then(|resp| parse_points_context(&resp)),
                info.and_then(|resp| parse_stream_info(&resp)),
            ));
        }
        overview
    }

    /// The analytics ("spade") endpoint used for minute-watched events, scraped on first use.
    pub async fn spade_url(&self, login: &str) -> Result<String> {
        if let Some(url) = self.spade_url.lock().unwrap().clone() {
            return Ok(url);
        }
        let url = self.fetch_spade_url(login).await?;
        *self.spade_url.lock().unwrap() = Some(url.clone());
        Ok(url)
    }

    /// Makes the next `spade_url` call scrape it again.
    pub fn forget_spade_url(&self) {
        *self.spade_url.lock().unwrap() = None;
    }

    async fn fetch_spade_url(&self, login: &str) -> Result<String> {
        let page = self
            .http
            .get(format!("{TWITCH_URL}/{login}"))
            .header("User-Agent", FIREFOX_USER_AGENT)
            .send()
            .await?
            .text()
            .await?;
        let settings_url = self
            .settings_re
            .captures(&page)
            .ok_or_else(|| anyhow!("settings.js URL not found on channel page"))?[1]
            .to_string();
        let settings = self
            .http
            .get(settings_url)
            .header("User-Agent", FIREFOX_USER_AGENT)
            .send()
            .await?
            .text()
            .await?;
        Ok(self
            .spade_re
            .captures(&settings)
            .ok_or_else(|| anyhow!("spade_url not found in settings.js"))?[1]
            .to_string())
    }

    /// `Ok(None)` means the channel does not exist.
    pub async fn channel_points_context(&self, login: &str) -> Result<Option<PointsContext>> {
        parse_points_context(
            &self
                .gql(&CHANNEL_POINTS_CONTEXT.with(json!({ "channelLogin": login })))
                .await?,
        )
    }

    pub async fn claim_bonus(&self, channel_id: &str, claim_id: &str) -> Result<()> {
        self.gql(&CLAIM_COMMUNITY_POINTS.with(json!({
            "input": { "channelID": channel_id, "claimID": claim_id }
        })))
        .await?;
        Ok(())
    }

    pub async fn claim_moment(&self, moment_id: &str) -> Result<()> {
        self.gql(&CLAIM_MOMENT.with(json!({ "input": { "momentID": moment_id } })))
            .await?;
        Ok(())
    }

    pub async fn join_raid(&self, raid_id: &str) -> Result<()> {
        self.gql(&JOIN_RAID.with(json!({ "input": { "raidID": raid_id } })))
            .await?;
        Ok(())
    }

    pub async fn campaign_ids_for_channel(&self, channel_id: &str) -> Vec<String> {
        let Ok(resp) = self
            .gql(&AVAILABLE_DROPS.with(json!({ "channelID": channel_id })))
            .await
        else {
            return Vec::new();
        };
        parse_campaign_ids(&resp)
    }

    /// Drop campaign ids for many channels; a failed lookup yields no campaigns.
    pub async fn campaign_ids_for_channels(&self, channel_ids: &[String]) -> Vec<Vec<String>> {
        let bodies = channel_ids
            .iter()
            .map(|id| AVAILABLE_DROPS.with(json!({ "channelID": id })))
            .collect();
        self.gql_batch(bodies)
            .await
            .into_iter()
            .map(|r| r.map(|resp| parse_campaign_ids(&resp)).unwrap_or_default())
            .collect()
    }

    /// The most watched live channels in a game (by its directory slug), as (id, login, viewers).
    pub async fn live_in_game(&self, slug: &str) -> Result<Vec<(String, String, i64)>> {
        let resp = self
            .gql(&DIRECTORY_PAGE_GAME.with(json!({
                "limit": 30,
                "slug": slug,
                "imageWidth": 50,
                "includeCostreaming": false,
                "options": {
                    "broadcasterLanguages": [],
                    "freeformTags": null,
                    "includeRestricted": ["SUB_ONLY_LIVE"],
                    "recommendationsContext": { "platform": "web" },
                    "sort": "VIEWER_COUNT",
                    "systemFilters": [],
                    "tags": [],
                    "requestID": "JIRA-VXP-2397",
                },
                "sortTypeIsRecency": false,
            })))
            .await?;
        if resp["data"]["game"].is_null() {
            bail!("no game directory for {slug}: {resp}");
        }
        Ok(parse_directory(&resp))
    }

    /// The current watch streak on each channel; `None` where Twitch reported none or the lookup failed.
    pub async fn watch_streaks(&self, channel_ids: &[String]) -> Vec<Option<u32>> {
        let bodies = channel_ids
            .iter()
            .map(|id| REWARD_LIST.with(json!({ "channelID": id, "shouldIncludeAllSuspendedStreaks": false })))
            .collect();
        self.gql_batch(bodies)
            .await
            .into_iter()
            .map(|r| r.ok().and_then(|resp| parse_watch_streak(&resp)))
            .collect()
    }

    pub async fn inventory(&self) -> Result<Value> {
        let resp = self
            .gql(&INVENTORY.with(json!({ "fetchRewardCampaigns": true })))
            .await?;
        Ok(resp["data"]["currentUser"]["inventory"].clone())
    }

    pub async fn active_drop_campaigns(&self) -> Result<Vec<Value>> {
        let resp = self
            .gql(&VIEWER_DROPS_DASHBOARD.with(json!({ "fetchRewardCampaigns": true })))
            .await?;
        Ok(resp["data"]["currentUser"]["dropCampaigns"]
            .as_array()
            .map(|c| {
                c.iter()
                    .filter(|x| x["status"].as_str() == Some("ACTIVE"))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    pub async fn campaign_details(&self, ids: &[String]) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        for chunk in ids.chunks(20) {
            let body: Vec<Value> = chunk
                .iter()
                .map(|id| {
                    DROP_CAMPAIGN_DETAILS.with(json!({
                        "dropID": id,
                        "channelLogin": self.creds.user_id,
                    }))
                })
                .collect();
            let resp = self.gql(&Value::Array(body)).await?;
            for item in resp.as_array().into_iter().flatten() {
                let campaign = &item["data"]["user"]["dropCampaign"];
                if !campaign.is_null() {
                    result.push(campaign.clone());
                }
            }
        }
        Ok(result)
    }

    pub async fn claim_drop(&self, drop_instance_id: &str) -> bool {
        let Ok(resp) = self
            .gql(&CLAIM_DROP_REWARDS.with(json!({ "input": { "dropInstanceID": drop_instance_id } })))
            .await
        else {
            return false;
        };
        matches!(
            resp["data"]["claimDropRewards"]["status"].as_str(),
            Some("ELIGIBLE_FOR_ALL" | "DROP_INSTANCE_ALREADY_CLAIMED")
        )
    }

    /// (login, channel id) of every followed channel; the id is `None` if Twitch didn't include it.
    pub async fn followers(&self, order: FollowersOrder) -> Result<Vec<(String, Option<String>)>> {
        let order = match order {
            FollowersOrder::Asc => "ASC",
            FollowersOrder::Desc => "DESC",
        };
        let mut follows = Vec::new();
        let mut cursor = Value::String(String::new());
        loop {
            let resp = self
                .gql(&CHANNEL_FOLLOWS.with(json!({ "limit": 100, "order": order, "cursor": cursor })))
                .await?;
            let page = &resp["data"]["user"]["follows"];
            let edges = page["edges"]
                .as_array()
                .ok_or_else(|| anyhow!("unexpected follows response: {resp}"))?;
            for edge in edges {
                if let Some(login) = edge["node"]["login"].as_str() {
                    let id = edge["node"]["id"].as_str().map(str::to_string);
                    follows.push((login.to_lowercase(), id));
                }
                cursor = edge["cursor"].clone();
            }
            if page["pageInfo"]["hasNextPage"].as_bool() != Some(true) || edges.is_empty() {
                return Ok(follows);
            }
        }
    }

    /// Simulates one minute of watching: fetch the HLS playlist, touch the newest
    /// segment of the lowest quality, then send the minute-watched analytics event.
    pub async fn watch(&self, login: &str, spade_url: &str, payload: &str) -> Result<bool> {
        let resp = self
            .gql(&PLAYBACK_ACCESS_TOKEN.with(json!({
                "login": login,
                "isLive": true,
                "isVod": false,
                "vodID": "",
                "playerType": "site",
                "platform": "web",
            })))
            .await?;
        let token = &resp["data"]["streamPlaybackAccessToken"];
        let (Some(signature), Some(value)) = (token["signature"].as_str(), token["value"].as_str()) else {
            bail!("missing playback access token in {resp}");
        };

        let timeout = Duration::from_secs(20);
        let master = self
            .http
            .get(format!("https://usher.ttvnw.net/api/channel/hls/{login}.m3u8"))
            .query(&[("sig", signature), ("token", value)])
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        if !master.status().is_success() {
            bail!("usher playlist returned {}", master.status());
        }
        let master = master.text().await?;
        let variant_url = last_url_line(&master).context("no variant in master playlist")?;

        let variant = self
            .http
            .get(variant_url)
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?
            .text()
            .await?;
        let segment_url = last_url_line(&variant).context("no segment in media playlist")?;

        let head = self
            .http
            .head(segment_url)
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        if !head.status().is_success() {
            bail!("segment HEAD returned {}", head.status());
        }

        let spade = self
            .http
            .post(spade_url)
            .form(&[("data", payload)])
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        debug!("minute-watched for {login}: {}", spade.status());
        Ok(spade.status().as_u16() == 204)
    }
}
