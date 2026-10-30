use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{
    sync::{broadcast, mpsc},
    time::Instant,
};
use tracing::{debug, info, warn};

use crate::{
    chat::ChatManager,
    config::{merge_settings, Config},
    entities::streamer::{ChatPresence, Source, Streamer},
    priority::{self, Priority},
    pubsub::PubSubPool,
    store::{ChannelRow, Store},
    twitch::{ChannelHit, PointsContext, StreamInfo, Twitch},
    utils::{millify, now},
};

mod badges;
mod drops;
mod messages;
mod predictions;

pub use badges::{BadgeView, EarnedBadge};
pub use predictions::{PredictionSettings, PredictionsView};
#[cfg(test)]
pub(crate) use predictions::{pick as pick_outcome, Outcome, Strategy};
#[cfg(test)]
pub(crate) use badges::parse_event_badges;

/// `meta` key for the watch priority saved in the web UI, which overrides config.toml.
const PRIORITY_KEY: &str = "priority";
/// `meta` key of the old "Streaks first" switch, only read to build the initial watch priority.
const LEGACY_STREAK_FIRST_KEY: &str = "streak_first";
/// `meta` key for whether mining was paused in the web UI.
const PAUSED_KEY: &str = "paused";
/// `meta` key for the "fill empty slots" switch ("0" or "1"; on when missing).
const FILL_SLOTS_KEY: &str = "fill_slots";
/// Seconds a channel has to be live before it gets a watch slot, so its stream info is loaded.
const STREAM_SETTLE_SECS: f64 = 10.0;
/// Recent events kept for web UI clients that connect later.
const MAX_EVENTS: usize = 50;

#[derive(Debug, Clone, Serialize)]
pub struct LogEvent {
    pub ts: f64,
    pub text: String,
}

pub struct Miner {
    pub twitch: Twitch,
    pub config: Config,
    store: Store,
    streamers: RwLock<Vec<Arc<Streamer>>>,
    pubsub: PubSubPool,
    chat: ChatManager,
    events: Mutex<VecDeque<LogEvent>>,
    /// New events, pushed to connected web UI clients.
    events_tx: broadcast::Sender<LogEvent>,
    adding: Mutex<HashSet<String>>,
    /// Rules for picking the channels to watch, see `load_priority`.
    priority: RwLock<Vec<Priority>>,
    /// While paused nothing is watched, chats are left and nothing is claimed. PubSub stays
    /// connected so balances and live status stay current in the web UI.
    paused: AtomicBool,
    /// Whether slots the priority rules leave free go to live channels in list order.
    fill_slots: AtomicBool,
    pub started_at: f64,
    badges: badges::BadgeState,
    predictions: predictions::PredictionState,
}

fn valid_login(name: &str) -> bool {
    (1..=25).contains(&name.len()) && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The watch priority saved in the web UI, or else `priority` from config.toml.
fn load_priority(config: &Config, store: &Store) -> Vec<Priority> {
    match store.meta(PRIORITY_KEY) {
        Ok(Some(saved)) => match serde_json::from_str(&saved) {
            Ok(rules) => return rules,
            Err(e) => warn!("Ignoring the saved watch priority: {e}"),
        },
        Ok(None) => {}
        Err(e) => warn!("Could not read the watch priority: {e:#}"),
    }
    let mut rules = config.priority.clone();
    // Before starred channels were a rule they always came first, unless the old "Streaks first"
    // switch put watch streaks ahead of them.
    if !rules.contains(&Priority::Prioritized) {
        rules.insert(0, Priority::Prioritized);
    }
    if store.meta(LEGACY_STREAK_FIRST_KEY).ok().flatten().as_deref() == Some("1") {
        rules.retain(|r| *r != Priority::Streak);
        rules.insert(0, Priority::Streak);
    }
    priority::dedup(rules)
}

impl Miner {
    pub fn new(
        twitch: Twitch,
        config: Config,
        store: Store,
        pubsub_messages: mpsc::UnboundedSender<String>,
    ) -> Self {
        let token = twitch.creds.auth_token.clone();
        let chat = ChatManager::new(twitch.creds.login.clone(), token.clone());
        let priority = load_priority(&config, &store);
        let paused = match store.meta(PAUSED_KEY) {
            Ok(v) => v.as_deref() == Some("1"),
            Err(e) => {
                warn!("Could not read the paused setting: {e:#}");
                false
            }
        };
        if paused {
            info!("Mining is paused, resume it in the web UI");
        }
        let fill_slots = store.meta(FILL_SLOTS_KEY).ok().flatten().as_deref() != Some("0");
        Self {
            twitch,
            config,
            streamers: RwLock::new(Vec::new()),
            pubsub: PubSubPool::new(token, pubsub_messages),
            chat,
            events: Mutex::new(VecDeque::new()),
            events_tx: broadcast::channel(256).0,
            adding: Mutex::new(HashSet::new()),
            priority: RwLock::new(priority),
            paused: AtomicBool::new(paused),
            fill_slots: AtomicBool::new(fill_slots),
            started_at: now(),
            badges: badges::BadgeState::load(&store),
            predictions: predictions::PredictionState::load(&store),
            store,
        }
    }

    /// Logs a notable event and keeps it for the web UI.
    pub fn event(&self, text: impl Into<String>) {
        let text = text.into();
        info!("{text}");
        let event = LogEvent { ts: now(), text };
        let mut events = self.events.lock().unwrap();
        // Sent while holding the lock, so `subscribe_events` never sees an event twice or misses one.
        let _ = self.events_tx.send(event.clone());
        events.push_back(event);
        while events.len() > MAX_EVENTS {
            events.pop_front();
        }
    }

    pub fn events(&self) -> Vec<LogEvent> {
        self.events.lock().unwrap().iter().cloned().collect()
    }

    /// The recent events plus a receiver for every event after them.
    pub fn subscribe_events(&self) -> (Vec<LogEvent>, broadcast::Receiver<LogEvent>) {
        let events = self.events.lock().unwrap();
        (events.iter().cloned().collect(), self.events_tx.subscribe())
    }

    pub fn streamers(&self) -> Vec<Arc<Streamer>> {
        self.streamers.read().unwrap().clone()
    }

    fn find_by_channel(&self, channel_id: &str) -> Option<Arc<Streamer>> {
        self.streamers
            .read()
            .unwrap()
            .iter()
            .find(|s| s.channel_id == channel_id)
            .cloned()
    }

    // === Startup / runtime management === //

    pub async fn run(self: Arc<Self>, pubsub_rx: mpsc::UnboundedReceiver<String>) {
        tokio::spawn(self.clone().dispatch_pubsub(pubsub_rx));
        self.pubsub
            .submit(format!("community-points-user-v1.{}", self.twitch.creds.user_id));
        // Channel prediction topics are added as channels load; this is the bet results topic.
        if self.prediction_settings().enabled {
            self.subscribe_predictions(true);
        }

        if self.config.claim_drops_startup {
            self.claim_all_drops_from_inventory().await;
        }

        self.import_config().await;
        if self.config.followers {
            if let Err(e) = self.sync_followers().await {
                warn!("Could not load followed channels: {e:#}");
            }
        }
        let mut rows: Vec<ChannelRow> = match self.store.channels() {
            Ok(rows) => rows,
            Err(e) => {
                warn!("Could not read the channel list from the database: {e:#}");
                Vec::new()
            }
        };
        // Followed channels keep their place in the list while `followers` is off, but aren't mined.
        rows.retain(|r| self.config.followers || r.source != Source::Followers);
        self.refresh_logins(&mut rows).await;
        self.load_streamers(rows).await;

        tokio::spawn(self.clone().watch_loop());
        tokio::spawn(self.clone().campaigns_loop());
        tokio::spawn(self.clone().refresh_context_loop());
    }

    /// One-time import of the `streamers` and `blacklist` lists from config.toml into the database.
    async fn import_config(&self) {
        const KEY: &str = "config_imported";
        match self.store.meta(KEY) {
            Ok(Some(_)) => {
                if !self.config.streamers.is_empty() || !self.config.blacklist.is_empty() {
                    warn!(
                        "`streamers` and `blacklist` in config.toml are no longer used: channels are kept in \
                         miner.db and managed in the web UI"
                    );
                }
                return;
            }
            Ok(None) => {}
            Err(e) => return warn!("Could not read the database: {e:#}"),
        }

        // (login, per-channel overrides, prioritized)
        let mut entries: Vec<(String, Value, bool)> = Vec::new();
        for entry in &self.config.streamers {
            let login = match normalize_login(&entry.username()) {
                Ok(login) => login,
                Err(e) => {
                    warn!("Not importing {}: {e:#}", entry.username());
                    continue;
                }
            };
            let mut overrides = match entry.settings(&self.config.streamer_defaults).and_then(|_| entry.overrides()) {
                Ok(overrides) => overrides,
                Err(e) => {
                    warn!("Importing {login} with the default settings: {e:#}");
                    json!({})
                }
            };
            let prioritized = overrides
                .as_object_mut()
                .and_then(|o| o.remove("prioritize"))
                .and_then(|p| p.as_bool())
                .unwrap_or(self.config.streamer_defaults.prioritize);
            entries.push((login, overrides, prioritized));
        }
        let blacklist: Vec<String> = self
            .config
            .blacklist
            .iter()
            .filter_map(|name| normalize_login(name).ok())
            .collect();
        if entries.is_empty() && blacklist.is_empty() {
            let _ = self.store.set_meta(KEY, "1");
            return;
        }

        let logins: Vec<String> = entries.iter().map(|(login, ..)| login.clone()).chain(blacklist.clone()).collect();
        let mut ids = self.twitch.channel_ids(&logins).await.into_iter();
        let mut complete = true;
        let mut imported = 0;
        for (login, overrides, prioritized) in &entries {
            match ids.next() {
                Some(Ok(Some(id))) => match self.store.add_channel(&id, login, Source::Manual, *prioritized, overrides) {
                    Ok(()) => imported += 1,
                    Err(e) => {
                        complete = false;
                        warn!("Could not import {login}: {e:#}");
                    }
                },
                Some(Ok(None)) => warn!("Not importing {login}: channel does not exist"),
                Some(Err(e)) => {
                    complete = false;
                    warn!("Could not import {login}: {e:#}");
                }
                None => complete = false,
            }
        }
        for login in &blacklist {
            match ids.next() {
                Some(Ok(Some(id))) => {
                    if let Err(e) = self.store.add_to_blacklist(&id, login) {
                        complete = false;
                        warn!("Could not import {login} into the blacklist: {e:#}");
                    }
                }
                Some(Ok(None)) => {}
                Some(Err(e)) => {
                    complete = false;
                    warn!("Could not import {login} into the blacklist: {e:#}");
                }
                None => complete = false,
            }
        }
        if complete {
            let _ = self.store.set_meta(KEY, "1");
            self.event(format!(
                "Imported {imported} channels from config.toml into miner.db. Channels are now managed in the web \
                 UI; `streamers` and `blacklist` in config.toml are no longer used."
            ));
        } else {
            warn!("Some channels could not be imported from config.toml; retrying on the next start");
        }
    }

    /// Adds newly followed channels to the end of the list and drops unfollowed ones.
    async fn sync_followers(&self) -> Result<()> {
        let follows = self.twitch.followers(self.config.followers_order).await?;
        let missing: Vec<String> = follows
            .iter()
            .filter(|(_, id)| id.is_none())
            .map(|(login, _)| login.clone())
            .collect();
        let mut resolved = self.twitch.channel_ids(&missing).await.into_iter();
        // Only drop unfollowed channels if every follow could be matched to an id.
        let mut complete = true;
        let mut followed: Vec<(String, String)> = Vec::new();
        for (login, id) in follows {
            let id = match id {
                Some(id) => Some(id),
                None => match resolved.next() {
                    Some(Ok(id)) => id,
                    _ => {
                        complete = false;
                        None
                    }
                },
            };
            if let Some(id) = id {
                followed.push((login, id));
            }
        }

        let blacklist = self.store.blacklist()?;
        let existing: HashMap<String, Source> =
            self.store.channels()?.into_iter().map(|r| (r.id, r.source)).collect();
        let no_overrides = json!({});
        let mut added = 0;
        for (login, id) in &followed {
            if !blacklist.contains(id) && !existing.contains_key(id) {
                self.store.add_channel(id, login, Source::Followers, false, &no_overrides)?;
                added += 1;
            }
        }
        let mut removed = 0;
        if complete {
            let followed_ids: HashSet<&String> = followed.iter().map(|(_, id)| id).collect();
            for (id, source) in &existing {
                if *source == Source::Followers && !followed_ids.contains(id) {
                    self.store.remove_channel(id)?;
                    removed += 1;
                }
            }
        }
        self.event(format!(
            "Loaded {} followed channels from your profile ({added} new, {removed} unfollowed)",
            followed.len()
        ));
        Ok(())
    }

    /// Updates stored logins from the channel ids, so renamed channels keep working.
    async fn refresh_logins(&self, rows: &mut [ChannelRow]) {
        let ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        let logins = match self.twitch.logins_by_id(&ids).await {
            Ok(logins) => logins,
            Err(e) => return warn!("Could not check for renamed channels: {e:#}"),
        };
        for row in rows.iter_mut() {
            let Some(login) = logins.get(&row.id) else { continue };
            if *login != row.login {
                self.event(format!("{} was renamed to {login}", row.login));
                if let Err(e) = self.store.set_login(&row.id, login) {
                    warn!("Could not save the new name of {login}: {e:#}");
                }
                row.login = login.clone();
            }
        }
    }

    /// Channels for the add field's suggestions: Twitch's search results, plus the channel spelled
    /// exactly like the query when the search left it out (offline and small channels often are).
    pub async fn search_channels(&self, query: &str) -> Vec<ChannelHit> {
        let mut hits = match self.twitch.search_channels(query).await {
            Ok(hits) => hits,
            Err(e) => {
                warn!("Channel search failed: {e:#}");
                Vec::new()
            }
        };
        let exact = query.trim().trim_start_matches('@').to_lowercase();
        if valid_login(&exact) && !hits.iter().any(|h| h.login == exact) {
            // Name, profile picture and stream in one request; nothing when the channel doesn't exist.
            match self.twitch.channel_card(&exact).await {
                Ok(Some(card)) => hits.insert(0, card),
                Ok(None) => {}
                Err(e) => warn!("Could not look up {exact}: {e:#}"),
            }
        }
        hits
    }

    /// Adds a channel by name (from the web UI) to the end of the list.
    pub async fn add_streamer(&self, username: &str) -> Result<Arc<Streamer>> {
        let username = normalize_login(username)?;
        let existing = self.streamers.read().unwrap().iter().find(|s| s.username == username).cloned();
        if existing.as_ref().is_some_and(|s| s.source != Source::Badge) {
            bail!("{username} is already being mined");
        }
        if !self.adding.lock().unwrap().insert(username.clone()) {
            bail!("{username} is already being added");
        }
        let result = match existing {
            // Quick-added for a badge: adding it by hand keeps it for good.
            Some(s) => self.keep_badge_channel(&s, &username).await,
            None => self.add_streamer_inner(&username).await,
        };
        self.adding.lock().unwrap().remove(&username);
        result
    }

    /// Turns a quick-added badge channel into a manual one, keeping its star.
    async fn keep_badge_channel(&self, streamer: &Streamer, username: &str) -> Result<Arc<Streamer>> {
        let starred = streamer.state().prioritized;
        self.unregister(&streamer.channel_id)?;
        let kept = self.add_channel(streamer.channel_id.clone(), username, Source::Manual).await?;
        kept.state().prioritized = starred;
        Ok(kept)
    }

    async fn add_streamer_inner(&self, username: &str) -> Result<Arc<Streamer>> {
        let channel_id = self
            .twitch
            .get_channel_id(username)
            .await?
            .ok_or_else(|| anyhow!("streamer {username} does not exist"))?;
        self.add_channel(channel_id, username, Source::Manual).await
    }

    /// Adds a channel whose id is known to the end of the list. The caller reserves the name in `adding`.
    async fn add_channel(&self, channel_id: String, username: &str, source: Source) -> Result<Arc<Streamer>> {
        if self.find_by_channel(&channel_id).is_some() {
            bail!("{username} is already being mined");
        }
        let mut settings = self.config.streamer_defaults.clone();
        let mut overrides = json!({});
        if source == Source::Badge {
            // Badge progress needs drops on, whatever the defaults say; saved so it survives a restart.
            settings.claim_drops = true;
            overrides = json!({ "claim_drops": true });
        }
        self.store
            .add_channel(&channel_id, username, source, settings.prioritize, &overrides)
            .context("could not save the channel")?;
        if source == Source::Manual {
            if let Err(e) = self.store.remove_from_blacklist(&channel_id) {
                warn!("Could not remove {username} from the blacklist: {e:#}");
            }
            // Added by hand: it stays, even if it was quick-added for a badge before.
            self.forget_badge_channel(&channel_id);
        }

        let streamer = Arc::new(Streamer::new(username.to_string(), channel_id, settings, source));
        self.init_streamers(std::slice::from_ref(&streamer)).await;
        self.register(&streamer);
        // A channel that was already in the database (e.g. followed) keeps its old place.
        self.sort_by_store_order();
        self.event(format!("Added {streamer}"));
        Ok(streamer)
    }

    /// Loads the streamers from the database with batched requests: one round for points + live
    /// status, one for drop campaigns of live channels.
    async fn load_streamers(&self, rows: Vec<ChannelRow>) {
        let mut streamers = Vec::with_capacity(rows.len());
        {
            // Reserve the names so a web UI add can't load the same streamer meanwhile.
            let existing: HashSet<String> = self.streamers().iter().map(|s| s.channel_id.clone()).collect();
            let mut adding = self.adding.lock().unwrap();
            for row in rows {
                if existing.contains(&row.id) || !adding.insert(row.login.clone()) {
                    continue;
                }
                let mut settings = match merge_settings(&self.config.streamer_defaults, &row.settings) {
                    Ok(settings) => settings,
                    Err(e) => {
                        warn!("Invalid settings for {} in the database, using the defaults: {e:#}", row.login);
                        self.config.streamer_defaults.clone()
                    }
                };
                settings.prioritize = row.prioritized;
                streamers.push(Arc::new(Streamer::new(row.login, row.id, settings, row.source)));
            }
        }
        self.event(format!("Loading data for {} streamers. Please wait...", streamers.len()));

        self.init_streamers(&streamers).await;
        for streamer in &streamers {
            self.register(streamer);
        }
        let mut adding = self.adding.lock().unwrap();
        for streamer in &streamers {
            adding.remove(&streamer.username);
        }
    }

    /// Loads channel points and live status for new streamers in as few requests as possible.
    async fn init_streamers(&self, streamers: &[Arc<Streamer>]) {
        let logins: Vec<String> = streamers.iter().map(|s| s.username.clone()).collect();
        let overview = self.twitch.channels_overview(&logins).await;

        let mut claims = Vec::new();
        let mut live = Vec::new();
        for (streamer, (points, info)) in streamers.iter().zip(overview) {
            match points {
                Ok(Some(ctx)) => {
                    if let Some(claim_id) = self.apply_points(streamer, ctx) {
                        claims.push((streamer, claim_id));
                    }
                }
                Ok(None) => warn!("Could not load channel points for {}: channel not found", streamer.username),
                Err(e) => warn!("Could not load channel points for {}: {e:#}", streamer.username),
            }
            match info {
                Ok(Some(info)) => live.push((streamer, info)),
                Ok(None) => self.set_offline(streamer),
                Err(e) => warn!("Could not update stream info for {}: {e:#}", streamer.username),
            }
        }

        let drops_channels: Vec<String> = live
            .iter()
            .filter(|(s, info)| wants_campaigns(s, info))
            .map(|(s, _)| s.channel_id.clone())
            .collect();
        let mut campaigns = self.twitch.campaign_ids_for_channels(&drops_channels).await.into_iter();
        for (streamer, info) in live {
            let campaigns_ids = match wants_campaigns(streamer, &info) {
                true => campaigns.next().unwrap_or_default(),
                false => Vec::new(),
            };
            self.apply_stream_info(streamer, info, campaigns_ids);
            self.set_online(streamer);
        }

        for (streamer, claim_id) in claims {
            self.claim_bonus(streamer, &claim_id).await;
        }
        self.refresh_watch_streaks(streamers).await;
    }

    async fn refresh_watch_streaks(&self, streamers: &[Arc<Streamer>]) {
        let ids: Vec<String> = streamers.iter().map(|s| s.channel_id.clone()).collect();
        let streaks = self.twitch.watch_streaks(&ids).await;
        for (streamer, streak) in streamers.iter().zip(streaks) {
            streamer.state().watch_streak = streak;
        }
    }

    /// Starts mining a loaded streamer: PubSub topics and chat.
    fn register(&self, streamer: &Arc<Streamer>) {
        self.streamers.write().unwrap().push(streamer.clone());
        for topic in channel_topics(streamer) {
            self.pubsub.submit(topic);
        }
        if self.prediction_settings().enabled {
            self.pubsub.submit(predictions::channel_topic(&streamer.channel_id));
        }
        self.update_chat(streamer);
    }

    /// Stops mining a channel and removes it from the list. It's also blacklisted, so it doesn't
    /// come back from the follow list; adding it again un-blacklists it.
    pub fn remove_streamer(&self, channel_id: &str) -> Result<()> {
        let streamer = self.unregister(channel_id)?;
        self.forget_badge_channel(channel_id);
        let saved = self
            .store
            .remove_channel(channel_id)
            .and_then(|_| self.store.add_to_blacklist(channel_id, &streamer.username));
        if let Err(e) = saved {
            warn!("Could not update the database: {e:#}");
        }
        self.event(format!("Removed {streamer}"));
        Ok(())
    }

    /// Stops mining a channel: takes it out of the list, PubSub and chat. The database is left alone.
    fn unregister(&self, channel_id: &str) -> Result<Arc<Streamer>> {
        let streamer = {
            let mut list = self.streamers.write().unwrap();
            let pos = list
                .iter()
                .position(|s| s.channel_id == channel_id)
                .ok_or_else(|| anyhow!("channel {channel_id} is not being mined"))?;
            list.remove(pos)
        };
        for topic in channel_topics(&streamer) {
            self.pubsub.remove(&topic);
        }
        self.pubsub.remove(&predictions::channel_topic(channel_id));
        self.chat.leave(&streamer.username);
        Ok(streamer)
    }

    pub fn set_prioritized(&self, channel_id: &str, prioritized: bool) -> Result<Arc<Streamer>> {
        let streamer = self
            .find_by_channel(channel_id)
            .ok_or_else(|| anyhow!("channel {channel_id} is not being mined"))?;
        streamer.state().prioritized = prioritized;
        if let Err(e) = self.store.set_prioritized(channel_id, prioritized) {
            warn!("Could not save the priority of {}: {e:#}", streamer.username);
        }
        self.event(if prioritized {
            format!("Prioritized {streamer}: it gets a watch slot whenever it's live")
        } else {
            format!("{streamer} is no longer prioritized")
        });
        Ok(streamer)
    }

    pub fn priority(&self) -> Vec<Priority> {
        self.priority.read().unwrap().clone()
    }

    /// Replaces the watch priority. It's saved and overrides config.toml from then on.
    pub fn set_priority(&self, rules: Vec<Priority>) -> Result<()> {
        let rules = priority::dedup(rules);
        if rules.is_empty() {
            bail!("pick at least one rule");
        }
        self.store
            .set_meta(PRIORITY_KEY, &serde_json::to_string(&rules)?)
            .context("could not save the watch priority")?;
        let summary = rules.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>().join(" > ");
        *self.priority.write().unwrap() = rules;
        self.event(format!("Watch priority: {summary}"));
        Ok(())
    }

    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub fn fill_slots(&self) -> bool {
        self.fill_slots.load(Ordering::Relaxed)
    }

    /// Turns "fill empty slots" on or off; saved in the database.
    pub fn set_fill_slots(&self, fill: bool) {
        if self.fill_slots.swap(fill, Ordering::Relaxed) == fill {
            return;
        }
        if let Err(e) = self.store.set_meta(FILL_SLOTS_KEY, if fill { "1" } else { "0" }) {
            warn!("Could not save the fill empty slots setting: {e:#}");
        }
        self.event(if fill {
            "Empty watch slots are filled with live channels in list order"
        } else {
            "Empty watch slots are left free"
        });
    }

    pub fn set_paused(&self, paused: bool) {
        if self.paused.swap(paused, Ordering::Relaxed) == paused {
            return;
        }
        if let Err(e) = self.store.set_meta(PAUSED_KEY, if paused { "1" } else { "0" }) {
            warn!("Could not save the paused setting: {e:#}");
        }
        for s in self.streamers() {
            if paused {
                s.state().watching = false;
            }
            self.update_chat(&s);
        }
        self.event(if paused { "Mining paused" } else { "Mining resumed" });
    }

    /// Saves a new list order (channel ids, most important first). Channels not in `ids` keep
    /// their relative order after the given ones.
    pub fn reorder(&self, ids: &[String]) -> Result<()> {
        self.store.reorder(ids)?;
        self.sort_by_store_order();
        Ok(())
    }

    /// Sorts the in-memory list like the database, which the `ORDER` priority relies on.
    fn sort_by_store_order(&self) {
        let rows = match self.store.channels() {
            Ok(rows) => rows,
            Err(e) => return warn!("Could not read the channel order: {e:#}"),
        };
        let index: HashMap<&str, usize> = rows.iter().enumerate().map(|(i, r)| (r.id.as_str(), i)).collect();
        self.streamers
            .write()
            .unwrap()
            .sort_by_key(|s| index.get(s.channel_id.as_str()).copied().unwrap_or(usize::MAX));
    }

    // === Online state === //

    fn set_online(&self, streamer: &Streamer) {
        let changed = {
            let mut st = streamer.state();
            let changed = !st.is_online || !st.checked;
            if !st.is_online {
                st.online_at = now();
                st.is_online = true;
                st.stream.init_watch_streak();
            }
            st.checked = true;
            changed
        };
        if changed {
            self.event(format!("{streamer} is Online!"));
        }
        self.update_chat(streamer);
    }

    fn set_offline(&self, streamer: &Streamer) {
        let changed = {
            let mut st = streamer.state();
            let changed = st.is_online || !st.checked;
            if st.is_online {
                st.offline_at = now();
                st.is_online = false;
            }
            st.watching = false;
            st.checked = true;
            st.stream.invalidate();
            changed
        };
        if changed {
            self.event(format!("{streamer} is Offline!"));
        }
        self.update_chat(streamer);
    }

    fn update_chat(&self, streamer: &Streamer) {
        let online = streamer.state().is_online;
        // A check that finishes after the channel was removed mustn't rejoin its chat.
        let join = !self.paused()
            && self.find_by_channel(&streamer.channel_id).is_some()
            && match streamer.settings.chat {
                ChatPresence::Always => true,
                ChatPresence::Never => false,
                ChatPresence::Online => online,
                ChatPresence::Offline => !online,
            };
        if join {
            self.chat.join(&streamer.username);
        } else {
            self.chat.leave(&streamer.username);
        }
    }

    async fn check_online(&self, streamer: &Streamer) {
        if now() < streamer.state().offline_at + 60.0 {
            return;
        }
        match self.update_stream(streamer).await {
            Ok(true) => self.set_online(streamer),
            Ok(false) => self.set_offline(streamer),
            Err(e) => warn!("Could not update stream info for {}: {e:#}", streamer.username),
        }
    }

    /// Returns whether the streamer is live.
    async fn update_stream(&self, streamer: &Streamer) -> Result<bool> {
        if !streamer.state().stream.update_required() {
            return Ok(true);
        }
        let Some(info) = self.twitch.stream_info(&streamer.username).await? else {
            return Ok(false);
        };
        let campaigns_ids = match wants_campaigns(streamer, &info) {
            true => self.twitch.campaign_ids_for_channel(&streamer.channel_id).await,
            false => Vec::new(),
        };
        self.apply_stream_info(streamer, info, campaigns_ids);
        Ok(true)
    }

    fn apply_stream_info(&self, streamer: &Streamer, info: StreamInfo, campaigns_ids: Vec<String>) {
        let drops_game = info.game.clone().filter(|_| streamer.settings.claim_drops);
        let user_id = &self.twitch.creds.user_id;
        // The same fields TwitchDropsMiner sends, whose drop progress is known to count.
        let properties = json!({
            "channel_id": streamer.channel_id,
            "broadcast_id": info.broadcast_id,
            "channel": streamer.username,
            "game": drops_game.as_ref().map(|g| g.name.clone()).unwrap_or_default(),
            "game_id": drops_game.as_ref().map(|g| g.id.clone()).unwrap_or_default(),
            "hidden": false,
            "is_live": true,
            "live": true,
            "logged_in": true,
            "minutes_logged": 1,
            "muted": false,
            "player": "site",
            "user_id": user_id.parse::<u64>().map(Value::from).unwrap_or_else(|_| user_id.clone().into()),
        });

        let mut st = streamer.state();
        st.stream
            .update(info.broadcast_id, &info.title, info.game, info.viewers);
        // Campaigns for a game the channel no longer plays would keep `has_unclaimed_drops` true.
        st.stream.campaigns.retain(|c| campaigns_ids.contains(&c.id));
        st.stream.campaigns_ids = campaigns_ids;
        st.stream.payload = Some(json!([{ "event": "minute-watched", "properties": properties }]));
    }

    async fn load_points_context(&self, streamer: &Streamer) -> Result<()> {
        let Some(ctx) = self.twitch.channel_points_context(&streamer.username).await? else {
            bail!("streamer {} does not exist", streamer.username);
        };
        if let Some(claim_id) = self.apply_points(streamer, ctx) {
            self.claim_bonus(streamer, &claim_id).await;
        }
        Ok(())
    }

    /// Returns the id of a bonus waiting to be claimed.
    fn apply_points(&self, streamer: &Streamer, ctx: PointsContext) -> Option<String> {
        let mut st = streamer.state();
        st.set_points(ctx.balance);
        st.multipliers = ctx.multipliers;
        ctx.claim_id
    }

    async fn claim_bonus(&self, streamer: &Streamer, claim_id: &str) {
        if self.paused() {
            return;
        }
        self.event(format!("Claiming the bonus for {streamer}!"));
        if let Err(e) = self.twitch.claim_bonus(&streamer.channel_id, claim_id).await {
            warn!("Bonus claim failed for {}: {e:#}", streamer.username);
        }
    }

    // === Minute watched === //

    async fn watch_loop(self: Arc<Self>) {
        // The channel watched alone for a picked badge, see below.
        let mut focus: Option<String> = None;
        loop {
            if self.paused() {
                // Pausing mid-pass can leave flags set below; nothing is watched while paused.
                for s in self.streamers() {
                    s.state().watching = false;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            let streamers = self.streamers();
            let now = now();
            let candidates: Vec<_> = streamers
                .iter()
                .filter(|s| {
                    let st = s.state();
                    // A few seconds after going live, so the stream info (broadcast id) is in.
                    st.is_online && (st.online_at == 0.0 || now - st.online_at > STREAM_SETTLE_SECS)
                })
                .cloned()
                .collect();

            // Online but not refreshed in 10 minutes? Double check it's still live.
            for s in &candidates {
                if s.state().stream.update_elapsed() / 60.0 > 10.0 {
                    self.check_online(s).await;
                }
            }

            let mut rules = self.priority();
            // Fill empty slots: whatever the rules leave free goes to live channels in list order.
            if self.fill_slots() && !rules.contains(&Priority::Order) {
                rules.push(Priority::Order);
            }
            let mut watching = priority::select_watching(&candidates, &rules);
            // Twitch counts drop progress on one stream at a time; switching between two every few
            // seconds keeps restarting it. So while a picked badge can be earned, only its channel
            // is watched (like TwitchDropsMiner), and the second slot waits.
            let badge_channel = watching.iter().find(|s| s.state().badge_drop).cloned();
            let focus_now = badge_channel.as_ref().map(|s| s.channel_id.clone());
            if focus_now != focus {
                match &badge_channel {
                    Some(s) => self.event(format!("Watching only {s} while it can earn a picked badge")),
                    None if focus.is_some() => self.event("Back to watching 2 channels"),
                    None => {}
                }
                focus = focus_now;
            }
            if let Some(s) = badge_channel {
                watching = vec![s];
            }
            for s in &streamers {
                s.state().watching = watching.iter().any(|w| Arc::ptr_eq(w, s));
            }
            if watching.is_empty() {
                // Nothing live: look again soon, so a channel that goes live is picked up quickly.
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            for s in &watching {
                if self.paused() {
                    break;
                }
                let next = Instant::now() + Duration::from_secs_f64(20.0 / watching.len() as f64);
                if let Err(e) = self.watch_once(s).await {
                    // Worth a warning while a badge depends on it.
                    if focus.is_some() {
                        warn!("Watching {} for a badge failed: {e:#}", s.username);
                    } else {
                        debug!("Minute watched failed for {}: {e:#}", s.username);
                    }
                }
                tokio::time::sleep_until(next).await;
            }
        }
    }

    async fn watch_once(&self, streamer: &Streamer) -> Result<()> {
        let payload = streamer
            .state()
            .stream
            .encode_payload()
            .ok_or_else(|| anyhow!("no stream payload yet"))?;
        let spade = self.twitch.spade_url(&streamer.username).await?;
        if !self.twitch.watch(&streamer.username, &spade, &payload).await? {
            // The cached endpoint may have changed; scrape it again next time.
            self.twitch.forget_spade_url();
            return Ok(());
        }

        let lines = {
            let mut st = streamer.state();
            st.session_minutes += st.stream.update_minute_watched();
            let header = format!("{} is streaming {}", streamer.username, st.stream);
            let mut lines = Vec::new();
            for campaign in &mut st.stream.campaigns {
                let campaign_line = format!("Campaign: {campaign}");
                for drop in &mut campaign.drops {
                    if drop.has_preconditions_met != Some(false) && drop.is_printable {
                        drop.is_printable = false;
                        lines.extend([
                            header.clone(),
                            campaign_line.clone(),
                            format!("Drop: {drop}"),
                            drop.progress_bar(),
                        ]);
                    }
                }
            }
            lines
        };
        for line in lines {
            self.event(line);
        }
        Ok(())
    }

    async fn refresh_context_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(30 * 60)).await;
            let streamers = self.streamers();
            for s in &streamers {
                if s.state().is_online {
                    if let Err(e) = self.load_points_context(s).await {
                        warn!("Could not refresh channel points for {}: {e:#}", s.username);
                    }
                }
            }
            // A missed stream resets a streak without any event, so check them all.
            self.refresh_watch_streaks(&streamers).await;
        }
    }

    /// Printed on shutdown, like the original's session report.
    pub fn report(&self) {
        let minutes = ((now() - self.started_at) / 60.0) as i64;
        info!("Session duration: {}h {}m", minutes / 60, minutes % 60);
        for s in self.streamers() {
            let st = s.state();
            let gained = st.channel_points - st.starting_points.unwrap_or(st.channel_points);
            info!(
                "{} ({} points), total gained: {} {}",
                s.username,
                millify(st.channel_points),
                millify(gained),
                st.print_history()
            );
        }
    }
}

fn channel_topics(streamer: &Streamer) -> Vec<String> {
    let id = &streamer.channel_id;
    let settings = &streamer.settings;
    let mut topics = vec![format!("video-playback-by-id.{id}")];
    if settings.follow_raid {
        topics.push(format!("raid.{id}"));
    }
    if settings.claim_moments {
        topics.push(format!("community-moments-channel-v1.{id}"));
    }
    topics
}

fn normalize_login(name: &str) -> Result<String> {
    let name = name.trim().trim_start_matches('@').to_lowercase();
    if !valid_login(&name) {
        bail!("'{name}' is not a valid Twitch username");
    }
    Ok(name)
}

/// Whether to look up drop campaigns for a live stream.
fn wants_campaigns(streamer: &Streamer, info: &StreamInfo) -> bool {
    streamer.settings.claim_drops && info.game.is_some()
}
