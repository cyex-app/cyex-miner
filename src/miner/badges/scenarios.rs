//! Badge scenarios on a real `Miner` with mocked Twitch data: an in-memory database, made-up
//! channels, and inventory, owned-badge and badge-page responses as Twitch and streamdatabase
//! send them. Nothing goes over the network: the chat task the miner starts sits on a runtime
//! that's never run.

use std::sync::Arc;

use serde_json::{json, Value};

use super::*;
use crate::{
    auth::Credentials,
    config::Config,
    entities::{
        stream::Game,
        streamer::{StreamerSettings, Source},
    },
    twitch::Twitch,
};

const GTA: &str = "32982";
const LOL: &str = "21779";

struct Harness {
    miner: Miner,
    _runtime: tokio::runtime::Runtime,
}

impl Harness {
    fn new() -> Self {
        Self::with_store(Store::open_in_memory().unwrap())
    }

    fn with_store(store: Store) -> Self {
        // `ChatManager::new` spawns its connection; on a runtime nobody runs, it never starts.
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _guard = runtime.enter();
        let creds = Credentials {
            auth_token: "test".into(),
            user_id: "1".into(),
            login: "tester".into(),
            device_id: "d".into(),
        };
        let twitch = Twitch::new(reqwest::Client::new(), creds);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let miner = Miner::new(twitch, Config::default(), store, tx);
        drop(_guard);
        Self { miner, _runtime: runtime }
    }

    /// A live channel streaming `game`, whose watch streak is already earned.
    fn live(&self, login: &str, id: &str, game: &str) -> Arc<Streamer> {
        let s = Arc::new(Streamer::new(login.into(), id.into(), StreamerSettings::default(), Source::Manual));
        {
            let mut st = s.state();
            st.is_online = true;
            st.checked = true;
            st.stream.watch_streak_missing = false;
            st.stream.game = Some(Game { id: game.into(), name: game.into(), display_name: game.into() });
        }
        self.miner.streamers.write().unwrap().push(s.clone());
        s
    }

    /// Loads these event badges as a badge list refresh would, and picks the new ones as auto
    /// select does (`pick_all_badges`: open badges not picked yet; picking one again would retry it).
    fn badges(&self, campaigns: &[Campaign]) {
        *self.miner.badges.event_campaigns.write().unwrap() = campaigns.to_vec();
        self.miner.set_badge_campaigns(&self.miner.event_campaigns());
        self.miner.pick_all_badges();
        self.miner.update_badge_targets();
    }

    fn view(&self, campaign_id: &str) -> Option<BadgeView> {
        self.miner.badges().into_iter().find(|b| b.campaign_id == campaign_id)
    }

    /// Minutes watched on `s`, a minute at a time, as the watch loop counts them.
    fn watch(&self, s: &Streamer, minutes: usize) {
        for _ in 0..minutes {
            self.miner.count_reward_minutes(s, 1.0);
        }
    }

    /// The miner restarting: badge state from the database again, the same lists loaded.
    fn restart(self, campaigns: &[Campaign]) -> Self {
        let store = self.miner.store;
        let h = Self::with_store(store);
        h.badges(campaigns);
        h
    }
}

/// An event badge as catquery and its streamdatabase page describe it.
fn event(id: &str, name: &str, minutes: i64, game: Option<&str>, channels: Value) -> Campaign {
    let badge = json!({
        "id": id, "name": name, "imageUrl": format!("https://static-cdn.jtvnw.net/badges/v1/{id}/3"),
        "startAt": "2020-01-01T00:00:00.000Z", "endAt": "2099-01-01T00:00:00.000Z", "free": true
    });
    let categories = match game {
        Some(g) => json!([{ "game": { "id": g, "name": "Game" }, "slug": null }]),
        None => json!([]),
    };
    let page = json!({ "props": { "pageProps": { "availabilities": [{
        "objectives": [[{ "type": "twitch_watch", "minutes": minutes, "days": null }]],
        "categories": categories, "channels": channels
    }] } } });
    Campaign::from_event_badge(&badge, &page).unwrap()
}

/// A drops inventory with one drop in progress giving `reward`.
fn inventory(reward: &str, watched: i64, required: i64, claimed: bool) -> Value {
    json!({
        "dropCampaignsInProgress": [{ "id": "gta-drops", "timeBasedDrops": [{
            "id": "drop-1", "requiredMinutesWatched": required,
            "self": { "currentMinutesWatched": watched, "isClaimed": claimed },
            "benefitEdges": [{ "benefit": { "id": "benefit-1", "name": reward } }]
        }] }],
        "gameEventDrops": []
    })
}

fn earned_names(h: &Harness) -> Vec<String> {
    h.miner.earned_badges().into_iter().map(|e| e.name).collect()
}

#[test]
fn purple_chain_follows_twitchs_count_not_the_miners() {
    // What went wrong: the miner counted 60 minutes on psp1g, Twitch had 22.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    let purple = event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null);
    h.badges(&[purple]);
    assert!(psp1g.state().badge_drop);

    h.miner.update_badge_progress(&inventory("Purple Chain", 22, 60, false));
    h.watch(&psp1g, 200);
    let v = h.view("event:purple-chain").unwrap();
    assert_eq!((v.minutes_watched, v.minutes_required), (22, 60), "Twitch's minutes, not the miner's 200");
    assert!(!v.earned && !v.unconfirmed, "neither earned nor given up while Twitch reports progress");
    assert!(earned_names(&h).is_empty());
    assert!(psp1g.state().badge_drop, "still watched for");

    // Twitch says 60/60 but not claimed yet: still not earned.
    h.miner.update_badge_progress(&inventory("Purple Chain", 60, 60, false));
    assert!(earned_names(&h).is_empty());

    // Claimed: earned, on psp1g.
    psp1g.state().watching = true;
    h.miner.update_badge_progress(&inventory("Purple Chain", 60, 60, true));
    let earned = h.miner.earned_badges();
    assert_eq!(earned.len(), 1);
    assert_eq!((earned[0].name.as_str(), earned[0].channel.as_deref()), ("Purple Chain", Some("psp1g")));
    assert!(h.view("event:purple-chain").is_none(), "earned badges leave the list");
    h.miner.update_badge_targets();
    assert!(!psp1g.state().badge_drop, "and aren't watched for anymore");
}

#[test]
fn twitchs_goal_wins_over_the_pages() {
    let h = Harness::new();
    h.live("psp1g", "100", GTA);
    h.badges(&[event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null)]);
    // The page says 60; Twitch's drop needs 120.
    h.miner.update_badge_progress(&inventory("purple chain", 30, 120, false));
    let v = h.view("event:purple-chain").unwrap();
    assert_eq!((v.minutes_watched, v.minutes_required), (30, 120));
    // A list refresh keeps Twitch's goal.
    h.badges(&[event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null)]);
    assert_eq!(h.view("event:purple-chain").unwrap().minutes_required, 120);
}

#[test]
fn a_badge_twitch_says_nothing_about_is_given_up_on_not_earned() {
    let h = Harness::new();
    let faker = h.live("faker", "200", LOL);
    let kda = event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null);
    h.badges(std::slice::from_ref(&kda));
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));

    // Past the goal (where the old code called it earned), and up to just before twice it plus 5.
    h.watch(&faker, 64);
    let v = h.view("event:kda-fan").unwrap();
    assert_eq!(v.minutes_watched, 30, "shown capped at the goal");
    assert!(!v.earned && !v.unconfirmed);
    assert!(faker.state().badge_drop);
    assert!(earned_names(&h).is_empty(), "the miner's own count never earns a badge");

    h.watch(&faker, 1);
    let v = h.view("event:kda-fan").unwrap();
    assert!(v.unconfirmed && !v.earned, "given up on at 65 minutes, still listed");
    assert!(!faker.state().badge_drop, "and not watched for");
    assert!(earned_names(&h).is_empty());

    // A restart keeps it given up on, not earned.
    let h = h.restart(std::slice::from_ref(&kda));
    let v = h.view("event:kda-fan").unwrap();
    assert!(v.unconfirmed && !v.earned);

    // Picking it again starts it over.
    h.miner.set_badge_picked("event:kda-fan", true).unwrap();
    h.miner.update_badge_targets();
    let v = h.view("event:kda-fan").unwrap();
    assert!(!v.unconfirmed && v.minutes_watched == 0);
    // And it's watched for again.
    let faker = h.live("faker", "200", LOL);
    h.miner.update_badge_targets();
    assert!(faker.state().badge_drop);
}

#[test]
fn twitch_confirming_a_badge_earns_it_by_name() {
    let h = Harness::new();
    let faker = h.live("faker", "200", LOL);
    h.badges(&[event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null)]);
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));
    h.watch(&faker, 40);
    faker.state().watching = true;
    // The inventory lists it among the badges given out.
    h.miner.update_badge_progress(&json!({
        "dropCampaignsInProgress": [], "gameEventDrops": [{ "id": "x", "name": "K/DA Fan" }]
    }));
    assert_eq!(earned_names(&h), ["K/DA Fan"]);
    assert_eq!(h.miner.earned_badges()[0].channel.as_deref(), Some("faker"));
}

#[test]
fn owned_badges_are_never_listed_or_watched_for() {
    let h = Harness::new();
    let faker = h.live("faker", "200", LOL);
    *h.miner.badges.owned.write().unwrap() = ["kda-fan".to_string()].into();
    h.badges(&[event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null)]);
    assert!(h.view("event:kda-fan").is_none());
    assert!(!faker.state().badge_drop);
    // Owned already at the start, so it isn't "earned this session".
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));
    assert!(earned_names(&h).is_empty());
}

#[test]
fn a_badge_marked_done_by_the_old_version_comes_back() {
    // The old version saved Purple Chain as done after counting 65 minutes itself.
    let store = Store::open_in_memory().unwrap();
    store.set_meta(REWARD_DONE_KEY, r#"["event:purple-chain"]"#).unwrap();
    let h = Harness::with_store(store);
    let psp1g = h.live("psp1g", "100", GTA);
    let purple = event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null);
    h.badges(&[purple]);
    let v = h.view("event:purple-chain").unwrap();
    assert!(v.unconfirmed && !v.earned, "given up on, not earned");
    assert!(!psp1g.state().badge_drop);

    // Twitch shows it half done: back to watching, with Twitch's minutes. Auto select picks
    // it again within the minute (as `run_auto_badges` does every pass).
    h.miner.update_badge_progress(&inventory("Purple Chain", 22, 60, false));
    h.miner.pick_all_badges();
    h.miner.update_badge_targets();
    let v = h.view("event:purple-chain").unwrap();
    assert!(!v.unconfirmed && !v.earned);
    assert_eq!(v.minutes_watched, 22);
    assert!(v.picked);
    assert!(psp1g.state().badge_drop);

    // A restart before the next inventory read doesn't give up on it again.
    let purple = event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null);
    let h = h.restart(&[purple]);
    assert!(!h.view("event:purple-chain").unwrap().unconfirmed);
}

#[test]
fn a_reward_named_differently_falls_back_to_the_miners_count() {
    // Twitch's drop reward named unlike the badge: no match, so no Twitch progress.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    h.badges(&[event("purple-chain", "Purple Chain", 60, Some(GTA), Value::Null)]);
    h.miner.update_badge_progress(&inventory("Purple Chain Badge", 22, 60, false));
    h.watch(&psp1g, 30);
    assert_eq!(h.view("event:purple-chain").unwrap().minutes_watched, 30, "the miner's own count");
    h.watch(&psp1g, 95);
    assert!(h.view("event:purple-chain").unwrap().unconfirmed, "given up on at 125, not earned");
    assert!(earned_names(&h).is_empty());
}

#[test]
fn minutes_count_only_where_the_badge_can_be_earned() {
    let h = Harness::new();
    let samham = h.live("samham", "54051059", "509672");
    let other = h.live("other", "300", "509672");
    let lol = h.live("lol", "400", LOL);
    let jordan = event(
        "jordan-x-space-jam",
        "Jordan x Space Jam",
        15,
        Some("509672"),
        json!([{ "user": { "id": "54051059", "login": "samham" } }]),
    );
    h.badges(&[jordan]);
    assert!(samham.state().badge_drop);
    assert!(!other.state().badge_drop, "same category, not a named channel");
    assert!(!lol.state().badge_drop);
    h.watch(&other, 10);
    h.watch(&lol, 10);
    assert_eq!(h.view("event:jordan-x-space-jam").unwrap().minutes_watched, 0);
    h.watch(&samham, 10);
    assert_eq!(h.view("event:jordan-x-space-jam").unwrap().minutes_watched, 10);

    // samham switches category, still named: a named channel counts on its own game here, as
    // the page's category is IRL, a category counted on any game (like Special Events).
    samham.state().stream.game = Some(Game { id: LOL.into(), ..Default::default() });
    h.miner.update_badge_targets();
    assert!(samham.state().badge_drop);
}

#[test]
fn two_badges_on_one_channel_both_count() {
    let h = Harness::new();
    let faker = h.live("faker", "200", LOL);
    h.badges(&[
        event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null),
        event("worlds", "Worlds", 60, None, Value::Null),
    ]);
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));
    h.watch(&faker, 20);
    assert_eq!(h.view("event:kda-fan").unwrap().minutes_watched, 20);
    assert_eq!(h.view("event:worlds").unwrap().minutes_watched, 20, "no game: any channel");
}

#[test]
fn offline_or_unpicked_channels_count_nothing() {
    let h = Harness::new();
    let faker = h.live("faker", "200", LOL);
    h.badges(&[event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null)]);
    h.miner.set_badge_picked("event:kda-fan", false).unwrap();
    h.watch(&faker, 10);
    assert_eq!(h.view("event:kda-fan").unwrap().minutes_watched, 0, "unpicked");
    h.miner.set_badge_picked("event:kda-fan", true).unwrap();
    faker.state().is_online = false;
    h.miner.update_badge_targets();
    h.watch(&faker, 10);
    assert_eq!(h.view("event:kda-fan").unwrap().minutes_watched, 0, "offline");
}

#[test]
fn channels_added_for_a_badge_go_when_it_does() {
    let h = Harness::new();
    let kda = event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null);
    let toying = event("toying", "Toying Around", 15, Some("27471"), Value::Null);
    h.badges(&[kda.clone(), toying]);
    // Channels the quick add put in for each badge.
    for (login, id, game, campaign) in [("lol1", "1001", LOL, "event:kda-fan"), ("mc1", "1002", "27471", "event:toying")] {
        let s = Arc::new(Streamer::new(login.into(), id.into(), StreamerSettings::default(), Source::Badge));
        {
            let mut st = s.state();
            st.is_online = true;
            st.checked = true;
            st.stream.game = Some(Game { id: game.into(), ..Default::default() });
        }
        h.miner.streamers.write().unwrap().push(s);
        let entry = AddedChannel { campaign_id: campaign.into(), ends_at: f64::MAX };
        h.miner.badges.added.lock().unwrap().insert(id.into(), entry);
    }
    h.miner.prune_badge_channels();
    assert_eq!(h.miner.streamers().len(), 2, "both badges listed: both stay");

    // Toying Around isn't listed anymore (ended early, or not a badge after all).
    h.badges(&[kda]);
    h.miner.prune_badge_channels();
    let left: Vec<String> = h.miner.streamers().iter().map(|s| s.username.clone()).collect();
    assert_eq!(left, ["lol1"]);

    // An empty list (a refresh that found nothing) doesn't remove everything.
    h.badges(&[]);
    h.miner.prune_badge_channels();
    assert_eq!(h.miner.streamers().len(), 1);
}

/// An event badge that asks for `minutes` on `days` days, like GTA VI's chains.
fn daily(id: &str, name: &str, minutes: i64, days: i64) -> Campaign {
    let badge = json!({
        "id": id, "name": name, "startAt": "2020-01-01T00:00:00.000Z", "endAt": "2099-01-01T00:00:00.000Z", "free": true
    });
    let page = json!({ "props": { "pageProps": { "availabilities": [{
        "objectives": [[{ "type": "twitch_watch", "minutes": minutes, "days": days }]],
        "categories": [{ "game": { "id": GTA, "name": "Grand Theft Auto VI" }, "slug": null }], "channels": null
    }] } } });
    Campaign::from_event_badge(&badge, &page).unwrap()
}

/// The GTA VI drops as the inventory lists them: (reward, watched, claimed, unlocked).
fn chain_inventory(drops: &[(&str, i64, bool, bool)]) -> Value {
    let drops: Vec<Value> = drops
        .iter()
        .enumerate()
        .map(|(i, (reward, watched, claimed, unlocked))| {
            json!({
                "id": format!("drop-{i}"), "requiredMinutesWatched": 60,
                "self": { "currentMinutesWatched": watched, "isClaimed": claimed, "hasPreconditionsMet": unlocked },
                "benefitEdges": [{ "benefit": { "id": format!("b-{i}"), "name": reward } }]
            })
        })
        .collect();
    json!({ "dropCampaignsInProgress": [{ "id": "gta-drops", "timeBasedDrops": drops }], "gameEventDrops": [] })
}

/// Moves a badge's last finished day back by `hours`, as if that much time went by.
fn age_day(h: &Harness, campaign_id: &str, hours: f64) {
    for b in h.miner.badges.badges.write().unwrap().iter_mut().filter(|b| b.campaign_id == campaign_id) {
        b.days.last_done_at -= hours * 3600.0;
    }
}

#[test]
fn bronze_chain_is_listed_and_follows_twitch() {
    // What went wrong: Bronze Chain (60 minutes on 2 days) wasn't listed at all.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    h.badges(&[daily("purple-chain", "Purple Chain", 60, 1), daily("bronze-chain", "Bronze Chain", 60, 2)]);
    // A series: Bronze waits behind Purple, picked already.
    assert!(h.view("event:bronze-chain").is_none());

    // As in the screenshot: Purple claimed (before the miner started, so no 24 hours to wait),
    // Bronze at 54 of 60.
    psp1g.state().watching = true;
    h.miner.update_badge_progress(&chain_inventory(&[("Purple Chain", 60, true, true), ("Bronze Chain", 54, false, true)]));
    h.miner.update_badge_targets();
    let bronze = h.view("event:bronze-chain").expect("Bronze Chain listed once Purple is done");
    assert_eq!((bronze.minutes_required, bronze.days_required), (60, 2));
    assert!(bronze.picked);
    assert_eq!(bronze.minutes_watched, 54);
    assert!(!bronze.locked && !bronze.done_today);
    assert!(psp1g.state().badge_drop, "watched for its last 6 minutes");
    // Purple was claimed before the miner first saw it: done, not "earned this session".
    assert!(h.view("event:purple-chain").is_none());
    assert!(earned_names(&h).is_empty());
    // The miner's own count doesn't touch a badge Twitch tracks.
    h.watch(&psp1g, 120);
    assert_eq!(h.view("event:bronze-chain").unwrap().minutes_watched, 54);

    h.miner.update_badge_progress(&chain_inventory(&[("Purple Chain", 60, true, true), ("Bronze Chain", 60, true, true)]));
    assert_eq!(earned_names(&h), ["Bronze Chain"]);
}

#[test]
fn a_chain_drop_twitch_hasnt_unlocked_waits() {
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    h.badges(&[daily("bronze-chain", "Bronze Chain", 60, 2), daily("silver-chain", "Silver Chain", 60, 3)]);
    // Bronze in progress; Silver waits behind it, and for its day (hasPreconditionsMet false).
    h.miner.update_badge_progress(&chain_inventory(&[("Bronze Chain", 10, false, true), ("Silver Chain", 0, false, false)]));
    h.miner.update_badge_targets();
    assert!(h.view("event:silver-chain").is_none(), "behind Bronze in the series");
    assert!(psp1g.state().badge_drop, "Bronze still needs it");

    // Bronze done (no time given): Silver shows, but Twitch has it locked, so nothing is watched.
    h.miner.update_badge_progress(&chain_inventory(&[("Bronze Chain", 60, true, true), ("Silver Chain", 0, false, false)]));
    h.miner.update_badge_targets();
    assert!(h.view("event:silver-chain").unwrap().locked);
    assert!(!psp1g.state().badge_drop, "no watching toward a locked drop");

    // Unlocked on Twitch, but Bronze was given just now: the daily reset is 24 hours after it.
    h.miner.update_badge_progress(&chain_inventory(&[("Bronze Chain", 60, true, true), ("Silver Chain", 0, false, true)]));
    h.miner.update_badge_targets();
    assert!(h.view("event:silver-chain").unwrap().done_today);
    assert!(!psp1g.state().badge_drop);

    // A day later (Twitch giving Bronze's time now): watched for again.
    let mut later = chain_inventory(&[("Bronze Chain", 60, true, true), ("Silver Chain", 0, false, true)]);
    let given = chrono::DateTime::from_timestamp((now() - 24.2 * 3600.0) as i64, 0).unwrap().to_rfc3339();
    later["gameEventDrops"] = json!([{ "id": "b", "name": "Bronze Chain", "lastAwardedAt": given }]);
    h.miner.update_badge_progress(&later);
    h.miner.update_badge_targets();
    assert!(!h.view("event:silver-chain").unwrap().locked);
    assert!(psp1g.state().badge_drop);
}

#[test]
fn a_badge_on_several_days_twitch_says_nothing_about_waits_24_hours_a_day() {
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    let bronze = daily("bronze-chain", "Bronze Chain", 60, 2);
    h.badges(std::slice::from_ref(&bronze));
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));

    // Day 1: 64 minutes, not in yet; 65 (goal plus 5): in.
    h.watch(&psp1g, 64);
    let v = h.view("event:bronze-chain").unwrap();
    assert_eq!((v.minutes_watched, v.days_done), (60, 0));
    assert!(psp1g.state().badge_drop);
    h.watch(&psp1g, 1);
    let v = h.view("event:bronze-chain").unwrap();
    assert_eq!(v.days_done, 1);
    assert!(v.done_today && !v.earned && !v.unconfirmed);
    let wait = v.next_day_at - now();
    assert!((24.0 * 3600.0..24.0 * 3600.0 + 6.0 * 60.0).contains(&wait), "24 hours after it was in, plus 5 minutes: {wait}");
    assert!(!psp1g.state().badge_drop, "waits for the next day");
    h.watch(&psp1g, 30);
    assert_eq!(h.view("event:bronze-chain").unwrap().days_done, 1, "nothing counted while waiting");

    // 23 hours later: still waiting. A restart keeps the day.
    age_day(&h, "event:bronze-chain", 23.0);
    h.miner.update_badge_targets();
    assert!(!psp1g.state().badge_drop);
    let h = h.restart(std::slice::from_ref(&bronze));
    let v = h.view("event:bronze-chain").unwrap();
    assert!(v.done_today && v.days_done == 1, "the day count survives a restart");
    // The campaigns loop reads the inventory every minute.
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));

    // 24 hours and 5 minutes after: day 2 opens, from 0. (The saved day has the real time,
    // which the 23 hours above didn't move.)
    let psp1g = h.live("psp1g", "100", GTA);
    age_day(&h, "event:bronze-chain", 24.1);
    h.miner.update_badge_targets();
    assert!(psp1g.state().badge_drop);
    let v = h.view("event:bronze-chain").unwrap();
    assert_eq!((v.minutes_watched, v.days_done), (0, 1));
    assert!(!v.done_today);
    h.watch(&psp1g, 65);
    let v = h.view("event:bronze-chain").unwrap();
    assert_eq!(v.days_done, 2);
    assert!(!v.earned, "only Twitch's word earns it");

    // A spare day past the two (for a day Twitch didn't count), then given up on.
    age_day(&h, "event:bronze-chain", 25.0);
    h.miner.update_badge_targets();
    h.watch(&psp1g, 65);
    let v = h.view("event:bronze-chain").unwrap();
    assert!(v.days_done == 3 && !v.unconfirmed, "one spare day");
    age_day(&h, "event:bronze-chain", 25.0);
    h.miner.update_badge_targets();
    h.watch(&psp1g, 65);
    let v = h.view("event:bronze-chain").unwrap();
    assert!(v.unconfirmed && !v.earned);
    assert!(earned_names(&h).is_empty());

    // Twitch confirming it any time earns it.
    h.miner.update_badge_progress(&json!({
        "dropCampaignsInProgress": [], "gameEventDrops": [{ "id": "x", "name": "Bronze Chain" }]
    }));
    assert_eq!(earned_names(&h), ["Bronze Chain"]);
}

/// GTA VI's chains as both badge lists have them: the same game and window, 1 to 6 days.
fn chains() -> Vec<Campaign> {
    [("purple-chain", "Purple Chain", 1), ("bronze-chain", "Bronze Chain", 2), ("silver-chain", "Silver Chain", 3),
     ("gold-chain", "Gold Chain", 4), ("diamond-chain", "Diamond Chain", 5), ("miami-chain", "Miami Chain", 6)]
    .iter()
    .map(|&(id, name, days)| daily(id, name, 60, days))
    .collect()
}

/// The inventory with these badges given (name, hours ago) and every chain drop at `minutes`.
fn chains_inventory(given: &[(&str, f64)], minutes: i64) -> Value {
    let at = |hours: f64| {
        chrono::DateTime::from_timestamp((now() - hours * 3600.0) as i64, 0).unwrap().to_rfc3339()
    };
    let names = ["Purple Chain", "Bronze Chain", "Silver Chain", "Gold Chain", "Diamond Chain", "Miami Chain"];
    let drops: Vec<Value> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let claimed = given.iter().any(|(n, _)| n == name);
            json!({
                "id": format!("drop-{i}"), "requiredMinutesWatched": 60,
                "self": { "currentMinutesWatched": if claimed { 60 } else { minutes }, "isClaimed": claimed },
                "benefitEdges": [{ "benefit": { "id": format!("b-{i}"), "name": name } }]
            })
        })
        .collect();
    let events: Vec<Value> = given
        .iter()
        .map(|(name, hours)| json!({ "id": name, "name": name, "lastAwardedAt": at(*hours) }))
        .collect();
    json!({ "dropCampaignsInProgress": [{ "id": "gta-drops", "timeBasedDrops": drops }], "gameEventDrops": events })
}

fn shown(h: &Harness) -> Vec<String> {
    h.miner.badges().into_iter().map(|b| b.name).collect()
}

#[test]
fn a_series_shows_only_its_next_badge_after_the_daily_reset() {
    // As in the screenshot: Purple and Bronze given (Bronze 3 hours ago), the rest at 35/60.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    h.badges(&chains());
    assert_eq!(shown(&h), ["Purple Chain"], "only the first one before anything's known");

    h.miner.update_badge_progress(&chains_inventory(&[("Purple Chain", 27.0), ("Bronze Chain", 3.0)], 35));
    h.miner.update_badge_targets();
    assert_eq!(shown(&h), ["Silver Chain"], "the next one only, not Gold and Diamond too");
    let silver = h.view("event:silver-chain").unwrap();
    assert!(silver.done_today, "today's is in: Bronze was given 3 hours ago");
    let wait = silver.next_day_at - now();
    assert!((20.9 * 3600.0..21.2 * 3600.0).contains(&wait), "24 hours after Bronze, plus 5 minutes: {wait}");
    assert!(!psp1g.state().badge_drop, "nothing watched until then");

    // 24 hours after Bronze: Silver is watched for.
    h.miner.update_badge_progress(&chains_inventory(&[("Purple Chain", 51.0), ("Bronze Chain", 24.2)], 35));
    h.miner.update_badge_targets();
    assert!(!h.view("event:silver-chain").unwrap().done_today);
    assert!(psp1g.state().badge_drop);

    // Silver given: Gold next, waiting its own 24 hours.
    psp1g.state().watching = true;
    h.miner.update_badge_progress(&chains_inventory(&[("Purple Chain", 51.0), ("Bronze Chain", 24.2), ("Silver Chain", 0.0)], 0));
    h.miner.update_badge_targets();
    assert_eq!(shown(&h), ["Gold Chain"]);
    assert!(h.view("event:gold-chain").unwrap().done_today);
    assert!(!psp1g.state().badge_drop);
    assert_eq!(earned_names(&h), ["Silver Chain"]);
}

#[test]
fn a_series_earned_before_the_miner_started_doesnt_wait_a_day() {
    // Bronze owned with no time known (Twitch's badge list, not the inventory): Silver is
    // watched for right away rather than losing a day.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    *h.miner.badges.owned.write().unwrap() = ["purple-chain".to_string(), "bronze-chain".to_string()].into();
    h.badges(&chains());
    h.miner.update_badge_progress(&json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] }));
    h.miner.update_badge_targets();
    assert_eq!(shown(&h), ["Silver Chain"]);
    assert!(!h.view("event:silver-chain").unwrap().done_today);
    assert!(psp1g.state().badge_drop);
}

#[test]
fn badges_sharing_a_game_and_window_arent_a_series_by_that_alone() {
    // Two one-day badges in League of Legends with the same window: both shown and watched for.
    let h = Harness::new();
    h.live("faker", "200", LOL);
    h.badges(&[
        event("kda-fan", "K/DA Fan", 30, Some(LOL), Value::Null),
        event("worlds", "Worlds", 60, Some(LOL), Value::Null),
    ]);
    let mut names = shown(&h);
    names.sort();
    assert_eq!(names, ["K/DA Fan", "Worlds"]);
}

#[test]
fn the_next_chain_waits_for_the_daily_reset_across_a_restart() {
    // What went wrong: Twitch's inventory doesn't list the chains, only its badge list says
    // Purple is owned, with no time. The miner saw it given at noon; a redeploy forgot that.
    let h = Harness::new();
    let psp1g = h.live("psp1g", "100", GTA);
    let empty = json!({ "dropCampaignsInProgress": [], "gameEventDrops": [] });
    h.badges(&chains());
    h.miner.update_badge_progress(&empty);
    *h.miner.badges.owned.write().unwrap() = ["purple-chain".to_string()].into();
    h.miner.update_badge_progress(&empty);
    h.miner.update_badge_targets();
    assert_eq!(shown(&h), ["Bronze Chain"]);
    let bronze = h.view("event:bronze-chain").unwrap();
    assert_eq!((bronze.days_done, bronze.days_required), (1, 2), "Purple's day counts toward Bronze");
    assert!(bronze.done_today && !psp1g.state().badge_drop);

    let h = h.restart(&chains());
    *h.miner.badges.owned.write().unwrap() = ["purple-chain".to_string()].into();
    let psp1g = h.live("psp1g", "100", GTA);
    h.miner.update_badge_progress(&empty);
    h.miner.update_badge_targets();
    let bronze = h.view("event:bronze-chain").unwrap();
    assert!(bronze.done_today, "the restart remembers when Purple was given");
    let wait = bronze.next_day_at - now();
    assert!((23.9 * 3600.0..24.2 * 3600.0).contains(&wait), "{wait}");
    assert!(!psp1g.state().badge_drop);

    // 24 hours later: one more day of 65 minutes and Bronze's days are in; Twitch owning it earns it.
    for b in h.miner.badges.badges.write().unwrap().iter_mut() {
        b.awarded_at -= 24.2 * 3600.0;
        b.days.last_done_at -= 24.2 * 3600.0;
    }
    h.miner.update_badge_progress(&empty);
    h.miner.update_badge_targets();
    assert!(psp1g.state().badge_drop);
    h.watch(&psp1g, 65);
    assert_eq!(h.view("event:bronze-chain").unwrap().days_done, 2);
    *h.miner.badges.owned.write().unwrap() = ["purple-chain".to_string(), "bronze-chain".to_string()].into();
    h.miner.update_badge_progress(&empty);
    assert_eq!(shown(&h), ["Silver Chain"]);
    assert_eq!(h.view("event:silver-chain").unwrap().days_done, 2);
}
