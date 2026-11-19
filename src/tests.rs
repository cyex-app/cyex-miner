use std::{collections::HashSet, sync::Arc};

use serde_json::{json, Value};

use crate::{
    config::{Config, EXAMPLE_CONFIG},
    entities::{
        drops::{game_slug, next_data, Campaign},
        streamer::{ChatPresence, Source, Streamer, StreamerSettings, STREAK_GIVE_UP_MINUTES},
    },
    miner::{inventory_progress, merge_badge_lists, TwitchProgress, parse_event_badges, parse_streamdatabase_list, pick_outcome, Outcome, PredictionSettings, Strategy},
    priority::{plan_watching, select_watching, Priority},
    pubsub::PubSubMessage,
    store::Store,
    twitch::{
        merge_campaign_ids, parse_active_campaigns, parse_channel_card, parse_channel_id, parse_directory,
        parse_search, parse_stream_info, parse_watch_streak,
    },
};

#[test]
fn example_config_parses() {
    let config: Config = toml::from_str(EXAMPLE_CONFIG).unwrap();
    assert_eq!(config.web.port, 5000);
    assert!(config.streamers.is_empty());
    assert_eq!(config.priority, Config::default().priority);
}

/// A live channel whose watch streak is already earned.
fn live(name: &str) -> Arc<Streamer> {
    let s = Arc::new(Streamer::new(name.into(), name.into(), StreamerSettings::default(), Source::Manual));
    {
        let mut st = s.state();
        st.is_online = true;
        st.stream.watch_streak_missing = false;
    }
    s
}

fn names(watching: &[Arc<Streamer>]) -> Vec<&str> {
    watching.iter().map(|s| s.username.as_str()).collect()
}

#[test]
fn watches_the_top_of_the_list_once_streaks_are_earned() {
    let list = [live("a"), live("b"), live("c"), live("d")];
    // Drops exist for d's game, but none are left to earn.
    list[3].state().stream.campaigns_ids = vec!["campaign".into()];
    list[2].state().stream.watch_streak_missing = true;
    let rules = [Priority::Streak, Priority::Drops, Priority::Order];
    assert_eq!(names(&select_watching(&list, &rules)), ["c", "a"]);

    list[2].state().stream.minute_watched = 7.0;
    assert_eq!(names(&select_watching(&list, &rules)), ["a", "b"]);
}

#[test]
fn rules_apply_in_order() {
    let list = [live("a"), live("b"), live("c")];
    list[2].state().prioritized = true;
    list[0].state().watch_streak = Some(3);
    list[1].state().watch_streak = Some(12);
    assert_eq!(names(&select_watching(&list, &[Priority::Prioritized, Priority::LongestStreak])), ["c", "b"]);
    assert_eq!(names(&select_watching(&list, &[Priority::LongestStreak])), ["b", "a"]);

    for (s, points) in list.iter().zip([500, 100, 900]) {
        s.state().channel_points = points;
    }
    assert_eq!(names(&select_watching(&list, &[Priority::PointsAscending])), ["b", "a"]);
    assert_eq!(names(&select_watching(&list, &[Priority::PointsDescending])), ["c", "a"]);
}

#[test]
fn parses_watch_streak() {
    let resp = |value: Value| {
        json!({ "data": { "channel": { "self": { "watchStreakMilestone": { "watchStreakMilestone": { "value": value } } } } } })
    };
    assert_eq!(parse_watch_streak(&resp(json!("7"))), Some(7));
    assert_eq!(parse_watch_streak(&resp(json!(3))), Some(3));
    assert_eq!(parse_watch_streak(&json!({ "data": { "channel": { "self": null } } })), None);
}

#[test]
fn badge_rule_comes_first_when_listed() {
    let list = [live("a"), live("b"), live("c")];
    list[0].state().prioritized = true;
    list[2].state().badge_drop = true;
    let rules = [Priority::Badges, Priority::Prioritized, Priority::Order];
    assert_eq!(names(&select_watching(&list, &rules)), ["c", "a"]);
}

fn campaign_json(drop: Value, allow: Value) -> Value {
    json!({
        "id": "camp", "name": "Launch", "startAt": "2020-01-01T00:00:00Z", "endAt": "2099-01-01T00:00:00Z",
        "game": { "id": "g1", "displayName": "Game", "slug": "game" },
        "allow": allow,
        "timeBasedDrops": [drop],
    })
}

fn drop_json(minutes: i64, subs: i64, kind: &str) -> Value {
    json!({
        "id": "drop", "name": "Watch", "requiredMinutesWatched": minutes, "requiredSubs": subs,
        "startAt": "2020-01-01T00:00:00Z", "endAt": "2099-01-01T00:00:00Z",
        "benefitEdges": [{ "benefit": { "id": "benefit", "name": "Launch badge", "distributionType": kind,
            "imageAssetURL": "https://example.com/badge.png" } }],
    })
}

#[test]
fn finds_watch_time_badge_drops() {
    let allow = json!({ "isEnabled": true, "channels": [{ "id": "1", "name": "Lumen_Fox" }] });
    let c = Campaign::from_json(&campaign_json(drop_json(30, 0, "BADGE"), allow)).unwrap();
    assert_eq!(c.game_slug, "game");
    assert_eq!(c.allowed_channels, [("1".to_string(), "lumen_fox".to_string())]);
    let none = HashSet::new();
    let (drop, badge) = c.watch_badge_drop(&none).unwrap();
    assert_eq!(drop.minutes_required, 30);
    assert_eq!(badge.benefit_id, "benefit");

    // A disabled allow list means any channel.
    let open = json!({ "isEnabled": false, "channels": [{ "id": "1", "name": "x" }] });
    assert!(Campaign::from_json(&campaign_json(drop_json(30, 0, "BADGE"), open)).unwrap().allowed_channels.is_empty());
    // Subscriptions, no watch time, or not a badge: not a watch-time badge.
    for drop in [drop_json(30, 1, "BADGE"), drop_json(0, 0, "BADGE"), drop_json(30, 0, "DIRECT_ENTITLEMENT")] {
        assert!(Campaign::from_json(&campaign_json(drop, Value::Null)).unwrap().watch_badge_drop(&none).is_none());
    }
    // ...unless the reward is a known event badge.
    let events: HashSet<String> = ["launch badge".to_string()].into();
    let other = Campaign::from_json(&campaign_json(drop_json(30, 0, "DIRECT_ENTITLEMENT"), Value::Null)).unwrap();
    assert_eq!(other.watch_badge_drop(&events).unwrap().1.name, "Launch badge");
}

fn outcome(id: &str, users: i64, points: i64) -> Outcome {
    Outcome { id: id.into(), title: id.into(), users, points }
}

#[test]
fn prediction_strategies_pick_outcomes() {
    let outcomes = [outcome("a", 10, 5_000), outcome("b", 40, 2_000), outcome("c", 0, 0), outcome("d", 40, 900)];
    assert_eq!(pick_outcome(&outcomes, Strategy::Crowd), Some(1)); // most voters, first of a tie
    assert_eq!(pick_outcome(&outcomes, Strategy::SmartMoney), Some(0));
    assert_eq!(pick_outcome(&outcomes, Strategy::Underdog), Some(3)); // fewest points among voted ones
    assert_eq!(pick_outcome(&[outcome("a", 0, 0)], Strategy::Crowd), None);
}

#[test]
fn prediction_bets_respect_limits() {
    let settings = PredictionSettings { percent: 5, max_points: 1_000, ..Default::default() };
    assert_eq!(settings.bet(10_000), Some(500));
    assert_eq!(settings.bet(1_000_000), Some(1_000));
    assert_eq!(settings.bet(100), None); // 5 points is under Twitch's minimum of 10
    assert!(!PredictionSettings::default().enabled);
}

#[test]
fn parses_event_badges() {
    let resp = json!({ "badges": [
        { "name": "True Damage Fan", "imageUrl": "https://x/1", "endAt": "2099-01-01T00:00:00.000Z", "free": true },
        { "name": "Worlds Supporter", "imageUrl": "https://x/2", "endAt": "2099-01-01T00:00:00.000Z", "free": false },
        { "name": "Old", "imageUrl": "https://x/3", "endAt": "2020-01-01T00:00:00.000Z", "free": true },
    ] });
    let events = parse_event_badges(&resp, 1_800_000_000.0);
    assert_eq!(events.len(), 1);
    assert_eq!(events["true damage fan"], "https://x/1");
}

#[test]
fn parses_channel_search() {
    let resp = json!({ "data": { "searchFor": { "channels": { "edges": [
        { "item": { "login": "lumen_fox", "displayName": "Lumen_Fox", "profileImageURL": "https://x/a.png",
            "followers": { "totalCount": 1200 }, "stream": { "viewersCount": 88, "game": { "displayName": "Art" } } } },
        { "item": { "login": "quietoak", "followers": null, "stream": null } },
        { "item": { "displayName": "no login" } },
    ] } } } });
    let hits = parse_search(&resp);
    assert_eq!(hits.len(), 2);
    assert_eq!((hits[0].live, hits[0].viewers, hits[0].followers), (true, 88, Some(1200)));
    assert_eq!(hits[0].game.as_deref(), Some("Art"));
    assert_eq!((hits[1].display_name.as_str(), hits[1].live, hits[1].followers), ("quietoak", false, None));
}

#[test]
fn parses_channel_card() {
    let offline = json!({ "data": { "user": { "displayName": "TinyOne", "profileImageURL": "https://x/p.png",
        "stream": null, "broadcastSettings": { "game": { "displayName": "Art" } } } } });
    let card = parse_channel_card(&offline, "tinyone").unwrap();
    assert_eq!((card.login.as_str(), card.display_name.as_str(), card.image.as_str()), ("tinyone", "TinyOne", "https://x/p.png"));
    assert!(!card.live && card.game.is_none()); // the last game only counts while live
    assert!(parse_channel_card(&json!({ "data": { "user": null } }), "nobody").is_none());
}

#[test]
fn parses_game_directory() {
    let resp = json!({ "data": { "game": { "streams": { "edges": [
        { "node": { "broadcaster": { "id": "1", "login": "a" }, "viewersCount": 50 } },
        { "node": { "broadcaster": null, "viewersCount": 9 } },
    ] } } } });
    assert_eq!(parse_directory(&resp), [("1".to_string(), "a".to_string(), 50)]);
}

/// A failed query must be an error, not "no such channel" or "offline": followers sync deletes on those.
#[test]
fn gql_errors_are_not_missing_channels() {
    let failed = json!({ "errors": [{ "message": "service timeout" }], "data": null });
    assert!(parse_channel_id(&failed).is_err());
    assert!(parse_stream_info(&failed).is_err());

    assert_eq!(parse_channel_id(&json!({ "data": { "user": { "id": "42" } } })).unwrap(), Some("42".into()));
    assert_eq!(parse_channel_id(&json!({ "data": { "user": null } })).unwrap(), None);
    assert!(parse_stream_info(&json!({ "data": { "user": { "stream": null } } })).unwrap().is_none());
}

#[test]
fn per_streamer_overrides_merge_with_defaults() {
    let text = EXAMPLE_CONFIG.replace(
        "streamers = []",
        r#"streamers = ["a", { username = "B", follow_raid = false, chat = "NEVER" }]"#,
    );
    let config: Config = toml::from_str(&text).unwrap();
    let defaults = &config.streamer_defaults;
    let b = &config.streamers[1];
    assert_eq!(b.username(), "b");
    let settings = b.settings(defaults).unwrap();
    assert!(!settings.follow_raid);
    assert!(settings.claim_drops);
    assert_eq!(settings.chat, ChatPresence::Never);
}

fn ids(store: &Store) -> Vec<String> {
    store.channels().unwrap().into_iter().map(|c| c.id).collect()
}

#[test]
fn store_adds_removes_and_blacklists() {
    let store = Store::open_in_memory().unwrap();
    let none = json!({});
    store.add_channel("1", "a", Source::Manual, false, &none).unwrap();
    store.add_channel("2", "b", Source::Followers, false, &none).unwrap();
    store.add_channel("3", "c", Source::Manual, true, &json!({ "chat": "NEVER" })).unwrap();
    assert_eq!(ids(&store), ["1", "2", "3"]);

    let c = &store.channels().unwrap()[2];
    assert!(c.prioritized);
    assert_eq!(c.settings, json!({ "chat": "NEVER" }));

    // Adding an existing channel keeps its place; adding it by hand makes it a manual channel.
    store.add_channel("2", "b_renamed", Source::Manual, false, &none).unwrap();
    store.add_channel("1", "a", Source::Followers, false, &none).unwrap();
    let rows = store.channels().unwrap();
    assert_eq!(ids(&store), ["1", "2", "3"]);
    assert_eq!(rows[0].source, Source::Manual);
    assert_eq!(rows[1].source, Source::Manual);
    assert_eq!(rows[1].login, "b_renamed");

    store.remove_channel("2").unwrap();
    store.add_to_blacklist("2", "b").unwrap();
    assert_eq!(ids(&store), ["1", "3"]);
    assert!(store.blacklist().unwrap().contains("2"));
    store.remove_from_blacklist("2").unwrap();
    assert!(store.blacklist().unwrap().is_empty());

    store.set_prioritized("1", true).unwrap();
    store.set_login("1", "new_name").unwrap();
    let a = &store.channels().unwrap()[0];
    assert!(a.prioritized);
    assert_eq!(a.login, "new_name");

    assert_eq!(store.meta("config_imported").unwrap(), None);
    store.set_meta("config_imported", "1").unwrap();
    assert_eq!(store.meta("config_imported").unwrap().as_deref(), Some("1"));
}

#[test]
fn store_reorders() {
    let store = Store::open_in_memory().unwrap();
    for id in ["1", "2", "3", "4"] {
        store.add_channel(id, id, Source::Manual, false, &json!({})).unwrap();
    }
    let order = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    store.reorder(&order(&["3", "1", "4", "2"])).unwrap();
    assert_eq!(ids(&store), ["3", "1", "4", "2"]);

    // Unknown ids are ignored, missing ones keep their relative order at the end.
    store.reorder(&order(&["2", "nope", "2"])).unwrap();
    assert_eq!(ids(&store), ["2", "3", "1", "4"]);

    // New channels go to the end.
    store.add_channel("5", "5", Source::Manual, false, &json!({})).unwrap();
    assert_eq!(ids(&store), ["2", "3", "1", "4", "5"]);
}

#[test]
fn parses_points_earned_message() {
    let inner = json!({
        "type": "points-earned",
        "data": {
            "timestamp": "2024-01-01T12:00:00.123Z",
            "channel_id": "123",
            "point_gain": { "total_points": 10, "reason_code": "WATCH" },
            "balance": { "balance": 510, "channel_id": "123" }
        }
    });
    let raw = json!({
        "type": "MESSAGE",
        "data": { "topic": "community-points-user-v1.999", "message": inner.to_string() }
    })
    .to_string();
    let msg = PubSubMessage::parse(&raw).unwrap();
    assert_eq!(msg.topic, "community-points-user-v1");
    assert_eq!(msg.kind, "points-earned");
    assert_eq!(msg.channel_id, "123");
    assert_eq!(msg.identifier(), "points-earned.community-points-user-v1.123");
}

#[test]
fn channel_topic_falls_back_to_topic_id() {
    let inner = json!({ "type": "stream-up", "server_time": 1700000000.5 });
    let raw = json!({ "type": "MESSAGE", "data": { "topic": "video-playback-by-id.42", "message": inner.to_string() } })
        .to_string();
    let msg = PubSubMessage::parse(&raw).unwrap();
    assert_eq!(msg.channel_id, "42");
    assert_eq!(msg.timestamp_raw, "1700000000.5");
}

#[test]
fn dashboard_with_null_campaigns_is_empty_not_an_error() {
    let null = json!({"data":{"currentUser":{"__typename":"User","dropCampaigns":null,"id":"1"}}});
    assert!(parse_active_campaigns(&null).unwrap().is_empty());
    let missing = json!({"data":{"currentUser":{"id":"1"}}});
    assert!(parse_active_campaigns(&missing).unwrap().is_empty());
    // A failed query is still an error.
    assert!(parse_active_campaigns(&json!({"data":{"currentUser":null}})).is_err());
    assert!(parse_active_campaigns(&json!({"data":null})).is_err());
    assert!(parse_active_campaigns(&json!({"errors":[{"message":"x"}],"data":{"currentUser":{"dropCampaigns":[]}}})).is_err());
    let list = json!({"data":{"currentUser":{"dropCampaigns":[
        {"id":"a","status":"ACTIVE"},{"id":"b","status":"EXPIRED"}]}}});
    let active = parse_active_campaigns(&list).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0]["id"], "a");
}

#[test]
fn campaign_ids_merge_once_in_first_seen_order() {
    let v = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let channels = vec![v(&["a", "b"]), v(&[]), v(&["b", "c"])];
    assert_eq!(merge_campaign_ids(&channels, &v(&["c", "d"])), v(&["a", "b", "c", "d"]));
    assert!(merge_campaign_ids(&[], &[]).is_empty());
}

fn event_badge(id: &str, name: &str) -> Value {
    json!({
        "id": id, "name": name, "imageUrl": "https://static-cdn.jtvnw.net/badges/v1/x/3",
        "startAt": "2020-01-01T00:00:00.000Z", "endAt": "2099-01-01T00:00:00.000Z", "free": true
    })
}

/// A streamdatabase page as it embeds its data, with one availability.
fn event_page(availability: Value) -> String {
    let data = json!({ "props": { "pageProps": { "availabilities": [availability] } } });
    format!(r#"<html><script id="__NEXT_DATA__" type="application/json">{data}</script><script>x</script></html>"#)
}

#[test]
fn event_badges_come_from_their_streamdatabase_page() {
    // As the K/DA Fan page has it: 30 minutes of League of Legends, any channel.
    let html = event_page(json!({
        "objectives": [[{ "type": "twitch_watch", "minutes": 30, "days": null }]],
        "categories": [{ "game": { "id": "21779", "name": "League of Legends" }, "slug": null }],
        "channels": null
    }));
    let c = Campaign::from_event_badge(&event_badge("kda-fan", "K/DA Fan"), &next_data(&html).unwrap()).unwrap();
    assert!(c.reward && c.dt_match());
    assert_eq!((c.id.as_str(), c.game_id.as_str(), c.game_slug.as_str()), ("event:kda-fan", "21779", "league-of-legends"));
    assert!(c.allowed_channels.is_empty());
    let (drop, badge) = c.watch_badge_drop(&HashSet::new()).unwrap();
    assert_eq!((drop.minutes_required, badge.name.as_str()), (30, "K/DA Fan"));

    // As the Jordan x Space Jam page has it: 15 minutes of IRL on samham.
    let html = event_page(json!({
        "objectives": [[{ "type": "twitch_watch", "minutes": 15, "days": null }]],
        "categories": [{ "game": { "id": "509672", "name": "IRL" }, "slug": null }],
        "channels": [{ "user": { "id": "54051059", "login": "samham" }, "stream": null }]
    }));
    let c = Campaign::from_event_badge(&event_badge("jordan", "Jordan x Space Jam"), &next_data(&html).unwrap()).unwrap();
    assert_eq!(c.allowed_channels, vec![("54051059".to_string(), "samham".to_string())]);
    assert_eq!(c.game_slug, "irl");

    // Not for watch time alone, or not filled in yet: no campaign.
    for objectives in [
        json!([[{ "type": "twitch_subscribe", "minutes": null, "days": null }]]),
        json!([[{ "type": "twitch_watch", "minutes": 15, "days": 15 }]]),
        json!([[{ "type": "twitch_watch", "minutes": null, "days": null }]]),
        json!([]),
    ] {
        let page = next_data(&event_page(json!({ "objectives": objectives, "categories": [], "channels": null }))).unwrap();
        assert!(Campaign::from_event_badge(&event_badge("x", "X"), &page).is_none(), "{objectives}");
    }
    // The quickest way that's watching alone wins.
    let page = next_data(&event_page(json!({ "objectives": [
        [{ "type": "twitch_watch", "minutes": 60, "days": null }],
        [{ "type": "twitch_watch", "minutes": 20, "days": null }, { "type": "twitch_cheer", "minutes": null, "days": null }],
        [{ "type": "twitch_watch", "minutes": 45, "days": null }]
    ], "categories": [], "channels": null }))).unwrap();
    assert_eq!(Campaign::from_event_badge(&event_badge("x", "X"), &page).unwrap().drops[0].minutes_required, 45);
    assert!(next_data("<html>no data</html>").is_none());
}

#[test]
fn makes_directory_slugs() {
    assert_eq!(game_slug("League of Legends"), "league-of-legends");
    assert_eq!(game_slug("IRL"), "irl");
    assert_eq!(game_slug("Counter-Strike"), "counter-strike");
    assert_eq!(game_slug("Tom Clancy's Rainbow Six Siege"), "tom-clancy-s-rainbow-six-siege");
}

/// The real pages, when saved to `$EVENT_PAGES_DIR` (kda.html, jordan.html).
#[test]
fn real_streamdatabase_pages() {
    let Ok(dir) = std::env::var("EVENT_PAGES_DIR") else { return };
    for (file, minutes, game) in [("kda.html", 30, "21779"), ("jordan.html", 15, "509672")] {
        let Ok(html) = std::fs::read_to_string(format!("{dir}/{file}")) else { continue };
        let c = Campaign::from_event_badge(&event_badge("x", "X"), &next_data(&html).unwrap()).unwrap();
        assert_eq!((c.drops[0].minutes_required, c.game_id.as_str()), (minutes, game), "{file}");
        println!("{file}: {:?} {} {}", c.allowed_channels, c.game_slug, c.drops[0].minutes_required);
    }
}

#[test]
fn event_badges_use_the_run_going_on_now() {
    // As the Jordan x Space Jam page had it on 2026-11-14: a run on samham that ended, and one on
    // realkatieb that evening whose minutes weren't filled in yet.
    let page = json!({ "props": { "pageProps": { "availabilities": [
        {
            "objectives": [[{ "type": "twitch_watch", "minutes": 15, "days": null }]],
            "categories": [{ "game": { "id": "509672", "name": "IRL" }, "slug": null }],
            "channels": [{ "user": { "id": "54051059", "login": "samham" } }],
            "start_at_date": "2026-11-13", "start_at_time": "20:25",
            "end_at_date": "2026-11-14", "end_at_time": "07:59"
        },
        {
            "objectives": [[{ "type": "twitch_watch", "minutes": null, "days": null }]],
            "categories": [],
            "channels": [{ "user": { "id": "165301937", "login": "realkatieb" } }],
            "start_at_date": "2026-11-14", "start_at_time": "20:00", "end_at_date": "2026-11-14"
        }
    ] } } });
    let badge = json!({ "id": "j", "name": "J", "startAt": "2026-11-13T00:00:00.000Z", "endAt": "2026-11-15T23:59:59.999Z" });
    let at = |t: &str| chrono::DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&chrono::Utc);
    let on = |t: &str| Campaign::from_event_badge_at(&badge, &page, at(t)).unwrap();

    // During the first run: samham, 15 minutes.
    let c = on("2026-11-13T21:00:00Z");
    assert!(c.start_at <= at("2026-11-13T21:00:00Z") && at("2026-11-13T21:00:00Z") < c.end_at);
    assert_eq!(c.allowed_channels, vec![("54051059".to_string(), "samham".to_string())]);
    // Between the runs: the next one, not running yet.
    let c = on("2026-11-14T12:00:00Z");
    assert_eq!(c.allowed_channels[0].1, "realkatieb");
    assert!(c.start_at > at("2026-11-14T12:00:00Z"));
    // During the second run: realkatieb, on any game, with the first run's 15 minutes.
    let c = on("2026-11-14T21:00:00Z");
    assert_eq!(c.allowed_channels, vec![("165301937".to_string(), "realkatieb".to_string())]);
    assert_eq!(c.game_id, "");
    assert_eq!(c.drops[0].minutes_required, 15);
    assert_eq!(c.end_at.to_rfc3339(), "2026-11-15T00:00:00+00:00");
}

#[test]
fn event_badges_use_the_page_window() {
    // catquery said Jordan x Space Jam ran until the 15th; the page says 20:25-21:59 UTC on the 13th.
    let mut page = json!({
        "objectives": [[{ "type": "twitch_watch", "minutes": 15, "days": null }]],
        "categories": [], "channels": null,
        "start_at_date": "2026-11-13", "start_at_time": "20:25",
        "end_at_date": "2026-11-13", "end_at_time": "21:59"
    });
    let badge = json!({ "id": "j", "name": "J", "startAt": "2026-11-13T00:00:00.000Z", "endAt": "2026-11-15T23:59:59.999Z" });
    let c = Campaign::from_event_badge(&badge, &next_data(&event_page(page.clone())).unwrap()).unwrap();
    assert_eq!(c.start_at.to_rfc3339(), "2026-11-13T20:25:00+00:00");
    assert_eq!(c.end_at.to_rfc3339(), "2026-11-13T21:59:59+00:00");
    assert_eq!(c.drops[0].end_at, c.end_at);
    // A date without a time runs to the end of that day; no date falls back to catquery.
    page["end_at_time"] = Value::Null;
    let c = Campaign::from_event_badge(&badge, &next_data(&event_page(page.clone())).unwrap()).unwrap();
    assert_eq!(c.end_at.to_rfc3339(), "2026-11-14T00:00:00+00:00");
    page["end_at_date"] = Value::Null;
    let c = Campaign::from_event_badge(&badge, &next_data(&event_page(page)).unwrap()).unwrap();
    assert_eq!(c.end_at.to_rfc3339(), "2026-11-15T23:59:59.999+00:00");
}

#[test]
fn reads_twitch_progress_for_a_badge_by_name() {
    // As the inventory lists a drop in progress: Purple Chain, 22 of 60 minutes watched.
    let inventory = json!({ "dropCampaignsInProgress": [{
        "id": "gta", "timeBasedDrops": [
            { "id": "d0", "requiredMinutesWatched": 30, "self": { "currentMinutesWatched": 30, "isClaimed": true },
              "benefitEdges": [{ "benefit": { "name": "Other" } }] },
            { "id": "d1", "requiredMinutesWatched": 60, "self": { "currentMinutesWatched": 22, "isClaimed": false },
              "benefitEdges": [{ "benefit": { "name": "Purple Chain " } }] }
        ]
    }]});
    let progress = |minutes, required, claimed, locked| Some(TwitchProgress { minutes, required, claimed, locked });
    assert_eq!(inventory_progress(&inventory, "purple chain"), progress(22, 60, false, false));
    assert_eq!(inventory_progress(&inventory, "Other"), progress(30, 30, true, false));
    // A drop waiting for its day.
    let mut locked = inventory.clone();
    locked["dropCampaignsInProgress"][0]["timeBasedDrops"][1]["self"]["hasPreconditionsMet"] = json!(false);
    assert_eq!(inventory_progress(&locked, "Purple Chain"), progress(22, 60, false, true));
    assert_eq!(inventory_progress(&inventory, "K/DA Fan"), None);
    assert_eq!(inventory_progress(&json!({}), "Purple Chain"), None);
}

#[test]
fn streak_streams_back_within_30_minutes_keep_their_state() {
    let s = live("a");
    {
        let mut st = s.state();
        st.stream.watch_streak_missing = true;
        st.stream.streak_at_start = Some(4);
        // Offline 10 minutes ago, before its streak was in.
        st.offline_at = crate::utils::now() - 600.0;
        st.prev_streak_missing = true;
    }
    assert!(s.streak_missing());
    // The previous stream had it: the same stream for Twitch, nothing to earn.
    s.state().prev_streak_missing = false;
    assert!(!s.streak_missing());
    // Long enough ago: a new stream.
    s.state().offline_at = crate::utils::now() - 31.0 * 60.0;
    assert!(s.streak_missing());
}

#[test]
fn a_streak_that_never_comes_gives_up_its_slot() {
    let s = live("a");
    {
        let mut st = s.state();
        st.stream.watch_streak_missing = true;
        st.stream.streak_at_start = Some(4);
        st.stream.minute_watched = STREAK_GIVE_UP_MINUTES - 1.0;
    }
    assert!(s.streak_missing());
    s.state().stream.minute_watched = STREAK_GIVE_UP_MINUTES;
    assert!(!s.streak_missing());
}

#[test]
fn started_streaks_are_finished_first() {
    let list = [live("a"), live("b"), live("c")];
    for s in &list {
        s.state().stream.watch_streak_missing = true;
    }
    assert_eq!(names(&select_watching(&list, &[Priority::Streak])), ["a", "b"]);
    list[2].state().stream.minute_watched = 3.0;
    assert_eq!(names(&select_watching(&list, &[Priority::Streak])), ["c", "a"]);
}

/// A streamdatabase list entry, as `initialTwitchGlobalBadges` has them.
fn sdb_badge(set_id: &str, title: &str, cost: Value, window: Option<(&str, &str, &str, &str)>) -> Value {
    let mut b = json!({
        "current": { "set_id": set_id, "version": { "id": "1", "title": title, "image_url_4x": format!("https://static-cdn.jtvnw.net/badges/v1/{set_id}/3") } },
        "removed": false, "cost": cost,
    });
    if let Some((sd, st, ed, et)) = window {
        for (k, v) in [("start_at_date", sd), ("start_at_time", st), ("end_at_date", ed), ("end_at_time", et)] {
            b[k] = json!(v);
        }
    }
    b
}

#[test]
fn reads_streamdatabase_badge_list() {
    let page = json!({ "props": { "pageProps": { "initialTwitchGlobalBadges": [
        sdb_badge("kda-fan", "K/DA Fan", json!("free"), Some(("2026-11-13", "16:00", "2026-11-15", "15:59"))),
        // Paid, or no window: left out.
        sdb_badge("gallica", "Gallica", json!("paid"), Some(("2026-11-14", "18:00", "2026-12-12", "17:59"))),
        sdb_badge("1979-revolution_1", "1979 Revolution", Value::Null, None),
        sdb_badge("old", "Old", json!("free"), None),
    ] } } });
    let list = parse_streamdatabase_list(&page);
    assert_eq!(list.len(), 1);
    let kda = &list[0];
    assert_eq!(kda["id"], "kda-fan");
    assert_eq!(kda["name"], "K/DA Fan");
    assert_eq!(kda["free"], true);
    assert_eq!(kda["startAt"], "2026-11-13T16:00:00+00:00");
    assert_eq!(kda["endAt"], "2026-11-15T15:59:59+00:00");
    assert_eq!(kda["streamdatabaseUrl"], "https://www.streamdatabase.com/twitch/global-badges/kda-fan/1");
    // Read like catquery's list.
    let events = parse_event_badges(&json!({ "badges": list }), 0.0);
    assert!(events.contains_key("k/da fan"));
}

#[test]
fn badge_lists_merge_without_duplicates() {
    let catquery = [
        json!({ "id": "kda-fan", "name": "K/DA Fan", "imageUrl": "", "startAt": "2026-11-13T16:00:00.000Z",
                "endAt": "2026-11-15T15:59:00.000Z", "free": true,
                "streamdatabaseUrl": "https://www.streamdatabase.com/twitch/global-badges/kda-fan/1" }),
        json!({ "id": "clover", "name": "Clover", "free": true }),
    ];
    let streamdatabase = [
        // The same badge: by set id, and one listed under another id with the same title.
        json!({ "id": "kda-fan", "name": "K/DA Fan", "imageUrl": "https://img/kda", "free": true }),
        json!({ "id": "clover_2", "name": " clover ", "free": true,
                "streamdatabaseUrl": "https://www.streamdatabase.com/twitch/global-badges/clover_2/1" }),
        // Only streamdatabase has it.
        json!({ "id": "purple-chain", "name": "Purple Chain", "free": true }),
    ];
    let merged = merge_badge_lists(&catquery, &streamdatabase);
    let badges = merged["badges"].as_array().unwrap();
    let ids: Vec<&str> = badges.iter().map(|b| b["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["kda-fan", "clover", "purple-chain"]);
    // Catquery's entry wins; its gaps come from streamdatabase's.
    assert_eq!(badges[0]["startAt"], "2026-11-13T16:00:00.000Z");
    assert_eq!(badges[0]["imageUrl"], "https://img/kda");
    assert_eq!(badges[1]["streamdatabaseUrl"], "https://www.streamdatabase.com/twitch/global-badges/clover_2/1");
    // Either list alone works.
    assert_eq!(merge_badge_lists(&[], &streamdatabase)["badges"].as_array().unwrap().len(), 3);
    assert_eq!(merge_badge_lists(&catquery, &[])["badges"].as_array().unwrap().len(), 2);
}

/// The real list page, when saved to `$SDB_LIST` (an HTML file).
#[test]
fn real_streamdatabase_list() {
    let Ok(path) = std::env::var("SDB_LIST") else { return };
    let page = next_data(&std::fs::read_to_string(path).unwrap()).unwrap();
    let list = parse_streamdatabase_list(&page);
    let kda = list.iter().find(|b| b["id"] == "kda-fan").expect("K/DA Fan is listed");
    assert_eq!(kda["startAt"], "2026-11-13T16:00:00+00:00");
    println!("{} free time-limited badges, e.g. {}", list.len(), kda);
}

// === Watch plan scenarios: badges and watch streaks === //

/// A live channel eligible for the picked badge.
fn badge_channel(name: &str) -> Arc<Streamer> {
    let s = live(name);
    s.state().badge_drop = true;
    s
}

/// A live channel whose streak (count `count`) this stream hasn't earned yet.
fn streak_pending(name: &str, count: u32) -> Arc<Streamer> {
    let s = live(name);
    {
        let mut st = s.state();
        st.stream.watch_streak_missing = true;
        st.watch_streak = Some(count);
        st.stream.streak_at_start = Some(count);
    }
    s
}

const RULES: [Priority; 4] = [Priority::Badges, Priority::Streak, Priority::Prioritized, Priority::Order];

#[test]
fn a_badge_is_watched_alone() {
    let list = [live("a"), badge_channel("b"), live("c")];
    let plan = plan_watching(&list, &RULES, None);
    assert_eq!(names(&plan.watching), ["b"]);
    assert!(!plan.streak_break);
    // Without BADGES in the rules: the normal two.
    let plan = plan_watching(&list, &[Priority::Order], None);
    assert_eq!(names(&plan.watching), ["a", "b"]);
    assert!(plan.badge_channel.is_none());
}

#[test]
fn the_focused_badge_channel_keeps_its_slot() {
    let list = [badge_channel("a"), badge_channel("b")];
    assert_eq!(names(&plan_watching(&list, &RULES, None).watching), ["a"]);
    // Watching b already: switching to a would split the progress.
    assert_eq!(names(&plan_watching(&list, &RULES, Some("b")).watching), ["b"]);
    // b went offline (no longer a candidate): a.
    assert_eq!(names(&plan_watching(&list[..1], &RULES, Some("b")).watching), ["a"]);
}

#[test]
fn a_streak_at_stake_pauses_the_badge_until_twitch_confirms_it() {
    let list = [badge_channel("badge"), streak_pending("s1", 40), streak_pending("s2", 12), streak_pending("s3", 3)];
    let plan = plan_watching(&list, &RULES, Some("badge"));
    assert!(plan.streak_break);
    assert_eq!(names(&plan.watching), ["s1", "s2"], "both slots on streaks");
    assert_eq!(plan.badge_channel.as_ref().map(|s| s.username.as_str()), Some("badge"), "focus kept");

    // s1 and s2 confirmed: s3 next.
    list[1].state().stream.watch_streak_missing = false;
    list[2].state().stream.watch_streak_missing = false;
    assert_eq!(names(&plan_watching(&list, &RULES, Some("badge")).watching), ["s3"]);
    // All confirmed: the badge alone again.
    list[3].state().stream.watch_streak_missing = false;
    let plan = plan_watching(&list, &RULES, Some("badge"));
    assert!(!plan.streak_break);
    assert_eq!(names(&plan.watching), ["badge"]);
}

#[test]
fn a_streak_that_never_comes_ends_its_break() {
    let list = [badge_channel("badge"), streak_pending("s1", 40)];
    assert!(plan_watching(&list, &RULES, Some("badge")).streak_break);
    // Twitch's count didn't go up in 30 minutes watched: back to the badge.
    list[1].state().stream.minute_watched = STREAK_GIVE_UP_MINUTES;
    let plan = plan_watching(&list, &RULES, Some("badge"));
    assert!(!plan.streak_break);
    assert_eq!(names(&plan.watching), ["badge"]);
}

#[test]
fn no_break_without_a_streak_to_lose() {
    // No streak yet (count 0 or unknown): nothing to lose, the badge goes on.
    let fresh = streak_pending("fresh", 0);
    let unknown = live("unknown");
    unknown.state().stream.watch_streak_missing = true;
    let list = [badge_channel("badge"), fresh, unknown];
    let plan = plan_watching(&list, &RULES, Some("badge"));
    assert!(!plan.streak_break);
    assert_eq!(names(&plan.watching), ["badge"]);
}

#[test]
fn no_break_without_the_streak_rule_or_for_the_badge_channels_own_streak() {
    let list = [badge_channel("badge"), streak_pending("s1", 40)];
    let plan = plan_watching(&list, &[Priority::Badges, Priority::Order], Some("badge"));
    assert!(!plan.streak_break, "STREAK not in the rules");
    assert_eq!(names(&plan.watching), ["badge"]);

    // The badge channel needs its own streak: watching it earns both.
    {
        let mut st = list[0].state();
        st.stream.watch_streak_missing = true;
        st.watch_streak = Some(9);
        st.stream.streak_at_start = Some(9);
    }
    let plan = plan_watching(&list, &RULES, Some("badge"));
    assert!(!plan.streak_break);
    assert_eq!(names(&plan.watching), ["badge"]);
}

#[test]
fn without_a_badge_streaks_come_first() {
    let list = [live("a"), streak_pending("s1", 4), live("c")];
    let plan = plan_watching(&list, &RULES, None);
    assert_eq!(names(&plan.watching), ["s1", "a"]);
    assert!(!plan.streak_break);
}

#[test]
fn a_busy_day_holds_the_badge_up_for_minutes_not_hours() {
    // 28 live channels with a streak pending, 8 of them with a streak to lose; each takes 6
    // minutes until Twitch confirms it. Count the minutes the badge waits.
    let mut list = vec![badge_channel("badge")];
    for i in 0..28 {
        list.push(streak_pending(&format!("s{i}"), if i < 8 { 10 } else { 0 }));
    }
    let mut waited = 0;
    for _minute in 0..240 {
        let plan = plan_watching(&list, &RULES, Some("badge"));
        if !plan.streak_break {
            break;
        }
        waited += 1;
        for s in &plan.watching {
            let mut st = s.state();
            st.stream.minute_watched += 1.0;
            if st.stream.minute_watched >= 6.0 {
                st.stream.watch_streak_missing = false;
            }
        }
    }
    // 8 streaks, 2 at a time, 6 minutes each.
    assert_eq!(waited, 24);
}

#[test]
fn time_away_from_a_slot_isnt_counted() {
    let s = live("a");
    {
        let mut st = s.state();
        st.stream.update_minute_watched();
        // Lost its slot, then got it back much later.
        st.stream.pause_minute_watched();
        let added = st.stream.update_minute_watched();
        assert_eq!(added, 0.0);
        assert_eq!(st.stream.minute_watched, 0.0);
    }
}

#[test]
fn badges_on_several_days_keep_their_days() {
    let goal = |objectives: Value| {
        let page = next_data(&event_page(json!({ "objectives": objectives, "categories": [], "channels": null }))).unwrap();
        Campaign::from_event_badge(&event_badge("x", "X"), &page).map(|c| (c.drops[0].minutes_required, c.drops[0].days_required))
    };
    // GTA VI's chains: Purple 60 minutes on 1 day, Bronze on 2, ... Miami on 6.
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 60, "days": 1 }]])), Some((60, 1)));
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 60, "days": 2 }]])), Some((60, 2)));
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 60, "days": 6 }]])), Some((60, 6)));
    // No days, or 0: one.
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 30, "days": null }]])), Some((30, 1)));
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 30, "days": 0 }]])), Some((30, 1)));
    // Fewest days first, then fewest minutes.
    assert_eq!(
        goal(json!([
            [{ "type": "twitch_watch", "minutes": 30, "days": 3 }],
            [{ "type": "twitch_watch", "minutes": 120, "days": 1 }],
            [{ "type": "twitch_watch", "minutes": 90, "days": 1 }]
        ])),
        Some((90, 1))
    );
    // Too many days, or watching plus something else: left out.
    assert_eq!(goal(json!([[{ "type": "twitch_watch", "minutes": 60, "days": 30 }]])), None);
    assert_eq!(
        goal(json!([[{ "type": "twitch_watch", "minutes": 60, "days": 2 }, { "type": "twitch_subscribe", "minutes": null, "days": null }]])),
        None
    );
}

/// The real chain pages, when saved to `$EVENT_PAGES_DIR` (bronze-chain.html, miami-chain.html).
#[test]
fn real_chain_pages() {
    let Ok(dir) = std::env::var("EVENT_PAGES_DIR") else { return };
    for (file, days) in [("bronze-chain.html", 2), ("miami-chain.html", 6)] {
        let Ok(html) = std::fs::read_to_string(format!("{dir}/{file}")) else { continue };
        let page = next_data(&html).unwrap();
        let badge = json!({ "id": "x", "name": "X", "startAt": "2026-11-18T11:00:00.000Z", "endAt": "2026-12-01T07:59:00.000Z" });
        let c = Campaign::from_event_badge(&badge, &page).unwrap();
        assert_eq!((c.drops[0].minutes_required, c.drops[0].days_required, c.game_id.as_str()), (60, days, "1867279146"), "{file}");
    }
}
