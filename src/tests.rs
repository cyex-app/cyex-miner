use std::sync::Arc;

use serde_json::{json, Value};

use crate::{
    config::{Config, EXAMPLE_CONFIG},
    entities::{
        drops::Campaign,
        streamer::{ChatPresence, Source, Streamer, StreamerSettings},
    },
    priority::{select_watching, Priority},
    pubsub::PubSubMessage,
    store::Store,
    twitch::{parse_channel_id, parse_directory, parse_stream_info, parse_watch_streak},
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
    let badge = c.watch_badge_drop().unwrap();
    assert_eq!(badge.minutes_required, 30);
    assert_eq!(badge.badge.as_ref().unwrap().benefit_id, "benefit");

    // A disabled allow list means any channel.
    let open = json!({ "isEnabled": false, "channels": [{ "id": "1", "name": "x" }] });
    assert!(Campaign::from_json(&campaign_json(drop_json(30, 0, "BADGE"), open)).unwrap().allowed_channels.is_empty());
    // Subscriptions, no watch time, or not a badge: not a watch-time badge.
    for drop in [drop_json(30, 1, "BADGE"), drop_json(0, 0, "BADGE"), drop_json(30, 0, "DIRECT_ENTITLEMENT")] {
        assert!(Campaign::from_json(&campaign_json(drop, Value::Null)).unwrap().watch_badge_drop().is_none());
    }
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
