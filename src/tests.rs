use std::{collections::HashSet, sync::Arc};

use serde_json::{json, Value};

use crate::{
    config::{Config, EXAMPLE_CONFIG},
    entities::{
        drops::{game_slug, next_data, twitch_channel_logins, Campaign},
        streamer::{ChatPresence, Source, Streamer, StreamerSettings},
    },
    miner::{with_event_badges, parse_event_badges, pick_outcome, Outcome, PredictionSettings, Strategy},
    priority::{select_watching, Priority},
    pubsub::PubSubMessage,
    store::Store,
    twitch::{
        merge_campaign_ids, parse_active_campaigns, parse_channel_card, parse_channel_id, parse_directory,
        parse_reward_campaigns, parse_search, parse_stream_info, parse_watch_streak,
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

fn reward_campaign_json() -> Value {
    json!({
        "id": "r1",
        "name": "K/DA",
        "status": "ACTIVE",
        "startsAt": "2020-01-01T00:00:00Z",
        "endsAt": "2099-01-01T00:00:00Z",
        "externalURL": "https://www.twitch.tv/directory/category/league-of-legends",
        "instructions": "Watch on twitch.tv/Samham for 15 minutes",
        "game": { "id": "21779", "slug": "league-of-legends", "displayName": "League of Legends" },
        "unlockRequirements": { "subsGoal": 0, "minuteWatchedGoal": 30 },
        "image": { "image1xURL": "https://example.com/campaign.png" },
        "rewards": [{ "id": "w1", "name": "K/DA Fan", "thumbnailImage": { "image1xURL": "https://example.com/kda.png" } }]
    })
}

#[test]
fn reward_campaigns_become_watch_time_badges() {
    let c = Campaign::from_reward_json(&reward_campaign_json()).unwrap();
    assert!(c.reward && c.dt_match());
    assert_eq!(c.game_id, "21779");
    assert_eq!(c.game_slug, "league-of-legends");
    // The category link isn't a channel; the one in the instructions is.
    assert_eq!(c.allowed_channels, vec![(String::new(), "samham".to_string())]);
    // Only a Twitch event badge counts; a game item (a Minecraft add-on) doesn't.
    assert!(c.watch_badge_drop(&HashSet::new()).is_none());
    let events: HashSet<String> = ["k/da fan".to_string()].into();
    let (drop, badge) = c.watch_badge_drop(&events).unwrap();
    assert_eq!(drop.minutes_required, 30);
    assert_eq!(drop.id, "r1:w1");
    assert_eq!(badge.name, "K/DA Fan");
    assert_eq!(badge.benefit_id, "w1");
    assert_eq!(badge.image, "https://example.com/kda.png");

    // Subscription goals aren't for watch time.
    let mut subs = reward_campaign_json();
    subs["unlockRequirements"]["subsGoal"] = json!(1);
    assert!(Campaign::from_reward_json(&subs).unwrap().watch_badge_drop(&events).is_none());
    // No game: any channel.
    let mut sitewide = reward_campaign_json();
    sitewide["game"] = Value::Null;
    assert_eq!(Campaign::from_reward_json(&sitewide).unwrap().game_id, "");
}

#[test]
fn parses_reward_campaign_list_in_either_place() {
    let top = json!({"data":{"currentUser":{"id":"1"},"rewardCampaignsAvailableToUser":[{"id":"a"}]}});
    assert_eq!(parse_reward_campaigns(&top).unwrap().len(), 1);
    let inside = json!({"data":{"currentUser":{"rewardCampaignsAvailableToUser":[{"id":"a"},{"id":"b"}]}}});
    assert_eq!(parse_reward_campaigns(&inside).unwrap().len(), 2);
    assert!(parse_reward_campaigns(&json!({"data":{"currentUser":{}}})).unwrap().is_empty());
    assert!(parse_reward_campaigns(&json!({"data":null})).is_err());
}

#[test]
fn finds_channel_links() {
    assert_eq!(twitch_channel_logins("https://www.twitch.tv/samham"), vec!["samham"]);
    assert_eq!(twitch_channel_logins("see twitch.tv/A_b1 and twitch.tv/a_b1."), vec!["a_b1"]);
    assert!(twitch_channel_logins("https://twitch.tv/directory/category/irl").is_empty());
    assert!(twitch_channel_logins("https://twitch.tv/drops").is_empty());
    assert!(twitch_channel_logins("no links").is_empty());
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
        json!([[{ "type": "twitch_watch", "minutes": 15, "days": 3 }]]),
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

#[test]
fn event_badges_twitch_already_gives_are_left_out() {
    let page = next_data(&event_page(json!({
        "objectives": [[{ "type": "twitch_watch", "minutes": 30, "days": null }]],
        "categories": [], "channels": null
    })))
    .unwrap();
    let event = Campaign::from_event_badge(&event_badge("kda-fan", "K/DA Fan"), &page).unwrap();
    let other = Campaign::from_event_badge(&event_badge("other", "Other"), &page).unwrap();
    let reward = Campaign::from_reward_json(&reward_campaign_json()).unwrap();
    let ids = |v: Vec<Campaign>| v.into_iter().map(|c| c.id).collect::<Vec<_>>();
    assert_eq!(ids(with_event_badges(&[], &[reward.clone(), event.clone(), other.clone()])), ["r1", "event:other"]);
    assert_eq!(ids(with_event_badges(&[], &[event, other])), ["event:kda-fan", "event:other"]);
}

/// The real pages, when saved to `$EVENT_PAGES_DIR` (kda.html, jordan.html).
#[test]
fn real_streamdatabase_pages() {
    let Ok(dir) = std::env::var("EVENT_PAGES_DIR") else { return };
    for (file, minutes, game) in [("kda.html", 30, "21779"), ("jordan.html", 15, "509672")] {
        let html = std::fs::read_to_string(format!("{dir}/{file}")).unwrap();
        let c = Campaign::from_event_badge(&event_badge("x", "X"), &next_data(&html).unwrap()).unwrap();
        assert_eq!((c.drops[0].minutes_required, c.game_id.as_str()), (minutes, game), "{file}");
        println!("{file}: {:?} {} {}", c.allowed_channels, c.game_slug, c.drops[0].minutes_required);
    }
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
