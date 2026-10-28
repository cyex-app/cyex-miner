use serde_json::{json, Value};

pub const TWITCH_URL: &str = "https://www.twitch.tv";
pub const GQL_URL: &str = "https://gql.twitch.tv/gql";
pub const PUBSUB_URL: &str = "wss://pubsub-edge.twitch.tv/v1";
pub const IRC_URL: &str = "wss://irc-ws.chat.twitch.tv:443";

/// Android TV client: the only first-party client that still offers the device-code login.
pub const CLIENT_ID: &str = "ue6666qo983tsx6so1t0vnawi233wa";
pub const CLIENT_VERSION: &str = "ef928475-9403-42f2-8a34-55784bd08e16";

pub const TV_USER_AGENT: &str = "Mozilla/5.0 (Linux; Android 7.1; Smart Box C1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36";
pub const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36";
/// The streamer page only embeds the settings.js URL for some user agents.
pub const FIREFOX_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0";

/// A persisted GraphQL query: operation name + sha256 hash.
pub struct Op(pub &'static str, pub &'static str);

impl Op {
    pub fn with(&self, variables: Value) -> Value {
        json!({
            "operationName": self.0,
            "variables": variables,
            "extensions": { "persistedQuery": { "version": 1, "sha256Hash": self.1 } },
        })
    }
}

pub const PLAYBACK_ACCESS_TOKEN: Op = Op(
    "PlaybackAccessToken",
    "ed230aa1e33e07eebb8928504583da78a5173989fadfb1ac94be06a04f3cdbe9",
);
pub const STREAM_INFO: Op = Op(
    "VideoPlayerStreamInfoOverlayChannel",
    "198492e0857f6aedead9665c81c5a06d67b25b58034649687124083ff288597d",
);
pub const CLAIM_COMMUNITY_POINTS: Op = Op(
    "ClaimCommunityPoints",
    "46aaeebe02c99afdf4fc97c7c0cba964124bf6b0af229395f1f6d1feed05b3d0",
);
pub const CLAIM_MOMENT: Op = Op(
    "CommunityMomentCallout_Claim",
    "e2d67415aead910f7f9ceb45a77b750a1e1d9622c936d832328a0689e054db62",
);
pub const CLAIM_DROP_REWARDS: Op = Op(
    "DropsPage_ClaimDropRewards",
    "a455deea71bdc9015b78eb49f4acfbce8baa7ccbedd28e549bb025bd0f751930",
);
pub const CHANNEL_POINTS_CONTEXT: Op = Op(
    "ChannelPointsContext",
    "7fe050e3761eb2cf258d70ee1a21cbd76fa8cf3d7e7b12fc437e7029d446b5e3",
);
pub const JOIN_RAID: Op = Op(
    "JoinRaid",
    "c6a332a86d1087fbbb1a8623aa01bd1313d2386e7c63be60fdb2d1901f01a4ae",
);
pub const INVENTORY: Op = Op(
    "Inventory",
    "8337eb8541b314040b0edde0c09c5c7a2783ba1960aa9edfbf3bac16d0fec404",
);
pub const VIEWER_DROPS_DASHBOARD: Op = Op(
    "ViewerDropsDashboard",
    "c16bb890cc8ce7647a96ee69cd313d423a378a3dedadf630a1017cde18975feb",
);
pub const DROP_CAMPAIGN_DETAILS: Op = Op(
    "DropCampaignDetails",
    "039277bf98f3130929262cc7c6efd9c141ca3749cb6dca442fc8ead9a53f77c1",
);
pub const AVAILABLE_DROPS: Op = Op(
    "DropsHighlightService_AvailableDrops",
    "782dad0f032942260171d2d80a654f88bdd0c5a9dddc392e9bc92218a0f42d20",
);
/// Live channels in a game's directory.
pub const DIRECTORY_PAGE_GAME: Op = Op(
    "DirectoryPage_Game",
    "86bcceb4e8b1a51256ff8eed8bd8aae4acacf80d737efe904f84f3aeadf8cafd",
);
pub const GET_ID_FROM_LOGIN: Op = Op(
    "GetIDFromLogin",
    "94e82a7b1e3c21e186daa73ee2afc4b8f23bade1fbbff6fe8ac133f50a2f58ca",
);
/// The viewer's rewards on a channel, including the current watch streak.
pub const REWARD_LIST: Op = Op(
    "RewardList",
    "0b1471876d7647993731b9e3c6a13bf304c67fb31d07f06a945d42286ee377c4",
);
pub const CHANNEL_FOLLOWS: Op = Op(
    "ChannelFollows",
    "eecf815273d3d949e5cf0085cc5084cd8a1b5b7b6f7990cf43cb0beadf546907",
);
