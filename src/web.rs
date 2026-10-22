//! Small web UI + JSON API to watch the miner and add/remove channels.

use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    async_trait,
    extract::{FromRequestParts, Path, Request, State},
    http::{header, request::Parts, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{
    auth::LoginPrompt,
    entities::streamer::{HistoryEntry, Source, Streamer},
    miner::{LogEvent, Miner},
    priority::Priority,
    utils::random_alnum,
};

pub struct AppState {
    pub login_prompt: Mutex<Option<LoginPrompt>>,
    pub miner: OnceLock<Arc<Miner>>,
    pub auth: WebAuth,
}

const AUTH_COOKIE: &str = "pm_auth";

/// The login token for the web UI. Generated on first start and stored in a file next to the
/// config; it can be regenerated from the UI, which logs out every other browser.
pub struct WebAuth {
    path: PathBuf,
    token: RwLock<String>,
}

impl WebAuth {
    pub fn load_or_create(path: PathBuf) -> Result<Self> {
        let existing = fs::read_to_string(&path)
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let auth = Self {
            path,
            token: RwLock::new(existing.clone().unwrap_or_default()),
        };
        match existing {
            Some(_) => tracing::info!("Web UI login token is in {}", auth.path.display()),
            None => {
                let token = auth.regenerate()?;
                tracing::info!(
                    "Generated web UI login token: {token} (saved to {}, can be regenerated in the UI)",
                    auth.path.display()
                );
            }
        }
        Ok(auth)
    }

    /// Replaces the token with a new random one and saves it, which logs out every existing session.
    pub fn regenerate(&self) -> Result<String> {
        let token = random_alnum(32);
        if let Some(dir) = self.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir)?;
        }
        fs::write(&self.path, format!("{token}\n"))
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600));
        }
        *self.token.write().unwrap() = token.clone();
        Ok(token)
    }

    fn check(&self, candidate: &str) -> bool {
        let token = self.token.read().unwrap();
        // Constant-time comparison, so response times don't leak how much of the token matched.
        !token.is_empty()
            && candidate.len() == token.len()
            && candidate.bytes().zip(token.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
    }
}

pub async fn serve(state: Arc<AppState>, host: &str, port: u16) -> Result<()> {
    let protected = Router::new()
        .route("/api/status", get(status))
        .route("/api/stream", get(stream))
        .route("/api/streamers", post(add_streamer))
        .route("/api/streamers/order", put(reorder))
        .route("/api/streamers/:id", delete(remove_streamer))
        .route("/api/streamers/:id/prioritize", put(set_prioritize))
        .route("/api/settings/priority", put(set_priority))
        .route("/api/settings/paused", put(set_paused))
        .route("/api/auth/regenerate", post(regenerate_token))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .route("/", get(index))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .merge(protected)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind((host, port)).await?;
    tracing::info!("Web UI listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

/// The token from the session cookie, or from an `Authorization: Bearer` header (for scripts).
fn request_token(headers: &HeaderMap) -> Option<String> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.trim().to_string());
    bearer.or_else(|| {
        headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .find_map(|c| c.trim().strip_prefix(AUTH_COOKIE)?.strip_prefix('=').map(str::to_string))
    })
}

async fn require_auth(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    if request_token(req.headers()).is_some_and(|t| state.auth.check(&t)) {
        next.run(req).await
    } else {
        error(StatusCode::UNAUTHORIZED, "Not logged in")
    }
}

fn session_cookie(token: &str, max_age: u32) -> String {
    format!("{AUTH_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}")
}

#[derive(Deserialize)]
struct Login {
    token: String,
}

async fn login(State(state): State<Arc<AppState>>, Json(body): Json<Login>) -> Response {
    let token = body.token.trim();
    if !state.auth.check(token) {
        return error(StatusCode::UNAUTHORIZED, "Wrong token");
    }
    ([(header::SET_COOKIE, session_cookie(token, 31_536_000))], StatusCode::NO_CONTENT).into_response()
}

async fn logout() -> Response {
    ([(header::SET_COOKIE, session_cookie("", 0))], StatusCode::NO_CONTENT).into_response()
}

async fn regenerate_token(State(state): State<Arc<AppState>>) -> Response {
    match state.auth.regenerate() {
        Ok(token) => {
            tracing::info!("Web UI login token regenerated, all other sessions are logged out");
            let cookie = session_cookie(&token, 31_536_000);
            ([(header::SET_COOKIE, cookie)], Json(json!({ "token": token }))).into_response()
        }
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../ui/index.html"))
}

#[derive(Serialize)]
struct StreamerView {
    id: String,
    username: String,
    source: Source,
    online: bool,
    watching: bool,
    prioritized: bool,
    points: i64,
    gained: i64,
    title: String,
    game: Option<String>,
    viewers: i64,
    minutes_watched: f64,
    watch_streak: Option<u32>,
    history: BTreeMap<String, HistoryEntry>,
}

impl StreamerView {
    fn new(s: &Streamer) -> Self {
        let st = s.state();
        Self {
            id: s.channel_id.clone(),
            username: s.username.clone(),
            source: s.source,
            online: st.is_online,
            watching: st.watching,
            prioritized: st.prioritized,
            points: st.channel_points,
            gained: st.channel_points - st.starting_points.unwrap_or(st.channel_points),
            title: st.stream.title.clone(),
            game: st.stream.game.as_ref().map(|g| g.display_name.clone()),
            viewers: st.stream.viewers_count,
            // Whole minutes, so the live stream only sends an update when the number shown changes.
            minutes_watched: st.stream.minute_watched.floor(),
            watch_streak: st.watch_streak,
            history: st.history.clone(),
        }
    }
}

#[derive(Serialize)]
struct Status {
    ready: bool,
    user: Option<String>,
    login_prompt: Option<LoginPrompt>,
    started_at: Option<f64>,
    priority: Vec<Priority>,
    paused: bool,
    streamers: Vec<StreamerView>,
    /// Only in `/api/status`; the live stream sends events separately.
    #[serde(skip_serializing_if = "Option::is_none")]
    events: Option<Vec<LogEvent>>,
}

fn snapshot(state: &AppState, with_events: bool) -> Status {
    let login_prompt = state.login_prompt.lock().unwrap().clone();
    match state.miner.get() {
        Some(miner) => Status {
            ready: true,
            user: Some(miner.twitch.creds.login.clone()),
            login_prompt,
            started_at: Some(miner.started_at),
            priority: miner.priority(),
            paused: miner.paused(),
            streamers: miner.streamers().iter().map(|s| StreamerView::new(s)).collect(),
            events: with_events.then(|| miner.events()),
        },
        None => Status {
            ready: false,
            user: None,
            login_prompt,
            started_at: None,
            priority: Vec::new(),
            paused: false,
            streamers: Vec::new(),
            events: with_events.then(Vec::new),
        },
    }
}

async fn status(State(state): State<Arc<AppState>>) -> Json<Status> {
    Json(snapshot(&state, true))
}

/// State of one `/api/stream` connection.
struct StreamConn {
    state: Arc<AppState>,
    /// The last status sent, to only send changes.
    last_status: String,
    events: Option<broadcast::Receiver<LogEvent>>,
    tick: tokio::time::Interval,
    queued: VecDeque<Event>,
}

async fn next_event(rx: &mut Option<broadcast::Receiver<LogEvent>>) -> Result<LogEvent, RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Server-Sent Events for the web UI:
/// - `status`: the dashboard state (like `/api/status` without events), sent on connect and when it changes
/// - `log-init`: the recent log lines, sent once the miner is running (and again if the client fell behind)
/// - `log`: one new log line
async fn stream(State(state): State<Arc<AppState>>) -> Response {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let conn = StreamConn { state, last_status: String::new(), events: None, tick, queued: VecDeque::new() };

    let events = futures_util::stream::unfold(conn, |mut c| async move {
        loop {
            if let Some(event) = c.queued.pop_front() {
                return Some((Ok::<_, Infallible>(event), c));
            }
            if c.events.is_none() {
                if let Some(miner) = c.state.miner.get() {
                    let (recent, rx) = miner.subscribe_events();
                    c.events = Some(rx);
                    let data = serde_json::to_string(&recent).unwrap_or_default();
                    c.queued.push_back(Event::default().event("log-init").data(data));
                    continue;
                }
            }
            tokio::select! {
                _ = c.tick.tick() => {
                    let status = serde_json::to_string(&snapshot(&c.state, false)).unwrap_or_default();
                    if status != c.last_status {
                        c.last_status = status.clone();
                        return Some((Ok(Event::default().event("status").data(status)), c));
                    }
                }
                event = next_event(&mut c.events) => match event {
                    Ok(event) => {
                        let data = serde_json::to_string(&event).unwrap_or_default();
                        return Some((Ok(Event::default().event("log").data(data)), c));
                    }
                    // Fell behind: start over with a fresh `log-init`.
                    Err(RecvError::Lagged(_)) => c.events = None,
                    Err(RecvError::Closed) => return None,
                },
            }
        }
    });

    // Keep-alive comments stop proxies (e.g. Cloudflare) from closing an idle stream, and
    // `X-Accel-Buffering: no` stops nginx from buffering it.
    (
        [("X-Accel-Buffering", "no")],
        Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))),
    )
        .into_response()
}

fn error(status: StatusCode, message: impl ToString) -> Response {
    (status, Json(json!({ "error": message.to_string() }))).into_response()
}

/// The running miner, for handlers that need it; answers 503 while it's still logging in.
struct Ready(Arc<Miner>);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for Ready {
    type Rejection = Response;

    async fn from_request_parts(_: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        state
            .miner
            .get()
            .cloned()
            .map(Ready)
            .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "The miner is still logging in"))
    }
}

#[derive(Deserialize)]
struct AddStreamer {
    username: String,
}

async fn add_streamer(Ready(miner): Ready, Json(body): Json<AddStreamer>) -> Response {
    match miner.add_streamer(&body.username).await {
        Ok(streamer) => Json(StreamerView::new(&streamer)).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, format!("{e:#}")),
    }
}

async fn remove_streamer(Ready(miner): Ready, Path(id): Path<String>) -> Response {
    match miner.remove_streamer(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error(StatusCode::NOT_FOUND, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct SetPrioritize {
    prioritize: bool,
}

async fn set_prioritize(Ready(miner): Ready, Path(id): Path<String>, Json(body): Json<SetPrioritize>) -> Response {
    match miner.set_prioritized(&id, body.prioritize) {
        Ok(streamer) => Json(StreamerView::new(&streamer)).into_response(),
        Err(e) => error(StatusCode::NOT_FOUND, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct SetPriority {
    /// Most important first.
    rules: Vec<Priority>,
}

async fn set_priority(Ready(miner): Ready, Json(body): Json<SetPriority>) -> Response {
    match miner.set_priority(body.rules) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, format!("{e:#}")),
    }
}

#[derive(Deserialize)]
struct SetPaused {
    paused: bool,
}

async fn set_paused(Ready(miner): Ready, Json(body): Json<SetPaused>) -> Response {
    miner.set_paused(body.paused);
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct Reorder {
    /// Channel ids, most important first.
    ids: Vec<String>,
}

async fn reorder(Ready(miner): Ready, Json(body): Json<Reorder>) -> Response {
    match miner.reorder(&body.ids) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}
