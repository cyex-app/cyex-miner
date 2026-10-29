//! Pool of Twitch PubSub websocket connections (max 50 topics per connection).

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::{
    constants::PUBSUB_URL,
    utils::{random_alnum, random_between},
};

const MAX_TOPICS: usize = 50;

enum Command {
    Listen(String),
    Unlisten(String),
}

struct Connection {
    topics: Arc<Mutex<Vec<String>>>,
    commands: mpsc::UnboundedSender<Command>,
}

pub struct PubSubPool {
    connections: Mutex<Vec<Connection>>,
    auth_token: String,
    messages: mpsc::UnboundedSender<String>,
}

impl PubSubPool {
    /// Raw `MESSAGE` frames are forwarded to `messages`.
    pub fn new(auth_token: String, messages: mpsc::UnboundedSender<String>) -> Self {
        Self {
            connections: Mutex::new(Vec::new()),
            auth_token,
            messages,
        }
    }

    pub fn submit(&self, topic: String) {
        let mut connections = self.connections.lock().unwrap();
        if connections
            .iter()
            .any(|c| c.topics.lock().unwrap().contains(&topic))
        {
            return;
        }
        if let Some(conn) = connections
            .iter()
            .find(|c| c.topics.lock().unwrap().len() < MAX_TOPICS)
        {
            conn.topics.lock().unwrap().push(topic.clone());
            let _ = conn.commands.send(Command::Listen(topic));
            return;
        }

        let index = connections.len();
        let topics = Arc::new(Mutex::new(vec![topic]));
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_connection(
            index,
            topics.clone(),
            rx,
            self.auth_token.clone(),
            self.messages.clone(),
        ));
        connections.push(Connection {
            topics,
            commands: tx,
        });
    }

    pub fn remove(&self, topic: &str) {
        for conn in self.connections.lock().unwrap().iter() {
            let mut topics = conn.topics.lock().unwrap();
            if let Some(pos) = topics.iter().position(|t| t == topic) {
                topics.remove(pos);
                let _ = conn.commands.send(Command::Unlisten(topic.to_string()));
            }
        }
    }
}

fn request(kind: &str, topic: &str, auth_token: &str) -> Message {
    Message::Text(
        json!({
            "type": kind,
            "nonce": random_alnum(30),
            "data": { "topics": [topic], "auth_token": auth_token },
        })
        .to_string(),
    )
}

async fn run_connection(
    index: usize,
    topics: Arc<Mutex<Vec<String>>>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    auth_token: String,
    messages: mpsc::UnboundedSender<String>,
) {
    loop {
        // A stalled handshake would otherwise hang this connection for good.
        match tokio::time::timeout(Duration::from_secs(30), connect_async(PUBSUB_URL)).await {
            Ok(Ok((ws, _))) => {
                debug!("#{index} - PubSub connected");
                let (mut sink, mut stream) = ws.split();
                let initial = topics.lock().unwrap().clone();
                let mut last_pong = Instant::now();
                let ping = tokio::time::sleep(Duration::ZERO);
                tokio::pin!(ping);

                let mut ok = true;
                for topic in &initial {
                    if sink.send(request("LISTEN", topic, &auth_token)).await.is_err() {
                        ok = false;
                        break;
                    }
                }

                while ok {
                    tokio::select! {
                        frame = stream.next() => match frame {
                            Some(Ok(Message::Text(text))) => {
                                let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
                                match parsed["type"].as_str() {
                                    Some("PONG") => last_pong = Instant::now(),
                                    Some("RECONNECT") => {
                                        info!("#{index} - Reconnection required by Twitch");
                                        break;
                                    }
                                    Some("RESPONSE") => {
                                        if let Some(err) = parsed["error"].as_str().filter(|e| !e.is_empty()) {
                                            warn!("#{index} - PubSub LISTEN error: {err}");
                                        }
                                    }
                                    Some("MESSAGE") => {
                                        let _ = messages.send(text);
                                    }
                                    _ => {}
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => break,
                            Some(Err(e)) => {
                                warn!("#{index} - WebSocket error: {e}");
                                break;
                            }
                            Some(Ok(_)) => {}
                        },
                        command = commands.recv() => {
                            let msg = match command {
                                Some(Command::Listen(t)) => request("LISTEN", &t, &auth_token),
                                Some(Command::Unlisten(t)) => request("UNLISTEN", &t, &auth_token),
                                None => return,
                            };
                            ok = sink.send(msg).await.is_ok();
                        },
                        _ = &mut ping => {
                            if last_pong.elapsed() > Duration::from_secs(5 * 60) {
                                info!("#{index} - The last PONG was received more than 5 minutes ago");
                                break;
                            }
                            ok = sink.send(Message::Text(r#"{"type":"PING"}"#.into())).await.is_ok();
                            ping.as_mut().reset(Instant::now() + random_between(25.0, 30.0));
                        },
                    }
                }
                let _ = sink.close().await;
            }
            Ok(Err(e)) => warn!("#{index} - Could not connect to PubSub: {e}"),
            Err(_) => warn!("#{index} - Connecting to PubSub timed out"),
        }
        info!("#{index} - Reconnecting to Twitch PubSub in ~30 seconds");
        tokio::time::sleep(random_between(25.0, 35.0)).await;
    }
}

/// A decoded PubSub `MESSAGE` frame.
#[derive(Debug)]
pub struct PubSubMessage {
    pub topic: String,
    pub kind: String,
    pub message: Value,
    pub data: Value,
    pub timestamp_raw: String,
    pub channel_id: String,
}

impl PubSubMessage {
    pub fn parse(raw: &str) -> Option<Self> {
        let frame: Value = serde_json::from_str(raw).ok()?;
        let (topic, topic_id) = frame["data"]["topic"].as_str()?.split_once('.')?;
        let message: Value = serde_json::from_str(frame["data"]["message"].as_str()?).ok()?;
        let kind = message["type"].as_str()?.to_string();
        let data = message.get("data").cloned().unwrap_or(Value::Null);

        let timestamp_raw = if let Some(ts) = data["timestamp"].as_str() {
            ts.to_string()
        } else if let Some(st) = data["server_time"].as_f64().or(message["server_time"].as_f64()) {
            st.to_string()
        } else {
            Utc::now().to_rfc3339()
        };

        let channel_id = [
            &data["claim"]["channel_id"],
            &data["channel_id"],
            &data["balance"]["channel_id"],
            // Predictions: the channel's event, and this account's bet on it.
            &data["event"]["channel_id"],
            &data["prediction"]["channel_id"],
        ]
        .into_iter()
        .find_map(|v| v.as_str())
        .unwrap_or(topic_id)
        .to_string();

        Some(Self {
            topic: topic.to_string(),
            kind,
            message,
            data,
            timestamp_raw,
            channel_id,
        })
    }

    /// Used to drop duplicates delivered by several connections.
    pub fn identifier(&self) -> String {
        format!("{}.{}.{}", self.kind, self.topic, self.channel_id)
    }
}
