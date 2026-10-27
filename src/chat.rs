//! IRC chat presence: being in chat counts towards some channel point features.
//!
//! All channels share a single IRC connection (over TLS websocket).

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::constants::IRC_URL;

/// Twitch allows 20 JOINs per 10 seconds for regular accounts.
const JOIN_BATCH: usize = 20;
const JOIN_INTERVAL: Duration = Duration::from_secs(11);
/// IRC lines are limited to 512 bytes.
const MAX_JOIN_LINE: usize = 450;

enum Command {
    Join(String),
    Leave(String),
}

pub struct ChatManager {
    /// Channels we want to be in; the connection re-joins all of them after a reconnect.
    channels: Arc<Mutex<HashSet<String>>>,
    commands: mpsc::UnboundedSender<Command>,
}

impl ChatManager {
    /// Must be called from within the tokio runtime.
    pub fn new(login: String, token: String) -> Self {
        let channels = Arc::new(Mutex::new(HashSet::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_connection(login, token, channels.clone(), rx));
        Self { channels, commands: tx }
    }

    pub fn join(&self, channel: &str) {
        if self.channels.lock().unwrap().insert(channel.to_string()) {
            info!("Join IRC chat: {channel}");
            let _ = self.commands.send(Command::Join(channel.to_string()));
        }
    }

    pub fn leave(&self, channel: &str) {
        if self.channels.lock().unwrap().remove(channel) {
            info!("Leave IRC chat: {channel}");
            let _ = self.commands.send(Command::Leave(channel.to_string()));
        }
    }
}

async fn run_connection(
    login: String,
    token: String,
    channels: Arc<Mutex<HashSet<String>>>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    loop {
        match session(&login, &token, &channels, &mut commands).await {
            Ok(true) => return,
            Ok(false) => info!("IRC chat closed, reconnecting in 60s"),
            Err(e) => warn!("IRC chat: {e:#}, reconnecting in 60s"),
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Returns `Ok(true)` when the manager was dropped and the connection should stop for good.
async fn session(
    login: &str,
    token: &str,
    channels: &Mutex<HashSet<String>>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
) -> Result<bool> {
    // A stalled handshake would otherwise hang chat for good.
    let (ws, _) = tokio::time::timeout(Duration::from_secs(30), connect_async(IRC_URL))
        .await
        .map_err(|_| anyhow::anyhow!("connecting to chat timed out"))??;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(format!("PASS oauth:{token}"))).await?;
    sink.send(Message::Text(format!("NICK {login}"))).await?;
    debug!("IRC chat connected");

    // Joins are queued and sent in rate-limited batches.
    let mut pending: VecDeque<String> = channels.lock().unwrap().iter().cloned().collect();
    let mut joined: HashSet<String> = HashSet::new();
    let next_join = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next_join);
    // Twitch pings roughly every 5 minutes; treat 10 minutes of silence as a dead connection.
    let idle = tokio::time::sleep(Duration::from_secs(600));
    tokio::pin!(idle);

    loop {
        tokio::select! {
            frame = stream.next() => {
                let text = match frame {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) | None => return Ok(false),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(_)) => continue,
                };
                idle.as_mut().reset(Instant::now() + Duration::from_secs(600));
                // One frame can carry several IRC lines.
                for line in text.lines() {
                    if let Some(rest) = line.strip_prefix("PING") {
                        sink.send(Message::Text(format!("PONG{rest}"))).await?;
                    } else if line.contains("Login authentication failed") {
                        bail!("login authentication failed");
                    } else if line.ends_with(" RECONNECT") {
                        info!("IRC reconnection required by Twitch");
                        return Ok(false);
                    }
                }
            }
            command = commands.recv() => match command {
                Some(Command::Join(channel)) => {
                    if !joined.contains(&channel) && !pending.contains(&channel) {
                        pending.push_back(channel);
                    }
                }
                Some(Command::Leave(channel)) => {
                    pending.retain(|c| *c != channel);
                    if joined.remove(&channel) {
                        sink.send(Message::Text(format!("PART #{channel}"))).await?;
                    }
                }
                None => return Ok(true),
            },
            _ = &mut next_join, if !pending.is_empty() => {
                let mut batch: Vec<String> = Vec::new();
                let mut len = 0;
                while let Some(channel) = pending.front() {
                    if batch.len() == JOIN_BATCH || len + channel.len() + 2 > MAX_JOIN_LINE {
                        break;
                    }
                    len += channel.len() + 2;
                    batch.push(pending.pop_front().unwrap());
                }
                let line = batch.iter().map(|c| format!("#{c}")).collect::<Vec<_>>().join(",");
                sink.send(Message::Text(format!("JOIN {line}"))).await?;
                joined.extend(batch);
                next_join.as_mut().reset(Instant::now() + JOIN_INTERVAL);
            }
            _ = &mut idle => bail!("no message from Twitch in 10 minutes"),
        }
    }
}
