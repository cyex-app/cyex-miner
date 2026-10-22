mod auth;
mod chat;
mod config;
mod constants;
mod entities;
mod miner;
mod priority;
mod pubsub;
mod store;
#[cfg(test)]
mod tests;
mod twitch;
mod utils;
mod web;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::{
    config::{Config, EXAMPLE_CONFIG},
    miner::Miner,
    store::Store,
    twitch::Twitch,
    web::{AppState, WebAuth},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("points_miner=info")),
        )
        .with_target(false)
        .init();

    let config_path = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "config.toml".into()));
    if !config_path.exists() {
        std::fs::write(&config_path, EXAMPLE_CONFIG)?;
        println!(
            "Created {} - set your Twitch username and channels in it, then run again.",
            config_path.display()
        );
        return Ok(());
    }
    let mut config = Config::load(&config_path)?;
    // The Docker image sets this to 0.0.0.0 so the UI is reachable through the published port.
    if let Ok(host) = std::env::var("POINTS_MINER_WEB_HOST") {
        config.web.host = host;
    }
    let data_dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let state = Arc::new(AppState {
        login_prompt: Mutex::new(None),
        miner: OnceLock::new(),
        auth: WebAuth::load_or_create(data_dir.join("web_token"))?,
    });
    if config.web.enabled {
        let (state, host, port) = (state.clone(), config.web.host.clone(), config.web.port);
        tokio::spawn(async move {
            if let Err(e) = web::serve(state, &host, port).await {
                error!("Web UI failed: {e:#}");
            }
        });
    }

    let http = reqwest::Client::builder()
        .gzip(true)
        .timeout(Duration::from_secs(30))
        .build()?;
    let cookies_dir = data_dir.join("cookies");
    let creds = auth::login(&http, &config.username, &cookies_dir, &state.login_prompt)
        .await
        .context("login failed")?;
    info!("Logged in as {} (user id {})", creds.login, creds.user_id);

    let store = Store::open(&data_dir.join("miner.db"))?;
    let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();
    let miner = Arc::new(Miner::new(Twitch::new(http, creds), config, store, pubsub_tx));
    let _ = state.miner.set(miner.clone());
    tokio::spawn(miner.clone().run(pubsub_rx));

    tokio::signal::ctrl_c().await?;
    info!("CTRL+C detected, shutting down");
    miner.report();
    Ok(())
}
