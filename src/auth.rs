//! Login through Twitch's OAuth device-code flow (the "activate your TV" flow).

use std::{fs, path::Path, sync::Mutex, time::Duration};

use anyhow::{bail, Context, Result};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    constants::{CLIENT_ID, TV_USER_AGENT},
    utils::{now, random_alnum},
};

const SCOPES: &str = "channel_read chat:read user_blocks_edit user_blocks_read user_follows_edit user_read";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub auth_token: String,
    pub user_id: String,
    pub login: String,
    pub device_id: String,
}

/// Shown in the web UI while waiting for the user to activate the device code.
#[derive(Debug, Clone, Serialize)]
pub struct LoginPrompt {
    pub code: String,
    pub url: String,
    pub expires_at: f64,
}

#[derive(Deserialize)]
struct Validation {
    login: String,
    user_id: String,
}

fn oauth_headers(req: RequestBuilder, device_id: &str) -> RequestBuilder {
    req.header("Accept", "application/json")
        .header("Client-Id", CLIENT_ID)
        .header("Origin", "https://android.tv.twitch.tv")
        .header("Referer", "https://android.tv.twitch.tv/")
        .header("User-Agent", TV_USER_AGENT)
        .header("X-Device-Id", device_id)
}

/// Returns `Ok(None)` if Twitch rejects the token.
async fn validate(http: &Client, token: &str) -> Result<Option<Validation>> {
    let resp = http
        .get("https://id.twitch.tv/oauth2/validate")
        .header("Authorization", format!("OAuth {token}"))
        .send()
        .await?;
    match resp.status() {
        StatusCode::UNAUTHORIZED => Ok(None),
        s if s.is_success() => Ok(Some(resp.json().await?)),
        s => bail!("token validation returned {s}"),
    }
}

pub async fn login(
    http: &Client,
    username: &str,
    dir: &Path,
    prompt: &Mutex<Option<LoginPrompt>>,
) -> Result<Credentials> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{username}.json"));

    if let Ok(text) = fs::read_to_string(&path) {
        match serde_json::from_str::<Credentials>(&text) {
            Ok(saved) => match validate(http, &saved.auth_token).await {
                Ok(Some(v)) => {
                    return Ok(Credentials {
                        login: v.login,
                        user_id: v.user_id,
                        ..saved
                    })
                }
                Ok(None) => warn!("Saved Twitch token is no longer valid, you need to log in again"),
                Err(e) => {
                    warn!("Could not validate the saved token ({e:#}), using it anyway");
                    return Ok(saved);
                }
            },
            Err(e) => warn!("Ignoring unreadable {}: {e}", path.display()),
        }
    }

    let device_id = random_alnum(32);
    let token = device_flow(http, &device_id, prompt).await?;
    *prompt.lock().unwrap() = None;

    let v = validate(http, &token)
        .await?
        .context("Twitch rejected the freshly issued token")?;
    if v.login != username {
        warn!(
            "Logged in as '{}' but the config says '{username}'; mining with '{}'",
            v.login, v.login
        );
    }
    let creds = Credentials {
        auth_token: token,
        user_id: v.user_id,
        login: v.login,
        device_id,
    };
    fs::write(&path, serde_json::to_string_pretty(&creds)?)?;
    // The Twitch token gives full account access: owner-only, like the web UI token.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
    }
    info!("Login successful, token saved to {}", path.display());
    Ok(creds)
}

#[derive(Deserialize)]
struct DeviceCode {
    device_code: String,
    user_code: String,
    verification_uri: String,
    interval: u64,
    expires_in: u64,
}

async fn device_flow(
    http: &Client,
    device_id: &str,
    prompt: &Mutex<Option<LoginPrompt>>,
) -> Result<String> {
    loop {
        let resp = oauth_headers(http.post("https://id.twitch.tv/oauth2/device"), device_id)
            .form(&[("client_id", CLIENT_ID), ("scopes", SCOPES)])
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("device code request failed: {} {}", resp.status(), resp.text().await?);
        }
        let code: DeviceCode = resp.json().await?;
        let expires_at = now() + code.expires_in as f64;

        info!("You need to log in to Twitch!");
        info!("Open {} and enter this code: {}", code.verification_uri, code.user_code);
        info!("The code expires in {} minutes.", code.expires_in / 60);
        *prompt.lock().unwrap() = Some(LoginPrompt {
            code: code.user_code.clone(),
            url: code.verification_uri.clone(),
            expires_at,
        });

        while now() < expires_at {
            tokio::time::sleep(Duration::from_secs(code.interval.max(1))).await;
            let resp = oauth_headers(http.post("https://id.twitch.tv/oauth2/token"), device_id)
                .form(&[
                    ("client_id", CLIENT_ID),
                    ("device_code", code.device_code.as_str()),
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ])
                .send()
                .await;
            // 400 means the user hasn't entered the code yet.
            let Ok(resp) = resp else { continue };
            if resp.status().is_success() {
                let body: serde_json::Value = resp.json().await?;
                if let Some(token) = body["access_token"].as_str() {
                    return Ok(token.to_string());
                }
            }
        }
        warn!("The login code expired, requesting a new one");
    }
}
