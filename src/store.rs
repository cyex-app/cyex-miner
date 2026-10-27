//! SQLite database for the channel list, which the miner and the web UI manage.
//!
//! Channels are keyed by their Twitch user id, so they survive name changes; the stored login is
//! refreshed from the id on startup.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::entities::streamer::Source;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS channels (
    id          TEXT PRIMARY KEY,
    login       TEXT NOT NULL,
    source      TEXT NOT NULL,
    position    INTEGER NOT NULL,
    prioritized INTEGER NOT NULL DEFAULT 0,
    settings    TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS blacklist (
    id    TEXT PRIMARY KEY,
    login TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

#[derive(Debug, Clone)]
pub struct ChannelRow {
    pub id: String,
    pub login: String,
    pub source: Source,
    pub prioritized: bool,
    /// Per-channel overrides of `streamer_defaults`, as a JSON object.
    pub settings: Value,
}

pub struct Store {
    conn: Mutex<Connection>,
}

fn source_name(source: Source) -> &'static str {
    match source {
        Source::Manual => "manual",
        Source::Followers => "followers",
        Source::Badge => "badge",
    }
}

fn parse_source(name: &str) -> Source {
    match name {
        "followers" => Source::Followers,
        "badge" => Source::Badge,
        _ => Source::Manual,
    }
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        Self::init(conn)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(SCHEMA).context("failed to create the database tables")?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn()
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0))
            .optional()?)
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [key, value],
        )?;
        Ok(())
    }

    /// All channels, in list order.
    pub fn channels(&self) -> Result<Vec<ChannelRow>> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT id, login, source, prioritized, settings FROM channels ORDER BY position, rowid")?;
        let rows = stmt
            .query_map([], |r| {
                let source: String = r.get(2)?;
                let settings: String = r.get(4)?;
                Ok(ChannelRow {
                    id: r.get(0)?,
                    login: r.get(1)?,
                    source: parse_source(&source),
                    prioritized: r.get(3)?,
                    settings: serde_json::from_str(&settings).unwrap_or_else(|_| Value::Object(Default::default())),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Appends a channel to the end of the list. If it's already there it keeps its place; adding
    /// it manually turns a followed channel into a manual one.
    pub fn add_channel(&self, id: &str, login: &str, source: Source, prioritized: bool, settings: &Value) -> Result<()> {
        self.conn().execute(
            "INSERT INTO channels (id, login, source, position, prioritized, settings)
             VALUES (?1, ?2, ?3, (SELECT COALESCE(MAX(position) + 1, 0) FROM channels), ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
                login = excluded.login,
                source = CASE WHEN excluded.source = 'manual' THEN 'manual' ELSE channels.source END",
            params![id, login, source_name(source), prioritized, settings.to_string()],
        )?;
        Ok(())
    }

    pub fn remove_channel(&self, id: &str) -> Result<()> {
        self.conn().execute("DELETE FROM channels WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn set_prioritized(&self, id: &str, prioritized: bool) -> Result<()> {
        self.conn()
            .execute("UPDATE channels SET prioritized = ?2 WHERE id = ?1", params![id, prioritized])?;
        Ok(())
    }

    pub fn set_login(&self, id: &str, login: &str) -> Result<()> {
        self.conn().execute("UPDATE channels SET login = ?2 WHERE id = ?1", [id, login])?;
        self.conn().execute("UPDATE blacklist SET login = ?2 WHERE id = ?1", [id, login])?;
        Ok(())
    }

    /// Saves a new list order: `ids` first, in that order, then any other channels in their current order.
    pub fn reorder(&self, ids: &[String]) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let current: Vec<String> = tx
            .prepare("SELECT id FROM channels ORDER BY position, rowid")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let known: HashSet<&String> = current.iter().collect();
        let mut seen = HashSet::new();
        let order = ids
            .iter()
            .filter(|id| known.contains(id))
            .chain(current.iter())
            .filter(|id| seen.insert(*id));
        {
            let mut update = tx.prepare("UPDATE channels SET position = ?2 WHERE id = ?1")?;
            for (position, id) in order.enumerate() {
                update.execute(params![id, position as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Ids of channels that must not come back from the follow list.
    pub fn blacklist(&self) -> Result<HashSet<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT id FROM blacklist")?;
        let ids = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    }

    pub fn add_to_blacklist(&self, id: &str, login: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO blacklist (id, login) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET login = excluded.login",
            [id, login],
        )?;
        Ok(())
    }

    pub fn remove_from_blacklist(&self, id: &str) -> Result<()> {
        self.conn().execute("DELETE FROM blacklist WHERE id = ?1", [id])?;
        Ok(())
    }
}
