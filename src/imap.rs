use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::{
    MailboxScanError, MailboxScanMessage, MailboxScanReport, ParsedEmail, parse_rfc822,
    parse_rfc822_headers,
};

#[derive(Clone, Debug, Default)]
pub struct ImapSyncOptions {
    /// If true, ignore any stored checkpoints and fetch from scratch.
    pub force_full: bool,

    /// If true, fetch only messages that are currently `UNSEEN` on the server.
    ///
    /// Note: `UNSEEN` is mutable and does not have stable incremental semantics by itself.
    /// Callers should treat this as a selection mode rather than a checkpointed sync.
    pub unseen_only: bool,
}

pub trait ImapStateBackend {
    fn load_state(&mut self, account_id: &str, mailbox: &str) -> Result<ImapSyncState>;
    fn save_state(&mut self, account_id: &str, mailbox: &str, state: &ImapSyncState) -> Result<()>;
}

#[derive(Clone, Debug, Deserialize)]
pub struct ImapConfigFile {
    #[serde(default)]
    pub accounts: Vec<ImapAccountConfig>,

    #[serde(default)]
    pub account: Option<ImapAccountConfig>,
}

impl ImapConfigFile {
    pub fn all_accounts(&self) -> Vec<ImapAccountConfig> {
        let mut out = self.accounts.clone();
        if let Some(acc) = self.account.clone() {
            out.push(acc);
        }
        out
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ImapAccountConfig {
    pub host: String,
    pub username: String,
    pub password: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_tls")]
    pub tls: bool,

    #[serde(default)]
    pub danger_skip_tls_verify: bool,

    #[serde(default = "default_mailbox")]
    pub mailbox: String,

    #[serde(default)]
    pub account_id: Option<String>,

    #[serde(default)]
    pub state_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImapSyncState {
    pub uidvalidity: Option<u32>,
    pub last_uid: u32,
    pub highest_modseq: Option<u64>,
    pub last_sync_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImapSyncResult {
    pub account_id: String,
    pub mailbox: String,
    pub state: ImapSyncState,
    pub supports_modseq: bool,
    pub server_capabilities: Vec<String>,
    pub messages: Vec<SyncedEmail>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vanished_uids: Vec<u32>,

    /// If present, the full set of UIDs currently in the mailbox (used for periodic expunge
    /// reconciliation when VANISHED/QRESYNC is not available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_uids: Option<Vec<u32>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncedEmail {
    pub uid: u32,
    pub internal_date: Option<String>,
    pub flags: Vec<String>,
    pub modseq: Option<u64>,
    pub rfc822_size: Option<u32>,
    pub parsed: ParsedEmail,

    #[serde(skip_serializing, skip_deserializing)]
    pub raw: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct ImapScanOptions {
    pub max_messages: Option<usize>,
}

pub fn scan_imap_headers(account: &ImapAccountConfig, options: ImapScanOptions) -> Result<MailboxScanReport> {
    scan_imap_headers_inner(account, options, None)
}

pub fn scan_imap_headers_with_progress<F: FnMut(usize, f64)>(
    account: &ImapAccountConfig,
    options: ImapScanOptions,
    progress_every: usize,
    mut on_progress: F,
) -> Result<MailboxScanReport> {
    scan_imap_headers_inner(account, options, Some((&mut on_progress, progress_every.max(1))))
}

fn scan_imap_headers_inner(
    account: &ImapAccountConfig,
    options: ImapScanOptions,
    mut progress: Option<(&mut dyn FnMut(usize, f64), usize)>,
) -> Result<MailboxScanReport> {
    let mailbox = account.mailbox.trim().to_string();
    let mut session = connect(account)?;
    session
        .select(&mailbox)
        .with_context(|| format!("imap select {mailbox}"))?;

    let query = "(UID INTERNALDATE RFC822.SIZE BODY.PEEK[HEADER])";
    let fetches = session
        .uid_fetch("1:*", query)
        .with_context(|| format!("imap fetch headers {mailbox}"))?;

    let mut report = MailboxScanReport::default();
    let t0 = std::time::Instant::now();

    for fetch in fetches.iter() {
        if let Some(max) = options.max_messages {
            if report.messages.len() >= max {
                break;
            }
        }

        let uid = fetch.uid.map(|v| v as u32);
        let internal_date = fetch
            .internal_date()
            .map(|d| d.to_rfc3339());
        let rfc822_size = fetch.size.map(|s| s as u32);

        let Some(header_bytes) = fetch.header() else {
            report.errors.push(MailboxScanError {
                source: format!("imap:{}:uid:{}", mailbox, uid.unwrap_or_default()),
                error: "missing header bytes".to_string(),
            });
            continue;
        };

        match parse_rfc822_headers(header_bytes) {
            Ok(headers) => report.messages.push(MailboxScanMessage {
                uid,
                internal_date,
                rfc822_size,
                mailbox: Some(mailbox.clone()),
                headers,
            }),
            Err(err) => report.errors.push(MailboxScanError {
                source: format!("imap:{}:uid:{}", mailbox, uid.unwrap_or_default()),
                error: err.to_string(),
            }),
        }

        if let Some((cb, every)) = progress.as_mut() {
            if report.messages.len() % *every == 0 {
                let elapsed = t0.elapsed().as_secs_f64().max(0.001);
                let rate = (report.messages.len() as f64) / elapsed;
                (cb)(report.messages.len(), rate);
            }
        }
    }

    if let Some((cb, _every)) = progress.as_mut() {
        let elapsed = t0.elapsed().as_secs_f64().max(0.001);
        let rate = (report.messages.len() as f64) / elapsed;
        (cb)(report.messages.len(), rate);
    }

    Ok(report)
}

pub fn sync_imap_with_backend(
    account: &ImapAccountConfig,
    backend: &mut dyn ImapStateBackend,
    options: ImapSyncOptions,
) -> Result<ImapSyncResult> {
    let account_id = account
        .account_id
        .clone()
        .unwrap_or_else(|| format!("{}@{}", account.username, account.host));
    let mailbox = account.mailbox.trim().to_string();

    let prior = backend.load_state(&account_id, &mailbox)?;
    let res = sync_imap_delta(account, &prior, options)?;
    backend.save_state(&account_id, &mailbox, &res.state)?;
    Ok(res)
}

pub fn sync_imap_delta(
    account: &ImapAccountConfig,
    prior: &ImapSyncState,
    options: ImapSyncOptions,
) -> Result<ImapSyncResult> {
    let account_id = account
        .account_id
        .clone()
        .unwrap_or_else(|| format!("{}@{}", account.username, account.host));
    let mailbox = account.mailbox.trim().to_string();

    let mut state = prior.clone();

    if options.force_full {
        state.last_uid = 0;
        state.highest_modseq = None;
    }

    let mut session = connect(account)?;
    let caps = session.capabilities().context("imap capability")?;
    let supports_modseq = caps.has_str("CONDSTORE") || caps.has_str("QRESYNC");
    let supports_qresync = caps.has_str("QRESYNC");
    if caps.has_str("ENABLE") && supports_qresync {
        // Best-effort: some servers require explicit ENABLE before they will emit VANISHED.
        let _ = session.run_command_and_check_ok("ENABLE QRESYNC");
    }
    // `Capability` does not implement Display; debug strings are sufficient for diagnostics.
    let server_capabilities = caps.iter().map(|c| format!("{c:?}")).collect::<Vec<_>>();

    let mailbox_info = session
        .select(&mailbox)
        .with_context(|| format!("imap select {mailbox}"))?;

    let uidvalidity = mailbox_info.uid_validity.map(|v| v as u32);
    if let (Some(prev), Some(next)) = (state.uidvalidity, uidvalidity) {
        if prev != next {
            state.last_uid = 0;
            state.highest_modseq = None;
        }
    }
    state.uidvalidity = uidvalidity.or(state.uidvalidity);

    // IMPORTANT: do not advance the modseq checkpoint before fetching CHANGEDSINCE,
    // otherwise we can skip changes that happened since the previous checkpoint.
    let changed_since = state.highest_modseq.unwrap_or(0);
    let mailbox_highest_modseq = mailbox_info.highest_mod_seq;

    let mut messages: Vec<SyncedEmail> = Vec::new();
    let mut vanished_uids: Vec<u32> = Vec::new();
    let mut mailbox_uids: Option<Vec<u32>> = None;
    let mut used_vanished = false;

    if options.unseen_only {
        // `UNSEEN` is a selection mode (mutable flag). We intentionally do not advance
        // `last_uid` / `highest_modseq` checkpoints here.
        let mut uids = session
            .uid_search("UNSEEN")
            .context("imap uid_search unseen")?
            .into_iter()
            .collect::<Vec<u32>>();
        uids.sort_unstable();

        // Avoid overlong commands: fetch in chunks.
        let chunk_size = 200usize;
        for chunk in uids.chunks(chunk_size) {
            if chunk.is_empty() {
                continue;
            }
            let set = chunk
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let query = if supports_modseq {
                "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ)"
            } else {
                "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE)"
            };

            let fetches = session
                .uid_fetch(set, query)
                .context("imap uid_fetch unseen")?;

            for fetch in fetches.iter() {
                let Some(uid) = fetch.uid else {
                    continue;
                };
                let raw = fetch.body().unwrap_or_default().to_vec();
                if raw.is_empty() {
                    continue;
                }
                let parsed = parse_rfc822(&raw).context("parse rfc822")?;
                let flags = fetch
                    .flags()
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>();
                let internal_date = fetch.internal_date().map(|d| d.to_rfc3339());
                let modseq = fetch.mod_seq();
                let rfc822_size = fetch.size.map(|s| s as u32);

                messages.push(SyncedEmail {
                    uid,
                    internal_date,
                    flags,
                    modseq,
                    rfc822_size,
                    parsed,
                    raw,
                });
            }
        }

        state.last_sync_ms = now_ms();

        let _ = session.logout();

        return Ok(ImapSyncResult {
            account_id,
            mailbox,
            state,
            supports_modseq,
            server_capabilities,
            messages,
            vanished_uids,
            mailbox_uids,
        });
    }

    // Use MODSEQ-based incremental sync only once we have an established checkpoint.
    if supports_modseq && changed_since > 0 {
        // Most servers expect the attribute list to be wrapped in parentheses.
        // If the server supports QRESYNC, request VANISHED so we can record expunges.
        let fetches = if supports_qresync {
            let query = format!(
                "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ) (CHANGEDSINCE {changed_since} VANISHED)"
            );
            match session.uid_fetch("1:*", query) {
                Ok(fetches) => {
                    used_vanished = true;
                    for resp in session.take_all_unsolicited() {
                        if let imap::types::UnsolicitedResponse::Vanished { uids, .. } = resp {
                            for range in uids {
                                vanished_uids.extend(range);
                            }
                        }
                    }
                    fetches
                }
                Err(_) => {
                    // Fallback: some servers advertise QRESYNC but reject VANISHED unless fully
                    // enabled via SELECT (QRESYNC) parameters.
                    let query = format!(
                        "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ) (CHANGEDSINCE {changed_since})"
                    );
                    session
                        .uid_fetch("1:*", query)
                        .context("imap uid_fetch changed since (fallback without VANISHED)")?
                }
            }
        } else {
            let query = format!(
                "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ) (CHANGEDSINCE {changed_since})"
            );
            session
                .uid_fetch("1:*", query)
                .context("imap uid_fetch changed since")?
        };

        let mut max_seen_modseq = changed_since;
        for fetch in fetches.iter() {
            let Some(uid) = fetch.uid else {
                continue;
            };
            let raw = fetch.body().unwrap_or_default().to_vec();
            if raw.is_empty() {
                continue;
            }
            let parsed = parse_rfc822(&raw).context("parse rfc822")?;
            let flags = fetch
                .flags()
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>();
            let internal_date = fetch.internal_date().map(|d| d.to_rfc3339());
            let modseq = fetch.mod_seq();
            let rfc822_size = fetch.size.map(|s| s as u32);

            if let Some(ms) = fetch.mod_seq() {
                max_seen_modseq = max_seen_modseq.max(ms);
            }

            messages.push(SyncedEmail {
                uid,
                internal_date,
                flags,
                modseq,
                rfc822_size,
                parsed,
                raw,
            });
            state.last_uid = state.last_uid.max(uid);
        }

        // Advance checkpoint to the max modseq we observed (or mailbox HIGHESTMODSEQ if higher).
        let mut next = max_seen_modseq;
        if let Some(h) = mailbox_highest_modseq {
            next = next.max(h);
        }
        state.highest_modseq = Some(next);
    } else {
        let start = state.last_uid.saturating_add(1).max(1);
        let seq = format!("{start}:*");
        let fetches = session
            .uid_fetch(seq, "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE)")
            .context("imap uid_fetch new messages")?;
        for fetch in fetches.iter() {
            let Some(uid) = fetch.uid else {
                continue;
            };
            let raw = fetch.body().unwrap_or_default().to_vec();
            if raw.is_empty() {
                continue;
            }
            let parsed = parse_rfc822(&raw).context("parse rfc822")?;
            let flags = fetch
                .flags()
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>();
            let internal_date = fetch.internal_date().map(|d| d.to_rfc3339());
            let modseq = fetch.mod_seq();
            let rfc822_size = fetch.size.map(|s| s as u32);
            messages.push(SyncedEmail {
                uid,
                internal_date,
                flags,
                modseq,
                rfc822_size,
                parsed,
                raw,
            });
            state.last_uid = state.last_uid.max(uid);
        }

        // If the server supports MODSEQ, record its current highest value as our initial checkpoint.
        if supports_modseq {
            if let Some(h) = mailbox_highest_modseq {
                state.highest_modseq = Some(state.highest_modseq.unwrap_or(0).max(h));
            }
        }
    }

    vanished_uids.sort_unstable();
    vanished_uids.dedup();

    if !used_vanished {
        let mut uids = session
            .uid_search("ALL")
            .context("imap uid_search all")?
            .into_iter()
            .collect::<Vec<u32>>();
        uids.sort_unstable();
        mailbox_uids = Some(uids);
    }

    state.last_sync_ms = now_ms();

    let _ = session.logout();

    Ok(ImapSyncResult {
        account_id,
        mailbox,
        state,
        supports_modseq,
        server_capabilities,
        messages,
        vanished_uids,
        mailbox_uids,
    })
}

fn connect(account: &ImapAccountConfig) -> Result<imap::Session<imap::Connection>> {
    if !account.tls {
        return Err(anyhow!("non-TLS IMAP is not supported"));
    }
    let mut builder = imap::ClientBuilder::new(&account.host, account.port);
    if account.danger_skip_tls_verify {
        builder = builder.danger_skip_tls_verify(true);
    }
    let client = builder
        .connect()
        .with_context(|| format!("connect imap {}:{}", account.host, account.port))?;
    let session = client
        .login(&account.username, &account.password)
        .map_err(|e| e.0)
        .context("imap login")?;
    Ok(session)
}

fn default_port() -> u16 {
    993
}

fn default_tls() -> bool {
    true
}

fn default_mailbox() -> String {
    "INBOX".to_string()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

