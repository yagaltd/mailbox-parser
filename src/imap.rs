use std::collections::HashMap;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_gm_thrid: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub x_gm_labels: Vec<String>,
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

pub fn scan_imap_headers(
    account: &ImapAccountConfig,
    options: ImapScanOptions,
) -> Result<MailboxScanReport> {
    scan_imap_headers_inner(account, options, None)
}

pub fn scan_imap_headers_with_progress<F: FnMut(usize, f64)>(
    account: &ImapAccountConfig,
    options: ImapScanOptions,
    progress_every: usize,
    mut on_progress: F,
) -> Result<MailboxScanReport> {
    scan_imap_headers_inner(
        account,
        options,
        Some((&mut on_progress, progress_every.max(1))),
    )
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
        let internal_date = fetch.internal_date().map(|d| d.to_rfc3339());
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
    sync_imap_delta_inner(account, prior, options, None)
}

pub fn sync_imap_delta_streaming(
    account: &ImapAccountConfig,
    prior: &ImapSyncState,
    options: ImapSyncOptions,
    on_message: &mut dyn FnMut(SyncedEmail) -> Result<()>,
) -> Result<ImapSyncResult> {
    sync_imap_delta_inner(account, prior, options, Some(on_message))
}

fn sync_imap_delta_inner(
    account: &ImapAccountConfig,
    prior: &ImapSyncState,
    options: ImapSyncOptions,
    mut on_message: Option<&mut dyn FnMut(SyncedEmail) -> Result<()>>,
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
    let supports_gmail_ext = caps.has_str("X-GM-EXT-1");
    if caps.has_str("ENABLE") && supports_qresync {
        let _ = session.run_command_and_check_ok("ENABLE QRESYNC");
    }
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

    let changed_since = state.highest_modseq.unwrap_or(0);
    let mailbox_highest_modseq = mailbox_info.highest_mod_seq;

    let mut messages: Vec<SyncedEmail> = Vec::new();
    let mut vanished_uids: Vec<u32> = Vec::new();
    let mut mailbox_uids: Option<Vec<u32>> = None;
    let mut used_vanished = false;

    let mut emit = |email: SyncedEmail| -> Result<()> {
        if let Some(cb) = on_message.as_mut() {
            cb(email)
        } else {
            messages.push(email);
            Ok(())
        }
    };

    if options.unseen_only {
        let mut uids = session
            .uid_search("UNSEEN")
            .context("imap uid_search unseen")?
            .into_iter()
            .collect::<Vec<u32>>();
        uids.sort_unstable();

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

            let query = build_fetch_attrs(supports_modseq, supports_gmail_ext);

            let fetches = session
                .uid_fetch(set.clone(), query)
                .context("imap uid_fetch unseen")?;
            let gmail_meta = fetch_gmail_metadata_map(
                &mut session,
                &set,
                query,
                supports_gmail_ext,
                "imap uid_fetch unseen gmail metadata",
            )?;

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
                let (x_gm_thrid, x_gm_labels) = gmail_fields_for_fetch(fetch, &gmail_meta);

                emit(SyncedEmail {
                    uid,
                    internal_date,
                    flags,
                    x_gm_thrid,
                    x_gm_labels,
                    modseq,
                    rfc822_size,
                    parsed,
                    raw,
                })?;
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

    if supports_modseq && changed_since > 0 {
        let query_with_vanished = build_changed_since_query(changed_since, true, supports_gmail_ext);
        let query_without_vanished =
            build_changed_since_query(changed_since, false, supports_gmail_ext);
        let fetches = if supports_qresync {
            match session.uid_fetch("1:*", query_with_vanished.clone()) {
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
                Err(_) => session
                    .uid_fetch("1:*", query_without_vanished.clone())
                    .context("imap uid_fetch changed since (fallback without VANISHED)")?,
            }
        } else {
            session
                .uid_fetch("1:*", query_without_vanished.clone())
                .context("imap uid_fetch changed since")?
        };

        let metadata_query = if supports_qresync && used_vanished {
            query_with_vanished.as_str()
        } else {
            query_without_vanished.as_str()
        };
        let gmail_meta = fetch_gmail_metadata_map(
            &mut session,
            "1:*",
            metadata_query,
            supports_gmail_ext,
            "imap uid_fetch changed since gmail metadata",
        )?;

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
            let (x_gm_thrid, x_gm_labels) = gmail_fields_for_fetch(fetch, &gmail_meta);

            if let Some(ms) = fetch.mod_seq() {
                max_seen_modseq = max_seen_modseq.max(ms);
            }

            emit(SyncedEmail {
                uid,
                internal_date,
                flags,
                x_gm_thrid,
                x_gm_labels,
                modseq,
                rfc822_size,
                parsed,
                raw,
            })?;
            state.last_uid = state.last_uid.max(uid);
        }

        let mut next = max_seen_modseq;
        if let Some(h) = mailbox_highest_modseq {
            next = next.max(h);
        }
        state.highest_modseq = Some(next);
    } else {
        let start = state.last_uid.saturating_add(1).max(1);
        let seq = format!("{start}:*");
        let query = build_fetch_attrs(false, supports_gmail_ext);
        let fetches = session
            .uid_fetch(seq.clone(), query)
            .context("imap uid_fetch new messages")?;
        let gmail_meta = fetch_gmail_metadata_map(
            &mut session,
            &seq,
            query,
            supports_gmail_ext,
            "imap uid_fetch new messages gmail metadata",
        )?;
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
            let (x_gm_thrid, x_gm_labels) = gmail_fields_for_fetch(fetch, &gmail_meta);
            emit(SyncedEmail {
                uid,
                internal_date,
                flags,
                x_gm_thrid,
                x_gm_labels,
                modseq,
                rfc822_size,
                parsed,
                raw,
            })?;
            state.last_uid = state.last_uid.max(uid);
        }

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

fn build_fetch_attrs(supports_modseq: bool, supports_gmail_ext: bool) -> &'static str {
    match (supports_modseq, supports_gmail_ext) {
        (true, true) => "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ X-GM-THRID X-GM-LABELS)",
        (true, false) => "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ)",
        (false, true) => "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE X-GM-THRID X-GM-LABELS)",
        (false, false) => "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE)",
    }
}

fn build_changed_since_query(
    changed_since: u64,
    with_vanished: bool,
    supports_gmail_ext: bool,
) -> String {
    let attrs = build_fetch_attrs(true, supports_gmail_ext);
    if with_vanished {
        format!("{attrs} (CHANGEDSINCE {changed_since} VANISHED)")
    } else {
        format!("{attrs} (CHANGEDSINCE {changed_since})")
    }
}

fn fetch_gmail_metadata_map(
    session: &mut imap::Session<imap::Connection>,
    sequence_set: &str,
    query: &str,
    supports_gmail_ext: bool,
    context_label: &str,
) -> Result<HashMap<u32, (Option<u64>, Vec<String>)>> {
    if !supports_gmail_ext {
        return Ok(HashMap::new());
    }

    let raw = session
        .run_command_and_read_response(format!("UID FETCH {sequence_set} {query}"))
        .with_context(|| context_label.to_string())?;

    parse_gmail_metadata_from_uid_fetch_response(&raw)
}

fn parse_gmail_metadata_from_uid_fetch_response(
    raw: &[u8],
) -> Result<HashMap<u32, (Option<u64>, Vec<String>)>> {
    let mut out = HashMap::new();
    let mut input = raw;

    while !input.is_empty() {
        let parsed = imap_proto::parser::parse_response(input);
        let (rest, response) = match parsed {
            Ok(v) => v,
            Err(_) => break,
        };

        if let imap_proto::types::Response::Fetch(_, attrs) = response {
            let mut uid: Option<u32> = None;
            let mut x_gm_thrid: Option<u64> = None;
            let mut x_gm_labels: Vec<String> = Vec::new();

            for attr in attrs {
                match attr {
                    imap_proto::types::AttributeValue::Uid(v) => uid = Some(v),
                    imap_proto::types::AttributeValue::GmailThrId(v) => x_gm_thrid = Some(v),
                    imap_proto::types::AttributeValue::GmailLabels(labels) => {
                        x_gm_labels = labels.into_iter().map(|v| v.into_owned()).collect();
                    }
                    _ => {}
                }
            }

            if let Some(uid) = uid {
                out.insert(uid, (x_gm_thrid, x_gm_labels));
            }
        }

        input = rest;
    }

    Ok(out)
}

fn gmail_fields_for_fetch(
    fetch: &imap::types::Fetch<'_>,
    metadata_map: &HashMap<u32, (Option<u64>, Vec<String>)>,
) -> (Option<u64>, Vec<String>) {
    let labels_from_fetch = fetch
        .gmail_labels()
        .map(|it| it.map(|x| x.to_string()).collect::<Vec<_>>())
        .unwrap_or_default();

    if let Some(uid) = fetch.uid {
        if let Some((thrid, labels)) = metadata_map.get(&uid) {
            let merged = if labels.is_empty() {
                labels_from_fetch
            } else {
                labels.clone()
            };
            return (*thrid, merged);
        }
    }

    (None, labels_from_fetch)
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

#[cfg(test)]
mod tests {
    use super::{
        build_changed_since_query, build_fetch_attrs, parse_gmail_metadata_from_uid_fetch_response,
    };

    #[test]
    fn fetch_attrs_include_gmail_when_supported() {
        let attrs = build_fetch_attrs(true, true);
        assert_eq!(
            attrs,
            "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE MODSEQ X-GM-THRID X-GM-LABELS)"
        );
    }

    #[test]
    fn fetch_attrs_exclude_gmail_when_not_supported() {
        let attrs = build_fetch_attrs(false, false);
        assert_eq!(attrs, "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE)");
    }

    #[test]
    fn fetch_attrs_include_gmail_without_modseq() {
        let attrs = build_fetch_attrs(false, true);
        assert_eq!(
            attrs,
            "(UID RFC822 INTERNALDATE FLAGS RFC822.SIZE X-GM-THRID X-GM-LABELS)"
        );
    }

    #[test]
    fn changed_since_query_toggles_vanished() {
        let with_vanished = build_changed_since_query(12, true, true);
        assert!(with_vanished.contains("CHANGEDSINCE 12 VANISHED"));

        let without_vanished = build_changed_since_query(12, false, true);
        assert!(without_vanished.contains("CHANGEDSINCE 12)"));
        assert!(!without_vanished.contains("VANISHED"));
    }

    #[test]
    fn parse_gmail_metadata_from_fetch_response_extracts_thrid_and_labels() {
        let raw = b"* 5 FETCH (UID 42 X-GM-THRID 1278455344230334865 X-GM-LABELS (\\Important inbox) RFC822.SIZE 12)\r\nA1 OK FETCH done\r\n";
        let map = parse_gmail_metadata_from_uid_fetch_response(raw).expect("parse response");
        let (thrid, labels) = map.get(&42).expect("uid 42 metadata");
        assert_eq!(*thrid, Some(1278455344230334865));
        assert_eq!(labels, &vec!["\\Important".to_string(), "inbox".to_string()]);
    }
}
