mod imap;
mod mbox;
mod email_text;
mod canonical;

pub use imap::{
    ImapAccountConfig, ImapConfigFile, ImapScanOptions, ImapStateBackend, ImapSyncOptions,
    ImapSyncResult, ImapSyncState, SyncedEmail, scan_imap_headers,
    scan_imap_headers_with_progress, sync_imap_delta, sync_imap_with_backend,
};
pub use mbox::{
    MboxMessage, MboxParseError, MboxParseOptions, MboxParseReport, MboxReadOptions,
    iter_mbox_messages, parse_mbox_file, scan_mbox_file_headers_only, scan_mbox_headers,
    scan_mbox_headers_with_progress,
};
pub use email_text::{EmailBlock, EmailBlockKind, forwarded_message_ids, normalize_email_text, reply_text, segment_email_body};
pub use canonical::{CanonicalAttachment, CanonicalMessage, CanonicalThread, canonicalize_threads};

use anyhow::{Result, anyhow};
pub use contacts::EmailAddress;
use mail_parser::{Message, MessageParser, MimeHeaders};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedEmail {
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub subject: Option<String>,
    pub date: Option<String>,
    pub date_raw: Option<String>,
    pub from: Vec<EmailAddress>,
    pub to: Vec<EmailAddress>,
    pub cc: Vec<EmailAddress>,
    pub bcc: Vec<EmailAddress>,
    pub reply_to: Vec<EmailAddress>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub body_canonical: String,
    pub attachments: Vec<ParsedAttachment>,
    pub forwarded_messages: Vec<ParsedForwardedMessage>,

    #[serde(default)]
    pub raw_headers: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedEmailHeaders {
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub subject: Option<String>,
    pub date: Option<String>,
    pub date_raw: Option<String>,
    pub from: Vec<EmailAddress>,
    pub to: Vec<EmailAddress>,
    pub cc: Vec<EmailAddress>,
    pub bcc: Vec<EmailAddress>,
    pub reply_to: Vec<EmailAddress>,
    #[serde(default)]
    pub raw_headers: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MailboxScanMessage {
    pub uid: Option<u32>,
    pub internal_date: Option<String>,
    pub rfc822_size: Option<u32>,
    pub mailbox: Option<String>,
    pub headers: ParsedEmailHeaders,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MailboxScanError {
    pub source: String,
    pub error: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MailboxScanReport {
    pub messages: Vec<MailboxScanMessage>,
    pub errors: Vec<MailboxScanError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedAttachment {
    pub filename: Option<String>,
    pub mime_type: String,
    pub size: usize,
    pub sha256: String,
    pub content_id: Option<String>,
    pub content_disposition: Option<String>,

    #[serde(skip_serializing, skip_deserializing)]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedForwardedMessage {
    pub message_id: Option<String>,
    pub subject: Option<String>,
    pub from: Vec<EmailAddress>,
    pub date: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedThreadMessage {
    pub message_key: String,
    pub uid: Option<u32>,
    pub internal_date: Option<String>,
    pub email: ParsedEmail,
}

#[derive(Clone, Debug)]
pub struct MailMessage {
    pub uid: Option<u32>,
    pub internal_date: Option<String>,
    pub flags: Vec<String>,
    pub parsed: ParsedEmail,
    pub raw: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedThread {
    pub thread_id: String,
    pub messages: Vec<ParsedThreadMessage>,
}

pub fn parse_rfc822(bytes: &[u8]) -> Result<ParsedEmail> {
    let message = MessageParser::default()
        .parse(bytes)
        .ok_or_else(|| anyhow!("failed to parse RFC822 message"))?;

    let raw_headers = parse_raw_headers(bytes);
    let message_id = extract_message_id(&raw_headers);
    let in_reply_to = extract_in_reply_to(&raw_headers);
    let references = extract_references(&raw_headers);

    let subject = message.subject().map(|s| s.to_string());
    let date = message.date().map(|d| d.to_rfc3339());
    let date_raw = raw_headers.get("date").cloned();

    let from = collect_addresses(message.from());
    let to = collect_addresses(message.to());
    let cc = collect_addresses(message.cc());
    let bcc = collect_addresses(message.bcc());
    let reply_to = collect_addresses(message.reply_to());

    let body_html = message.body_html(0).map(|s| s.to_string());
    let body_text = message.body_text(0).map(|s| s.to_string());
    let body_canonical = build_canonical_body(body_text.as_deref(), body_html.as_deref());

    let (attachments, mut forwarded_messages) = collect_attachments_and_forwards(&message)?;
    if !body_canonical.trim().is_empty() {
        let blocks = crate::email_text::segment_email_body(&body_canonical);
        for id in crate::email_text::forwarded_message_ids(&body_canonical, &blocks) {
            let norm = normalize_message_id(&id);
            if norm.trim().is_empty() {
                continue;
            }
            if forwarded_messages
                .iter()
                .any(|f| f.message_id.as_deref() == Some(norm.as_str()))
            {
                continue;
            }
            forwarded_messages.push(ParsedForwardedMessage {
                message_id: Some(norm),
                subject: None,
                from: Vec::new(),
                date: None,
            });
        }
    }

    Ok(ParsedEmail {
        message_id,
        in_reply_to,
        references,
        subject,
        date,
        date_raw,
        from,
        to,
        cc,
        bcc,
        reply_to,
        body_text,
        body_html,
        body_canonical,
        attachments,
        forwarded_messages,
        raw_headers,
    })
}

pub fn parse_rfc822_headers(bytes: &[u8]) -> Result<ParsedEmailHeaders> {
    let header_bytes = header_slice(bytes);
    let mut msg_buf = header_bytes.to_vec();
    msg_buf.extend_from_slice(b"\r\n\r\n");

    let message = MessageParser::default()
        .parse(&msg_buf)
        .ok_or_else(|| anyhow!("failed to parse RFC822 headers"))?;

    let raw_headers = parse_raw_headers(header_bytes);
    let message_id = extract_message_id(&raw_headers);
    let in_reply_to = extract_in_reply_to(&raw_headers);
    let references = extract_references(&raw_headers);

    let subject = message.subject().map(|s| s.to_string());
    let date = message.date().map(|d| d.to_rfc3339());
    let date_raw = raw_headers.get("date").cloned();

    let from = collect_addresses(message.from());
    let to = collect_addresses(message.to());
    let cc = collect_addresses(message.cc());
    let bcc = collect_addresses(message.bcc());
    let reply_to = collect_addresses(message.reply_to());

    Ok(ParsedEmailHeaders {
        message_id,
        in_reply_to,
        references,
        subject,
        date,
        date_raw,
        from,
        to,
        cc,
        bcc,
        reply_to,
        raw_headers,
    })
}

pub fn normalize_message_id(s: &str) -> String {
    s.trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim()
        .to_ascii_lowercase()
}

pub fn thread_root_id(message_id: Option<&str>, in_reply_to: Option<&str>, references: &[String]) -> Option<String> {
    if let Some(first) = references.first() {
        if !first.trim().is_empty() {
            return Some(first.to_string());
        }
    }
    if let Some(v) = in_reply_to {
        if !v.trim().is_empty() {
            return Some(v.to_string());
        }
    }
    message_id.map(|s| s.to_string())
}

pub fn thread_id_for(root: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(normalize_message_id(root).as_bytes());
    let hex = format!("{:x}", h.finalize());
    hex.chars().take(16).collect()
}

pub fn message_key_for(message_id: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(normalize_message_id(message_id).as_bytes());
    let hex = format!("{:x}", h.finalize());
    hex.chars().take(16).collect()
}

fn normalize_subject_for_threading(subject: Option<&str>) -> Option<String> {
    let mut s = subject?.trim().to_string();
    if s.is_empty() {
        return None;
    }
    // Strip common prefixes repeatedly (gmail-ish).
    loop {
        let lower = s.to_ascii_lowercase();
        let stripped = if let Some(rest) = lower.strip_prefix("re:") {
            Some(rest)
        } else if let Some(rest) = lower.strip_prefix("fw:") {
            Some(rest)
        } else if let Some(rest) = lower.strip_prefix("fwd:") {
            Some(rest)
        } else {
            None
        };
        if let Some(rest) = stripped {
            s = rest.trim().to_string();
            continue;
        }
        break;
    }
    if s.is_empty() {
        return None;
    }

    // Collapse whitespace.
    let mut out = String::with_capacity(s.len());
    let mut prev_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_ws {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(ch);
            prev_ws = false;
        }
    }
    Some(out.trim().to_ascii_lowercase())
}

fn participants_key(email: &ParsedEmail) -> String {
    use std::collections::BTreeSet;
    let mut set: BTreeSet<String> = BTreeSet::new();
    for a in email
        .from
        .iter()
        .chain(email.to.iter())
        .chain(email.cc.iter())
        .chain(email.bcc.iter())
        .chain(email.reply_to.iter())
    {
        let addr = a.address.trim().to_ascii_lowercase();
        if !addr.is_empty() {
            set.insert(addr);
        }
    }
    set.into_iter().collect::<Vec<_>>().join(",")
}

pub fn thread_messages(messages: &[SyncedEmail]) -> Vec<ParsedThread> {
    use std::collections::HashMap;

    let mut by_thread: HashMap<String, Vec<ParsedThreadMessage>> = HashMap::new();
    for msg in messages {
        let fallback = fallback_message_id(&msg.parsed);
        let msg_id = msg
            .parsed
            .message_id
            .as_deref()
            .unwrap_or_else(|| fallback.as_str());

        let root = if msg.parsed.message_id.is_none()
            && msg.parsed.in_reply_to.is_none()
            && msg.parsed.references.is_empty()
        {
            // Gmail-ish fallback when threading headers are missing.
            if let Some(subj) = normalize_subject_for_threading(msg.parsed.subject.as_deref()) {
                let p = participants_key(&msg.parsed);
                if !p.is_empty() {
                    format!("subject:{subj}|participants:{p}")
                } else {
                    subj
                }
            } else {
                msg_id.to_string()
            }
        } else {
            thread_root_id(
                Some(msg_id),
                msg.parsed.in_reply_to.as_deref(),
                &msg.parsed.references,
            )
            .unwrap_or_else(|| msg_id.to_string())
        };
        let thread_id = thread_id_for(&root);
        let message_key = message_key_for(msg_id);

        by_thread
            .entry(thread_id)
            .or_default()
            .push(ParsedThreadMessage {
                message_key,
                uid: Some(msg.uid),
                internal_date: msg.internal_date.clone(),
                email: msg.parsed.clone(),
            });
    }

    let mut threads: Vec<ParsedThread> = by_thread
        .into_iter()
        .map(|(thread_id, mut messages)| {
            messages.sort_by(|a, b| message_sort_key(a).cmp(&message_sort_key(b)));
            ParsedThread { thread_id, messages }
        })
        .collect();

    threads.sort_by(|a, b| thread_sort_key(a).cmp(&thread_sort_key(b)));
    threads
}

pub fn thread_messages_from_mail_messages(messages: &[MailMessage]) -> Vec<ParsedThread> {
    use std::collections::HashMap;

    let mut by_thread: HashMap<String, Vec<ParsedThreadMessage>> = HashMap::new();
    for msg in messages {
        let fallback = fallback_message_id(&msg.parsed);
        let msg_id = msg
            .parsed
            .message_id
            .as_deref()
            .unwrap_or_else(|| fallback.as_str());

        let root = if msg.parsed.message_id.is_none()
            && msg.parsed.in_reply_to.is_none()
            && msg.parsed.references.is_empty()
        {
            if let Some(subj) = normalize_subject_for_threading(msg.parsed.subject.as_deref()) {
                let p = participants_key(&msg.parsed);
                if !p.is_empty() {
                    format!("subject:{subj}|participants:{p}")
                } else {
                    subj
                }
            } else {
                msg_id.to_string()
            }
        } else {
            thread_root_id(
                Some(msg_id),
                msg.parsed.in_reply_to.as_deref(),
                &msg.parsed.references,
            )
            .unwrap_or_else(|| msg_id.to_string())
        };
        let thread_id = thread_id_for(&root);
        let message_key = message_key_for(msg_id);

        by_thread
            .entry(thread_id)
            .or_default()
            .push(ParsedThreadMessage {
                message_key,
                uid: msg.uid,
                internal_date: msg.internal_date.clone(),
                email: msg.parsed.clone(),
            });
    }

    let mut threads: Vec<ParsedThread> = by_thread
        .into_iter()
        .map(|(thread_id, mut messages)| {
            messages.sort_by(|a, b| message_sort_key(a).cmp(&message_sort_key(b)));
            ParsedThread { thread_id, messages }
        })
        .collect();

    threads.sort_by(|a, b| thread_sort_key(a).cmp(&thread_sort_key(b)));
    threads
}

fn message_sort_key(msg: &ParsedThreadMessage) -> (i64, u32, String) {
    let ts = msg
        .email
        .date
        .as_deref()
        .and_then(parse_rfc3339_ms)
        .or_else(|| msg.internal_date.as_deref().and_then(parse_rfc3339_ms))
        .unwrap_or(0);
    let uid = msg.uid.unwrap_or(0);
    (ts, uid, msg.message_key.clone())
}

fn thread_sort_key(thread: &ParsedThread) -> (i64, String) {
    let ts = thread
        .messages
        .iter()
        .map(|m| message_sort_key(m).0)
        .max()
        .unwrap_or(0);
    (ts, thread.thread_id.clone())
}

fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn fallback_message_id(email: &ParsedEmail) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    if let Some(subject) = email.subject.as_deref() {
        h.update(subject.as_bytes());
    }
    if let Some(date) = email.date.as_deref() {
        h.update(date.as_bytes());
    }
    for addr in &email.from {
        h.update(addr.address.as_bytes());
    }
    let snippet: String = email.body_canonical.chars().take(512).collect();
    h.update(snippet.as_bytes());
    format!("fallback:{}", format!("{:x}", h.finalize()))
}

fn header_slice(bytes: &[u8]) -> &[u8] {
    if bytes.len() < 2 {
        return bytes;
    }
    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
        return &bytes[..pos];
    }
    if let Some(pos) = bytes.windows(2).position(|w| w == b"\n\n") {
        return &bytes[..pos];
    }
    bytes
}

fn collect_addresses(list: Option<&mail_parser::Address<'_>>) -> Vec<EmailAddress> {
    let mut out = Vec::new();
    let Some(list) = list else {
        return out;
    };

    match list {
        mail_parser::Address::List(addrs) => {
            for addr in addrs {
                if let Some(parsed) = email_from_addr(addr) {
                    out.push(parsed);
                }
            }
        }
        mail_parser::Address::Group(groups) => {
            for group in groups {
                for addr in &group.addresses {
                    if let Some(parsed) = email_from_addr(addr) {
                        out.push(parsed);
                    }
                }
            }
        }
    }

    out
}

fn email_from_addr(addr: &mail_parser::Addr<'_>) -> Option<EmailAddress> {
    let address = addr.address.as_deref()?;
    let name = addr.name.as_deref().map(|s| s.to_string());
    EmailAddress::new(address, name)
}

fn build_canonical_body(text: Option<&str>, html: Option<&str>) -> String {
    let mut out = String::new();
    if let Some(t) = text.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        out = t.to_string();
    } else if let Some(h) = html.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let t = html_to_text(h);
        if !t.trim().is_empty() {
            out = t;
        }
    }
    if out.trim().is_empty() {
        return String::new();
    }
    crate::email_text::normalize_email_text(&out)
}

fn collect_attachments_and_forwards(
    message: &Message<'_>,
) -> Result<(Vec<ParsedAttachment>, Vec<ParsedForwardedMessage>)> {
    let mut attachments = Vec::new();
    let mut forwarded = Vec::new();

    let mut idx = 0usize;
    loop {
        let Some(att) = message.attachment(idx) else {
            break;
        };

        let mut is_forward = false;
        if let Some(nested) = att.message() {
            let parsed = ParsedForwardedMessage {
                message_id: nested.message_id().map(|s| normalize_message_id(s)),
                subject: nested.subject().map(|s| s.to_string()),
                from: collect_addresses(nested.from()),
                date: nested.date().map(|d| d.to_rfc3339()),
            };
            forwarded.push(parsed);
            is_forward = true;
        }

        if is_forward {
            idx += 1;
            continue;
        }

        let bytes = att.contents().to_vec();
        if !bytes.is_empty() {
            let sha256 = sha256_hex(&bytes);
            let mime_type = att
                .content_type()
                .map(|ct| {
                    let subtype = ct.subtype().unwrap_or("octet-stream");
                    format!("{}/{}", ct.ctype(), subtype)
                })
                .unwrap_or_else(|| "application/octet-stream".to_string());
            let filename = att.attachment_name().map(|s| s.to_string());
            let content_id = att.content_id().map(|s| s.to_string());
            let content_disposition =
                att.content_disposition().map(|s| s.ctype().to_string());

            attachments.push(ParsedAttachment {
                filename,
                mime_type,
                size: bytes.len(),
                sha256,
                content_id,
                content_disposition,
                bytes,
            });
        }

        idx += 1;
    }

    Ok((attachments, forwarded))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn parse_raw_headers(bytes: &[u8]) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    let raw = String::from_utf8_lossy(bytes);
    let normalized = raw.replace("\r\n", "\n");
    let mut cur_key = String::new();
    let mut cur_val = String::new();

    let flush = |k: &str, v: &str, out: &mut std::collections::BTreeMap<String, String>| {
        let k = k.trim().to_ascii_lowercase();
        let v = v.trim();
        if k.is_empty() || v.is_empty() {
            return;
        }
        out.insert(k, v.to_string());
    };

    for line in normalized.lines() {
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if !cur_key.is_empty() {
                cur_val.push(' ');
                cur_val.push_str(line.trim());
            }
            continue;
        }
        if !cur_key.is_empty() {
            flush(&cur_key, &cur_val, &mut out);
        }
        cur_key.clear();
        cur_val.clear();
        if let Some((k, v)) = line.split_once(':') {
            cur_key = k.to_string();
            cur_val = v.trim().to_string();
        }
    }
    if !cur_key.is_empty() {
        flush(&cur_key, &cur_val, &mut out);
    }

    out
}

fn extract_message_id(headers: &std::collections::BTreeMap<String, String>) -> Option<String> {
    headers
        .get("message-id")
        .map(|s| normalize_message_id(s))
        .filter(|s| !s.is_empty())
}

fn extract_in_reply_to(headers: &std::collections::BTreeMap<String, String>) -> Option<String> {
    headers
        .get("in-reply-to")
        .map(|s| normalize_message_id(s))
        .filter(|s| !s.is_empty())
}

fn extract_references(headers: &std::collections::BTreeMap<String, String>) -> Vec<String> {
    let Some(raw) = headers.get("references") else {
        return Vec::new();
    };
    raw.split(|c: char| c.is_whitespace() || c == ',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| normalize_message_id(t))
        .filter(|t| !t.is_empty())
        .collect()
}

fn html_to_text(input: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    let mut tag_buf = String::new();
    let mut in_script = false;
    let mut in_style = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_tag {
            if ch == '>' {
                let tag = tag_buf.trim().to_ascii_lowercase();
                if tag.starts_with("script") {
                    in_script = true;
                } else if tag.starts_with("/script") {
                    in_script = false;
                } else if tag.starts_with("style") {
                    in_style = true;
                } else if tag.starts_with("/style") {
                    in_style = false;
                }

                if tag.starts_with("br")
                    || tag.starts_with("/p")
                    || tag.starts_with("/li")
                    || tag.starts_with("p")
                    || tag.starts_with("li")
                {
                    out.push('\n');
                }

                tag_buf.clear();
                in_tag = false;
            } else {
                tag_buf.push(ch);
            }
            continue;
        }

        if ch == '<' {
            in_tag = true;
            tag_buf.clear();
            continue;
        }

        if in_script || in_style {
            continue;
        }

        out.push(ch);
    }

    decode_html_entities(out.trim())
}

fn decode_html_entities(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '&' {
            let mut entity = String::new();
            while let Some(&c) = chars.peek() {
                chars.next();
                if c == ';' || entity.len() > 12 {
                    break;
                }
                entity.push(c);
            }
            let decoded = match entity.as_str() {
                "nbsp" => Some(' '),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "amp" => Some('&'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ => None,
            };
            if let Some(d) = decoded {
                out.push(d);
            } else if let Some(num) = entity.strip_prefix('#') {
                let parsed = if let Some(hex) = num.strip_prefix('x') {
                    u32::from_str_radix(hex, 16).ok()
                } else {
                    num.parse::<u32>().ok()
                };
                if let Some(code) = parsed.and_then(char::from_u32) {
                    out.push(code);
                }
            } else {
                out.push('&');
                out.push_str(&entity);
            }
        } else {
            out.push(ch);
        }
    }
    out
}
