mod canonical;
mod email_text;
mod imap;
mod mbox;

pub use canonical::{CanonicalAttachment, CanonicalMessage, CanonicalThread, canonicalize_threads};
pub use email_text::{
    EmailBlock, EmailBlockKind, forwarded_message_ids, normalize_email_text, reply_text,
    segment_email_body,
};
pub use imap::{
    ImapAccountConfig, ImapConfigFile, ImapScanOptions, ImapStateBackend, ImapSyncOptions,
    ImapSyncResult, ImapSyncState, SyncedEmail, scan_imap_headers, scan_imap_headers_with_progress,
    sync_imap_delta, sync_imap_with_backend,
};
pub use mbox::{
    MboxMessage, MboxParseError, MboxParseOptions, MboxParseReport, MboxReadOptions,
    iter_mbox_messages, parse_mbox_file, scan_mbox_file_headers_only, scan_mbox_headers,
    scan_mbox_headers_with_progress,
};

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contact_hints: Vec<ParsedContactHint>,
    #[serde(default, skip_serializing_if = "ParsedSignatureEntities::is_empty")]
    pub signature_entities: ParsedSignatureEntities,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachment_hints: Vec<ParsedAttachmentHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_hints: Vec<ParsedEventHint>,

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

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HintConfidence {
    High,
    Medium,
    #[default]
    Low,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContactHintSource {
    FromHeader,
    ToHeader,
    CcHeader,
    BccHeader,
    ReplyToHeader,
    Salutation,
    Signature,
}
impl Default for ContactHintSource {
    fn default() -> Self {
        Self::Signature
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContactHintRole {
    From,
    To,
    Cc,
    Bcc,
    ReplyTo,
    Mentioned,
}
impl Default for ContactHintRole {
    fn default() -> Self {
        Self::Mentioned
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedContactHint {
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub source: ContactHintSource,
    pub role: ContactHintRole,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub company_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedSignatureEntities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emails: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phones: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub organization_lines: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub title_lines: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address_lines: Vec<String>,
    #[serde(default)]
    pub is_partial: bool,
}

impl ParsedSignatureEntities {
    pub(crate) fn is_empty(&self) -> bool {
        self.emails.is_empty()
            && self.phones.is_empty()
            && self.urls.is_empty()
            && self.organization_lines.is_empty()
            && self.title_lines.is_empty()
            && self.address_lines.is_empty()
            && !self.is_partial
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentSizeBucket {
    Tiny,
    Small,
    Medium,
    Large,
}
impl Default for AttachmentSizeBucket {
    fn default() -> Self {
        Self::Small
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedAttachmentHint {
    pub sha256: String,
    pub is_inline: bool,
    pub is_probable_logo: bool,
    pub is_tracking_pixel_like: bool,
    pub size_bucket: AttachmentSizeBucket,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventHintKind {
    Meeting,
    Shipping,
    Deadline,
    Availability,
    Generic,
}
impl Default for EventHintKind {
    fn default() -> Self {
        Self::Generic
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventMissingField {
    Date,
    Time,
    Timezone,
    Location,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedDateTimeCandidate {
    pub raw: String,
    #[serde(default)]
    pub has_time: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedEventHint {
    pub kind: EventHintKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datetime_candidates: Vec<ParsedDateTimeCandidate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub location_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub meeting_links: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timezone_candidates: Vec<String>,
    #[serde(default)]
    pub is_complete: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_fields: Vec<EventMissingField>,
    #[serde(default)]
    pub confidence: HintConfidence,
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
    let blocks = if body_canonical.trim().is_empty() {
        Vec::new()
    } else {
        crate::email_text::segment_email_body(&body_canonical)
    };
    if !blocks.is_empty() {
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

    let salutation = first_block_of_kind(&body_canonical, &blocks, crate::EmailBlockKind::Salutation);
    let signature = first_block_of_kind(&body_canonical, &blocks, crate::EmailBlockKind::Signature);
    let reply = crate::email_text::reply_text(&body_canonical, &blocks);
    let signature_entities = extract_signature_entities(signature.as_deref());
    let contact_hints = extract_contact_hints(
        &from,
        &to,
        &cc,
        &bcc,
        &reply_to,
        salutation.as_deref(),
        &signature_entities,
    );
    let attachment_hints = derive_attachment_hints(&attachments);
    let event_hints = extract_event_hints(subject.as_deref(), &reply);

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
        contact_hints,
        signature_entities,
        attachment_hints,
        event_hints,
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

pub fn thread_root_id(
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    references: &[String],
) -> Option<String> {
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
            ParsedThread {
                thread_id,
                messages,
            }
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
            ParsedThread {
                thread_id,
                messages,
            }
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
            let content_disposition = att.content_disposition().map(|s| s.ctype().to_string());

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

fn first_block_of_kind(
    text: &str,
    blocks: &[crate::EmailBlock],
    kind: crate::EmailBlockKind,
) -> Option<String> {
    for b in blocks {
        if b.kind != kind {
            continue;
        }
        let s = text.get(b.byte_start..b.byte_end)?.trim();
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

fn normalize_name_like(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn extract_email_candidates(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split_whitespace() {
        let t = token
            .trim_matches(|c: char| {
                c == '<'
                    || c == '>'
                    || c == '('
                    || c == ')'
                    || c == '['
                    || c == ']'
                    || c == ','
                    || c == ';'
                    || c == ':'
                    || c == '"'
                    || c == '\''
            })
            .to_ascii_lowercase();
        if t.contains('@') && t.contains('.') {
            let t = t.strip_prefix("mailto:").unwrap_or(&t);
            let norm = t.trim_matches('.');
            if !norm.is_empty() && !out.iter().any(|e| e == norm) {
                out.push(norm.to_string());
            }
        }
    }
    out
}

fn extract_phone_candidates(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split_whitespace() {
        let digits: String = token.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 8 && digits.len() <= 15 && !out.iter().any(|d| d == &digits) {
            out.push(digits);
        }
    }
    out
}

fn extract_url_candidates(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split_whitespace() {
        let t = token
            .trim_matches(|c: char| c == '<' || c == '>' || c == '(' || c == ')' || c == ',');
        let lower = t.to_ascii_lowercase();
        if (lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("www."))
            && !out.iter().any(|u| u == t)
        {
            out.push(t.to_string());
        }
    }
    out
}

fn derive_signature_line_buckets(sig: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let org_tokens = [
        "inc", "llc", "corp", "company", "sas", "gmbh", "ltd", "sa", "sarl", "ag",
        "technologies",
    ];
    let title_tokens = [
        "engineer",
        "manager",
        "director",
        "expert",
        "responsable",
        "technician",
        "support",
        "sales",
        "consultant",
    ];
    let addr_tokens = [
        "avenue", "street", "road", "blvd", "boulevard", "city", "france", "germany", "canada",
        "usa",
    ];

    let mut org = Vec::new();
    let mut title = Vec::new();
    let mut addr = Vec::new();

    for line in sig.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if org_tokens.iter().any(|t| lower.contains(t)) && !org.iter().any(|v| v == l) {
            org.push(l.to_string());
        }
        if title_tokens.iter().any(|t| lower.contains(t)) && !title.iter().any(|v| v == l) {
            title.push(l.to_string());
        }
        let has_postal = l.chars().filter(|c| c.is_ascii_digit()).count() >= 4;
        if (addr_tokens.iter().any(|t| lower.contains(t)) || has_postal)
            && !addr.iter().any(|v| v == l)
        {
            addr.push(l.to_string());
        }
    }
    (org, title, addr)
}

fn extract_signature_entities(signature: Option<&str>) -> ParsedSignatureEntities {
    let Some(sig) = signature.map(str::trim).filter(|s| !s.is_empty()) else {
        return ParsedSignatureEntities::default();
    };
    let emails = extract_email_candidates(sig);
    let phones = extract_phone_candidates(sig);
    let urls = extract_url_candidates(sig);
    let (organization_lines, title_lines, address_lines) = derive_signature_line_buckets(sig);
    ParsedSignatureEntities {
        is_partial: emails.is_empty() && phones.is_empty(),
        emails,
        phones,
        urls,
        organization_lines,
        title_lines,
        address_lines,
    }
}

fn email_domain(email: &str) -> Option<String> {
    let (_, domain) = email.split_once('@')?;
    let d = domain.trim().to_ascii_lowercase();
    if d.is_empty() { None } else { Some(d) }
}

fn compute_link_key(name: Option<&str>, email: Option<&str>) -> Option<String> {
    let n = name
        .map(normalize_name_like)
        .filter(|s| !s.is_empty())
        .unwrap_or_default();
    let d = email.and_then(email_domain).unwrap_or_default();
    if n.is_empty() && d.is_empty() {
        None
    } else {
        Some(format!("{n}|{d}"))
    }
}

fn push_header_contact_hints(
    out: &mut Vec<ParsedContactHint>,
    list: &[EmailAddress],
    source: ContactHintSource,
    role: ContactHintRole,
) {
    for a in list {
        let email = a.address.trim().to_ascii_lowercase();
        if email.is_empty() {
            continue;
        }
        out.push(ParsedContactHint {
            name: a.name.clone(),
            email: Some(email.clone()),
            phone: None,
            source: source.clone(),
            role: role.clone(),
            confidence: HintConfidence::High,
            company_domain: email_domain(&email),
            link_key: compute_link_key(a.name.as_deref(), Some(&email)),
        });
    }
}

fn extract_contact_hints(
    from: &[EmailAddress],
    to: &[EmailAddress],
    cc: &[EmailAddress],
    bcc: &[EmailAddress],
    reply_to: &[EmailAddress],
    salutation: Option<&str>,
    signature_entities: &ParsedSignatureEntities,
) -> Vec<ParsedContactHint> {
    let mut out = Vec::new();
    push_header_contact_hints(
        &mut out,
        from,
        ContactHintSource::FromHeader,
        ContactHintRole::From,
    );
    push_header_contact_hints(&mut out, to, ContactHintSource::ToHeader, ContactHintRole::To);
    push_header_contact_hints(&mut out, cc, ContactHintSource::CcHeader, ContactHintRole::Cc);
    push_header_contact_hints(
        &mut out,
        bcc,
        ContactHintSource::BccHeader,
        ContactHintRole::Bcc,
    );
    push_header_contact_hints(
        &mut out,
        reply_to,
        ContactHintSource::ReplyToHeader,
        ContactHintRole::ReplyTo,
    );

    if let Some(line) = salutation {
        let n = line.trim().trim_end_matches(',').trim();
        if !n.is_empty() {
            out.push(ParsedContactHint {
                name: Some(n.to_string()),
                email: None,
                phone: None,
                source: ContactHintSource::Salutation,
                role: ContactHintRole::Mentioned,
                confidence: HintConfidence::Low,
                company_domain: None,
                link_key: compute_link_key(Some(n), None),
            });
        }
    }

    for email in &signature_entities.emails {
        out.push(ParsedContactHint {
            name: None,
            email: Some(email.clone()),
            phone: None,
            source: ContactHintSource::Signature,
            role: ContactHintRole::Mentioned,
            confidence: HintConfidence::Medium,
            company_domain: email_domain(email),
            link_key: compute_link_key(None, Some(email)),
        });
    }
    for phone in &signature_entities.phones {
        out.push(ParsedContactHint {
            name: None,
            email: None,
            phone: Some(phone.clone()),
            source: ContactHintSource::Signature,
            role: ContactHintRole::Mentioned,
            confidence: HintConfidence::Low,
            company_domain: None,
            link_key: None,
        });
    }
    out
}

fn derive_attachment_hints(attachments: &[ParsedAttachment]) -> Vec<ParsedAttachmentHint> {
    let logo_tokens = ["logo", "signature", "icon", "linkedin", "twitter", "facebook"];
    attachments
        .iter()
        .map(|a| {
            let name = a.filename.as_deref().unwrap_or("").to_ascii_lowercase();
            let inline = a.content_id.is_some()
                || a.content_disposition
                    .as_deref()
                    .map(|d| d.eq_ignore_ascii_case("inline"))
                    .unwrap_or(false);
            let is_image = a.mime_type.to_ascii_lowercase().starts_with("image/");
            let is_probable_logo = inline
                && is_image
                && a.size <= 40_000
                && logo_tokens.iter().any(|t| name.contains(t));
            let is_tracking_pixel_like = is_image && a.size <= 2_000;
            let size_bucket = if a.size < 4_000 {
                AttachmentSizeBucket::Tiny
            } else if a.size < 64_000 {
                AttachmentSizeBucket::Small
            } else if a.size < 1_000_000 {
                AttachmentSizeBucket::Medium
            } else {
                AttachmentSizeBucket::Large
            };
            ParsedAttachmentHint {
                sha256: a.sha256.clone(),
                is_inline: inline,
                is_probable_logo,
                is_tracking_pixel_like,
                size_bucket,
            }
        })
        .collect()
}

fn extract_event_hints(subject: Option<&str>, reply_text: &str) -> Vec<ParsedEventHint> {
    let text = reply_text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    let mut datetime_candidates = Vec::new();
    let mut location_candidates = Vec::new();
    let mut meeting_links = Vec::new();
    let mut timezone_candidates = Vec::new();

    let month_tokens = [
        "jan",
        "january",
        "feb",
        "february",
        "mar",
        "march",
        "apr",
        "april",
        "may",
        "jun",
        "june",
        "jul",
        "july",
        "aug",
        "august",
        "sep",
        "sept",
        "september",
        "oct",
        "october",
        "nov",
        "november",
        "dec",
        "december",
    ];
    let weekday_tokens = ["mon", "monday", "tue", "tuesday", "wed", "wednesday", "thu", "thursday", "fri", "friday", "sat", "saturday", "sun", "sunday"];
    let tz_tokens = [
        "utc", "gmt", "cet", "cest", "pst", "pdt", "est", "edt", "bst", "cst", "cdt", "mst",
        "mdt",
    ];
    let meeting_hosts = [
        "zoom.us",
        "meet.google.com",
        "teams.microsoft.com",
        "webex.com",
        "calendly.com",
    ];
    let header_prefixes = [
        "from:",
        "sent:",
        "to:",
        "cc:",
        "subject:",
        "message-id:",
        "de:",
        "para:",
        "asunto:",
        "enviado:",
        "enviado el:",
        "envoyé:",
        "objet:",
        "von:",
        "gesendet:",
        "betreff:",
    ];
    let location_tokens = [
        "avenue",
        "street",
        "road",
        "site",
        "venue",
        "office",
        "room",
        "unit",
        "building",
        "campus",
    ];
    let clean_token = |raw: &str| -> String {
        raw.trim_matches(move |c: char| {
            c.is_whitespace()
                || matches!(
                    c,
                    ',' | ';' | '.' | ':' | '(' | ')' | '[' | ']' | '<' | '>' | '"' | '\''
                )
        })
        .to_string()
    };
    let is_time_like = |line: &str| -> bool {
        line.split_whitespace().any(|tok| {
            let t = clean_token(tok);
            let Some((h, m)) = t.as_str().split_once(':') else {
                return false;
            };
            if h.is_empty() || m.is_empty() {
                return false;
            }
            let m2: String = m.chars().take_while(|c| c.is_ascii_digit()).collect();
            h.chars().all(|c| c.is_ascii_digit())
                && m2.len() >= 2
                && m2.chars().all(|c| c.is_ascii_digit())
        })
    };
    let is_numeric_date_token = |token: &str| -> bool {
        let t = clean_token(token);
        for sep in ['-', '/'] {
            let parts: Vec<&str> = t.as_str().split(sep).collect();
            if parts.len() != 3 {
                continue;
            }
            if !parts.iter().all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())) {
                continue;
            }
            let lens: Vec<usize> = parts.iter().map(|p| p.len()).collect();
            let has_year = lens.contains(&4);
            if !has_year {
                continue;
            }
            let year = parts
                .iter()
                .find(|p| p.len() == 4)
                .and_then(|y| y.parse::<u32>().ok())
                .unwrap_or(0);
            if !(1900..=2100).contains(&year) {
                continue;
            }
            return true;
        }
        false
    };
    let has_month_and_day = |line: &str| -> bool {
        let lower = line.to_ascii_lowercase();
        if !month_tokens.iter().any(|m| lower.contains(m)) {
            return false;
        }
        lower
            .split_whitespace()
            .map(clean_token)
            .any(|tok| tok.chars().all(|c| c.is_ascii_digit()) && (1..=2).contains(&tok.len()))
    };
    let has_month_range = |line: &str| -> bool {
        let lower = line.to_ascii_lowercase();
        if !month_tokens.iter().any(|m| lower.contains(m)) {
            return false;
        }
        lower.split_whitespace().any(|tok| {
            let t = clean_token(tok);
            let Some((a, b)) = t.as_str().split_once('-') else {
                return false;
            };
            !a.is_empty()
                && !b.is_empty()
                && a.chars().all(|c| c.is_ascii_digit())
                && b.chars().all(|c| c.is_ascii_digit())
                && (1..=2).contains(&a.len())
                && (1..=2).contains(&b.len())
        })
    };
    let has_weekday_and_date = |line: &str| -> bool {
        let lower = line.to_ascii_lowercase();
        weekday_tokens.iter().any(|d| lower.contains(d))
            && (line.split_whitespace().any(is_numeric_date_token) || has_month_and_day(line))
    };
    let has_strong_date_anchor = |line: &str| -> bool {
        line.split_whitespace().any(is_numeric_date_token)
            || has_month_and_day(line)
            || has_month_range(line)
            || has_weekday_and_date(line)
    };
    let extract_meeting_link = |line: &str| -> Option<String> {
        for tok in line.split_whitespace() {
            let t = clean_token(tok);
            if !(t.starts_with("http://") || t.starts_with("https://")) {
                continue;
            }
            let lower = t.to_ascii_lowercase();
            if meeting_hosts.iter().any(|h| lower.contains(h)) {
                return Some(t);
            }
        }
        None
    };
    let has_strict_timezone = |line: &str| -> bool {
        let lower = line.to_ascii_lowercase();
        if lower.contains("utc+") || lower.contains("utc-") || lower.contains("gmt+") || lower.contains("gmt-") {
            return true;
        }
        line.split_whitespace().any(|tok| {
            let t = clean_token(tok);
            if t.is_empty() {
                return false;
            }
            let upper = t.to_ascii_uppercase();
            tz_tokens.iter().any(|z| upper == z.to_ascii_uppercase())
        })
    };

    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if header_prefixes.iter().any(|p| lower.starts_with(p)) {
            continue;
        }
        let has_time = is_time_like(l);
        if has_strong_date_anchor(l) {
            datetime_candidates.push(ParsedDateTimeCandidate {
                raw: l.to_string(),
                has_time,
            });
        }
        if has_strict_timezone(l) {
            timezone_candidates.push(l.to_string());
        }
        if let Some(link) = extract_meeting_link(l) {
            if !meeting_links.iter().any(|m| m == &link) {
                meeting_links.push(link);
            }
        }
        if location_tokens.iter().any(|t| lower.contains(t))
            || (l.chars().filter(|c| c.is_ascii_digit()).count() >= 4 && lower.contains("france"))
        {
            location_candidates.push(l.to_string());
        }
    }

    if datetime_candidates.is_empty() {
        return Vec::new();
    }

    let kind_source = format!(
        "{}\n{}",
        subject.unwrap_or_default().to_ascii_lowercase(),
        text.to_ascii_lowercase()
    );
    let kind = if ["meeting", "call", "visit", "onboarding", "training"]
        .iter()
        .any(|k| kind_source.contains(k))
    {
        EventHintKind::Meeting
    } else if ["ship", "shipment", "delivery"].iter().any(|k| kind_source.contains(k)) {
        EventHintKind::Shipping
    } else if ["deadline", "due"].iter().any(|k| kind_source.contains(k)) {
        EventHintKind::Deadline
    } else if ["available", "availability"].iter().any(|k| kind_source.contains(k)) {
        EventHintKind::Availability
    } else {
        EventHintKind::Generic
    };

    let has_time = datetime_candidates.iter().any(|c| c.has_time);
    let has_date = !datetime_candidates.is_empty();
    let has_location_or_link = !location_candidates.is_empty() || !meeting_links.is_empty();
    let has_tz = !timezone_candidates.is_empty();
    let is_complete = has_date && has_location_or_link && (!has_time || has_tz);
    let mut missing_fields = Vec::new();
    if !has_date {
        missing_fields.push(EventMissingField::Date);
    }
    if !has_location_or_link {
        missing_fields.push(EventMissingField::Location);
    }
    if has_time && !has_tz {
        missing_fields.push(EventMissingField::Timezone);
    }

    let confidence = if is_complete {
        HintConfidence::High
    } else if has_date || has_location_or_link {
        HintConfidence::Medium
    } else {
        HintConfidence::Low
    };

    vec![ParsedEventHint {
        kind,
        datetime_candidates,
        location_candidates,
        meeting_links,
        timezone_candidates,
        is_complete,
        missing_fields,
        confidence,
    }]
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
