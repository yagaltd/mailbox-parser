use serde::{Deserialize, Serialize};

use crate::{
    EmailBlockKind, ParsedAttachment, ParsedAttachmentHint, ParsedContactHint, ParsedEmail,
    ParsedEventHint, ParsedForwardedMessage, ParsedForwardedSegment, ParsedSignatureEntities, ParsedThread,
    ParsedThreadMessage, normalize_message_id, reply_text, segment_email_body,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalThread {
    pub thread_id: String,
    pub messages: Vec<CanonicalMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalMessage {
    pub message_key: String,
    pub uid: Option<u32>,
    pub internal_date: Option<String>,

    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub subject: Option<String>,
    pub date: Option<String>,
    pub date_raw: Option<String>,

    pub from: Vec<crate::EmailAddress>,
    pub to: Vec<crate::EmailAddress>,
    pub cc: Vec<crate::EmailAddress>,
    pub bcc: Vec<crate::EmailAddress>,
    pub reply_to: Vec<crate::EmailAddress>,

    /// Reply/top-level text, normalized and with quoted history removed.
    pub reply_text: String,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub quoted_blocks: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub forwarded_blocks: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub disclaimer_blocks: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub salutation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub signature: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attachments: Vec<CanonicalAttachment>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub contact_hints: Vec<ParsedContactHint>,
    #[serde(skip_serializing_if = "ParsedSignatureEntities::is_empty", default)]
    pub signature_entities: ParsedSignatureEntities,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attachment_hints: Vec<ParsedAttachmentHint>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub event_hints: Vec<ParsedEventHint>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub forwarded_messages: Vec<ParsedForwardedMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub forwarded_segments: Vec<ParsedForwardedSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalAttachment {
    pub filename: Option<String>,
    pub mime_type: String,
    pub size: usize,
    pub sha256: String,
    pub content_id: Option<String>,
    pub content_disposition: Option<String>,

    /// Optional file path for attachment bytes when exporting a "mailbox package".
    ///
    /// This is populated by the CLI when `--attachments` is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

pub fn canonicalize_threads(threads: &[ParsedThread]) -> Vec<CanonicalThread> {
    threads
        .iter()
        .map(|t| CanonicalThread {
            thread_id: t.thread_id.clone(),
            messages: t.messages.iter().map(canonicalize_thread_message).collect(),
        })
        .collect()
}

fn canonicalize_thread_message(m: &ParsedThreadMessage) -> CanonicalMessage {
    let email = &m.email;
    canonicalize_email_message(m.message_key.clone(), m.uid, m.internal_date.clone(), email)
}

fn canonicalize_email_message(
    message_key: String,
    uid: Option<u32>,
    internal_date: Option<String>,
    email: &ParsedEmail,
) -> CanonicalMessage {
    let blocks = segment_email_body(&email.body_canonical);
    let reply = reply_text(&email.body_canonical, &blocks);

    let mut quoted_blocks = Vec::new();
    let mut forwarded_blocks = Vec::new();
    let mut disclaimer_blocks = Vec::new();
    let mut salutation = None;
    let mut signature = None;

    for b in &blocks {
        let Some(s) = email.body_canonical.get(b.byte_start..b.byte_end) else {
            continue;
        };
        let t = s.trim();
        if t.is_empty() {
            continue;
        }
        match b.kind {
            EmailBlockKind::Quoted => quoted_blocks.push(t.to_string()),
            EmailBlockKind::Forwarded => forwarded_blocks.push(t.to_string()),
            EmailBlockKind::Signature => {
                if signature.is_none() {
                    signature = Some(t.to_string());
                }
            }
            EmailBlockKind::Disclaimer => disclaimer_blocks.push(t.to_string()),
            EmailBlockKind::Salutation => {
                if salutation.is_none() {
                    salutation = Some(t.to_string());
                }
            }
            EmailBlockKind::Reply => {}
        }
    }

    let mut attachments = Vec::new();
    for a in &email.attachments {
        attachments.push(canonicalize_attachment(a));
    }

    let message_id = email.message_id.as_ref().map(|s| normalize_message_id(s));
    let in_reply_to = email.in_reply_to.as_ref().map(|s| normalize_message_id(s));
    let references = email
        .references
        .iter()
        .map(|r| normalize_message_id(r))
        .collect();

    CanonicalMessage {
        message_key,
        uid,
        internal_date,

        message_id,
        in_reply_to,
        references,
        subject: email.subject.clone(),
        date: email.date.clone(),
        date_raw: email.date_raw.clone(),

        from: email.from.clone(),
        to: email.to.clone(),
        cc: email.cc.clone(),
        bcc: email.bcc.clone(),
        reply_to: email.reply_to.clone(),

        reply_text: reply,
        quoted_blocks,
        forwarded_blocks,
        disclaimer_blocks,
        salutation,
        signature,
        attachments,
        contact_hints: email.contact_hints.clone(),
        signature_entities: email.signature_entities.clone(),
        attachment_hints: email.attachment_hints.clone(),
        event_hints: email.event_hints.clone(),
        forwarded_messages: email.forwarded_messages.clone(),
        forwarded_segments: email.forwarded_segments.clone(),
    }
}

fn canonicalize_attachment(a: &ParsedAttachment) -> CanonicalAttachment {
    CanonicalAttachment {
        filename: a.filename.clone(),
        mime_type: a.mime_type.clone(),
        size: a.size,
        sha256: a.sha256.clone(),
        content_id: a.content_id.clone(),
        content_disposition: a.content_disposition.clone(),
        path: None,
    }
}
