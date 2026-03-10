use serde::{Deserialize, Serialize};

use crate::{
    EmailBlockKind, ParsedAttachment, ParsedAttachmentHint, ParsedBillingActionHint,
    ParsedContactHint, ParsedDirectionHint, ParsedEmail, ParsedEventHint, ParsedForwardedMessage,
    ParsedForwardedSegment, ParsedMailKindHint, ParsedServiceLifecycleHint,
    ParsedSignatureEntities, ParsedThread, ParsedThreadMessage, ParsedUnsubscribeHint,
    normalize_message_id, reply_text, segment_email_body,
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
    pub mail_kind_hints: Vec<ParsedMailKindHint>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub direction_hint: Option<ParsedDirectionHint>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub unsubscribe_hints: Vec<ParsedUnsubscribeHint>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub service_lifecycle_hints: Vec<ParsedServiceLifecycleHint>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub billing_action_hints: Vec<ParsedBillingActionHint>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sender_domain_hint: Option<CanonicalDomainHint>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub participant_domain_hints: Vec<CanonicalDomainHint>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub forwarded_messages: Vec<ParsedForwardedMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub forwarded_segments: Vec<ParsedForwardedSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalDomainBucket {
    Personal,
    Company,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalDomainHint {
    pub role: String,
    pub email: String,
    pub domain: String,
    pub bucket: CanonicalDomainBucket,
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
    let mut reply = reply_text(&email.body_canonical, &blocks);

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
                    let mut sig = t.to_string();
                    // Cap signature at 300 chars to avoid capturing promotional/footer content
                    const MAX_SIG_LEN: usize = 300;
                    if sig.len() > MAX_SIG_LEN {
                        // Find last newline before limit to avoid cutting words
                        // Use char boundaries to handle Unicode correctly
                        let truncated: String = sig.chars().take(MAX_SIG_LEN).collect();
                        if let Some(last_newline) = truncated.rfind('\n') {
                            sig.truncate(last_newline);
                        } else {
                            sig = truncated;
                        }
                    }
                    signature = Some(sig);
                }
            }
            EmailBlockKind::Disclaimer => disclaimer_blocks.push(t.to_string()),
            EmailBlockKind::Salutation => {
                if salutation.is_none() {
                    let mut s = t.to_string();
                    // Truncate salutation to max 50 chars to avoid capturing entire first sentence
                    if s.len() > 50 {
                        let cutoff = s
                            .chars()
                            .take(50)
                            .enumerate()
                            .find_map(|(i, c)| {
                                if matches!(c, ',' | '.' | '!' | '?' | ';' | ':') && i > 3 {
                                    Some(i + 1)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(50);
                        s = s.chars().take(cutoff).collect();
                    }
                    salutation = Some(s);
                }
            }
            EmailBlockKind::Reply => {}
        }
    }
    if signature.is_none() {
        if let Some((trimmed_reply, footer_signature)) = split_signature_footer_fallback(&reply) {
            reply = trimmed_reply;
            // Cap fallback signature at 300 chars
            let mut sig = footer_signature;
            const MAX_SIG_LEN: usize = 300;
            if sig.len() > MAX_SIG_LEN {
                // Use char boundaries to handle Unicode correctly
                let truncated: String = sig.chars().take(MAX_SIG_LEN).collect();
                if let Some(last_newline) = truncated.rfind('\n') {
                    sig.truncate(last_newline);
                } else {
                    sig = truncated;
                }
            }
            signature = Some(sig);
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
    let sender_domain_hint = first_domain_hint("from", &email.from);
    let mut participant_domain_hints = Vec::new();
    participant_domain_hints.extend(domain_hints("to", &email.to));
    participant_domain_hints.extend(domain_hints("cc", &email.cc));
    participant_domain_hints.extend(domain_hints("bcc", &email.bcc));
    participant_domain_hints.extend(domain_hints("reply_to", &email.reply_to));

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
        mail_kind_hints: email.mail_kind_hints.clone(),
        direction_hint: email.direction_hint.clone(),
        unsubscribe_hints: email.unsubscribe_hints.clone(),
        service_lifecycle_hints: email.service_lifecycle_hints.clone(),
        billing_action_hints: email.billing_action_hints.clone(),
        sender_domain_hint,
        participant_domain_hints,
        forwarded_messages: email.forwarded_messages.clone(),
        forwarded_segments: email.forwarded_segments.clone(),
    }
}

fn first_domain_hint(role: &str, addrs: &[crate::EmailAddress]) -> Option<CanonicalDomainHint> {
    domain_hints(role, addrs).into_iter().next()
}

fn domain_hints(role: &str, addrs: &[crate::EmailAddress]) -> Vec<CanonicalDomainHint> {
    let mut out = Vec::new();
    for a in addrs {
        let email = a.address.trim().to_ascii_lowercase();
        if email.is_empty() {
            continue;
        }
        let Some((_, domain)) = email.split_once('@') else {
            continue;
        };
        let domain = domain.trim().to_ascii_lowercase();
        if domain.is_empty() {
            continue;
        }
        let bucket = if is_generic_personal_domain(&domain) {
            CanonicalDomainBucket::Personal
        } else {
            CanonicalDomainBucket::Company
        };
        out.push(CanonicalDomainHint {
            role: role.to_string(),
            email,
            domain,
            bucket,
        });
    }
    out
}

fn is_generic_personal_domain(domain: &str) -> bool {
    const PROVIDERS: &[&str] = &[
        "gmail.com",
        "googlemail.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "msn.com",
        "yahoo.com",
        "ymail.com",
        "rocketmail.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "proton.me",
        "protonmail.com",
        "zoho.com",
        "aol.com",
        "gmx.com",
        "mail.com",
        "yandex.com",
        "yandex.ru",
    ];
    PROVIDERS.iter().any(|p| *p == domain)
}

fn split_signature_footer_fallback(reply: &str) -> Option<(String, String)> {
    let lines: Vec<&str> = reply.lines().collect();
    if lines.len() < 4 {
        return None;
    }
    let marker_score = |line: &str| -> usize {
        let lower = line.to_ascii_lowercase();
        let mut s = 0usize;
        for token in [
            "unsubscribe",
            "manage preferences",
            "manage your notification settings",
            "do not reply",
            "please do not reply",
            "all rights reserved",
            "copyright",
            "view in browser",
            "powered by",
            "notification settings",
            "harap jangan membalas",
        ] {
            if lower.contains(token) {
                s += 1;
            }
        }
        if lower.contains("http://") || lower.contains("https://") {
            s += 1;
        }
        s
    };
    let lower_full = reply.to_ascii_lowercase();
    let footer_markers = [
        "all rights reserved",
        "manage your notification settings",
        "unsubscribe",
        "do not reply",
        "please do not reply",
        "notification settings",
        "copyright",
    ];
    let marker_hits = |s: &str| footer_markers.iter().filter(|m| s.contains(**m)).count();
    if let Some(cut) = footer_markers
        .iter()
        .filter_map(|m| lower_full.find(m))
        .filter(|idx| *idx > reply.len() / 3)
        .min()
    {
        let suffix = &reply[cut..];
        let suffix_lower = &lower_full[cut..];
        if marker_hits(suffix_lower) >= 2
            || (marker_hits(suffix_lower) >= 1
                && (suffix_lower.contains("http://") || suffix_lower.contains("https://")))
        {
            let head = reply[..cut].trim().to_string();
            let tail = suffix.trim().to_string();
            if head.len() >= 24 && tail.len() >= 24 {
                return Some((head, tail));
            }
        }
    }
    let start_floor = lines.len().saturating_sub(40);
    for i in start_floor..lines.len() {
        let tail = &lines[i..];
        let non_empty = tail.iter().filter(|l| !l.trim().is_empty()).count();
        if non_empty < 2 || non_empty > 24 {
            continue;
        }
        let score: usize = tail.iter().map(|l| marker_score(l)).sum();
        if score < 2 {
            continue;
        }
        let head = lines[..i].join("\n").trim().to_string();
        let tail_text = tail.join("\n").trim().to_string();
        if head.len() < 24 || tail_text.len() < 24 {
            continue;
        }
        return Some((head, tail_text));
    }
    None
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
