mod canonical;
mod email_text;
mod imap;
mod lifecycle_lexicon;
mod mbox;
mod projection;

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
pub use lifecycle_lexicon::{
    LifecycleLexicon, LifecycleRuleMatch, default_lifecycle_lexicon,
    load_lifecycle_lexicon_from_yaml, load_lifecycle_lexicon_with_overrides,
};
pub use mbox::{
    MboxMessage, MboxParseError, MboxParseOptions, MboxParseReport, MboxReadOptions,
    iter_mbox_messages, parse_mbox_file, scan_mbox_file_headers_only, scan_mbox_headers,
    scan_mbox_headers_with_progress,
};
pub use projection::{
    ProjectionDataset, ProjectionFacets, ProjectionLink, ProjectionNode, ProjectionQuery,
    ProjectionRow, ProjectionStats, apply_query as project_apply_query, build_graph as project_build_graph,
    dataset as project_dataset, facets as project_facets, rows_from_canonical_threads,
};

use anyhow::{Result, anyhow};
pub use contacts::EmailAddress;
use mail_parser::{Message, MessageParser, MimeHeaders};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub forwarded_segments: Vec<ParsedForwardedSegment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contact_hints: Vec<ParsedContactHint>,
    #[serde(default, skip_serializing_if = "ParsedSignatureEntities::is_empty")]
    pub signature_entities: ParsedSignatureEntities,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachment_hints: Vec<ParsedAttachmentHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_hints: Vec<ParsedEventHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mail_kind_hints: Vec<ParsedMailKindHint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction_hint: Option<ParsedDirectionHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsubscribe_hints: Vec<ParsedUnsubscribeHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub service_lifecycle_hints: Vec<ParsedServiceLifecycleHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub billing_action_hints: Vec<ParsedBillingActionHint>,

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

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedForwardedHeaders {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<EmailAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<EmailAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<EmailAddress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub raw_lines: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedForwardedSegment {
    pub depth: usize,
    #[serde(default)]
    pub headers: ParsedForwardedHeaders,
    #[serde(default)]
    pub reply_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub salutation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disclaimer_blocks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quoted_blocks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forwarded_blocks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nested: Vec<ParsedForwardedSegment>,
    #[serde(default)]
    pub parse_confidence: HintConfidence,
    #[serde(default)]
    pub has_unparsed_tail: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_type: Option<ContactProfileType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    pub source: ContactHintSource,
    pub role: ContactHintRole,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub company_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linked_entity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContactProfileType {
    Website,
    LinkedinPerson,
    LinkedinCompany,
    TwitterX,
    Instagram,
    Tiktok,
    Youtube,
    Facebook,
    Github,
    Other,
}
impl Default for ContactProfileType {
    fn default() -> Self {
        Self::Other
    }
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
    Reservation,
    Deadline,
    Availability,
    Generic,
}
impl Default for EventHintKind {
    fn default() -> Self {
        Self::Generic
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MailKind {
    Personal,
    Newsletter,
    Promotion,
    Transactional,
    Notification,
    Unknown,
}
impl Default for MailKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedMailKindHint {
    pub kind: MailKind,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<String>,
    #[serde(default)]
    pub is_primary: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MailDirection {
    Inbound,
    Outbound,
    SelfMessage,
    Unknown,
}
impl Default for MailDirection {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedDirectionHint {
    pub direction: MailDirection,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnsubscribeSource {
    HeaderListUnsubscribe,
    HeaderListUnsubscribePost,
    BodyLink,
    BodyText,
}
impl Default for UnsubscribeSource {
    fn default() -> Self {
        Self::BodyText
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnsubscribeKind {
    OneClick,
    Url,
    MailTo,
    ManagePreferences,
    Unknown,
}
impl Default for UnsubscribeKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedUnsubscribeHint {
    pub kind: UnsubscribeKind,
    pub source: UnsubscribeSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default)]
    pub confidence: HintConfidence,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceLifecycleKind {
    SubscriptionCreated,
    SubscriptionRenewed,
    SubscriptionCanceled,
    MembershipUpdated,
    OrderConfirmation,
    TicketConfirmation,
    BillingNotice,
    Unknown,
}
impl Default for ServiceLifecycleKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedServiceLifecycleHint {
    pub kind: ServiceLifecycleKind,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer_email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount_raw: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_date_raw: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BillingActionKind {
    ViewInvoice,
    PayNow,
    ManageSubscription,
    UpdatePaymentMethod,
    BillingPortal,
    Unknown,
}
impl Default for BillingActionKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BillingActionSource {
    Header,
    BodyLink,
    BodyText,
}
impl Default for BillingActionSource {
    fn default() -> Self {
        Self::BodyText
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ParsedBillingActionHint {
    pub kind: BillingActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub source: BillingActionSource,
    #[serde(default)]
    pub confidence: HintConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_lifecycle_kind: Option<ServiceLifecycleKind>,
}

#[derive(Clone, Debug, Default)]
pub struct ParseRfc822Options {
    pub owner_emails: Vec<String>,
    pub lifecycle_lexicon: Option<Arc<LifecycleLexicon>>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_type: Option<ReservationType>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReservationType {
    Hotel,
    Restaurant,
    Spa,
    Salon,
    Bar,
    Other,
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
    parse_rfc822_with_options(bytes, &ParseRfc822Options::default())
}

pub fn parse_rfc822_with_options(
    bytes: &[u8],
    options: &ParseRfc822Options,
) -> Result<ParsedEmail> {
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

    let salutation =
        first_block_of_kind(&body_canonical, &blocks, crate::EmailBlockKind::Salutation);
    let mut signature =
        first_block_of_kind(&body_canonical, &blocks, crate::EmailBlockKind::Signature);
    let mut reply = crate::email_text::reply_text(&body_canonical, &blocks);
    if signature.is_none() {
        if let Some((trimmed_reply, footer_signature)) = split_signature_footer_fallback(&reply) {
            reply = trimmed_reply;
            signature = Some(footer_signature);
        }
    }
    let forwarded_segments = parse_forwarded_segments_from_blocks(&body_canonical, &blocks);
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
    let provisional_mail_kind_hints =
        extract_mail_kind_hints(subject.as_deref(), &body_canonical, &raw_headers, &[]);
    let provisional_primary_kind = provisional_mail_kind_hints
        .iter()
        .find(|h| h.is_primary)
        .map(|h| h.kind.clone());
    let lifecycle_lexicon: &LifecycleLexicon = match options.lifecycle_lexicon.as_deref() {
        Some(lexicon) => lexicon,
        None => default_lifecycle_lexicon(),
    };
    let event_hints = extract_event_hints(
        subject.as_deref(),
        &reply,
        provisional_primary_kind.as_ref(),
        lifecycle_lexicon,
    );
    let unsubscribe_hints = extract_unsubscribe_hints(&raw_headers, &body_canonical);
    let lifecycle_gate = decide_lifecycle_gate(subject.as_deref(), &body_canonical, &raw_headers);
    let service_lifecycle_hints = extract_service_lifecycle_hints(
        subject.as_deref(),
        &body_canonical,
        &raw_headers,
        &from,
        lifecycle_gate,
        lifecycle_lexicon,
    );
    let billing_action_hints = extract_billing_action_hints(
        &raw_headers,
        &body_canonical,
        service_lifecycle_hints.first().map(|h| h.kind.clone()),
        lifecycle_lexicon,
    );
    let mail_kind_hints = extract_mail_kind_hints(
        subject.as_deref(),
        &body_canonical,
        &raw_headers,
        &service_lifecycle_hints,
    );
    let direction_hint =
        infer_direction_hint(&from, &to, &cc, &bcc, &reply_to, &options.owner_emails);

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
        forwarded_segments,
        contact_hints,
        signature_entities,
        attachment_hints,
        event_hints,
        mail_kind_hints,
        direction_hint,
        unsubscribe_hints,
        service_lifecycle_hints,
        billing_action_hints,
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
    let has_html = html.map(|s| !s.trim().is_empty()).unwrap_or(false);
    let mut out = String::new();
    if let Some(t) = text.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        out = t.to_string();
    } else if let Some(h) = html.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let t = cleanup_html_boilerplate(&html_to_text(h));
        if !t.trim().is_empty() {
            out = t;
        }
    }
    if out.trim().is_empty() {
        return String::new();
    }
    if has_html {
        out = cleanup_html_boilerplate(&out);
    }
    crate::email_text::normalize_email_text(&out)
}

fn cleanup_html_boilerplate(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() < 4 {
        return text.trim().to_string();
    }

    let filtered: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|line| {
            let l = line.trim().to_ascii_lowercase();
            !(l.starts_with("view image:")
                || l.starts_with("follow image link:")
                || l == "caption:"
                || l.starts_with("caption:"))
        })
        .collect();
    let lines = if filtered.is_empty() { lines } else { filtered };

    let is_strong_footer_line = |line: &str| -> bool {
        let l = line.trim().to_ascii_lowercase();
        l.contains("unsubscribe")
            || l.contains("manage preferences")
            || l.contains("view in browser")
            || l.contains("mailing list")
    };
    let is_footer_line = |line: &str| -> bool {
        let l = line.trim().to_ascii_lowercase();
        if l.is_empty() {
            return false;
        }
        is_strong_footer_line(&l)
            || l.contains("privacy policy")
            || l.contains("terms")
            || l.contains("all rights reserved")
            || l.contains("follow us")
            || l.contains("facebook.com")
            || l.contains("instagram.com")
            || l.contains("linkedin.com")
            || l.contains("youtube.com")
            || l.contains("x.com/")
            || l.contains("twitter.com/")
            || l.starts_with("http://")
            || l.starts_with("https://")
    };

    let mut footer_start: Option<usize> = None;
    for i in (lines.len() / 3)..lines.len() {
        if !is_footer_line(lines[i]) {
            continue;
        }
        let footer_signal_count = lines[i..].iter().filter(|l| is_footer_line(l)).count();
        let strong_signal_count = lines[i..]
            .iter()
            .filter(|l| is_strong_footer_line(l))
            .count();
        if footer_signal_count >= 2 && strong_signal_count >= 1 {
            footer_start = Some(i);
            break;
        }
    }

    let mut kept: Vec<&str> = if let Some(start) = footer_start {
        lines[..start].to_vec()
    } else {
        lines
    };

    let reddit_tail_markers = [
        "this email was intended for",
        "unsubscribefrom daily digest messages",
        "visit your settings to manage",
    ];
    let tail_start = kept.len() / 2;
    for i in tail_start..kept.len() {
        let l = kept[i].trim().to_ascii_lowercase();
        if reddit_tail_markers.iter().any(|m| l.contains(m)) {
            kept.truncate(i);
            break;
        }
    }

    while kept.last().is_some_and(|l| l.trim().is_empty()) {
        kept.pop();
    }

    let mut out = kept.join("\n").trim().to_string();
    if !out.is_empty() {
        let lower = out.to_ascii_lowercase();
        let cutoff_markers = [
            "this email was intended for",
            "unsubscribefrom daily digest messages",
            "visit your settings to manage",
        ];
        for marker in cutoff_markers {
            if let Some(pos) = lower.find(marker) {
                if pos >= lower.len() / 3 {
                    out.truncate(pos);
                    break;
                }
            }
        }
    }
    out.trim().to_string()
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

fn parse_forwarded_segments_from_blocks(
    body_canonical: &str,
    blocks: &[crate::EmailBlock],
) -> Vec<ParsedForwardedSegment> {
    let mut out = Vec::new();
    let mut stack = std::collections::HashSet::new();
    for b in blocks {
        if b.kind != crate::EmailBlockKind::Forwarded {
            continue;
        }
        let Some(raw) = body_canonical.get(b.byte_start..b.byte_end) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        out.push(parse_forwarded_segment(raw, 0, &mut stack));
    }
    out
}

fn parse_forwarded_segment(
    raw_block: &str,
    depth: usize,
    stack: &mut std::collections::HashSet<u64>,
) -> ParsedForwardedSegment {
    let raw = raw_block.trim();
    if raw.is_empty() {
        return ParsedForwardedSegment {
            depth,
            parse_confidence: HintConfidence::Low,
            has_unparsed_tail: false,
            ..Default::default()
        };
    }

    let (headers, body_start_line) = parse_forwarded_headers(raw);
    let mut lines: Vec<&str> = raw.lines().collect();
    if lines.is_empty() {
        lines.push(raw);
    }
    let body = if body_start_line < lines.len() {
        lines[body_start_line..].join("\n")
    } else {
        raw.to_string()
    };
    let body = body.trim();
    let mut segment = ParsedForwardedSegment {
        depth,
        headers,
        ..Default::default()
    };
    if body.is_empty() {
        segment.parse_confidence = if !segment.headers.raw_lines.is_empty() {
            HintConfidence::Medium
        } else {
            HintConfidence::Low
        };
        return segment;
    }

    let nested_blocks = crate::email_text::segment_email_body(body);
    segment.reply_text = strip_embedded_header_bundle_from_reply(&crate::email_text::reply_text(
        body,
        &nested_blocks,
    ));
    segment.salutation =
        first_block_of_kind(body, &nested_blocks, crate::EmailBlockKind::Salutation);
    segment.signature = first_block_of_kind(body, &nested_blocks, crate::EmailBlockKind::Signature);

    for b in &nested_blocks {
        let Some(s) = body.get(b.byte_start..b.byte_end) else {
            continue;
        };
        let t = s.trim();
        if t.is_empty() {
            continue;
        }
        match b.kind {
            crate::EmailBlockKind::Disclaimer => segment.disclaimer_blocks.push(t.to_string()),
            crate::EmailBlockKind::Quoted => segment.quoted_blocks.push(t.to_string()),
            crate::EmailBlockKind::Forwarded => segment.forwarded_blocks.push(t.to_string()),
            crate::EmailBlockKind::Reply
            | crate::EmailBlockKind::Salutation
            | crate::EmailBlockKind::Signature => {}
        }
    }

    segment.parse_confidence =
        if segment.headers.raw_lines.len() >= 2 && !segment.reply_text.is_empty() {
            HintConfidence::High
        } else if !segment.headers.raw_lines.is_empty()
            || !segment.reply_text.is_empty()
            || !segment.signature.as_deref().unwrap_or("").is_empty()
        {
            HintConfidence::Medium
        } else {
            HintConfidence::Low
        };

    for nested_raw in &segment.forwarded_blocks {
        let nested_raw = nested_raw.trim();
        if nested_raw.len() < 48 {
            segment.has_unparsed_tail = true;
            continue;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        nested_raw.hash(&mut hasher);
        let fp = hasher.finish();
        if stack.contains(&fp) {
            segment.has_unparsed_tail = true;
            continue;
        }
        stack.insert(fp);
        segment
            .nested
            .push(parse_forwarded_segment(nested_raw, depth + 1, stack));
        stack.remove(&fp);
    }

    segment
}

fn strip_embedded_header_bundle_from_reply(reply: &str) -> String {
    let text = reply.trim();
    if text.is_empty() {
        return String::new();
    }
    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() {
        return String::new();
    }

    let normalize_key = |line: &str| -> Option<String> {
        let mut s = line.trim_start();
        while let Some(rest) = s.strip_prefix('>') {
            s = rest.trim_start();
        }
        let s = s.trim();
        let colon = s.find(':')?;
        let key = s[..colon].replace(' ', "").to_ascii_lowercase();
        if key.is_empty() { None } else { Some(key) }
    };
    let is_header_key = |key: &str| -> bool {
        matches!(
            key,
            "from"
                | "sent"
                | "date"
                | "to"
                | "cc"
                | "subject"
                | "message-id"
                | "de"
                | "para"
                | "asunto"
                | "enviado"
                | "enviadoel"
                | "envoyé"
                | "objet"
                | "von"
                | "gesendet"
                | "betreff"
                | "da"
                | "inviato"
                | "oggetto"
                | "van"
                | "verzonden"
                | "onderwerp"
                | "od"
                | "wysłano"
                | "temat"
        )
    };
    let is_anchor_key = |key: &str| {
        matches!(
            key,
            "from"
                | "sent"
                | "date"
                | "subject"
                | "de"
                | "enviado"
                | "enviadoel"
                | "envoyé"
                | "objet"
                | "von"
                | "gesendet"
                | "betreff"
                | "inviato"
                | "oggetto"
                | "verzonden"
                | "onderwerp"
                | "wysłano"
                | "temat"
        )
    };

    let mut bundle_start: Option<usize> = None;
    for start in 0..lines.len() {
        let mut header_hits = 0usize;
        let mut anchor_hits = 0usize;
        let end = (start + 10).min(lines.len());
        for line in lines.iter().take(end).skip(start) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Some(key) = normalize_key(line) else {
                continue;
            };
            if is_header_key(&key) {
                header_hits += 1;
                if is_anchor_key(&key) {
                    anchor_hits += 1;
                }
            }
        }
        if header_hits >= 3 && anchor_hits >= 1 {
            bundle_start = Some(start);
            break;
        }
    }

    if let Some(start) = bundle_start {
        lines[..start].join("\n").trim().to_string()
    } else {
        text.to_string()
    }
}

fn parse_forwarded_headers(raw_block: &str) -> (ParsedForwardedHeaders, usize) {
    let mut headers = ParsedForwardedHeaders::default();
    let lines: Vec<&str> = raw_block.lines().collect();
    if lines.is_empty() {
        return (headers, 0);
    }

    let normalize_space_before_colon = |s: &str| -> String {
        let mut out = String::with_capacity(s.len());
        let mut it = s.chars().peekable();
        while let Some(ch) = it.next() {
            if ch == ' ' {
                while matches!(it.peek(), Some(' ')) {
                    let _ = it.next();
                }
                if matches!(it.peek(), Some(':')) {
                    continue;
                }
            }
            out.push(ch);
        }
        out
    };
    let normalize_forwarded_header_line = |line: &str| -> String {
        let mut s = line.trim_start();
        while let Some(rest) = s.strip_prefix('>') {
            s = rest.trim_start();
        }
        let s = normalize_space_before_colon(s.trim());
        let parse_emphasis = |input: &str, marker: char| -> Option<String> {
            if !input.starts_with(marker) {
                return None;
            }
            let colon = input.find(':')?;
            let key_raw = input.get(..colon)?.trim();
            let key = key_raw.trim_matches(marker).trim();
            if key.is_empty() {
                return None;
            }
            let mut after = input.get(colon + 1..)?.trim_start();
            if let Some(rest) = after.strip_prefix(marker) {
                after = rest.trim_start();
            }
            Some(format!("{key}:{after}"))
        };
        if let Some(v) = parse_emphasis(&s, '*') {
            return v;
        }
        if let Some(v) = parse_emphasis(&s, '_') {
            return v;
        }
        s
    };
    let split_header_key_value = |line: &str| -> Option<(String, String)> {
        let normalized = normalize_forwarded_header_line(line);
        let colon_idx = normalized.find(':')?;
        let key = normalized[..colon_idx].replace(' ', "").to_lowercase();
        let value = normalized[colon_idx + 1..].trim().to_string();
        Some((key, value))
    };
    let split_address_tokens = |value: &str| -> Vec<String> {
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut angle_depth = 0usize;
        let mut in_quote = false;
        for ch in value.chars() {
            match ch {
                '"' => {
                    in_quote = !in_quote;
                    cur.push(ch);
                }
                '<' if !in_quote => {
                    angle_depth += 1;
                    cur.push(ch);
                }
                '>' if !in_quote => {
                    angle_depth = angle_depth.saturating_sub(1);
                    cur.push(ch);
                }
                ',' | ';' if !in_quote && angle_depth == 0 => {
                    let t = cur.trim();
                    if !t.is_empty() {
                        out.push(t.to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(ch),
            }
        }
        let t = cur.trim();
        if !t.is_empty() {
            out.push(t.to_string());
        }
        out
    };
    let parse_addr_list = |value: &str| -> Vec<EmailAddress> {
        let mut out: Vec<EmailAddress> = Vec::new();
        for part in split_address_tokens(value) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let part = part.replace("mailto:", "").replace("MAILTO:", "");
            if let Some(a) = EmailAddress::parse(&part) {
                if !out.iter().any(|x| x.address == a.address) {
                    out.push(a);
                }
                continue;
            }
            for email in extract_email_candidates(&part) {
                if let Some(a) = EmailAddress::new(email, None) {
                    if !out.iter().any(|x| x.address == a.address) {
                        out.push(a);
                    }
                }
            }
        }
        out
    };

    let mut last_header_idx = None;
    let mut header_hits = 0usize;
    let scan_end = lines.len().min(80);
    let mut idx = 0usize;
    while idx < scan_end {
        let line = lines[idx];
        let t = line.trim();
        if t.is_empty() {
            if header_hits >= 2 {
                break;
            }
            idx += 1;
            continue;
        }
        if t.starts_with("====") || t.starts_with("----") {
            if header_hits >= 2 {
                break;
            }
            idx += 1;
            continue;
        }
        let Some((key, mut value)) = split_header_key_value(t) else {
            if header_hits >= 2 {
                break;
            }
            idx += 1;
            continue;
        };

        let is_addr_key = matches!(
            key.as_str(),
            "from"
                | "de"
                | "von"
                | "da"
                | "van"
                | "od"
                | "to"
                | "a"
                | "à"
                | "para"
                | "an"
                | "aan"
                | "do"
                | "cc"
        );
        let mut last_consumed = idx;
        let mut j = idx + 1;
        while j < scan_end {
            let next = lines[j].trim();
            if next.is_empty() {
                break;
            }
            if next.starts_with("====") || next.starts_with("----") {
                break;
            }
            if split_header_key_value(next).is_some() {
                break;
            }
            if is_addr_key || lines[j].starts_with(' ') || lines[j].starts_with('\t') {
                value.push(' ');
                value.push_str(next);
                last_consumed = j;
                j += 1;
                continue;
            }
            break;
        }

        let mut matched = true;
        match key.as_str() {
            "from" | "de" | "von" | "da" | "van" | "od" => {
                let parsed = parse_addr_list(&value);
                if !parsed.is_empty() {
                    headers.from = parsed;
                }
            }
            "to" | "a" | "à" | "para" | "an" | "aan" | "do" => {
                let parsed = parse_addr_list(&value);
                if !parsed.is_empty() {
                    headers.to = parsed;
                }
            }
            "cc" => {
                let parsed = parse_addr_list(&value);
                if !parsed.is_empty() {
                    headers.cc = parsed;
                }
            }
            "subject" | "objet" | "betreff" | "asunto" | "oggetto" | "onderwerp" | "temat" => {
                if !value.is_empty() {
                    headers.subject = Some(value.to_string());
                }
            }
            "date" | "sent" | "envoyé" | "gesendet" | "enviado" | "enviadoel" | "inviato"
            | "verzonden" | "wysłano" => {
                if !value.is_empty() {
                    headers.date = Some(value.to_string());
                }
            }
            "message-id" => {
                if !value.is_empty() {
                    headers.message_id = Some(normalize_message_id(&value));
                }
            }
            _ => matched = false,
        }
        if !matched {
            if header_hits >= 2 {
                break;
            }
            idx += 1;
            continue;
        }
        let raw_line = normalize_forwarded_header_line(t);
        headers.raw_lines.push(raw_line);
        header_hits += 1;
        last_header_idx = Some(last_consumed);
        idx = last_consumed + 1;
    }

    let body_start = if header_hits >= 2 {
        let mut i = last_header_idx.unwrap_or(0) + 1;
        while i < lines.len() && lines[i].trim().is_empty() {
            i += 1;
        }
        i
    } else {
        0
    };
    (headers, body_start)
}

fn normalize_name_like(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn parse_salutation_name(line: &str) -> Option<String> {
    const SALUTATION_PREFIXES: &[&str] = &[
        "hi",
        "hello",
        "dear",
        "bonjour",
        "salut",
        "hola",
        "buenos",
        "hallo",
        "hej",
        "ciao",
        "dzień dobry",
        "witam",
    ];
    const SALUTATION_TAIL_MARKERS: &[&str] = &[
        " thank ",
        " thanks ",
        " merci ",
        " gracias ",
        " danke ",
        " grazie ",
        " dank ",
        " dziękuj",
        " please ",
        " veuillez ",
        " por favor ",
        " bitte ",
        " per favore ",
        " alstublieft ",
        " proszę ",
        " transaction id",
        " order id",
        " confirmation id",
        " discount ",
        " sale ",
        " offer ",
        " deal ",
        " black friday ",
        " cyber monday ",
    ];
    let mut trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    for prefix in SALUTATION_PREFIXES {
        let exact = lower == *prefix;
        let spaced = lower.starts_with(&format!("{prefix} "));
        let punct = [',', ':', ';', '-', '.', '!']
            .iter()
            .any(|ch| lower.starts_with(&format!("{prefix}{ch}")));
        if !(exact || spaced || punct) {
            continue;
        }
        if exact {
            return None;
        }
        if let Some(rest) = trimmed.get(prefix.len()..) {
            trimmed = rest.trim_start();
            trimmed = trimmed.trim_start_matches(|c: char| {
                c.is_ascii_whitespace() || matches!(c, ',' | ':' | ';' | '-' | '.')
            });
        }
        break;
    }
    let mut candidate = trimmed;
    if let Some(pos) = candidate.find(',') {
        candidate = &candidate[..pos];
    } else if let Some(pos) = candidate.find(';') {
        candidate = &candidate[..pos];
    } else if let Some(pos) = format!(" {} ", candidate.to_ascii_lowercase()).find(" from ") {
        let cut = pos.saturating_sub(1);
        candidate = candidate.get(..cut).unwrap_or(candidate);
    } else {
        let lower = format!(" {} ", candidate.to_ascii_lowercase());
        for marker in SALUTATION_TAIL_MARKERS {
            if let Some(pos) = lower.find(marker) {
                let cut = pos.saturating_sub(1);
                candidate = candidate.get(..cut).unwrap_or(candidate);
                break;
            }
        }
    }
    let cleaned = candidate
        .trim()
        .trim_end_matches(|c: char| {
            c.is_ascii_whitespace() || matches!(c, ',' | '.' | ';' | ':' | '!' | '?')
        })
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if cleaned.is_empty() || !cleaned.chars().any(|c| c.is_alphanumeric()) {
        None
    } else {
        Some(cleaned)
    }
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
    fn trim_url_token(token: &str) -> &str {
        token.trim_matches(|c: char| {
            c == '<'
                || c == '>'
                || c == '('
                || c == ')'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == ','
                || c == ';'
                || c == '.'
                || c == '!'
                || c == '?'
                || c == '"'
                || c == '\''
        })
    }
    fn looks_like_host_path(raw: &str) -> bool {
        if raw.contains(' ') {
            return false;
        }
        if raw.contains('@') && !raw.contains('/') {
            return false;
        }
        let Some((host, _)) = raw.split_once('.') else {
            return false;
        };
        !host.is_empty()
    }
    fn looks_like_prefixed(raw: &str) -> bool {
        let lower = raw.to_ascii_lowercase();
        lower.starts_with("http://")
            || lower.starts_with("https://")
            || lower.starts_with("www.")
            || looks_like_host_path(raw)
    }
    fn canonicalize_url(raw: &str) -> Option<String> {
        let mut t = trim_url_token(raw).trim();
        if t.is_empty() {
            return None;
        }
        if let Some(rbracket) = t.find(']') {
            let after = t.get(rbracket + 1..)?.trim();
            if !after.is_empty() && looks_like_prefixed(after) {
                t = after;
            }
        }
        if let Some(pos) = t.to_ascii_lowercase().find("<http") {
            let inner = t.get(pos + 1..)?.trim();
            if !inner.is_empty() {
                t = inner;
            }
        } else if let Some(pos) = t.to_ascii_lowercase().find("<www.") {
            let inner = t.get(pos + 1..)?.trim();
            if !inner.is_empty() {
                t = inner;
            }
        }
        t = trim_url_token(t).trim();
        if let Some(close) = t.find('>') {
            let before = t.get(..close)?.trim();
            if !before.is_empty() {
                t = before;
            }
        }
        if let Some(md_mid) = t.find("](") {
            let inner = t.get(md_mid + 2..)?.trim();
            let inner = inner.strip_suffix(')').unwrap_or(inner);
            if !inner.is_empty() {
                t = inner;
            }
        }
        t = trim_url_token(t).trim();
        if t.is_empty() {
            return None;
        }
        let lower = t.to_ascii_lowercase();
        if lower.starts_with("mailto:") {
            return None;
        }
        let with_scheme = if lower.starts_with("http://") || lower.starts_with("https://") {
            t.to_string()
        } else if lower.starts_with("www.") || looks_like_host_path(t) {
            format!("https://{}", t)
        } else {
            return None;
        };
        let mut url = with_scheme.trim().to_string();
        while url.ends_with(['.', ',', ';', '!', '?', ')', ']']) {
            url.pop();
        }
        if url.is_empty() {
            return None;
        }
        let (host, _) = split_url_host_path(&url)?;
        if host.is_empty()
            || host.contains('[')
            || host.contains(']')
            || host.contains('<')
            || host.contains('>')
            || host.contains(' ')
            || !host.contains('.')
        {
            return None;
        }
        Some(url)
    }

    fn push_candidate_from_token(token: &str, out: &mut Vec<String>) {
        let t = token.trim();
        if t.is_empty() {
            return;
        }
        out.push(t.to_string());
        if let Some(start) = t.find("](") {
            if let Some(end_rel) = t.get(start + 2..).and_then(|s| s.find(')')) {
                if let Some(inner) = t.get(start + 2..start + 2 + end_rel) {
                    out.push(inner.to_string());
                }
            }
        }
        if let Some(pos) = t.to_ascii_lowercase().find("<http") {
            if let Some(inner) = t.get(pos + 1..) {
                out.push(inner.to_string());
            }
        }
        if let Some(pos) = t.to_ascii_lowercase().find("<www.") {
            if let Some(inner) = t.get(pos + 1..) {
                out.push(inner.to_string());
            }
        }
        if let Some(rbracket) = t.find(']') {
            if let Some(after) = t.get(rbracket + 1..) {
                out.push(after.to_string());
            }
        }
    }

    let mut out = Vec::new();
    let mut raw_candidates = Vec::new();
    for token in text.split_whitespace() {
        push_candidate_from_token(token, &mut raw_candidates);
    }
    for raw in raw_candidates {
        if let Some(url) = canonicalize_url(&raw) {
            if !out.iter().any(|u| u == &url) {
                out.push(url);
            }
        }
    }
    out
}

fn split_url_host_path(url: &str) -> Option<(String, String)> {
    let mut u = url.trim();
    if let Some(rest) = u.strip_prefix("http://") {
        u = rest;
    } else if let Some(rest) = u.strip_prefix("https://") {
        u = rest;
    }
    let u = u.trim_start_matches("www.");
    let mut end = u.len();
    for sep in ['/', '?', '#'] {
        if let Some(i) = u.find(sep) {
            end = end.min(i);
        }
    }
    let host = u.get(..end)?.trim().trim_matches('.');
    if host.is_empty() {
        return None;
    }
    let rest = u.get(end..).unwrap_or("").to_string();
    Some((host.to_ascii_lowercase(), rest))
}

fn root_domain(domain: &str) -> String {
    let parts: Vec<&str> = domain.split('.').filter(|p| !p.is_empty()).collect();
    if parts.len() >= 2 {
        format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        domain.to_string()
    }
}

fn first_path_segment(path: &str) -> Option<String> {
    let p = path.trim_start_matches('/');
    let p = p.split(['/', '?', '#']).next().unwrap_or("").trim();
    if p.is_empty() {
        None
    } else {
        Some(p.to_ascii_lowercase())
    }
}

fn normalize_token_alnum(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn tokenize_name_for_match(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_ascii_alphanumeric())
        .filter_map(|t| {
            let t = t.trim().to_ascii_lowercase();
            if t.len() >= 3 { Some(t) } else { None }
        })
        .collect()
}

fn classify_profile_url(url: &str) -> (ContactProfileType, Option<String>, String) {
    let Some((host, path)) = split_url_host_path(url) else {
        return (ContactProfileType::Other, None, String::new());
    };
    let seg = first_path_segment(&path);
    if host.ends_with("linkedin.com") {
        let p = path.trim_start_matches('/').to_ascii_lowercase();
        if p.starts_with("company/") {
            let handle = p
                .trim_start_matches("company/")
                .split('/')
                .next()
                .unwrap_or("")
                .trim();
            let h = if handle.is_empty() {
                None
            } else {
                Some(handle.to_string())
            };
            return (ContactProfileType::LinkedinCompany, h, host);
        }
        if p.starts_with("in/") {
            let handle = p
                .trim_start_matches("in/")
                .split('/')
                .next()
                .unwrap_or("")
                .trim();
            let h = if handle.is_empty() {
                None
            } else {
                Some(handle.to_string())
            };
            return (ContactProfileType::LinkedinPerson, h, host);
        }
        return (ContactProfileType::Other, seg, host);
    }
    if host.ends_with("x.com") || host.ends_with("twitter.com") {
        return (ContactProfileType::TwitterX, seg, host);
    }
    if host.ends_with("instagram.com") {
        return (ContactProfileType::Instagram, seg, host);
    }
    if host.ends_with("tiktok.com") {
        return (ContactProfileType::Tiktok, seg, host);
    }
    if host.ends_with("youtube.com") || host.ends_with("youtu.be") {
        return (ContactProfileType::Youtube, seg, host);
    }
    if host.ends_with("facebook.com") {
        return (ContactProfileType::Facebook, seg, host);
    }
    if host.ends_with("github.com") {
        return (ContactProfileType::Github, seg, host);
    }
    (ContactProfileType::Website, None, host)
}

fn derive_signature_line_buckets(sig: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let org_tokens = [
        "inc",
        "llc",
        "corp",
        "company",
        "sas",
        "gmbh",
        "ltd",
        "sa",
        "sarl",
        "ag",
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
        "avenue",
        "street",
        "road",
        "blvd",
        "boulevard",
        "city",
        "france",
        "germany",
        "canada",
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
            url: None,
            profile_type: None,
            handle: None,
            source: source.clone(),
            role: role.clone(),
            confidence: HintConfidence::High,
            company_domain: email_domain(&email),
            link_key: compute_link_key(a.name.as_deref(), Some(&email)),
            linked_entity_key: None,
            link_reason: None,
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
    push_header_contact_hints(
        &mut out,
        to,
        ContactHintSource::ToHeader,
        ContactHintRole::To,
    );
    push_header_contact_hints(
        &mut out,
        cc,
        ContactHintSource::CcHeader,
        ContactHintRole::Cc,
    );
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
        if let Some(n) = parse_salutation_name(line) {
            out.push(ParsedContactHint {
                name: Some(n.clone()),
                email: None,
                phone: None,
                url: None,
                profile_type: None,
                handle: None,
                source: ContactHintSource::Salutation,
                role: ContactHintRole::Mentioned,
                confidence: HintConfidence::Low,
                company_domain: None,
                link_key: compute_link_key(Some(&n), None),
                linked_entity_key: None,
                link_reason: None,
            });
        }
    }

    for email in &signature_entities.emails {
        out.push(ParsedContactHint {
            name: None,
            email: Some(email.clone()),
            phone: None,
            url: None,
            profile_type: None,
            handle: None,
            source: ContactHintSource::Signature,
            role: ContactHintRole::Mentioned,
            confidence: HintConfidence::Medium,
            company_domain: email_domain(email),
            link_key: compute_link_key(None, Some(email)),
            linked_entity_key: None,
            link_reason: None,
        });
    }
    for phone in &signature_entities.phones {
        out.push(ParsedContactHint {
            name: None,
            email: None,
            phone: Some(phone.clone()),
            url: None,
            profile_type: None,
            handle: None,
            source: ContactHintSource::Signature,
            role: ContactHintRole::Mentioned,
            confidence: HintConfidence::Low,
            company_domain: None,
            link_key: None,
            linked_entity_key: None,
            link_reason: None,
        });
    }

    #[derive(Clone)]
    struct LinkAnchor {
        entity_key: String,
        email_domain: Option<String>,
        name_tokens: Vec<String>,
        from_header: bool,
    }
    let mut anchors = Vec::new();
    for h in &out {
        let Some(entity_key) = h
            .link_key
            .clone()
            .or_else(|| compute_link_key(h.name.as_deref(), h.email.as_deref()))
        else {
            continue;
        };
        let domain = h
            .email
            .as_deref()
            .and_then(email_domain)
            .or_else(|| h.company_domain.clone());
        let mut name_tokens = Vec::new();
        if let Some(name) = h.name.as_deref() {
            name_tokens.extend(tokenize_name_for_match(name));
        }
        anchors.push(LinkAnchor {
            entity_key,
            email_domain: domain,
            name_tokens,
            from_header: h.source == ContactHintSource::FromHeader,
        });
    }

    let mut seen_urls = std::collections::HashSet::new();
    let org_tokens: Vec<String> = signature_entities
        .organization_lines
        .iter()
        .flat_map(|l| tokenize_name_for_match(l))
        .collect();
    for url in &signature_entities.urls {
        let norm = url.trim();
        if norm.is_empty() {
            continue;
        }
        let key = norm.to_ascii_lowercase();
        if !seen_urls.insert(key) {
            continue;
        }
        let (profile_type, handle, host) = classify_profile_url(norm);
        let handle_norm = handle
            .as_deref()
            .map(normalize_token_alnum)
            .filter(|h| !h.is_empty());
        let host_root = if host.is_empty() {
            None
        } else {
            Some(root_domain(&host))
        };

        let mut best: Option<(usize, &LinkAnchor, &'static str)> = None;
        for a in &anchors {
            let mut score = 0usize;
            let mut reason = "weak";
            if let (Some(ad), Some(hr)) = (a.email_domain.as_deref(), host_root.as_deref()) {
                if root_domain(ad) == *hr {
                    score += 3;
                    reason = "domain_match";
                }
            }
            if let Some(handle_value) = handle_norm.as_deref() {
                if a.name_tokens
                    .iter()
                    .any(|t| handle_value.contains(t) || t.contains(handle_value))
                {
                    score += 2;
                    reason = "name_handle_match";
                }
                if matches!(profile_type, ContactProfileType::LinkedinCompany)
                    && org_tokens
                        .iter()
                        .any(|t| handle_value.contains(t) || t.contains(handle_value))
                {
                    score += 2;
                    reason = "org_slug_match";
                }
            }
            if a.from_header {
                score += 1;
            }
            match best {
                Some((best_score, best_anchor, _))
                    if score < best_score || (score == best_score && best_anchor.from_header) => {}
                _ => {
                    if score > 0 {
                        best = Some((score, a, reason));
                    }
                }
            }
        }

        let mut confidence = if matches!(profile_type, ContactProfileType::Website) {
            HintConfidence::Low
        } else {
            HintConfidence::Medium
        };
        let mut linked_entity_key = None;
        let mut link_reason = None;
        let mut hint_link_key = None;
        if let Some((score, anchor, reason)) = best {
            if score >= 3 {
                linked_entity_key = Some(anchor.entity_key.clone());
                link_reason = Some(reason.to_string());
                hint_link_key = Some(anchor.entity_key.clone());
                confidence = if score >= 5 {
                    HintConfidence::High
                } else {
                    HintConfidence::Medium
                };
            }
        }

        out.push(ParsedContactHint {
            name: None,
            email: None,
            phone: None,
            url: Some(norm.to_string()),
            profile_type: Some(profile_type),
            handle,
            source: ContactHintSource::Signature,
            role: ContactHintRole::Mentioned,
            confidence,
            company_domain: if host.is_empty() {
                None
            } else {
                Some(host.clone())
            },
            link_key: hint_link_key,
            linked_entity_key,
            link_reason,
        });
    }
    out
}

fn derive_attachment_hints(attachments: &[ParsedAttachment]) -> Vec<ParsedAttachmentHint> {
    let logo_tokens = [
        "logo",
        "signature",
        "icon",
        "linkedin",
        "twitter",
        "facebook",
    ];
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

fn extract_event_hints(
    subject: Option<&str>,
    reply_text: &str,
    primary_mail_kind: Option<&MailKind>,
    lexicon: &LifecycleLexicon,
) -> Vec<ParsedEventHint> {
    let text = reply_text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    let mut datetime_candidates = Vec::new();
    let mut location_candidates = Vec::new();
    let mut meeting_links = Vec::new();
    let mut timezone_candidates = Vec::new();
    let is_news_like = matches!(
        primary_mail_kind,
        Some(MailKind::Newsletter) | Some(MailKind::Promotion)
    );

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
    let weekday_tokens = [
        "mon",
        "monday",
        "tue",
        "tuesday",
        "wed",
        "wednesday",
        "thu",
        "thursday",
        "fri",
        "friday",
        "sat",
        "saturday",
        "sun",
        "sunday",
    ];
    let tz_tokens = [
        "utc", "gmt", "cet", "cest", "pst", "pdt", "est", "edt", "bst", "cst", "cdt", "mst", "mdt",
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
    let street_tokens = ["street", "road", "avenue", "boulevard", "lane", "drive"];
    let location_tokens = [
        "venue", "office", "room", "suite", "unit", "building", "campus", "floor",
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
            if !parts
                .iter()
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
            {
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
    let has_measurement_unit_noise = |line: &str| -> bool {
        const UNITS: &[&str] = &[
            "mm", "cm", "m", "km", "kg", "g", "mg", "lb", "lbs", "oz", "ml", "l", "bar", "psi",
            "v", "kv", "ma", "a", "w", "kw", "db", "hz", "khz", "mhz", "ghz", "c", "f",
        ];
        line.split_whitespace().any(|tok| {
            let t = clean_token(tok).to_ascii_lowercase();
            if t.len() < 3 {
                return false;
            }
            let digit_prefix_len = t.chars().take_while(|c| c.is_ascii_digit()).count();
            if digit_prefix_len == 0 || digit_prefix_len >= t.len() {
                return false;
            }
            let suffix = &t[digit_prefix_len..];
            UNITS.iter().any(|u| suffix == *u)
        })
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
        if lower.contains("utc+")
            || lower.contains("utc-")
            || lower.contains("gmt+")
            || lower.contains("gmt-")
        {
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
    let is_location_candidate_line = |line: &str| -> bool {
        let lower = line.to_ascii_lowercase();
        if lower.contains("http://") || lower.contains("https://") {
            return false;
        }
        if line.len() < 8 || line.len() > 120 {
            return false;
        }
        if lower.starts_with('*') || lower.starts_with('#') || lower.starts_with("- ") {
            return false;
        }
        if lower.contains('[') || lower.contains(']') || lower.contains('(') || lower.contains(')')
        {
            return false;
        }
        let has_country = [
            "indonesia",
            "singapore",
            "france",
            "germany",
            "spain",
            "italy",
            "poland",
            "netherlands",
            "belgium",
            "switzerland",
            "usa",
            "united states",
            "uk",
        ]
        .iter()
        .any(|t| lower.contains(t));
        let has_street_like = street_tokens.iter().any(|t| lower.contains(t))
            && line.chars().any(|c| c.is_ascii_digit());
        let has_room_like = location_tokens.iter().any(|t| lower.contains(t))
            && line.chars().any(|c| c.is_ascii_digit());
        let has_city_country_like = line.contains(',')
            && line.len() <= 64
            && has_country
            && line
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .count()
                >= 2;
        has_street_like || has_room_like || has_city_country_like
    };
    let looks_like_numbered_marketing_line = |line: &str| -> bool {
        let l = line.trim();
        let after_number = if let Some(pos) = l.find(". ") {
            if l[..pos].chars().all(|c| c.is_ascii_digit()) {
                &l[pos + 2..]
            } else {
                l
            }
        } else if let Some(pos) = l.find(") ") {
            if l[..pos].chars().all(|c| c.is_ascii_digit()) {
                &l[pos + 2..]
            } else {
                l
            }
        } else {
            l
        };
        if after_number == l {
            return false;
        }
        let lower = after_number.to_ascii_lowercase();
        lexicon.has_event_marketing_list_noise(&lower)
    };
    let is_marketing_bullet_line = |line: &str| -> bool {
        let l = line.trim_start();
        if !(l.starts_with("* ") || l.starts_with("- ")) {
            return false;
        }
        let lower = l.to_ascii_lowercase();
        lexicon.has_event_marketing_list_noise(&lower)
    };
    let is_numbered_list_item = |line: &str| -> bool {
        let l = line.trim_start_matches(['#', '-', '*', ' ']).trim_start();
        if let Some(pos) = l.find(". ") {
            return pos > 0 && l[..pos].chars().all(|c| c.is_ascii_digit());
        }
        if let Some(pos) = l.find(") ") {
            return pos > 0 && l[..pos].chars().all(|c| c.is_ascii_digit());
        }
        false
    };
    let is_marketing_list_heading = |line: &str| -> bool {
        let l = line.trim();
        let lower = l.to_ascii_lowercase();
        let has_heading_pattern = l.starts_with('#')
            || lower
                .split_whitespace()
                .next()
                .map(|tok| tok.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false);
        has_heading_pattern && lexicon.has_event_marketing_list_noise(&lower)
    };
    let mut marketing_list_block_countdown = 0usize;

    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if header_prefixes.iter().any(|p| lower.starts_with(p)) {
            continue;
        }
        let likely_noise_prose = l.len() > 180 || (l.len() > 120 && lower.contains("http"));
        let has_time = is_time_like(l);
        let has_date_anchor = has_strong_date_anchor(l);
        let has_explicit_date_shape = l.split_whitespace().any(is_numeric_date_token)
            || has_month_range(l)
            || has_weekday_and_date(l);
        if is_news_like && is_marketing_list_heading(l) {
            marketing_list_block_countdown = 8;
        }
        let is_marketing_numbered_list_noise = is_news_like
            && !has_time
            && !has_explicit_date_shape
            && (looks_like_numbered_marketing_line(l)
                || is_marketing_list_heading(l)
                || (marketing_list_block_countdown > 0 && is_numbered_list_item(l))
                || is_marketing_bullet_line(l));
        if has_date_anchor
            && !likely_noise_prose
            && !is_marketing_numbered_list_noise
            && !(has_measurement_unit_noise(l) && !has_time && !has_explicit_date_shape)
        {
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
        if is_location_candidate_line(l) {
            location_candidates.push(l.to_string());
        }
        if marketing_list_block_countdown > 0 {
            marketing_list_block_countdown -= 1;
        }
    }
    location_candidates.dedup();
    if location_candidates.len() > 3 {
        location_candidates.truncate(3);
    }

    if datetime_candidates.is_empty() {
        return Vec::new();
    }

    let kind_source = format!(
        "{}\n{}",
        subject.unwrap_or_default().to_ascii_lowercase(),
        text.to_ascii_lowercase()
    );
    let has_shipping_intent = lexicon.has_event_shipping_intent(&kind_source);
    let has_shipping_structure = lexicon.has_event_shipping_structure(&kind_source);
    let has_hard_shipping_structure = lexicon.has_event_shipping_hard_structure(&kind_source);
    let has_meeting_intent = lexicon.has_event_meeting_intent(&kind_source);
    let has_meeting_invite_verb = lexicon.has_event_meeting_invite_verb(&kind_source);
    let has_meeting_link = !meeting_links.is_empty();
    let has_reservation_intent = lexicon.has_event_reservation_intent(&kind_source);
    let reservation_type = lexicon.classify_reservation_type(&kind_source);
    let has_short_structured_datetime = datetime_candidates
        .iter()
        .any(|c| c.has_time || c.raw.len() <= 96);
    let has_explicit_schedule_anchor = text.lines().any(|line| {
        let l = line.trim();
        if l.is_empty() {
            return false;
        }
        is_time_like(l) || l.split_whitespace().any(is_numeric_date_token) || has_month_range(l)
            || has_weekday_and_date(l)
    });
    let allow_shipping = if is_news_like {
        has_shipping_intent && has_hard_shipping_structure && has_short_structured_datetime
    } else {
        has_shipping_intent && has_shipping_structure && has_short_structured_datetime
    };
    let allow_meeting = if is_news_like {
        has_meeting_link
            && (has_meeting_intent || has_meeting_invite_verb)
            && has_short_structured_datetime
    } else {
        has_meeting_intent && (has_meeting_link || has_meeting_invite_verb)
    };
    let has_promo_sale_signal = [
        "sale",
        "discount",
        "% off",
        "flash sale",
        "deal",
        "lightning deal",
        "coupon",
        "promo",
    ]
    .iter()
    .any(|k| kind_source.contains(k));
    let allow_reservation = has_reservation_intent
        && has_short_structured_datetime
        && !(is_news_like && has_promo_sale_signal);

    let kind = if allow_shipping {
        EventHintKind::Shipping
    } else if allow_meeting {
        EventHintKind::Meeting
    } else if allow_reservation {
        EventHintKind::Reservation
    } else if lexicon.has_event_deadline_signal(&kind_source) && has_short_structured_datetime {
        EventHintKind::Deadline
    } else if lexicon.has_event_availability_signal(&kind_source)
        && has_short_structured_datetime
        && has_explicit_schedule_anchor
        && !(is_news_like && has_promo_sale_signal)
    {
        EventHintKind::Availability
    } else {
        EventHintKind::Generic
    };
    if is_news_like && kind == EventHintKind::Generic {
        return Vec::new();
    }

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
        reservation_type: if allow_reservation {
            reservation_type
        } else {
            None
        },
        datetime_candidates,
        location_candidates,
        meeting_links,
        timezone_candidates,
        is_complete,
        missing_fields,
        confidence,
    }]
}

fn extract_mail_kind_hints(
    subject: Option<&str>,
    body_canonical: &str,
    raw_headers: &std::collections::BTreeMap<String, String>,
    service_lifecycle_hints: &[ParsedServiceLifecycleHint],
) -> Vec<ParsedMailKindHint> {
    use std::collections::HashMap;
    let mut scores: HashMap<MailKind, i32> = HashMap::new();
    let mut signals: HashMap<MailKind, Vec<String>> = HashMap::new();
    let mut add = |kind: MailKind, weight: i32, signal: &str| {
        *scores.entry(kind.clone()).or_insert(0) += weight;
        signals.entry(kind).or_default().push(signal.to_string());
    };

    let header = |k: &str| raw_headers.get(k).map(|s| s.to_ascii_lowercase());
    if raw_headers.contains_key("list-unsubscribe") {
        add(MailKind::Newsletter, 3, "header:list_unsubscribe");
    }
    if raw_headers.contains_key("list-id") {
        add(MailKind::Newsletter, 3, "header:list_id");
    }
    if let Some(p) = header("precedence") {
        if p.contains("bulk") || p.contains("list") {
            add(MailKind::Newsletter, 2, "header:precedence_bulk");
        }
    }
    if let Some(a) = header("auto-submitted") {
        if a.contains("auto-generated") {
            add(MailKind::Notification, 2, "header:auto_submitted");
        }
    }

    let text = format!(
        "{}\n{}",
        subject.unwrap_or_default().to_ascii_lowercase(),
        body_canonical.to_ascii_lowercase()
    );

    for token in [
        "sale",
        "discount",
        "% off",
        "coupon",
        "limited time",
        "deal",
        "promo",
        "promotion",
    ] {
        if text.contains(token) {
            add(MailKind::Promotion, 2, &format!("token:{token}"));
        }
    }
    for token in [
        "unsubscribe",
        "manage preferences",
        "view in browser",
        "newsletter",
    ] {
        if text.contains(token) {
            add(MailKind::Newsletter, 2, &format!("token:{token}"));
        }
    }
    for token in [
        "receipt", "invoice", "order", "shipment", "tracking", "waybill", "courier",
    ] {
        if text.contains(token) {
            add(MailKind::Transactional, 2, &format!("token:{token}"));
        }
    }
    for token in ["alert", "notification", "reminder", "digest", "update"] {
        if text.contains(token) {
            add(MailKind::Notification, 1, &format!("token:{token}"));
        }
    }
    for token in ["thanks", "thank you", "please", "can you", "could you"] {
        if text.contains(token) {
            add(MailKind::Personal, 1, &format!("token:{token}"));
        }
    }

    if let Some(from) = raw_headers.get("from").map(|s| s.to_ascii_lowercase()) {
        if from.contains("no-reply")
            || from.contains("noreply")
            || from.contains("newsletter")
            || from.contains("updates@")
        {
            add(MailKind::Notification, 1, "header:from_automation");
        }
    }
    if service_lifecycle_hints
        .iter()
        .any(|h| h.confidence == HintConfidence::High && h.kind != ServiceLifecycleKind::Unknown)
    {
        add(
            MailKind::Transactional,
            3,
            "service_lifecycle:high_confidence",
        );
    }

    let mut ordered: Vec<(MailKind, i32, Vec<String>)> = scores
        .into_iter()
        .map(|(k, s)| (k.clone(), s, signals.remove(&k).unwrap_or_default()))
        .collect();
    ordered.sort_by(|a, b| b.1.cmp(&a.1));

    if ordered.is_empty() {
        return vec![ParsedMailKindHint {
            kind: MailKind::Unknown,
            confidence: HintConfidence::Low,
            signals: vec!["fallback:no_signal".to_string()],
            is_primary: true,
        }];
    }

    let top = &ordered[0];
    let second_score = ordered.get(1).map(|x| x.1).unwrap_or(0);
    let lead = top.1 - second_score;
    let confidence = if top.1 >= 5 && lead >= 2 {
        HintConfidence::High
    } else if top.1 >= 3 {
        HintConfidence::Medium
    } else {
        HintConfidence::Low
    };

    let mut out = vec![ParsedMailKindHint {
        kind: top.0.clone(),
        confidence,
        signals: top.2.clone(),
        is_primary: true,
    }];
    if let Some(second) = ordered.get(1) {
        if top.1 - second.1 <= 1 && second.1 >= 3 {
            out.push(ParsedMailKindHint {
                kind: second.0.clone(),
                confidence: HintConfidence::Medium,
                signals: second.2.clone(),
                is_primary: false,
            });
        }
    }
    out
}

fn extract_unsubscribe_hints(
    raw_headers: &std::collections::BTreeMap<String, String>,
    body_canonical: &str,
) -> Vec<ParsedUnsubscribeHint> {
    use std::collections::HashSet;
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut push_hint = |hint: ParsedUnsubscribeHint| {
        let key = format!(
            "{:?}|{:?}|{}|{}",
            hint.source,
            hint.kind,
            hint.url.clone().unwrap_or_default(),
            hint.email.clone().unwrap_or_default()
        );
        if seen.insert(key) {
            out.push(hint);
        }
    };

    let parse_unsub_header = |value: &str| -> (Vec<String>, Vec<String>) {
        let mut urls = Vec::new();
        let mut mails = Vec::new();
        for part in value.split(',') {
            let p = part.trim().trim_matches(['<', '>']);
            if p.is_empty() {
                continue;
            }
            if let Some(addr) = p.strip_prefix("mailto:") {
                mails.push(addr.trim().to_string());
            } else if p.starts_with("http://") || p.starts_with("https://") {
                urls.push(p.to_string());
            }
        }
        (urls, mails)
    };

    if let Some(v) = raw_headers.get("list-unsubscribe") {
        let (urls, mails) = parse_unsub_header(v);
        for u in urls {
            push_hint(ParsedUnsubscribeHint {
                kind: UnsubscribeKind::Url,
                source: UnsubscribeSource::HeaderListUnsubscribe,
                url: Some(u),
                email: None,
                label: Some("list_unsubscribe".to_string()),
                confidence: HintConfidence::High,
            });
        }
        for m in mails {
            push_hint(ParsedUnsubscribeHint {
                kind: UnsubscribeKind::MailTo,
                source: UnsubscribeSource::HeaderListUnsubscribe,
                url: None,
                email: Some(m),
                label: Some("list_unsubscribe".to_string()),
                confidence: HintConfidence::High,
            });
        }
    }
    if let Some(v) = raw_headers.get("list-unsubscribe-post") {
        let lower = v.to_ascii_lowercase();
        push_hint(ParsedUnsubscribeHint {
            kind: if lower.contains("one-click") {
                UnsubscribeKind::OneClick
            } else {
                UnsubscribeKind::Unknown
            },
            source: UnsubscribeSource::HeaderListUnsubscribePost,
            url: None,
            email: None,
            label: Some(v.clone()),
            confidence: HintConfidence::High,
        });
    }

    let extract_urls = |s: &str| -> Vec<String> {
        s.split_whitespace()
            .filter_map(|tok| {
                let t = tok.trim_matches(|c: char| {
                    c.is_whitespace()
                        || matches!(c, '<' | '>' | '(' | ')' | '[' | ']' | '"' | '\'' | ',')
                });
                if t.starts_with("http://") || t.starts_with("https://") {
                    Some(t.to_string())
                } else {
                    None
                }
            })
            .collect()
    };

    for line in body_canonical.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if lower.contains("unsubscribe") || lower.contains("manage preferences") {
            let urls = extract_urls(l);
            if urls.is_empty() {
                push_hint(ParsedUnsubscribeHint {
                    kind: UnsubscribeKind::Unknown,
                    source: UnsubscribeSource::BodyText,
                    url: None,
                    email: None,
                    label: Some(l.to_string()),
                    confidence: HintConfidence::Low,
                });
            } else {
                for u in urls {
                    push_hint(ParsedUnsubscribeHint {
                        kind: if lower.contains("manage preferences") {
                            UnsubscribeKind::ManagePreferences
                        } else {
                            UnsubscribeKind::Url
                        },
                        source: UnsubscribeSource::BodyLink,
                        url: Some(u),
                        email: None,
                        label: Some("unsubscribe_link".to_string()),
                        confidence: HintConfidence::Medium,
                    });
                }
            }
        }
    }

    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleGateDecision {
    Allow,
    Block,
    AllowStrongOnly,
}

fn decide_lifecycle_gate(
    subject: Option<&str>,
    body_canonical: &str,
    raw_headers: &std::collections::BTreeMap<String, String>,
) -> LifecycleGateDecision {
    let mk = extract_mail_kind_hints(subject, body_canonical, raw_headers, &[]);
    let primary = mk
        .iter()
        .find(|h| h.is_primary)
        .map(|h| h.kind.clone())
        .unwrap_or(MailKind::Unknown);
    match primary {
        MailKind::Transactional | MailKind::Notification => LifecycleGateDecision::Allow,
        MailKind::Newsletter | MailKind::Promotion => LifecycleGateDecision::Block,
        MailKind::Personal | MailKind::Unknown => LifecycleGateDecision::AllowStrongOnly,
    }
}

fn has_strong_lifecycle_structure(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let markers = [
        "customer name:",
        "customer email:",
        "offer:",
        "plan:",
        "invoice number:",
        "order id:",
        "payment failed",
        "due date:",
    ];
    let marker_count = markers.iter().filter(|m| lower.contains(**m)).count();
    marker_count >= 2
}

fn extract_service_lifecycle_hints(
    subject: Option<&str>,
    body_canonical: &str,
    raw_headers: &std::collections::BTreeMap<String, String>,
    from: &[EmailAddress],
    gate: LifecycleGateDecision,
    lexicon: &LifecycleLexicon,
) -> Vec<ParsedServiceLifecycleHint> {
    let text = format!(
        "{}\n{}",
        subject.unwrap_or_default().to_ascii_lowercase(),
        body_canonical.to_ascii_lowercase()
    );
    let strong_structure = has_strong_lifecycle_structure(&text);
    let has_keyword = lexicon.has_lifecycle_keyword(&text);
    let has_order_or_ticket_confirmation = lexicon.has_confirmation_gate_match(&text);
    let sender = raw_headers
        .get("from")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let known_billing_sender = lexicon.is_known_billing_sender(&sender);
    let gate_allows = match gate {
        LifecycleGateDecision::Allow => true,
        LifecycleGateDecision::Block => strong_structure && has_keyword,
        LifecycleGateDecision::AllowStrongOnly => {
            (strong_structure && has_keyword)
                || known_billing_sender
                || has_order_or_ticket_confirmation
        }
    };
    if !gate_allows {
        return Vec::new();
    }

    let Some(rule_match) = lexicon.classify_lifecycle(&text) else {
        return Vec::new();
    };
    let kind = rule_match.kind;

    if kind == ServiceLifecycleKind::Unknown {
        return Vec::new();
    }
    let mut signals = Vec::new();
    if let Some(s) = rule_match.signal {
        signals.push(s);
    }

    let mut customer_name = None;
    let mut customer_email = None;
    let mut plan_name = None;
    let mut amount_raw = None;
    let mut currency = None;
    let mut effective_date_raw = None;

    for line in body_canonical.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if lower.starts_with("customer name:") {
            customer_name = Some(l["customer name:".len()..].trim().to_string());
        } else if lower.starts_with("customer email:") {
            customer_email = Some(l["customer email:".len()..].trim().to_string());
        } else if lower.starts_with("offer:") || lower.starts_with("plan:") {
            let idx = l.find(':').unwrap_or(0);
            plan_name = Some(l[idx + 1..].trim().to_string());
        } else if lower.starts_with("date:")
            || lower.starts_with("effective date:")
            || lower.starts_with("renewal date:")
        {
            let idx = l.find(':').unwrap_or(0);
            effective_date_raw = Some(l[idx + 1..].trim().to_string());
        } else if lower.contains('$')
            || lower.contains(" usd")
            || lower.contains(" eur")
            || lower.contains(" gbp")
        {
            amount_raw = Some(l.to_string());
            currency = if lower.contains('$') || lower.contains(" usd") {
                Some("USD".to_string())
            } else if lower.contains(" eur") {
                Some("EUR".to_string())
            } else if lower.contains(" gbp") {
                Some("GBP".to_string())
            } else {
                None
            };
        }
    }

    let provider = raw_headers
        .get("from")
        .and_then(|f| f.split('@').nth(1))
        .map(|s| s.trim().trim_matches('>').to_string())
        .or_else(|| from.first().map(|a| a.address.clone()));

    let confidence = if strong_structure {
        HintConfidence::High
    } else {
        match kind {
            ServiceLifecycleKind::SubscriptionCanceled
            | ServiceLifecycleKind::SubscriptionRenewed => HintConfidence::Medium,
            ServiceLifecycleKind::SubscriptionCreated
            | ServiceLifecycleKind::MembershipUpdated
            | ServiceLifecycleKind::OrderConfirmation
            | ServiceLifecycleKind::TicketConfirmation
            | ServiceLifecycleKind::BillingNotice => HintConfidence::Low,
            ServiceLifecycleKind::Unknown => HintConfidence::Low,
        }
    };

    vec![ParsedServiceLifecycleHint {
        kind: kind.clone(),
        confidence,
        provider,
        plan_name,
        customer_name,
        customer_email,
        amount_raw,
        currency,
        effective_date_raw,
        signals,
    }]
}

fn extract_billing_action_hints(
    _raw_headers: &std::collections::BTreeMap<String, String>,
    body_canonical: &str,
    related_lifecycle_kind: Option<ServiceLifecycleKind>,
    lexicon: &LifecycleLexicon,
) -> Vec<ParsedBillingActionHint> {
    use std::collections::HashSet;
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for line in body_canonical.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let ll = l.to_ascii_lowercase();
        let Some(kind) = lexicon.classify_billing_action_line(&ll) else {
            continue;
        };
        let urls: Vec<String> = l
            .split_whitespace()
            .filter_map(|tok| {
                let t = tok.trim_matches(|c: char| {
                    c.is_whitespace()
                        || matches!(c, '<' | '>' | '(' | ')' | '[' | ']' | '"' | '\'' | ',')
                });
                if t.starts_with("http://") || t.starts_with("https://") {
                    Some(t.to_string())
                } else {
                    None
                }
            })
            .collect();
        if urls.is_empty() {
            let key = format!("text|{:?}|{}", kind, ll);
            if seen.insert(key) {
                out.push(ParsedBillingActionHint {
                    kind,
                    url: None,
                    label: Some(l.to_string()),
                    source: BillingActionSource::BodyText,
                    confidence: HintConfidence::Low,
                    related_lifecycle_kind: related_lifecycle_kind.clone(),
                });
            }
        } else {
            for u in urls {
                let key = format!("url|{:?}|{}", kind, u);
                if seen.insert(key) {
                    out.push(ParsedBillingActionHint {
                        kind: kind.clone(),
                        url: Some(u),
                        label: Some(l.to_string()),
                        source: BillingActionSource::BodyLink,
                        confidence: HintConfidence::Medium,
                        related_lifecycle_kind: related_lifecycle_kind.clone(),
                    });
                }
            }
        }
    }

    out
}

fn infer_direction_hint(
    from: &[EmailAddress],
    to: &[EmailAddress],
    cc: &[EmailAddress],
    bcc: &[EmailAddress],
    reply_to: &[EmailAddress],
    owner_emails: &[String],
) -> Option<ParsedDirectionHint> {
    use std::collections::HashSet;
    let owners: HashSet<String> = owner_emails
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if owners.is_empty() {
        return None;
    }

    let addr_is_owner = |a: &EmailAddress| owners.contains(&a.address.to_ascii_lowercase());
    let from_has_owner = from.iter().any(addr_is_owner);
    let from_has_non_owner = from.iter().any(|a| !addr_is_owner(a));
    let recipients: Vec<&EmailAddress> = to
        .iter()
        .chain(cc.iter())
        .chain(bcc.iter())
        .chain(reply_to.iter())
        .collect();
    let rec_has_owner = recipients.iter().any(|a| addr_is_owner(a));
    let rec_has_non_owner = recipients.iter().any(|a| !addr_is_owner(a));
    let matched_owner = owners.iter().next().cloned();

    let (direction, confidence, reason) =
        if from_has_owner && rec_has_non_owner && !from_has_non_owner {
            (MailDirection::Outbound, HintConfidence::High, "from_owner")
        } else if !from_has_owner && rec_has_owner {
            (MailDirection::Inbound, HintConfidence::High, "to_owner")
        } else if from_has_owner && rec_has_owner {
            (MailDirection::SelfMessage, HintConfidence::Medium, "both")
        } else {
            (MailDirection::Unknown, HintConfidence::Low, "no_match")
        };

    Some(ParsedDirectionHint {
        direction,
        confidence,
        matched_owner,
        reason: Some(reason.to_string()),
    })
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
