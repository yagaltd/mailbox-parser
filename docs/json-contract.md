# JSON Contract

This document describes the JSON output format from `mailbox-parser-cli --json-profile canonical`.

## Canonical Thread Format

```json
{
  "thread_id": "string",
  "messages": [CanonicalMessage, ...]
}
```

## CanonicalMessage

| Field | Type | Description |
|-------|------|-------------|
| `message_key` | `string` | Unique message key (derived from References/In-Reply-To/Message-ID) |
| `uid` | `u32?` | IMAP UID (if synced via IMAP) |
| `internal_date` | `string?` | IMAP internal date timestamp |
| `message_id` | `string?` | RFC822 Message-ID header |
| `in_reply_to` | `string?` | In-Reply-To header |
| `references` | `string[]` | References header (decoded) |
| `subject` | `string?` | Subject header |
| `date` | `string?` | Date header (normalized) |
| `date_raw` | `string?` | Raw date header |
| `from` | `EmailAddress[]` | From addresses |
| `to` | `EmailAddress[]` | To addresses |
| `cc` | `EmailAddress[]` | Cc addresses |
| `bcc` | `EmailAddress[]` | Bcc addresses |
| `reply_to` | `EmailAddress[]` | Reply-To addresses |
| `reply_text` | `string` | Reply text with quoted history removed |
| `quoted_blocks` | `string[]` | Quoted history blocks (if any) |
| `forwarded_blocks` | `string[]` | Forwarded content blocks |
| `disclaimer_blocks` | `string[]` | Legal disclaimer blocks |
| `salutation` | `string?` | Detected salutation (e.g., "Hi John,") |
| `signature` | `string?` | Detected signature block |
| `attachments` | `CanonicalAttachment[]` | Attachments |
| `contact_hints` | `ParsedContactHint[]` | Extracted contact entities |
| `signature_entities` | `ParsedSignatureEntities` | Signature-detected entities |
| `attachment_hints` | `ParsedAttachmentHint[]` | Attachment classification hints |
| `event_hints` | `ParsedEventHint[]` | Event/date/location hints |
| `mail_kind_hints` | `ParsedMailKindHint[]` | Mail kind classification |
| `direction_hint` | `ParsedDirectionHint?` | Inbound/outbound classification |
| `unsubscribe_hints` | `ParsedUnsubscribeHint[]` | Unsubscribe URLs and metadata |
| `service_lifecycle_hints` | `ParsedServiceLifecycleHint[]` | Lifecycle event classification |
| `billing_action_hints` | `ParsedBillingActionHint[]` | Billing action URLs |
| `sender_domain_hint` | `CanonicalDomainHint?` | Sender domain bucket |
| `participant_domain_hints` | `CanonicalDomainHint[]` | All participant domain buckets |
| `forwarded_messages` | `ParsedForwardedMessage[]` | Parsed forwarded messages |
| `forwarded_segments` | `ParsedForwardedSegment[]` | Structured forwarded segments |

## Sub-types

### EmailAddress

```json
{
  "name": "string?",
  "address": "string"
}
```

### CanonicalAttachment

```json
{
  "filename": "string?",
  "mime_type": "string",
  "size": "number",
  "sha256": "string",
  "content_id": "string?",
  "content_disposition": "string?",
  "path": "string?"  // Only when --attachments flag is used
}
```

### CanonicalDomainHint

```json
{
  "role": "string",
  "email": "string",
  "domain": "string",
  "bucket": "personal | company | unknown"
}
```

### ParsedContactHint

```json
{
  "source": "header | body | signature | salutation",
  "role": "from | to | cc | sender",
  "hint_type": "email | phone | url | org | title | address",
  "value": "string",
  "confidence": "low | medium | high"
}
```

### ParsedSignatureEntities

```json
{
  "emails": ["string"],
  "phones": ["string"],
  "urls": ["string"],
  "org": "string?",
  "title": "string?",
  "address_lines": ["string"]
}
```

### ParsedAttachmentHint

```json
{
  "hint_type": "inline | logo | pixel_like",
  "size_bucket": "tiny | small | medium | large | xlarge",
  "filename_hint": "string?"
}
```

### ParsedEventHint

```json
{
  "kind": "meeting | shipping | deadline | availability | reservation",
  "reservation_type": "restaurant | hotel | spa | salon | bar | other?",
  "date_candidates": ["string"],
  "time_candidates": ["string"],
  "timezone_candidates": ["string"],
  "location_candidates": ["string"],
  "meeting_link": "string?",
  "tracking_numbers": ["string"],
  "confidence": "low | medium | high"
}
```

### ParsedMailKindHint

```json
{
  "kind": "personal | newsletter | promotion | transactional | notification | unknown",
  "confidence": "low | medium | high"
}
```

### ParsedDirectionHint

```json
{
  "direction": "inbound | outbound | self_message | unknown"
}
```

### ParsedUnsubscribeHint

```json
{
  "url": "string",
  "one_click": "boolean",
  "list_unsubscribe": "boolean"
}
```

### ParsedServiceLifecycleHint

```json
{
  "kind": "subscription_canceled | subscription_renewed | subscription_created | membership_updated | ticket_confirmation | order_confirmation | billing_notice",
  "confidence": "low | medium | high",
  "extracted_entities": {"key": "value"}
}
```

### ParsedBillingActionHint

```json
{
  "action_kind": "view_invoice | pay_now | manage_subscription | update_payment_method | billing_portal",
  "url": "string",
  "label": "string?"
}
```

### ParsedForwardedMessage

```json
{
  "headers": {
    "from": "string?",
    "to": "string?",
    "cc": "string?",
    "date": "string?",
    "subject": "string?",
    "message_id": "string?"
  },
  "reply_text": "string",
  "salutation": "string?",
  "signature": "string?",
  "disclaimer_blocks": ["string"],
  "quoted_blocks": ["string"],
  "nested": ["ParsedForwardedMessage"]
}
```

### ParsedForwardedSegment

```json
{
  "headers": {
    "from": "string?",
    "to": "string?",
    "date": "string?",
    "subject": "string?"
  },
  "reply_text": "string",
  "salutation": "string?",
  "signature": "string?",
  "disclaimer_blocks": ["string"],
  "quoted_blocks": ["string"]
}
```

## Tree Profile Alternative

For nested conversation structure, use `--json-profile tree`:

```json
{
  "thread_id": "string",
  "root": {
    "message": CanonicalMessage,
    "children": [TreeNode, ...]
  }
}
```

## Ingestion Guidance

### Required Fields for Ingest

At minimum, ingest pipelines should store:
- `message_key` - Primary identifier
- `message_id` - RFC822 Message-ID
- `subject`, `date` - Basic metadata
- `from`, `to` - Participant addresses
- `reply_text` - Main content for embedding

### Optional Fields for Enrichment

- `contact_hints` - For contact extraction pipelines
- `signature_entities` - For contact enrichment
- `event_hints` - For calendar/action extraction
- `mail_kind_hints` - For inbox organization
- `service_lifecycle_hints` - For CRM/lead scoring
- `billing_action_hints` - For billing follow-up

### Chunking for Embedding

The `reply_text` field is designed to be chunked:
- Use `2000` char max chunks
- Prefer paragraph > line > sentence boundaries
- Salutation and signature are already stripped

### Attachment Handling

- `sha256` enables deduplication
- `mime_type` guides content classification
- Use `--attachments` flag to export binary data to filesystem (emits `path` field)
