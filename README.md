# mailbox-parser

Rust library for:

- Parsing RFC822 email messages into `ParsedEmail`
- Threading messages into `ParsedThread`
- Canonicalizing into a stable export/ingest model (`CanonicalThread` / `CanonicalMessage`)
- Incremental IMAP sync via a pluggable `ImapStateBackend`
- Streaming `.mbox` reads

## The pieces you typically use

### Parse one RFC822 message

`parse_rfc822()` parses raw RFC822 bytes using `mail-parser` and extracts:

- Common headers (`Message-ID`, `In-Reply-To`, `References`, `Subject`, `Date`, address fields)
- `body_text` and `body_html` (when present)
- `body_canonical` (a cleaned-up body intended for downstream processing)
- Attachments (including inline parts) with `sha256` and (in-memory) `bytes`
- Parser hints for downstream ingestion:
  - `contact_hints` (header/salutation/signature entities)
  - `signature_entities` (emails/phones/urls/org/title/address lines)
  - `attachment_hints` (inline/logo/pixel-like + size bucket)
  - `event_hints` (meeting/shipping/deadline/availability candidates + completeness)

```rust
use mailbox_parser::parse_rfc822;

let raw: Vec<u8> = std::fs::read("message.eml")?;
let email = parse_rfc822(&raw)?;
println!("subject={:?} attachments={} ", email.subject, email.attachments.len());
# Ok::<(), anyhow::Error>(())
```

### Thread messages

`thread_messages()` groups synced IMAP messages into `ParsedThread`s.

- Primary key: `References[0]` → `In-Reply-To` → `Message-ID`
- Fallback (when all of the above are missing): normalized subject + participant set

```rust
use mailbox_parser::thread_messages;

let threads = thread_messages(&synced_messages);
println!("threads={}", threads.len());
```

### Canonicalize for stable export/ingest

For a stable export/ingest format, you can convert `ParsedThread` → `CanonicalThread`.

- `CanonicalMessage.reply_text`: the top-level reply text (quoted history stripped)
- `CanonicalMessage.quoted_blocks` / `forwarded_blocks` / `signature`: preserved separately
- `CanonicalMessage.salutation` and `CanonicalMessage.disclaimer_blocks`: preserved when detected
- `CanonicalMessage.contact_hints` / `signature_entities` / `attachment_hints` / `event_hints`: passthrough parser hints for backend enrichment pipelines

```rust
use mailbox_parser::{canonicalize_threads, thread_messages};

let threads = thread_messages(&synced_messages);
let canonical = canonicalize_threads(&threads);
println!("canonical_threads={}", canonical.len());
```

This canonical representation is what `mailbox-parser-cli --json-profile canonical` exports, and what the SDK ingest layers consume.

### End-to-end flow (high level)

1) `sync_imap_*` or `.mbox` iterator yields raw RFC822 bytes
2) `parse_rfc822()` → `ParsedEmail`
3) `thread_messages()` → `ParsedThread`
4) `canonicalize_threads()` → `CanonicalThread`

Export/render is done by `mailbox-parser-cli`. Ingest into the store is done by SDK crates (V3 pipeline adapters, and legacy V2 tooling where still needed).

### Email body segmentation model

`segment_email_body()` emits deterministic blocks from `body_canonical`:

- `salutation` (optional)
- `reply`
- `signature` (optional)
- `disclaimer` (optional)
- `quoted` or `forwarded` (optional)

Detection is line-based and includes multilingual quote/header cues plus adaptive signature/disclaimer heuristics.

Notable heuristics:

- Outlook-style header bundles are recognized across multiple locales (for example `From:/Sent:/...`, `Von:/Gesendet:/...`, `De :/Envoyé :/...`).
- Outlook-style header bundles are recognized across multiple locales (for example `From:/Sent:/...`, `Von:/Gesendet:/...`, `De :/Envoyé :/...`, `De:/Enviado el:/Para:/Asunto:`).
- Dashed quote separators like `---- on ... wrote ----` are treated as quoted-history boundaries.
- Mixed-language sign-offs like `Freundliche Grüße / Best regards` and short forms like `Rgds` are treated as signature cues.
- Additional strict short sign-offs are supported (`thank you`, `many thanks`, `merci`, `a+`, `cheers`) while avoiding sentence-level false positives.
- Signature extraction now uses contact-card signals (email/phone/url/cid/address lines) so long corporate signatures are still split correctly even when the sign-off is far from the end of the message.

### Event hint model

`event_hints` are intentionally precision-first:

- A hint is emitted only when a strong date anchor is detected (`YYYY-MM-DD`, numeric date, month+day, weekday+date, or month date ranges like `16-18 April`).
- Meeting links are detected from URL host allowlists (for example `zoom.us`, `meet.google.com`, `teams.microsoft.com`) rather than generic words.
- Timezones require explicit tokens/offsets (`UTC+`, `GMT-`, `CET`, etc.), avoiding substring false positives.
- Header metadata lines (`From:`, `Sent:`, `Enviado el:`, `Asunto:`, etc.) are ignored before event extraction to reduce quote/header contamination.

### V3 email ingest/chunking flow

`mailbox-parser` provides segmentation; V3 SDK adapters/builders decide what is embedded.

```mermaid
flowchart TD
    A[Raw RFC822 email] --> B[mailbox-parser segmentation]
    B --> C{Has reply chain?}

    C -->|No standalone| S1[Keep: salutation + body reply + signature]
    C -->|Yes replied| R1[Keep: salutation + newest reply body + signature]

    S1 --> D[build_email_ingest_text]
    R1 --> D

    B --> X[quoted/forwarded/disclaimer/post-signature blocks]
    X --> Y[Dropped from embed payload]

    A --> Z[Raw bytes stored separately]
    Z --> H[Node.content_hash]

    D --> E[Document.text_content for embedding]
    E --> F[Boundary-aware chunker max 2000 chars]
    F --> G[Prefer split: paragraph > line break > sentence end]
    G --> N[Chunks 1..n: clean reply-first text only]
    E --> M[Doc node: metadata + links]
```

Chunking behavior in V3 SDK:

- Target max size is `chunk_max_chars` (default `2000`).
- Splits avoid mid-sentence when possible by preferring paragraph, then line, then sentence boundaries.
- If no boundary exists in range, fallback is whitespace/hard split.
- Very short messages under the limit remain a single chunk.

### MBOX parsing

For `.mbox` files:

```rust
use std::path::Path;
use mailbox_parser::{parse_mbox_file, thread_messages_from_mail_messages, MboxParseOptions};

let mail = parse_mbox_file(Path::new("mailbox.mbox"), MboxParseOptions::default())?;
let threads = thread_messages_from_mail_messages(&mail.messages);
println!("threads={}", threads.len());
# Ok::<(), anyhow::Error>(())
```

### IMAP sync (incremental)

The IMAP sync returns parsed emails plus some IMAP metadata:

- `uid`
- `internal_date`
- `flags`
- `modseq` (when available)

Incremental strategy:

- If the server supports `CONDSTORE` / `QRESYNC`, we use `MODSEQ` + `CHANGEDSINCE`.
- Otherwise we fall back to `UID > last_uid` (new messages only).

State persistence is owned by the caller:

- `sync_imap_delta(...)` returns messages + next checkpoint.
- `sync_imap_with_backend(...)` loads/saves checkpoints via `ImapStateBackend`.

`mailbox-parser` does not ship a concrete backend; `mailbox-parser-cli` uses a JSON state file.

```rust
use mailbox_parser::{ImapAccountConfig, ImapSyncOptions, ImapSyncState, sync_imap_delta};

let account = ImapAccountConfig {
    host: "imap.example.com".to_string(),
    username: "you@example.com".to_string(),
    password: "APP_PASSWORD_OR_TOKEN".to_string(),
    port: 993,
    tls: true,
    danger_skip_tls_verify: false,
    mailbox: "INBOX".to_string(),
    account_id: Some("work".to_string()),
    state_path: None,
};

let prior = ImapSyncState {
    uidvalidity: None,
    last_uid: 0,
    highest_modseq: None,
    last_sync_ms: 0,
};

let sync = sync_imap_delta(&account, &prior, ImapSyncOptions::default())?;
println!(
    "fetched_messages={} vanished_uids={}",
    sync.messages.len(),
    sync.vanished_uids.len()
);
# Ok::<(), anyhow::Error>(())
```

## Notes / limitations

- IMAP is **TLS only** (port `993` by default).
- `danger_skip_tls_verify=true` is supported for testing with self-signed certs; do not use in production.
- This crate does **not** implement OAuth flows; use provider “app passwords” / tokens as needed.
- IMAP does **not** provide contacts; `contacts::EmailAddress` is just a shared type used for parsed address fields.
- Attachment `bytes` are stored in memory but are `serde(skip_...)` by default (i.e. they will not be included in JSON if you serialize `ParsedEmail`).

## Related

- `mailbox-parser-cli` (in this repo): convenience CLI for IMAP sync + export.
- `parsers/mailbox-parser/imap.example.toml`: config template for the CLI.

## V3 boundary note

In the V2→V3 redesign, this crate remains a parser/extraction library.

- It parses/syncs email sources and exports stable parser-level structures.
- V3-specific domain modeling and ingest orchestration live in V3 crates (`v3/crates/sdk`, `v3/crates/domain_*`).
- V3 consumes parser output through adapters plus the V3 `ingest_formats` crate.
