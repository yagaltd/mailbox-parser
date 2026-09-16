## Unreleased

### Changed

- **Data layer: universal text cleanup at the canonicalization boundary** — `reply_text`, `quoted_blocks`, `forwarded_blocks`, `disclaimer_blocks`, `salutation`, `signature` and forwarded-segment fields now have `<mailto:addr>` autolink wrappers stripped (all four real-world forms) and HTML entities (`&nbsp;` etc.) decoded, so every consumer — JSON, HTML, markdown, SDKs — sees the same clean text. `body_text`/`body_canonical`/`body_html` stay raw (provenance). Also fixes a panic: Turkish `İ` (U+0130) lowercases to two chars and crashed offset-based case-insensitive scans (found by the 13.6 GB gmail corpus).

- **`--format markdown` now emits MorphEditor-compatible markdown (OKF frontmatter)** instead of the old debug view: frontmatter (`kind: email`, `threadId`, `title` with RE:/FW:/AW:/TR:/Ré:-chains stripped, `ingested`), one `## {from} · {date}` heading per message, blockquoted history (single-level), real `- ` list items for genuine bullets (markers normalized), `*italic*` signatures, `<mailto:addr>` wrappers stripped, percent-encoded `![](path)` / `[filename](path)` attachment links. Grammar validated byte-stable against MorphEditor's BlockModel — golden fixtures in `tests/fixtures/okf/` (Rust: `markdown_fixture_goldens`; MorphEditor: `tests/unit/okf-roundtrip.test.js`).

## 0.5.0 - 2026-07-01

### Added

- **Four pass-through fields on `CanonicalMessage`:** `body_text`, `body_html`, `body_canonical`, and `raw_headers` are now preserved in canonical JSON output (`--json-profile canonical`). Previously these fields were dropped during canonicalization, forcing email UI consumers to rely on sidecar files (`--bodies-dir`) or re-parse raw sources. All four are `Option<T>` with `#[serde(skip_serializing_if = "Option::is_none")]` for backward compatibility.
- **CLI `--keep-body-html` flag** for `imap sync`, `mbox threads`, and `dir threads`. When set, the original HTML body is retained in `ParsedEmail.body_html` and flows through to `CanonicalMessage.body_html` in canonical output. The flag is independent of `--bodies-dir` (which still works for backward-compatible sidecar export).

### Changed

- **`CanonicalMessage` struct** now carries 4 additional fields after `forwarded_segments`. Old JSON without these fields deserializes correctly (fields default to `None`). New JSON includes `body_text`, `body_canonical`, and `raw_headers` by default; `body_html` requires `--keep-body-html`.

## 0.4.2 - 2026-06-29

### Added

- **`--account-id <NAME>` flag for `mbox threads` and `dir threads`.** Lets you stamp a custom account identifier onto every exported thread, so multiple mbox / directory imports can be distinguished in a unified UI. Previously both paths hardcoded `account_id: "mbox"` / `"dir"`, making two imported mailboxes indistinguishable. The flag defaults to the old values (`"mbox"` / `"dir"`) for backward compatibility. IMAP already uses its configured `account_id`.

### Limitations

- PST attachment bytes remain unavailable (outlook-pst crate limitation).

---

## 0.4.1 - 2026-06-29

### Added

- **IMAP sync now supports `keep_body_html` + `keep_attachment_bytes`** via `ImapSyncOptions`. The sync path parses with `parse_rfc822_with_options` (was `parse_rfc822` with no options), so the IMAP source gains the same body-HTML and attachment-byte retention as mbox/dir/MSG. The CLI's `imap sync` `--bodies-dir` and `--attachments-dir` flags now take effect for IMAP too. The whole feature set is now **uniform across all sources** (mbox, IMAP, dir, MSG).

### Limitations

- PST attachment bytes remain unavailable (outlook-pst crate limitation).

---

## 0.4.0 - 2026-06-29

### Added

- **`keep_attachment_bytes` option** on `ParseRfc822Options` (default false). When true, raw attachment bytes are decoded from the MIME body and retained in `ParsedAttachment._bytes` for RFC 822 / mbox messages (previously only MSG populated `_bytes`). Lets downstream consumers write attachment files to disk without re-reading the source.
- **`keep_attachment_bytes` on `MboxParseOptions`**, plumbed into the inner RFC 822 parse.
- **CLI `--attachments-dir` now works for mbox/dir** (not just MSG): the CLI sets `keep_attachment_bytes` when an attachments dir is requested, so `export_attachments` writes real files for mbox. Attachment paths are injected into the canonical JSON (`attachments[].path`).

### Limitations

- IMAP sync still parses via `parse_rfc822` (no options) — `keep_body_html` / `keep_attachment_bytes` are not yet wired through `SyncImapOptions`. mbox/dir/MSG paths are fully supported.
- PST attachment bytes remain unavailable (outlook-pst crate limitation).

---

## 0.3.0 - 2026-06-11

### Added

- **MSG (.msg) parsing** — `parse_msg()` reads Outlook `.msg` files into `ParsedEmail` with full attachment binary data, SHA256 hashes, and MIME round-trip. Uses the `msg-parser` crate.
- **PST (.pst) parsing** — `parse_pst_messages()` walks Outlook `.pst` archive folder hierarchies and yields `PstMessage { parsed, rfc822, folder }` per message. Uses the `outlook-pst` crate (Microsoft-authored, MIT licensed).
- `ParsedAttachment._bytes` field (`Option<Vec<u8>>`, `#[serde(skip)]`) for formats that can provide in-memory attachment data (MSG).
- PST folder name preserved in `PstMessage.folder` for mailbox tagging.
- **`keep_body_html` option** on `MboxParseOptions` (default false). When true, the original message HTML is retained on `ParsedEmail.body_html` instead of being dropped after computing `body_canonical`. Keeps the canonical output token-lean for LLM ingestion while letting downstream renderers (UIs) access the original HTML with links and inline images.
- **CLI `--bodies-dir <dir>`** (mbox/imap/dir `threads`). Writes one `bodies/{message_key}.html` per message that has an HTML body — separate render assets fetched on demand, so the canonical JSON stays text-only. Realises the canonical/render split: LLM gets compact text; UI gets original HTML.

### Known limitations

- PST attachment binary data is unavailable due to the `outlook-pst` crate's public API (see README). Metadata (filename, MIME type, size) is extracted; SHA256 is empty.

---

## 0.2.0 - 2026-06-03

### Changed

- **Breaking (API):** `ParsedAttachment.bytes` field removed — attachment contents are no longer retained in memory after hashing. Downstream code that accessed `.bytes` must re-read from the original source.
- **Breaking (API):** `MboxParseOptions` and `ImapSyncOptions` now require a `keep_raw: bool` field (defaults to `false`). `ImapScanOptions` also gains `keep_raw: bool`.
- Attachment SHA256 hashes are now computed in-place from the parser's borrowed slice, eliminating a full `to_vec()` copy per attachment.
- `MailMessage.raw`, `SyncedEmail.raw`, and `MboxMessage.raw` are now empty `Vec<u8>` by default; set `keep_raw: true` to retain full RFC822 bytes.

### Added

- `examples/mbox_mem_bench.rs` — memory profiling benchmark that reports RSS at key milestones.

### Performance

- Peak RSS reduced from ~20+ GB (guaranteed OOM) to ~1.37 GB when parsing a 12.7 GB / 39.5k-message mbox with 8.5 GB of attachments.

## 0.1.1 - 2026-03-10

- expanded email-body segmentation coverage for additional multilingual salutations, sign-offs, mobile footer cues, and quote markers to reduce `reply_text` leakage
- tightened canonical salutation/signature truncation to avoid over-capturing long prose and promotional footer content
- added deterministic sender/participant domain projection hints and refreshed related JSON/README contract references
- introduced the `mailbox-parser-cli` crate with JSON, Markdown, CSV, and interactive HTML export flows
- hardened HTML toolbar rendering so zoom/reset/import/labels controls remain visible locally and the theme toggle uses embedded moon/sun SVG icons instead of a remote icon dependency
- added crate-local docs for the JSON contract and lifecycle lexicon, plus an offline local-LLM review roadmap for human-approved parser improvement
