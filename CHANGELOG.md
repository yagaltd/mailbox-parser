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
