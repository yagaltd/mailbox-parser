# OKF Thread Export — Canonical JSON → MorphEditor Markdown

> **Target:** v0.6.0 (library) / v0.5.0 (CLI)
> **Status:** Implemented (2026-09-16) — `--format markdown` emits the OKF dialect; goldens validated in both repos. Follow-up: MorphEditor serializer must preserve backslash escapes (pinned as a known-gap test there).

## 1. Goal

`CanonicalThread` → a complete, importable **MorphEditor document** (its markdown
dialect, "OKF": the BlockModel parse/serialize grammar). One command produces the
files MorphEditor ingests; MorphEditor adds no email-specific parsing.

```
mailbox-parser-cli sync --json-profile canonical --split-by thread \
                        --attachments --format markdown --out threads/
  → threads/thread_{thread_id}.md          (one MorphEditor doc per thread)
  → threads/attachments/{sha256}.{ext}     (media files)
```

## 2. Output shape

Per `thread_{id}.md`:

```markdown
---
kind: email
threadId: a1b2c3d4e5f60718
title: Quarterly planning
ingested: [msgkey_abc, msgkey_def]
---

## Alice Martin · 2026-09-15 14:03
Hi Bob, here's the update…

> ---- On Sep 14, Bob wrote: ----
> earlier text

_Sent from my iPhone_

---

## Bob · 2026-09-15 15:12
Thanks — shipping today.
```

- Frontmatter carries the thread-level bookkeeping MorphEditor reconciliation
  reads (`ingested` = every rendered `message_key` — ingest appends only missing).
- **T1 finding (2026-09-16):** MorphEditor has NO YAML frontmatter parser in the
  content path — doc metadata is out-of-band (`DocumentStore` record,
  `{title, description, author}` per `properties-schema.js`). Our frontmatter
  block round-trips byte-stable anyway (parses as HR + paragraph + HR).
  `kind`/`threadId`/`ingested` have no consumer yet; the reconciliation reader
  is future work in MorphEditor-email's ingest pipeline. Title is quoted
  (double-quoted, `"`/`\\` escaped) so `Re:`/`Fwd:` subjects survive strict YAML.

## 3. Mapping table (canonical segment → block)

| Canonical field | OKF output |
|---|---|
| `from` + `date` | `## {from.name ?? address} · {YYYY-MM-DD HH:MM}` heading opens each message |
| `salutation` + `reply_text` | paragraphs (salutation joins first paragraph) |
| `quoted_blocks` | blockquote lines (`> `) — **single level only**: leading `>` markers are stripped because nested quotes serialize with an indent in MorphEditor |
| `forwarded_segments` | `---` + `## Fwd: {orig subject} · {orig from}` + nested mapping (quoted/signature/disclaimer); **suppresses** the raw block below |
| `forwarded_blocks` (raw fallback) | `---` + `## Forwarded` + escaped paragraphs — only when no parsed segments exist (bytes are the same content) |
| `signature` | plain italic paragraph (`*…*` — asterisks, not underscores: MorphEditor's serializer emits `*` marks) |
| `disclaimer_blocks` | plain paragraph |
| `attachments` | media blocks `![]({path})` for image/video/audio; `[filename]({path})` line for others. **Path = real on-disk path** (`attachments/{sha256}_{filename}` from `--attachments-dir`, injected as `CanonicalAttachment.path`); fallback `attachments/{sha256}.{ext}` only when no export happened |
| message boundaries | blank line + `---` between messages |
| hints, `raw_headers`, bodies | NOT exported — they live in the canonical JSON sidecar (data layer) |

Escaping: a body line that would re-detect as a heading/list/ordered item/
quote/fence/hr in MorphEditor's BlockModel is backslash-escaped (`1\.`
CommonMark dot-escape). **Grammar-mirroring, not blanket** — loose pipe rows,
`#hashtag`, `*bold*` leaders stay bare (they re-parse as paragraphs anyway).
Byte-parity is held by mirror implementations: `markdown_escape_line` (Rust,
cli/src/main.rs) and `escapeParagraphSigils` (MorphEditor BlockModel.js,
serializer-side since 2026-09-16 — toMarkdown now re-escapes paragraph lines,
the earlier escape-stripping gap is fixed). The `sigil-escape` fixture pins
the shared behavior in both repos. Accepted edge: a paragraph line that is
exactly ``` ``` ``` interacts with inline-code pairing at parse; fixtures
avoid it.

## 4. Implementation surface

**Decision (2026-09-16):** no separate `--md` flag and no `src/markdown_export.rs` library module —
the existing `--format markdown` renderer IS the OKF converter. Markdown is a debug/
export view, not a stable production surface, so it was rewritten in place.

Converter: `render_thread_markdown` + `render_message_block` in `cli/src/main.rs`
(pure view over canonical threads, no I/O beyond the writer). Promote to a library
function (`thread_to_markdown_md`) only when a second consumer appears.

Attachment paths: the real on-disk path wins (`attachments/{sha256}_{filename}` from
`--attachments-dir`), else `attachments/{sha256}.{ext}` fallback. Deviates from the
`{sha256}.{ext}` convention above — confirm which convention MorphEditor wants in T1.

## 5. Out of scope

MorphEditor ingest wiring (saveWithName pipeline), send path, IMAP changes,
chat (no parser needed — chat docs are authored directly).

## 6. Test contract — golden fixtures, validated in BOTH repos

Family doctrine (protocol/ test vectors): shared vectors, each side validates.

- `tests/fixtures/markdown/` in mailbox-parser: fixture pairs `{name}.canonical.json` +
  `{name}.expected.md` (+ `{name}/` dirs of source .eml for regeneration), for
  1. simple — 2 messages, no quotes
  2. quoted-forwarded — forwarded segment with nested quote + signature + disclaimer
  3. attachments — image (media block) + pdf (link)
  4. sigil-escape — body lines that look like headings/lists/hr (pinned escapes
     + non-escaping of `#hashtag` / loose pipes)
  Regenerate: `cargo run --manifest-path cli/Cargo.toml -- dir threads --path
  tests/fixtures/markdown/{name} --out OUT --format markdown --split-by thread
  [--attachments-dir OUT/attachments]` (same --out for the JSON run).
- mailbox-parser tests: `markdown_fixture_goldens` in cli/src/main.rs —
  `render(canonical.json) == expected.md`
- MorphEditor tests: `tests/unit/markdown-roundtrip.test.js` —
  `parseBlocks(expected.md).toMarkdown() === expected.md` (byte-stable round-trip);
  fixtures copied into MorphEditor `tests/fixtures/markdown/`; drift breaks CI on
  whichever side causes it.

## 7. Tasks

| # | Task | Size | Status |
|---|---|---|---|
| T1 | Confirm frontmatter grammar vs MorphEditor properties parser; write convention + 3 goldens | M | **done** — no YAML parser in content path; frontmatter round-trips as HR+para+HR; consumers are future MorphEditor-email ingest |
| T2 | OKF converter (mapping table + escaping) | M | **done** — `render_thread_markdown`/`render_message_block` in cli/src/main.rs |
| T3 | CLI wiring | S | **done** — existing `--format markdown` flag, no new flag |
| T4 | Rust tests | S | **done** — inline `markdown_*` goldens + `markdown_fixture_goldens` over tests/fixtures/markdown/ |
| T5 | MorphEditor round-trip fixture test | S | **done** — `tests/unit/markdown-roundtrip.test.js` + `tests/fixtures/markdown/` in MorphEditor (4 pass) |
| T6 | README + CHANGELOG rows | S | **done** |

Resolved decisions: frontmatter = `kind/threadId/title/ingested`, title quoted;
attachment path = real on-disk `{sha256}_{filename}` when exported; signature =
`*…*`; quotes single-level. Open follow-up: MorphEditor serializer escape
preservation.

## 8. Real-mail run findings (2026-09-16, ~/Downloads/Support — 17 .eml)

Validated end-to-end: 8 threads, 4 Message-ID dedupes (17→13), 30 attachments,
~2s. Frontmatter {kind,threadId,title,ingested}, per-message headings, `---`
separators, blockquoted history, `{sha256}_{filename}` attachment paths — all
per convention. Refinements found (feed T1/goldens):

1. **List markers escaped instead of mapped** — body bullets/numbers render as
   ` \*` / `1\.` lines. Map genuine list lines (`*`/`-`/`N.` + space) to real
   markdown list items; keep backslash-escaping only for accidental sigils.
   **→ resolved (2026-09-16):** unordered (`-`/`*`/`+` + space) map to real
   list items, marker normalized to `-` (MorphEditor re-emits every item as
   `- `). Ordered `N.` stays escaped: their serializer renumbers to `1.`, so
   bare `2.` is not byte-stable — escaping preserves the real numbers.
   Pinned in `sigil-escape` fixture + `markdown_escape_line_grammar` test.
2. **`<mailto:addr>` wrappers leak verbatim** — strip to the bare address.
   **→ resolved (2026-09-16, hardened on re-run):** `markdown_strip_mailto` handles
   all four real-world forms — dedup `addr<mailto:addr>`, bare `<mailto:addr>`,
   space-padded `< mailto:addr >` (Apple Mail), bracket-less `mailto:addr`
   (forwarded headers) — in paragraphs, quoted lines and signatures.
   Pinned in `refinements` fixture + `markdown_strip_mailto_wrappers` test.
3. **Title keeps RE:/FW:/AW:/TR:/Ré: chains** — strip prefixes for the exported
   title (threading already normalizes; display title should too). threadId
   unaffected.
   **→ resolved (2026-09-16, hardened on re-run):** `markdown_display_title`
   loop-strips 13 prefixes (longest-first, mandatory colon so `reply:` never
   strips), accepts French spacing (`Ré :`), keeps bracketed ticket tags while
   stripping the chain wrapped AROUND them (`AW: Re:[## 349 ##] Re: AW: x` →
   `[## 349 ##] x`); falls back to the raw subject when the chain strips to
   nothing. Pinned in `refinements` fixture + unit test.
4. **Attachment paths contain spaces** — invalid markdown URL syntax. Either
   percent-encode spaces in the emitted path (on-disk name unchanged) or keep
   verbatim if MorphEditor's image parser tolerates — goldens pin the choice.
   **→ resolved (2026-09-16):** percent-encode the emitted path (URL-unreserved
   set + `/`, `markdown_encode_path` — spaces/parens/UTF-8 → %XX); on-disk names
   untouched; `%XX` round-trips byte-stable through BlockModel links/images
   (probed). MorphEditor media loader must decode — plan 041 TASK 4. Pinned in
   `refinements` fixture.
5. **Inline logo alt-text leaks** ("[A purple and black logo AI-generated
   content may be incorrect.]") + one undetected signature card — segmentation
   edge, cosmetic; park (data-layer concern, not converter).
6. **Bare-URL autolinks not byte-stable** (found in the 2026-09-16 re-run):
   MorphEditor linkifies bare `http(s)://…` on parse and re-emits `[url](url)`,
   so verbatim `<https://…>` wrappers and bare URLs in quotes diverged.
   **→ resolved:** `markdown_linkify_urls` mirrors their `^https?://[^\s<>\"]+`
   regex exactly (trailing dots/parens included), unwraps angle autolinks,
   copies explicit `[text](href)` verbatim; applied in paragraphs, quotes and
   signatures. All 8 real threads round-trip byte-stable through BlockModel
   (`/tmp/mail-threads` re-run, 2026-09-16).

**FWM-CS.mbox validation (2026-09-16, 452 MB / 3524 messages / 420
attachments / 1365 threads): 1363/1365 byte-stable through BlockModel.**
Fixes driven by it (all in the renderer's shared `markdown_body_line` pipeline):

7. **HTML→text link shapes** — the data layer emits `[imgUrl]<linkUrl>` (for
   `<a><img></a>`) and bare `[url]`; piecewise linkifying nested brackets.
   **→ resolved:** `markdown_linkify_urls` recognizes `[url]<url2>` → `[url](url2)`,
   `[url]` → `[url](url)`, and the resulting `[[url]](url])` soup → `[a](b)`.
8. **Indented list items** — MorphEditor's list detector is anchored (no
   leading whitespace) and its serializer emits 2 spaces per indent level.
   **→ resolved:** genuine list lines map to column-0 `- ` items.
9. **Signature lines bypassed the pipeline** — bare `2.`, `----` separators,
   mailto/urls in signatures. **→ resolved:** signatures now run the same
   `markdown_body_line` pipeline.
10. **Italic signature wrapper dropped** — `*…*` fused with signature content
    into hr/list shapes (`*--`, `--*`, `*1\.`) that no line-based escaper can
    stabilize. **→ resolved:** signatures are plain paragraphs. (§3 mapping
    updated accordingly.)
11. **Frontmatter title quoting** — `\"` escapes don't survive BlockModel.
    **→ resolved:** YAML single-quoted style (`'…'` with `''` doubling).
12. **`&nbsp;` entities** — decoded to spaces by their parser.
    **→ resolved:** decode in the pipeline.
13. **Images inside quotes** — the quote grammar linkifies the url; empty
    link text isn't a link in their parser. **→ resolved:** demote `![alt](url)`
    → `[alt](url)` (and `![](url)` → `[url](url)`) inside quotes.

Known edges (2/1365 FWM files + 143/28484 gmail files, parked — require mirroring
MorphEditor's inline-mark canonicalization for pathological input; revisit if it matters):
- `*[url/*](url/*)` — italic mark adjacent to a link whose url ends in `*`
- `_**"…"**_` — their serializer merges adjacent emphasis marks to `***`
- mid-line literal backslash-escapes in source text (`\[Company Name\]`) — their
  parser consumes `\X` anywhere and never re-emits it
- nested bracket+link text (`[Article: [url](url)]`) — their parser merges the
  nesting into one link
- escaped fence lines (`\``` `) — their own escapeParagraphSigils emits a form
  their parser cannot re-read (their-side fix needed)
- link hrefs containing spaces (raw query strings)

**Gmail corpus validation (2026-09-16, 13.6 GB / 39,505 messages → 28,484
threads, release build ~2 min, RSS ~2.2 GB under a 9 GB watchdog):
28,341/28,484 = 99.5% byte-stable.** Fixes it drove (all in the markdown
view, plus one data-layer crash fix):
14. **Turkish İ panic (data layer)** — `İ` (U+0130) lowercases to TWO chars;
  any scanner slicing a lowercased haystack with original byte offsets
  panics mid-char. **→ resolved:** ASCII-case matching on original bytes
  (`ascii_ci_starts_with`/`ascii_ci_find` in src/text_clean.rs).
15. **Empty-text links** `[](url)` — not links in their parser; url becomes
  the text. **→ resolved:** in `md_linkify_urls` (and quote/body image
  demotion).
16. **Underscore emphasis** `_x_`/`__x__`/`___x___` — their serializer emits
  asterisk marks only. **→ resolved:** `md_normalize_emphasis` mirrors their
  word-boundary rule (snake_case/urls safe).
17. **Mid-paragraph and indented images** — only col-0 `![](url)` survives as
  an image block in their grammar. **→ resolved:** demote to links anywhere
  else (body + quotes).
18. **Multi-line subjects** — title lines run the full body pipeline
  (list mapping, escaping, linkify), e.g. ` - ` lines inside titles.
19. **URL subjects** — bare-URL titles linkified to the canonical form.
