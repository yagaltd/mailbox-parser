# mailbox-parser-cli

CLI for syncing + threading email and exporting as Markdown, JSON/JSONL, or HTML.

## Installation

```bash
git clone https://github.com/<user>/<repo>
cd <repo>/mailbox-parser/cli
cargo install --path .
```

## Development (without installing)

If you have Cargo installed, you can run directly without installing:

```bash
cargo run -- <command> [options]
```

Example:

```bash
cargo run -- imap sync --config /tmp/imap.toml --out ./output --format json --json-profile canonical
```

## Setup

```bash
cp ../imap.example.toml /tmp/imap.toml
```

Edit `/tmp/imap.toml` with credentials.

## Usage

```bash
mailbox-parser-cli <command> [options]
```

### Common commands

```bash
# IMAP sync to JSON (canonical profile)
mailbox-parser-cli imap sync --config /tmp/imap.toml --out ./output --format json --json-profile canonical

# MBOX to HTML
mailbox-parser-cli mbox threads --path /path/to/mailbox.mbox --out ./output --format html

# Directory of .eml files to Markdown
mailbox-parser-cli dir threads --path /path/to/emails --out ./output --format markdown --recursive
```

## Commands

| Command | Description |
|---------|-------------|
| `imap sync` | Sync from IMAP server |
| `mbox threads` | Parse .mbox file |
| `dir threads` | Parse directory of .eml files |

## Output formats

| Format | Description |
|--------|-------------|
| `--format json` | JSON output |
| `--format jsonl` | JSONL (one thread per line) |
| `--format markdown` | Markdown output — OKF frontmatter + one `## {from} · {date}` block per message (MorphEditor-compatible) |
| `--format html` | Interactive HTML with graph/table views |

## JSON profiles

| Profile | Description |
|---------|-------------|
| `--json-profile canonical` | Flat `messages[]` per thread with full parser hints |
| `--json-profile tree` | Nested `root` + `children` structure |

Both profiles preserve canonical extraction fields on every message (`contact_hints`, `signature_entities`, `attachment_hints`, `event_hints`, `mail_kind_hints`, `direction_hint`, `unsubscribe_hints`, `service_lifecycle_hints`, `billing_action_hints`).

## HTML export options

- `--html-default-view graph|table` - default view mode
- `--html-data-mode inline|external` - data embedding (external writes sibling `.data.json`)
- `--html-max-table-rows <n>` - table row limit
- `--html-enable-advanced` - enable advanced filter controls

HTML export supports graph/table views, filterable analytics (subject/date/mail-kind/event/lifecycle), grouping by thread or subject, and CSV/Excel export of filtered data.
Toolbar controls ship with built-in labels, and the theme toggle uses embedded moon/sun SVGs, so local HTML viewing does not depend on a remote icon CDN.

## Useful flags

| Flag | Description |
|------|-------------|
| `--unseen-only` | Sync only unseen messages |
| `--full` | Full sync (ignore checkpoint) |
| `--max <N>` | Limit number of messages |
| `--attachments` | Include attachment data |
| `--split-by thread` | Split output per thread |
| `--owner-email <email>` | Owner email for direction hints (repeatable) |
| `--keep-body-html` | Retain HTML body in canonical JSON output |
| `--strip-raw-headers` | Exclude raw_headers from canonical/tree JSON output |
| `--bodies-dir <dir>` | Write per-message HTML bodies as sidecar files (deprecated, prefer `--keep-body-html`) |
| `--attachments-dir <dir>` | Write attachment files to directory |
| `--lifecycle-lexicon <path>` | Custom lifecycle lexicon YAML |
| `--lifecycle-override-jsonl <path>` | Append-only JSONL lexicon overrides |

## Help

```bash
mailbox-parser-cli --help
mailbox-parser-cli imap sync --help
mailbox-parser-cli mbox threads --help
mailbox-parser-cli dir threads --help
```

For format-specific behavior and parser hints, see the [library README](../README.md).

## Optional: TypeSafe AI refinement (`typesafe` feature)

Build with `--features typesafe` to add `typesafe refine` — a teacher-vs-parser
diff over your own mail that reports where the signature split disagrees with
a TypeSafe (Jev) judgment, plus a mail-kind taxonomy per message. Useful to
find segmentation gaps on your own corpora; findings feed rule fixes (see
`docs/typesafe-signature-probe.md`).

```bash
cargo build --release --manifest-path cli/Cargo.toml --features typesafe
# key: chmod 600 file containing the key (or export TYPESAFEAI_API_KEY)
mailbox-parser-cli typesafe refine --path big.mbox --stride 600 --limit 40 \
  --key-file /tmp/typesafe.key --out /tmp/verdicts.jsonl
```

Streaming stride sampling keeps memory flat on multi-GB mboxes. The key is
never logged; output contains subjects only. Network calls are opt-in: the
default build has no HTTP client dependency at all.

### `typesafe signals`

One batched TypeSafe call per message carrying the full signal battery
(15 questions): embed-worthiness (Score), mail_kind (Choice), 8 topic
labels (Noul, multi-label), sentiment (Score, 4 levels), urgency (Score),
action_requested (Choice), commitment_made / churn_risk (Noul), language
(Choice). Output: per-message signals JSONL + summary distributions +
thread timelines (sentiment/topic evolution computed locally from
per-message metadata — no extra API calls). For timelines, sample a
*contiguous* window (`--stride 1` over a sliced mbox); stride sampling
shreds threads.
