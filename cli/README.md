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
| `--format markdown` | Markdown output |
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
