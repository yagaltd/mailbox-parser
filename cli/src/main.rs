use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use mailbox_parser::{
    CanonicalThread, ImapAccountConfig, ImapConfigFile, ImapStateBackend, ImapSyncOptions,
    ImapSyncState, LifecycleLexicon, MailMessage, MboxParseOptions, ParseRfc822Options,
    ParsedAttachment, ParsedThread, ParsedThreadMessage, SyncedEmail, canonicalize_threads,
    load_lifecycle_lexicon_from_yaml, load_lifecycle_lexicon_with_overrides, normalize_message_id,
    parse_mbox_file, parse_rfc822_with_options, reply_text, segment_email_body,
    sync_imap_with_backend, thread_messages, thread_messages_from_mail_messages,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    /// JSON array (single JSON document).
    Json,
    /// JSON Lines (one JSON object per line). Prefer a `.jsonl` extension.
    Jsonl,
    /// Markdown conversation view.
    Markdown,
    /// Interactive HTML with D3.js force-directed graph visualization.
    Html,
    /// CSV flat table export (one row per message).
    Csv,
}

#[derive(Clone, Debug, ValueEnum)]
enum JsonProfile {
    /// Export full `ParsedEmail` objects (verbose).
    Full,
    /// Export a compact schema similar to the markdown output.
    Compact,
    /// Export canonical thread/message schema with reply/quoted/forwarded segmentation.
    Canonical,
    /// Export nested thread tree (root + reply children) with canonical message fields.
    Tree,
}

#[derive(Clone, Debug, ValueEnum)]
enum JsonBody {
    Canonical,
    Text,
}

#[derive(Clone, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
enum HtmlDefaultView {
    Graph,
    Table,
}

#[derive(Clone, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
enum HtmlDataMode {
    Inline,
    External,
}

#[derive(Clone, Debug, Serialize)]
struct HtmlUiConfig {
    default_view: HtmlDefaultView,
    data_mode: HtmlDataMode,
    max_table_rows: usize,
    enable_advanced: bool,
}

#[derive(Clone, Debug, ValueEnum)]
enum SplitBy {
    None,
    Thread,
}

#[derive(Clone, Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Subcommand)]
enum Command {
    Imap(ImapArgs),
    Mbox(MboxArgs),
    /// Process a directory of .eml and .mbox files.
    Dir(DirArgs),
}

#[derive(Clone, Debug, Parser)]
struct ImapArgs {
    #[command(subcommand)]
    command: ImapCommand,
}

#[derive(Clone, Debug, Parser)]
struct MboxArgs {
    #[command(subcommand)]
    command: MboxCommand,
}

#[derive(Clone, Debug, Subcommand)]
enum ImapCommand {
    Sync {
        #[arg(long)]
        config: PathBuf,

        #[arg(long)]
        out: PathBuf,

        #[arg(long, default_value_t = false)]
        full: bool,

        /// Limit output to the newest N threads.
        #[arg(long = "max")]
        max_threads: Option<usize>,

        /// JSON output schema.
        #[arg(long, value_enum, default_value_t = JsonProfile::Compact)]
        json_profile: JsonProfile,

        /// Which body field to use for compact JSON.
        #[arg(long, value_enum, default_value_t = JsonBody::Canonical)]
        json_body: JsonBody,

        /// Pretty-print JSON (larger files).
        #[arg(long, default_value_t = false)]
        pretty: bool,

        /// Export email attachments to disk.
        #[arg(long, default_value_t = false)]
        attachments: bool,

        /// Directory to write attachments to. If omitted and --attachments is set, defaults to
        /// `./attachments` next to the output file/dir.
        #[arg(long)]
        attachments_dir: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t = SplitBy::None)]
        split_by: SplitBy,

        #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
        format: OutputFormat,

        /// Default initial view for HTML export.
        #[arg(long = "html-default-view", value_enum, default_value_t = HtmlDefaultView::Graph)]
        html_default_view: HtmlDefaultView,

        /// HTML payload mode: inline in one file or external `.data.json`.
        #[arg(long = "html-data-mode", value_enum, default_value_t = HtmlDataMode::Inline)]
        html_data_mode: HtmlDataMode,

        /// Maximum number of rows rendered in the HTML table view.
        #[arg(long = "html-max-table-rows", default_value_t = 10_000usize)]
        html_max_table_rows: usize,

        /// Enable advanced HTML analytics controls (table/filter/export).
        #[arg(long = "html-enable-advanced", default_value_t = true)]
        html_enable_advanced: bool,
    },
}

#[derive(Clone, Debug, Subcommand)]
enum MboxCommand {
    Threads {
        #[arg(long)]
        path: PathBuf,

        #[arg(long)]
        out: PathBuf,

        /// Limit output to the newest N threads.
        #[arg(long = "max")]
        max_threads: Option<usize>,

        /// Stop after reading this many messages from the mbox.
        #[arg(long = "max-messages")]
        max_messages: Option<usize>,

        /// Require strict separator validation ("From <addr> <date>").
        #[arg(long, default_value_t = false)]
        strict: bool,

        /// Fail fast on parse errors instead of continuing.
        #[arg(long, default_value_t = false)]
        fail_fast: bool,

        /// JSON output schema.
        #[arg(long, value_enum, default_value_t = JsonProfile::Compact)]
        json_profile: JsonProfile,

        /// Which body field to use for compact JSON.
        #[arg(long, value_enum, default_value_t = JsonBody::Canonical)]
        json_body: JsonBody,

        /// Pretty-print JSON (larger files).
        #[arg(long, default_value_t = false)]
        pretty: bool,

        /// Export email attachments to disk.
        #[arg(long, default_value_t = false)]
        attachments: bool,

        /// Directory to write attachments to. If omitted and --attachments is set, defaults to
        /// `./attachments` next to the output file/dir.
        #[arg(long)]
        attachments_dir: Option<PathBuf>,

        /// Mailbox owner email(s) used to infer message direction (inbound/outbound/self).
        #[arg(long = "owner-email")]
        owner_emails: Vec<String>,

        /// Optional lifecycle lexicon YAML override. If omitted, uses embedded defaults.
        #[arg(long = "lifecycle-lexicon")]
        lifecycle_lexicon: Option<PathBuf>,

        /// Optional append-only JSONL ops to extend lifecycle/event patterns.
        #[arg(long = "lifecycle-override-jsonl")]
        lifecycle_override_jsonl: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t = SplitBy::None)]
        split_by: SplitBy,

        #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
        format: OutputFormat,

        /// Default initial view for HTML export.
        #[arg(long = "html-default-view", value_enum, default_value_t = HtmlDefaultView::Graph)]
        html_default_view: HtmlDefaultView,

        /// HTML payload mode: inline in one file or external `.data.json`.
        #[arg(long = "html-data-mode", value_enum, default_value_t = HtmlDataMode::Inline)]
        html_data_mode: HtmlDataMode,

        /// Maximum number of rows rendered in the HTML table view.
        #[arg(long = "html-max-table-rows", default_value_t = 10_000usize)]
        html_max_table_rows: usize,

        /// Enable advanced HTML analytics controls (table/filter/export).
        #[arg(long = "html-enable-advanced", default_value_t = true)]
        html_enable_advanced: bool,
    },
}

#[derive(Clone, Debug, Parser)]
struct DirArgs {
    #[command(subcommand)]
    command: DirCommand,
}

#[derive(Clone, Debug, Subcommand)]
enum DirCommand {
    /// Process all .eml and .mbox files in a directory.
    Threads {
        /// Directory containing .eml and/or .mbox files.
        #[arg(long)]
        path: PathBuf,

        /// Output file or directory.
        #[arg(long)]
        out: PathBuf,

        /// Process files recursively in subdirectories.
        #[arg(long, default_value_t = false)]
        recursive: bool,

        /// Limit output to the newest N threads.
        #[arg(long = "max")]
        max_threads: Option<usize>,

        /// JSON output schema.
        #[arg(long, value_enum, default_value_t = JsonProfile::Compact)]
        json_profile: JsonProfile,

        /// Which body field to use for compact JSON.
        #[arg(long, value_enum, default_value_t = JsonBody::Canonical)]
        json_body: JsonBody,

        /// Pretty-print JSON (larger files).
        #[arg(long, default_value_t = false)]
        pretty: bool,

        /// Export email attachments to disk.
        #[arg(long, default_value_t = false)]
        attachments: bool,

        /// Directory to write attachments to.
        #[arg(long)]
        attachments_dir: Option<PathBuf>,

        /// Mailbox owner email(s) used to infer message direction (inbound/outbound/self).
        #[arg(long = "owner-email")]
        owner_emails: Vec<String>,

        /// Optional lifecycle lexicon YAML override. If omitted, uses embedded defaults.
        #[arg(long = "lifecycle-lexicon")]
        lifecycle_lexicon: Option<PathBuf>,

        /// Optional append-only JSONL ops to extend lifecycle/event patterns.
        #[arg(long = "lifecycle-override-jsonl")]
        lifecycle_override_jsonl: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t = SplitBy::None)]
        split_by: SplitBy,

        #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
        format: OutputFormat,

        /// Default initial view for HTML export.
        #[arg(long = "html-default-view", value_enum, default_value_t = HtmlDefaultView::Graph)]
        html_default_view: HtmlDefaultView,

        /// HTML payload mode: inline in one file or external `.data.json`.
        #[arg(long = "html-data-mode", value_enum, default_value_t = HtmlDataMode::Inline)]
        html_data_mode: HtmlDataMode,

        /// Maximum number of rows rendered in the HTML table view.
        #[arg(long = "html-max-table-rows", default_value_t = 10_000usize)]
        html_max_table_rows: usize,

        /// Enable advanced HTML analytics controls (table/filter/export).
        #[arg(long = "html-enable-advanced", default_value_t = true)]
        html_enable_advanced: bool,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Imap(imap) => run_imap(imap),
        Command::Mbox(mbox) => run_mbox(mbox),
        Command::Dir(dir) => run_dir(dir),
    }
}

fn run_imap(args: ImapArgs) -> Result<()> {
    match args.command {
        ImapCommand::Sync {
            config,
            out,
            full,
            max_threads,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir,
            split_by,
            format,
            html_default_view,
            html_data_mode,
            html_max_table_rows,
            html_enable_advanced,
        } => run_imap_sync(
            &config,
            &out,
            full,
            max_threads,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir.as_deref(),
            split_by,
            format,
            HtmlUiConfig {
                default_view: html_default_view,
                data_mode: html_data_mode,
                max_table_rows: html_max_table_rows,
                enable_advanced: html_enable_advanced,
            },
        ),
    }
}

fn run_mbox(args: MboxArgs) -> Result<()> {
    match args.command {
        MboxCommand::Threads {
            path,
            out,
            max_threads,
            max_messages,
            strict,
            fail_fast,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir,
            owner_emails,
            lifecycle_lexicon,
            lifecycle_override_jsonl,
            split_by,
            format,
            html_default_view,
            html_data_mode,
            html_max_table_rows,
            html_enable_advanced,
        } => run_mbox_threads(
            &path,
            &out,
            max_threads,
            max_messages,
            strict,
            fail_fast,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir.as_deref(),
            &owner_emails,
            lifecycle_lexicon.as_deref(),
            lifecycle_override_jsonl.as_deref(),
            split_by,
            format,
            HtmlUiConfig {
                default_view: html_default_view,
                data_mode: html_data_mode,
                max_table_rows: html_max_table_rows,
                enable_advanced: html_enable_advanced,
            },
        ),
    }
}

fn run_imap_sync(
    config_path: &PathBuf,
    out: &PathBuf,
    full: bool,
    max_threads: Option<usize>,
    json_profile: JsonProfile,
    json_body: JsonBody,
    pretty: bool,
    attachments: bool,
    attachments_dir: Option<&Path>,
    split_by: SplitBy,
    format: OutputFormat,
    html_ui: HtmlUiConfig,
) -> Result<()> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("read config {}", config_path.display()))?;
    let cfg: ImapConfigFile = toml::from_str(&raw).context("parse imap config")?;
    let accounts = cfg.all_accounts();
    if accounts.is_empty() {
        return Err(anyhow!("no accounts defined in config"));
    }

    let mut by_account: HashMap<String, Vec<ImapAccountConfig>> = HashMap::new();
    for acc in accounts {
        by_account
            .entry(effective_account_id(&acc))
            .or_default()
            .push(acc);
    }

    let state_path = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("imap_state.json");
    let mut backend =
        JsonFileImapStateBackend::open(state_path).context("open imap json state backend")?;

    let mut all_threads: Vec<JsonThreadOut> = Vec::new();
    for (account_id, mailboxes) in &by_account {
        all_threads.extend(collect_threads_for_account_group(
            full,
            account_id,
            mailboxes,
            &mut backend,
        )?);
    }
    write_threads_output(
        all_threads,
        out,
        max_threads,
        json_profile,
        json_body,
        pretty,
        attachments,
        attachments_dir,
        split_by,
        format,
        html_ui,
    )
}

fn load_cli_lifecycle_lexicon(
    path: Option<&Path>,
    override_jsonl: Option<&Path>,
) -> Result<Option<Arc<LifecycleLexicon>>> {
    if let Some(ops_path) = override_jsonl {
        let lexicon = load_lifecycle_lexicon_with_overrides(path, ops_path).with_context(|| {
            if let Some(base) = path {
                format!(
                    "load lifecycle lexicon {} with overrides {}",
                    base.display(),
                    ops_path.display()
                )
            } else {
                format!(
                    "load embedded lifecycle lexicon with overrides {}",
                    ops_path.display()
                )
            }
        })?;
        return Ok(Some(Arc::new(lexicon)));
    }
    if let Some(path) = path {
        let lexicon = load_lifecycle_lexicon_from_yaml(path)
            .with_context(|| format!("load lifecycle lexicon {}", path.display()))?;
        return Ok(Some(Arc::new(lexicon)));
    }
    Ok(None)
}

fn run_mbox_threads(
    path: &PathBuf,
    out: &PathBuf,
    max_threads: Option<usize>,
    max_messages: Option<usize>,
    strict: bool,
    fail_fast: bool,
    json_profile: JsonProfile,
    json_body: JsonBody,
    pretty: bool,
    attachments: bool,
    attachments_dir: Option<&Path>,
    owner_emails: &[String],
    lifecycle_lexicon_path: Option<&Path>,
    lifecycle_override_jsonl_path: Option<&Path>,
    split_by: SplitBy,
    format: OutputFormat,
    html_ui: HtmlUiConfig,
) -> Result<()> {
    let lifecycle_lexicon =
        load_cli_lifecycle_lexicon(lifecycle_lexicon_path, lifecycle_override_jsonl_path)?;
    let report = parse_mbox_file(
        path,
        MboxParseOptions {
            strict,
            max_messages,
            fail_fast,
            owner_emails: owner_emails.to_vec(),
            lifecycle_lexicon: lifecycle_lexicon.clone(),
        },
    )
    .with_context(|| format!("parse mbox {}", path.display()))?;

    if !report.errors.is_empty() {
        for err in &report.errors {
            if let Some(from_line) = err.from_line.as_deref() {
                eprintln!(
                    "mbox_error index={} from=\"{}\" error={}",
                    err.index, from_line, err.error
                );
            } else {
                eprintln!("mbox_error index={} error={}", err.index, err.error);
            }
        }
    }

    let mut threads = thread_messages_from_mail_messages(&report.messages);
    for t in &mut threads {
        t.messages
            .sort_by(|a, b| message_sort_key(a).cmp(&message_sort_key(b)));
    }
    threads.sort_by(|a, b| thread_latest_key(b).cmp(&thread_latest_key(a)));

    let all_threads: Vec<JsonThreadOut> = threads
        .into_iter()
        .map(|t| JsonThreadOut {
            account_id: "mbox".to_string(),
            mailboxes: vec![path.to_string_lossy().to_string()],
            thread: t,
        })
        .collect();

    write_threads_output(
        all_threads,
        out,
        max_threads,
        json_profile,
        json_body,
        pretty,
        attachments,
        attachments_dir,
        split_by,
        format,
        html_ui,
    )
}

fn run_dir(args: DirArgs) -> Result<()> {
    match args.command {
        DirCommand::Threads {
            path,
            out,
            recursive,
            max_threads,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir,
            owner_emails,
            lifecycle_lexicon,
            lifecycle_override_jsonl,
            split_by,
            format,
            html_default_view,
            html_data_mode,
            html_max_table_rows,
            html_enable_advanced,
        } => run_dir_threads(
            &path,
            &out,
            recursive,
            max_threads,
            json_profile,
            json_body,
            pretty,
            attachments,
            attachments_dir.as_deref(),
            &owner_emails,
            lifecycle_lexicon.as_deref(),
            lifecycle_override_jsonl.as_deref(),
            split_by,
            format,
            HtmlUiConfig {
                default_view: html_default_view,
                data_mode: html_data_mode,
                max_table_rows: html_max_table_rows,
                enable_advanced: html_enable_advanced,
            },
        ),
    }
}

fn run_dir_threads(
    dir_path: &PathBuf,
    out: &PathBuf,
    recursive: bool,
    max_threads: Option<usize>,
    json_profile: JsonProfile,
    json_body: JsonBody,
    pretty: bool,
    attachments: bool,
    attachments_dir: Option<&Path>,
    owner_emails: &[String],
    lifecycle_lexicon_path: Option<&Path>,
    lifecycle_override_jsonl_path: Option<&Path>,
    split_by: SplitBy,
    format: OutputFormat,
    html_ui: HtmlUiConfig,
) -> Result<()> {
    let lifecycle_lexicon =
        load_cli_lifecycle_lexicon(lifecycle_lexicon_path, lifecycle_override_jsonl_path)?;
    if !dir_path.is_dir() {
        return Err(anyhow!("{} is not a directory", dir_path.display()));
    }

    let mut all_messages: Vec<MailMessage> = Vec::new();
    let mut files_processed = 0usize;
    let mut errors: Vec<(String, String)> = Vec::new();

    let entries = if recursive {
        walkdir::WalkDir::new(dir_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .map(|e| e.path().to_path_buf())
            .collect::<Vec<_>>()
    } else {
        fs::read_dir(dir_path)
            .with_context(|| format!("read directory {}", dir_path.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect()
    };

    for entry in entries {
        let ext = entry
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if ext != "eml" && ext != "mbox" {
            continue;
        }

        let file_name = entry
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        eprintln!("processing: {}", entry.display());

        if ext == "eml" {
            match fs::read(&entry) {
                Ok(bytes) => match parse_rfc822_with_options(
                    &bytes,
                    &ParseRfc822Options {
                        owner_emails: owner_emails.to_vec(),
                        lifecycle_lexicon: lifecycle_lexicon.clone(),
                    },
                ) {
                    Ok(parsed) => {
                        all_messages.push(MailMessage {
                            uid: None,
                            internal_date: parsed.date.clone(),
                            flags: Vec::new(),
                            parsed,
                            raw: bytes,
                        });
                        files_processed += 1;
                    }
                    Err(e) => {
                        errors.push((file_name, format!("parse error: {}", e)));
                    }
                },
                Err(e) => {
                    errors.push((file_name, format!("read error: {}", e)));
                }
            }
        } else if ext == "mbox" {
            let report = parse_mbox_file(
                &entry,
                MboxParseOptions {
                    strict: false,
                    max_messages: None,
                    fail_fast: false,
                    owner_emails: owner_emails.to_vec(),
                    lifecycle_lexicon: lifecycle_lexicon.clone(),
                },
            );

            match report {
                Ok(report) => {
                    for msg in report.messages {
                        all_messages.push(msg);
                    }
                    files_processed += 1;
                    if !report.errors.is_empty() {
                        for err in &report.errors {
                            errors
                                .push((format!("{}:{}", file_name, err.index), err.error.clone()));
                        }
                    }
                }
                Err(e) => {
                    errors.push((file_name, format!("mbox parse error: {}", e)));
                }
            }
        }
    }

    if !errors.is_empty() {
        eprintln!("\n{} errors occurred:", errors.len());
        for (file, err) in &errors {
            eprintln!("  {}: {}", file, err);
        }
    }

    eprintln!(
        "\nprocessed {} files, {} messages total",
        files_processed,
        all_messages.len()
    );

    if all_messages.is_empty() {
        eprintln!("no messages found");
        return Ok(());
    }

    let before = all_messages.len();
    let (all_messages, deduped) = dedupe_messages_by_message_id(all_messages);
    if deduped > 0 {
        eprintln!(
            "deduped {} messages by Message-ID ({} -> {})",
            deduped,
            before,
            all_messages.len()
        );
    }

    let mut threads = thread_messages_from_mail_messages(&all_messages);
    for t in &mut threads {
        t.messages
            .sort_by(|a, b| message_sort_key(a).cmp(&message_sort_key(b)));
    }
    threads.sort_by(|a, b| thread_latest_key(b).cmp(&thread_latest_key(a)));

    let all_threads: Vec<JsonThreadOut> = threads
        .into_iter()
        .map(|t| JsonThreadOut {
            account_id: "dir".to_string(),
            mailboxes: vec![dir_path.to_string_lossy().to_string()],
            thread: t,
        })
        .collect();

    write_threads_output(
        all_threads,
        out,
        max_threads,
        json_profile,
        json_body,
        pretty,
        attachments,
        attachments_dir,
        split_by,
        format,
        html_ui,
    )
}

fn dedupe_messages_by_message_id(messages: Vec<MailMessage>) -> (Vec<MailMessage>, usize) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<MailMessage> = Vec::with_capacity(messages.len());
    let mut deduped = 0usize;

    for msg in messages {
        let Some(mid) = msg.parsed.message_id.as_deref() else {
            out.push(msg);
            continue;
        };
        let norm = normalize_message_id(mid);
        if norm.is_empty() {
            out.push(msg);
            continue;
        }
        if seen.insert(norm) {
            out.push(msg);
        } else {
            deduped += 1;
        }
    }

    (out, deduped)
}

#[derive(Clone, Debug, Serialize)]
struct JsonThreadOut {
    account_id: String,
    mailboxes: Vec<String>,
    #[serde(flatten)]
    thread: ParsedThread,
}

#[derive(Clone, Debug, Serialize)]
struct JsonThreadCanonicalOut {
    account_id: String,
    mailboxes: Vec<String>,
    #[serde(flatten)]
    thread: CanonicalThread,
}

#[derive(Clone, Debug, Serialize)]
struct JsonThreadCompactOut {
    account_id: String,
    mailboxes: Vec<String>,
    thread_id: String,
    messages: Vec<JsonMessageCompactOut>,
}

#[derive(Clone, Debug, Serialize)]
struct JsonTreeMessageNode {
    #[serde(flatten)]
    message: mailbox_parser::CanonicalMessage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    children: Vec<JsonTreeMessageNode>,
}

#[derive(Clone, Debug, Serialize)]
struct JsonThreadTreeOut {
    account_id: String,
    mailboxes: Vec<String>,
    thread_id: String,
    root: JsonTreeMessageNode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    orphans: Vec<JsonTreeMessageNode>,
}

#[derive(Clone, Debug, Serialize)]
struct JsonMessageCompactOut {
    message_key: String,
    uid: Option<u32>,
    internal_date: Option<String>,

    message_id: Option<String>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    subject: Option<String>,
    date: Option<String>,

    from: Vec<String>,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    reply_to: Vec<String>,

    body: String,
    attachments: Vec<JsonAttachmentCompactOut>,
    contact_hints_count: usize,
    attachment_hints_count: usize,
    event_hints_count: usize,
    mail_kind_hints_count: usize,
    unsubscribe_hints_count: usize,
    service_lifecycle_hints_count: usize,
    billing_action_hints_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct JsonAttachmentCompactOut {
    filename: Option<String>,
    mime_type: String,
    size: usize,
    sha256: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

fn effective_account_id(acc: &ImapAccountConfig) -> String {
    acc.account_id
        .clone()
        .unwrap_or_else(|| format!("{}@{}", acc.username, acc.host))
}

fn empty_imap_state() -> ImapSyncState {
    ImapSyncState {
        uidvalidity: None,
        last_uid: 0,
        highest_modseq: None,
        last_sync_ms: 0,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JsonImapStateFileV1 {
    schema_version: u32,

    #[serde(default)]
    accounts: HashMap<String, HashMap<String, ImapSyncState>>,
}

impl Default for JsonImapStateFileV1 {
    fn default() -> Self {
        Self {
            schema_version: 1,
            accounts: HashMap::new(),
        }
    }
}

struct JsonFileImapStateBackend {
    path: PathBuf,
    state: JsonImapStateFileV1,
}

impl JsonFileImapStateBackend {
    fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create state dir {}", parent.display()))?;
        }

        let state = if path.exists() {
            let bytes =
                fs::read(&path).with_context(|| format!("read state {}", path.display()))?;
            match serde_json::from_slice::<JsonImapStateFileV1>(&bytes) {
                Ok(s) => s,
                Err(err) => {
                    eprintln!(
                        "warning: failed to parse IMAP state file {}; starting fresh: {}",
                        path.display(),
                        err
                    );
                    JsonImapStateFileV1::default()
                }
            }
        } else {
            JsonImapStateFileV1::default()
        };

        Ok(Self { path, state })
    }

    fn persist(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.state).context("serialize state json")?;
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, bytes).with_context(|| format!("write temp state {}", tmp.display()))?;
        fs::rename(&tmp, &self.path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), self.path.display()))?;
        Ok(())
    }
}

impl ImapStateBackend for JsonFileImapStateBackend {
    fn load_state(&mut self, account_id: &str, mailbox: &str) -> Result<ImapSyncState> {
        Ok(self
            .state
            .accounts
            .get(account_id)
            .and_then(|m| m.get(mailbox))
            .cloned()
            .unwrap_or_else(empty_imap_state))
    }

    fn save_state(&mut self, account_id: &str, mailbox: &str, state: &ImapSyncState) -> Result<()> {
        self.state
            .accounts
            .entry(account_id.to_string())
            .or_default()
            .insert(mailbox.to_string(), state.clone());
        self.persist()
    }
}

fn collect_threads_for_account_group(
    full: bool,
    account_id: &str,
    mailboxes: &[ImapAccountConfig],
    backend: &mut dyn ImapStateBackend,
) -> Result<Vec<JsonThreadOut>> {
    let mut all_messages: Vec<SyncedEmail> = Vec::new();
    let mut mbs: Vec<String> = Vec::new();

    for acc in mailboxes {
        eprintln!(
            "[{account_id}] syncing mailbox={}{}",
            acc.mailbox,
            if full { " (full)" } else { "" }
        );
        let sync = sync_imap_with_backend(
            acc,
            backend,
            ImapSyncOptions {
                force_full: full,
                unseen_only: false,
            },
        )
        .with_context(|| {
            if full {
                "sync imap (full)"
            } else {
                "sync imap"
            }
        })?;
        eprintln!(
            "[{account_id}] mailbox={} fetched_new_messages={}",
            sync.mailbox,
            sync.messages.len()
        );
        mbs.push(sync.mailbox);
        all_messages.extend(sync.messages);
    }

    if all_messages.is_empty() {
        return Ok(Vec::new());
    }

    let mut threads = thread_messages(&all_messages);
    for t in &mut threads {
        // Conversation view: oldest -> newest.
        t.messages
            .sort_by(|a, b| message_sort_key(a).cmp(&message_sort_key(b)));
    }
    threads.sort_by(|a, b| thread_latest_key(b).cmp(&thread_latest_key(a)));

    Ok(threads
        .into_iter()
        .map(|t| JsonThreadOut {
            account_id: account_id.to_string(),
            mailboxes: mbs.clone(),
            thread: t,
        })
        .collect())
}

fn write_threads_output(
    mut all_threads: Vec<JsonThreadOut>,
    out: &PathBuf,
    max_threads: Option<usize>,
    json_profile: JsonProfile,
    json_body: JsonBody,
    pretty: bool,
    attachments: bool,
    attachments_dir: Option<&Path>,
    split_by: SplitBy,
    format: OutputFormat,
    html_ui: HtmlUiConfig,
) -> Result<()> {
    all_threads.sort_by(|a, b| thread_latest_key(&b.thread).cmp(&thread_latest_key(&a.thread)));
    if let Some(n) = max_threads {
        all_threads.truncate(n);
    }

    let base_dir = match split_by {
        SplitBy::None => out.parent().unwrap_or_else(|| Path::new(".")),
        SplitBy::Thread => out.as_path(),
    };
    let default_attachments_dir = base_dir.join("attachments");
    let attachments_dir = attachments_dir.map(|p| p.to_path_buf()).or_else(|| {
        if attachments {
            Some(default_attachments_dir)
        } else {
            None
        }
    });

    let attachment_paths = if let Some(dir) = attachments_dir.as_deref() {
        export_attachments(&all_threads, base_dir, dir)
            .with_context(|| format!("export attachments to {}", dir.display()))?
    } else {
        HashMap::new()
    };

    // Canonical threads are used for canonical JSON output *and* as the single input model for
    // markdown rendering (so markdown is a pure view over the canonical schema).
    let mut canonical_threads_all: Vec<JsonThreadCanonicalOut> = all_threads
        .iter()
        .map(|t| {
            let thread = canonicalize_threads(std::slice::from_ref(&t.thread))
                .into_iter()
                .next()
                .expect("canonicalize thread");
            JsonThreadCanonicalOut {
                account_id: t.account_id.clone(),
                mailboxes: t.mailboxes.clone(),
                thread,
            }
        })
        .collect();

    inject_canonical_attachment_paths(&mut canonical_threads_all, &attachment_paths);

    let canonical_threads: Option<&[JsonThreadCanonicalOut]> = match json_profile {
        JsonProfile::Canonical => Some(&canonical_threads_all),
        _ => None,
    };
    let tree_threads: Option<Vec<JsonThreadTreeOut>> = match json_profile {
        JsonProfile::Tree => Some(
            canonical_threads_all
                .iter()
                .map(to_tree_thread)
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };

    let compact_threads: Option<Vec<JsonThreadCompactOut>> = match json_profile {
        JsonProfile::Compact => Some(
            all_threads
                .iter()
                .map(|t| to_compact_thread(t, json_body.clone(), &attachment_paths))
                .collect(),
        ),
        _ => None,
    };

    match split_by {
        SplitBy::None => {
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create output dir {}", parent.display()))?;
            }

            let ext = out.extension().and_then(|s| s.to_str()).unwrap_or("");
            match format {
                OutputFormat::Json => {
                    eprintln!("writing --format json (JSON array) to {}", out.display());
                    if ext.eq_ignore_ascii_case("jsonl") {
                        eprintln!(
                            "warning: output path ends with .jsonl but --format is json (array)"
                        );
                    }
                }
                OutputFormat::Jsonl => {
                    eprintln!("writing --format jsonl (JSON Lines) to {}", out.display());
                    if ext.eq_ignore_ascii_case("json") {
                        eprintln!(
                            "warning: output path ends with .json but --format is jsonl (consider .jsonl)"
                        );
                    }
                }
                OutputFormat::Markdown => {
                    eprintln!("writing --format markdown to {}", out.display());
                }
                OutputFormat::Html => {
                    eprintln!("writing --format html to {}", out.display());
                }
                OutputFormat::Csv => {
                    eprintln!("writing --format csv to {}", out.display());
                }
            }

            match format {
                OutputFormat::Html => write_html_file(out, &canonical_threads_all, &html_ui),
                OutputFormat::Csv => {
                    let rows = projection_rows_from_threads(&canonical_threads_all);
                    write_csv_file(out, &rows)
                }
                OutputFormat::Markdown => {
                    write_markdown_file(out, &canonical_threads_all, &attachment_paths)
                }
                OutputFormat::Json => match (&canonical_threads, &compact_threads, &json_profile) {
                    (Some(ct), _, JsonProfile::Canonical) => write_json_file(out, ct, pretty),
                    (_, _, JsonProfile::Tree) => {
                        write_json_file(out, tree_threads.as_deref().unwrap_or(&[]), pretty)
                    }
                    (_, Some(ct), JsonProfile::Compact) => write_json_file(out, ct, pretty),
                    (None, None, JsonProfile::Full) => write_json_file(out, &all_threads, pretty),
                    _ => unreachable!(),
                },
                OutputFormat::Jsonl => {
                    match (&canonical_threads, &compact_threads, &json_profile) {
                        (Some(ct), _, JsonProfile::Canonical) => write_jsonl_file(out, ct),
                        (_, _, JsonProfile::Tree) => {
                            write_jsonl_file(out, tree_threads.as_deref().unwrap_or(&[]))
                        }
                        (_, Some(ct), JsonProfile::Compact) => write_jsonl_file(out, ct),
                        (None, None, JsonProfile::Full) => write_jsonl_file(out, &all_threads),
                        _ => unreachable!(),
                    }
                }
            }
        }
        SplitBy::Thread => {
            fs::create_dir_all(out)
                .with_context(|| format!("create output dir {}", out.display()))?;

            match format {
                OutputFormat::Json => {
                    eprintln!(
                        "writing --format json (one file per thread) under {}",
                        out.display()
                    );
                }
                OutputFormat::Markdown => {
                    eprintln!(
                        "writing --format markdown (one file per thread) under {}",
                        out.display()
                    );
                }
                OutputFormat::Html => {
                    eprintln!(
                        "writing --format html (one file per thread) under {}",
                        out.display()
                    );
                }
                OutputFormat::Csv => {
                    eprintln!(
                        "writing --format csv (one file per thread) under {}",
                        out.display()
                    );
                }
                OutputFormat::Jsonl => {}
            }

            match format {
                OutputFormat::Html => write_html_split(out, &canonical_threads_all, &html_ui),
                OutputFormat::Csv => write_csv_split(out, &canonical_threads_all),
                OutputFormat::Markdown => {
                    write_markdown_split(out, &canonical_threads_all, &attachment_paths)
                }
                OutputFormat::Json => match (&canonical_threads, &compact_threads, &json_profile) {
                    (Some(ct), _, JsonProfile::Canonical) => {
                        write_json_split(out, ct, pretty, |t| {
                            format!("thread_{}.json", t.thread.thread_id)
                        })
                    }
                    (_, _, JsonProfile::Tree) => {
                        write_json_split(out, tree_threads.as_deref().unwrap_or(&[]), pretty, |t| {
                            format!("thread_{}.json", t.thread_id)
                        })
                    }
                    (_, Some(ct), JsonProfile::Compact) => write_json_split(out, ct, pretty, |t| {
                        format!("thread_{}.json", t.thread_id)
                    }),
                    (None, None, JsonProfile::Full) => {
                        write_json_split(out, &all_threads, pretty, |t| {
                            format!("thread_{}.json", t.thread.thread_id)
                        })
                    }
                    _ => unreachable!(),
                },
                OutputFormat::Jsonl => {
                    return Err(anyhow!(
                        "--split-by thread is not supported for --format jsonl; use --format json"
                    ));
                }
            }
        }
    }
}

fn write_jsonl_file<T: Serialize>(out: &Path, threads: &[T]) -> Result<()> {
    let f = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = BufWriter::new(f);

    for t in threads {
        serde_json::to_writer(&mut w, t).context("serialize thread")?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

fn write_json_file<T: Serialize>(out: &Path, threads: &[T], pretty: bool) -> Result<()> {
    let f = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = BufWriter::new(f);
    w.write_all(b"[\n")?;
    let mut first = true;

    for t in threads {
        if !first {
            w.write_all(b",\n")?;
        }
        if pretty {
            serde_json::to_writer_pretty(&mut w, t).context("serialize thread")?;
        } else {
            serde_json::to_writer(&mut w, t).context("serialize thread")?;
        }
        first = false;
    }

    w.write_all(b"\n]\n")?;
    w.flush()?;
    Ok(())
}

fn write_csv_file(out: &Path, rows: &[mailbox_parser::ProjectionRow]) -> Result<()> {
    let f = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = BufWriter::new(f);

    // Header
    writeln!(
        w,
        "thread_id,message_key,subject,date,from,to,cc,reply_text,mail_kinds,event_kinds,lifecycle_kinds"
    )?;

    for row in rows {
        writeln!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{}",
            csv_escape(&row.thread_id),
            csv_escape(&row.message_key),
            csv_escape(&row.subject),
            csv_escape(&row.date),
            csv_escape(&row.from.join("; ")),
            csv_escape(&row.to.join("; ")),
            csv_escape(&row.cc.join("; ")),
            csv_escape(&row.reply_text),
            csv_escape(&row.mail_kinds.join("; ")),
            csv_escape(&row.event_kinds.join("; ")),
            csv_escape(&row.lifecycle_kinds.join("; ")),
        )?;
    }

    w.flush()?;
    Ok(())
}

fn write_csv_split(out_dir: &Path, threads: &[JsonThreadCanonicalOut]) -> Result<()> {
    for t in threads {
        let path = out_dir.join(format!("thread_{}.csv", t.thread.thread_id));
        let rows = projection_rows_from_threads(std::slice::from_ref(t));
        write_csv_file(&path, &rows)?;
    }
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn write_markdown_file(
    out: &Path,
    threads: &[JsonThreadCanonicalOut],
    attachment_paths: &HashMap<String, String>,
) -> Result<()> {
    let f = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = BufWriter::new(f);

    for t in threads {
        render_thread_markdown(&mut w, t, attachment_paths)?;
    }
    w.flush()?;
    Ok(())
}

fn write_html_file(
    out: &Path,
    threads: &[JsonThreadCanonicalOut],
    html_ui: &HtmlUiConfig,
) -> Result<()> {
    let data_file_name = out
        .file_stem()
        .map(|s| format!("{}.data.json", s.to_string_lossy()))
        .unwrap_or_else(|| "report.data.json".to_string());
    let data_path = out.with_file_name(&data_file_name);
    let external_data_path = if matches!(html_ui.data_mode, HtmlDataMode::External) {
        write_json_file(&data_path, threads, false)
            .with_context(|| format!("create {}", data_path.display()))?;
        Some(data_file_name)
    } else {
        None
    };

    let f = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = BufWriter::new(f);

    let html_with_data = render_html_template(threads, html_ui, external_data_path.as_deref())?;

    write!(w, "{}", html_with_data)?;
    w.flush()?;
    Ok(())
}

fn write_html_split(
    out_dir: &Path,
    threads: &[JsonThreadCanonicalOut],
    html_ui: &HtmlUiConfig,
) -> Result<()> {
    for t in threads {
        let path = out_dir.join(format!("thread_{}.html", t.thread.thread_id));
        let external_data_path = if matches!(html_ui.data_mode, HtmlDataMode::External) {
            let data_path = out_dir.join(format!("thread_{}.data.json", t.thread.thread_id));
            write_json_file(&data_path, std::slice::from_ref(t), false)
                .with_context(|| format!("create {}", data_path.display()))?;
            Some(format!("thread_{}.data.json", t.thread.thread_id))
        } else {
            None
        };
        let f = fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let mut w = BufWriter::new(f);
        let html_with_data = render_html_template(
            std::slice::from_ref(t),
            html_ui,
            external_data_path.as_deref(),
        )?;

        write!(w, "{}", html_with_data)?;
        w.flush()?;
    }
    Ok(())
}

fn render_html_template(
    threads: &[JsonThreadCanonicalOut],
    html_ui: &HtmlUiConfig,
    external_data_path: Option<&str>,
) -> Result<String> {
    let ui_data = serde_json::to_string(html_ui).context("serialize html ui config")?;
    let ui_data = escape_inline_script_json(&ui_data);
    let projection_rows = projection_rows_from_threads(threads);
    let projection_rows = escape_inline_script_json(
        &serde_json::to_string(&projection_rows).context("serialize projection rows")?,
    );
    let payload_script = match external_data_path {
        Some(path) => {
            let data_path =
                serde_json::to_string(path).context("serialize external html data path")?;
            format!(
                "<script>window.THREAD_DATA = []; window.PROJECTION_ROWS = []; window.HTML_DATA_URL = {};</script>",
                data_path
            )
        }
        None => {
            let json_data = serde_json::to_string(threads).context("serialize threads for html")?;
            let json_data = escape_inline_script_json(&json_data);
            format!(
                "<script>window.THREAD_DATA = {}; window.PROJECTION_ROWS = {};</script>",
                json_data, projection_rows
            )
        }
    };

    let Some((head, tail)) = HTML_TEMPLATE_START.rsplit_once("</body>") else {
        return Err(anyhow!("html template missing </body> marker"));
    };

    Ok(format!(
        "{}{}<script>window.HTML_UI_CONFIG = {};</script><script>init();</script></body>{}",
        head, payload_script, ui_data, tail
    ))
}

fn projection_rows_from_threads(
    threads: &[JsonThreadCanonicalOut],
) -> Vec<mailbox_parser::ProjectionRow> {
    let mut out = Vec::new();
    for t in threads {
        for msg in &t.thread.messages {
            out.push(mailbox_parser::ProjectionRow {
                thread_id: t.thread.thread_id.clone(),
                message_key: msg.message_key.clone(),
                subject: msg.subject.clone().unwrap_or_default(),
                date: msg.date.clone().unwrap_or_default(),
                from: msg.from.iter().map(format_email).collect(),
                to: msg.to.iter().map(format_email).collect(),
                cc: msg.cc.iter().map(format_email).collect(),
                reply_text: msg.reply_text.clone(),
                mail_kinds: msg
                    .mail_kind_hints
                    .iter()
                    .map(|h| format!("{:?}", h.kind).to_lowercase())
                    .collect(),
                event_kinds: msg
                    .event_hints
                    .iter()
                    .map(|h| format!("{:?}", h.kind).to_lowercase())
                    .collect(),
                lifecycle_kinds: msg
                    .service_lifecycle_hints
                    .iter()
                    .map(|h| format!("{:?}", h.kind).to_lowercase())
                    .collect(),
            });
        }
    }
    out
}

fn escape_inline_script_json(s: &str) -> String {
    s.replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn write_json_split<T: Serialize>(
    out_dir: &Path,
    threads: &[T],
    pretty: bool,
    file_name: impl Fn(&T) -> String,
) -> Result<()> {
    for t in threads {
        let path = out_dir.join(file_name(t));
        let f = fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let mut w = BufWriter::new(f);
        if pretty {
            serde_json::to_writer_pretty(&mut w, t).context("serialize thread")?;
        } else {
            serde_json::to_writer(&mut w, t).context("serialize thread")?;
        }
        w.write_all(b"\n")?;
        w.flush()?;
    }
    Ok(())
}

fn write_markdown_split(
    out_dir: &Path,
    threads: &[JsonThreadCanonicalOut],
    attachment_paths: &HashMap<String, String>,
) -> Result<()> {
    for t in threads {
        let path = out_dir.join(format!("thread_{}.md", t.thread.thread_id));
        let f = fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let mut w = BufWriter::new(f);
        render_thread_markdown(&mut w, t, attachment_paths)?;
        w.flush()?;
    }
    Ok(())
}

fn render_thread_markdown<W: Write>(
    out: &mut W,
    t: &JsonThreadCanonicalOut,
    attachment_paths: &HashMap<String, String>,
) -> Result<()> {
    if t.thread.messages.is_empty() {
        return Ok(());
    }

    fn thread_heading_variant(thread: &CanonicalThread) -> Option<&'static str> {
        if thread.messages.iter().any(|m| {
            !m.forwarded_blocks.is_empty()
                || m.subject
                    .as_deref()
                    .is_some_and(|s| s.to_ascii_lowercase().contains("fwd:"))
        }) {
            return Some("fwd");
        }

        if thread.messages.len() >= 2 {
            return Some("re");
        }

        None
    }

    // Messages are already sorted oldest->newest.
    let root = &t.thread.messages[0];
    let root_from = root
        .from
        .first()
        .map(format_email)
        .unwrap_or_else(|| "(unknown)".to_string());

    let root_snip = first_nonempty_line(&root.reply_text)
        .or_else(|| {
            root.forwarded_blocks
                .first()
                .and_then(|b| first_nonempty_line(b))
        })
        .or_else(|| {
            root.quoted_blocks
                .first()
                .and_then(|b| first_nonempty_line(b))
        })
        .unwrap_or("(no body)")
        .trim();

    let heading = match thread_heading_variant(&t.thread) {
        Some(v) => format!("Thread - {v}"),
        None => "Thread".to_string(),
    };

    writeln!(
        out,
        "# [{} {}] from {}: {}\n",
        heading, t.thread.thread_id, root_from, root_snip
    )?;
    writeln!(out, "Account: {}\n", t.account_id)?;
    writeln!(out, "Mailboxes: {}\n", t.mailboxes.join(", "))?;

    // Root message body/metadata is printed under the H1.
    render_message_block(out, root, false, attachment_paths)?;

    for msg in t.thread.messages.iter().skip(1) {
        let from = msg
            .from
            .first()
            .map(format_email)
            .unwrap_or_else(|| "(unknown)".to_string());
        let snip = first_nonempty_line(&msg.reply_text)
            .unwrap_or("(no body)")
            .trim();
        writeln!(out, "## from {}: {}\n", from, snip)?;
        render_message_block(out, msg, true, attachment_paths)?;
    }

    Ok(())
}

fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
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

fn thread_latest_key(thread: &ParsedThread) -> (i64, String) {
    let ts = thread
        .messages
        .iter()
        .map(|m| message_sort_key(m).0)
        .max()
        .unwrap_or(0);
    (ts, thread.thread_id.clone())
}

fn render_message_block<W: Write>(
    out: &mut W,
    msg: &mailbox_parser::CanonicalMessage,
    include_subject_heading: bool,
    attachment_paths: &HashMap<String, String>,
) -> Result<()> {
    if include_subject_heading {
        if let Some(subj) = msg.subject.as_deref().filter(|s| !s.trim().is_empty()) {
            let subj = normalize_line_terminators(subj);
            writeln!(out, "Subject: {}", subj.trim_end())?;
        }
    } else if let Some(subj) = msg.subject.as_deref().filter(|s| !s.trim().is_empty()) {
        let subj = normalize_line_terminators(subj);
        writeln!(out, "Subject: {}", subj.trim_end())?;
    }

    if let Some(from) = msg.from.first() {
        writeln!(out, "From: {}", format_email(from))?;
    }
    if !msg.to.is_empty() {
        let tos = msg
            .to
            .iter()
            .map(format_email)
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(out, "To: {tos}")?;
    }
    if let Some(date) = msg.date.as_deref() {
        writeln!(out, "Date: {date}")?;
    }
    if let Some(mid) = msg.message_id.as_deref() {
        writeln!(out, "Message-ID: {mid}")?;
    }
    if msg.uid.is_some() {
        writeln!(out, "UID: {}", msg.uid.unwrap_or(0))?;
    }

    writeln!(out)?;
    let body = normalize_line_terminators(&msg.reply_text);
    if !body.trim().is_empty() {
        writeln!(out, "{}\n", body.trim_end())?;
    }

    if !msg.forwarded_blocks.is_empty() {
        for (i, b) in msg.forwarded_blocks.iter().enumerate() {
            writeln!(out, "### Forwarded {}\n", i + 1)?;
            let b = normalize_line_terminators(b);
            if !b.trim().is_empty() {
                writeln!(out, "{}\n", b.trim_end())?;
            }
        }
    }

    if !msg.quoted_blocks.is_empty() {
        for (i, b) in msg.quoted_blocks.iter().enumerate() {
            writeln!(out, "### Quoted {}\n", i + 1)?;
            let b = normalize_line_terminators(b);
            if !b.trim().is_empty() {
                writeln!(out, "{}\n", b.trim_end())?;
            }
        }
    }

    if !msg.attachments.is_empty() {
        writeln!(out, "Attachments:")?;
        for a in &msg.attachments {
            let name = a.filename.as_deref().unwrap_or("(unnamed)");
            let p = a
                .path
                .as_deref()
                .map(|s| s.to_string())
                .or_else(|| attachment_paths.get(&a.sha256).cloned());

            if let Some(p) = p {
                writeln!(
                    out,
                    "- {} ({}, {} bytes) [{}]",
                    name, a.mime_type, a.size, p
                )?;
            } else {
                writeln!(out, "- {} ({}, {} bytes)", name, a.mime_type, a.size)?;
            }
        }
        writeln!(out)?;
    }

    Ok(())
}

fn to_compact_thread(
    t: &JsonThreadOut,
    body: JsonBody,
    attachment_paths: &HashMap<String, String>,
) -> JsonThreadCompactOut {
    JsonThreadCompactOut {
        account_id: t.account_id.clone(),
        mailboxes: t.mailboxes.clone(),
        thread_id: t.thread.thread_id.clone(),
        messages: t
            .thread
            .messages
            .iter()
            .map(|m| to_compact_message(m, body.clone(), attachment_paths))
            .collect(),
    }
}

fn to_tree_thread(t: &JsonThreadCanonicalOut) -> JsonThreadTreeOut {
    let (root, orphans) = build_message_tree(&t.thread.messages).unwrap_or_else(|| {
        let root = JsonTreeMessageNode {
            message: t
                .thread
                .messages
                .first()
                .expect("canonical thread must have at least one message")
                .clone(),
            children: Vec::new(),
        };
        (root, Vec::new())
    });

    JsonThreadTreeOut {
        account_id: t.account_id.clone(),
        mailboxes: t.mailboxes.clone(),
        thread_id: t.thread.thread_id.clone(),
        root,
        orphans,
    }
}

fn build_message_tree(
    messages: &[mailbox_parser::CanonicalMessage],
) -> Option<(JsonTreeMessageNode, Vec<JsonTreeMessageNode>)> {
    if messages.is_empty() {
        return None;
    }

    let mut by_message_id: HashMap<String, usize> = HashMap::new();
    for (idx, m) in messages.iter().enumerate() {
        if let Some(mid) = m.message_id.as_deref() {
            let norm = normalize_message_id(mid);
            if !norm.is_empty() {
                by_message_id.insert(norm, idx);
            }
        }
    }

    let mut parent: Vec<Option<usize>> = vec![None; messages.len()];
    for (idx, m) in messages.iter().enumerate() {
        if let Some(ir) = m.in_reply_to.as_deref() {
            let norm = normalize_message_id(ir);
            if let Some(&pidx) = by_message_id.get(&norm)
                && pidx != idx
            {
                parent[idx] = Some(pidx);
                continue;
            }
        }
        for r in m.references.iter().rev() {
            let norm = normalize_message_id(r);
            let Some(&pidx) = by_message_id.get(&norm) else {
                continue;
            };
            if pidx == idx {
                continue;
            }
            parent[idx] = Some(pidx);
            break;
        }
    }

    let mut children_idx: Vec<Vec<usize>> = vec![Vec::new(); messages.len()];
    for (idx, p) in parent.iter().enumerate() {
        if let Some(pidx) = p
            && *pidx < messages.len()
        {
            children_idx[*pidx].push(idx);
        }
    }

    let roots: Vec<usize> = (0..messages.len())
        .filter(|&i| parent[i].is_none())
        .collect();
    let root_idx = roots.first().copied().unwrap_or(0);

    let mut visited: HashSet<usize> = HashSet::new();
    let mut visiting: HashSet<usize> = HashSet::new();

    let root = build_tree_node(
        root_idx,
        messages,
        &children_idx,
        &mut visited,
        &mut visiting,
    );

    let mut orphans = Vec::new();
    for &idx in roots.iter().skip(1) {
        if visited.contains(&idx) {
            continue;
        }
        orphans.push(build_tree_node(
            idx,
            messages,
            &children_idx,
            &mut visited,
            &mut visiting,
        ));
    }
    for idx in 0..messages.len() {
        if visited.contains(&idx) {
            continue;
        }
        orphans.push(build_tree_node(
            idx,
            messages,
            &children_idx,
            &mut visited,
            &mut visiting,
        ));
    }

    Some((root, orphans))
}

fn build_tree_node(
    idx: usize,
    messages: &[mailbox_parser::CanonicalMessage],
    children_idx: &[Vec<usize>],
    visited: &mut HashSet<usize>,
    visiting: &mut HashSet<usize>,
) -> JsonTreeMessageNode {
    debug_assert!(idx < messages.len());

    if visiting.contains(&idx) {
        return JsonTreeMessageNode {
            message: messages[idx].clone(),
            children: Vec::new(),
        };
    }

    visiting.insert(idx);
    visited.insert(idx);
    let mut children = Vec::new();
    for &cidx in &children_idx[idx] {
        if cidx == idx || visiting.contains(&cidx) {
            continue;
        }
        children.push(build_tree_node(
            cidx,
            messages,
            children_idx,
            visited,
            visiting,
        ));
    }
    visiting.remove(&idx);

    JsonTreeMessageNode {
        message: messages[idx].clone(),
        children,
    }
}

fn to_compact_message(
    m: &ParsedThreadMessage,
    body: JsonBody,
    attachment_paths: &HashMap<String, String>,
) -> JsonMessageCompactOut {
    let body = match body {
        JsonBody::Canonical => reply_text(
            &m.email.body_canonical,
            &segment_email_body(&m.email.body_canonical),
        ),
        JsonBody::Text => m.email.body_text.clone().unwrap_or_default(),
    };

    JsonMessageCompactOut {
        message_key: m.message_key.clone(),
        uid: m.uid,
        internal_date: m.internal_date.clone(),

        message_id: m.email.message_id.clone(),
        in_reply_to: m.email.in_reply_to.clone(),
        references: m.email.references.clone(),
        subject: m.email.subject.clone(),
        date: m.email.date.clone(),

        from: m.email.from.iter().map(format_email).collect(),
        to: m.email.to.iter().map(format_email).collect(),
        cc: m.email.cc.iter().map(format_email).collect(),
        bcc: m.email.bcc.iter().map(format_email).collect(),
        reply_to: m.email.reply_to.iter().map(format_email).collect(),

        body: normalize_line_terminators(&body),
        attachments: m
            .email
            .attachments
            .iter()
            .map(|a| to_compact_attachment(a, attachment_paths))
            .collect(),
        contact_hints_count: m.email.contact_hints.len(),
        attachment_hints_count: m.email.attachment_hints.len(),
        event_hints_count: m.email.event_hints.len(),
        mail_kind_hints_count: m.email.mail_kind_hints.len(),
        unsubscribe_hints_count: m.email.unsubscribe_hints.len(),
        service_lifecycle_hints_count: m.email.service_lifecycle_hints.len(),
        billing_action_hints_count: m.email.billing_action_hints.len(),
    }
}

fn to_compact_attachment(
    a: &ParsedAttachment,
    attachment_paths: &HashMap<String, String>,
) -> JsonAttachmentCompactOut {
    JsonAttachmentCompactOut {
        filename: a.filename.clone(),
        mime_type: a.mime_type.clone(),
        size: a.size,
        sha256: a.sha256.clone(),
        path: attachment_paths.get(&a.sha256).cloned(),
    }
}

fn first_nonempty_line(s: &str) -> Option<&str> {
    // NOTE: we intentionally treat U+2028/U+2029 as line terminators too.
    s.split(['\n', '\u{2028}', '\u{2029}'])
        .map(|l| l.trim())
        .find(|line| !line.is_empty())
}

fn normalize_line_terminators(s: &str) -> String {
    // VS Code warns on U+2028/U+2029; normalize them away for generated files.
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(ch) = it.next() {
        match ch {
            '\r' => {
                if matches!(it.peek(), Some('\n')) {
                    let _ = it.next();
                }
                out.push('\n');
            }
            '\u{2028}' | '\u{2029}' => out.push('\n'),
            _ => out.push(ch),
        }
    }
    out
}

fn export_attachments(
    threads: &[JsonThreadOut],
    base_dir: &Path,
    attachments_dir: &Path,
) -> Result<HashMap<String, String>> {
    fs::create_dir_all(attachments_dir)
        .with_context(|| format!("create attachments dir {}", attachments_dir.display()))?;

    let mut out: HashMap<String, String> = HashMap::new();
    let mut wrote = 0usize;
    for t in threads {
        for m in &t.thread.messages {
            for a in &m.email.attachments {
                let sha = a.sha256.trim();
                if sha.is_empty() || a.bytes.is_empty() {
                    continue;
                }
                if out.contains_key(sha) {
                    continue;
                }

                let file_name = attachment_file_name(a);
                let path = attachments_dir.join(file_name);
                if !path.exists() {
                    fs::write(&path, &a.bytes)
                        .with_context(|| format!("write attachment {}", path.display()))?;
                    wrote += 1;
                    if wrote.is_multiple_of(100) {
                        eprintln!("attachments_written={wrote}");
                    }
                }

                let rel = rel_path_for_output(base_dir, &path);
                out.insert(sha.to_string(), rel);
            }
        }
    }

    if wrote > 0 {
        eprintln!(
            "attachments_written_total={} dir={}",
            wrote,
            attachments_dir.display()
        );
    }
    Ok(out)
}

fn inject_canonical_attachment_paths(
    threads: &mut [JsonThreadCanonicalOut],
    attachment_paths: &HashMap<String, String>,
) {
    if attachment_paths.is_empty() {
        return;
    }

    for t in threads {
        for m in &mut t.thread.messages {
            for a in &mut m.attachments {
                if a.path.is_some() {
                    continue;
                }
                if let Some(p) = attachment_paths.get(&a.sha256) {
                    a.path = Some(p.clone());
                }
            }
        }
    }
}

fn rel_path_for_output(base_dir: &Path, path: &Path) -> String {
    match path.strip_prefix(base_dir) {
        Ok(rel) => rel.to_string_lossy().to_string(),
        Err(_) => path.to_string_lossy().to_string(),
    }
}

fn attachment_file_name(a: &ParsedAttachment) -> String {
    let sha = a.sha256.trim();
    let mut name = if let Some(f) = a
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let f = sanitize_filename(f);
        if f.is_empty() {
            sha.to_string()
        } else {
            format!("{}_{}", sha, f)
        }
    } else {
        sha.to_string()
    };

    if name.is_empty() {
        name = "attachment.bin".to_string();
    }
    name
}

fn sanitize_filename(s: &str) -> String {
    // Keep it safe and stable (no path separators, no control chars).
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ' ') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let out = out.trim().trim_matches('.').to_string();
    if out.len() > 120 {
        out.chars().take(120).collect()
    } else {
        out
    }
}

const HTML_TEMPLATE_START: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mailbox Parser HTML Export</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>
:root {
  --background: 0 0% 100%;
  --foreground: 240 10% 3.9%;
  --card: 0 0% 100%;
  --card-foreground: 240 10% 3.9%;
  --popover: 0 0% 100%;
  --popover-foreground: 240 10% 3.9%;
  --primary: 240 5.9% 10%;
  --primary-foreground: 0 0% 98%;
  --secondary: 240 4.8% 95.9%;
  --secondary-foreground: 240 5.9% 10%;
  --muted: 240 4.8% 95.9%;
  --muted-foreground: 240 3.8% 46.1%;
  --accent: 240 4.8% 95.9%;
  --accent-foreground: 240 5.9% 10%;
  --destructive: 0 84.2% 60.2%;
  --destructive-foreground: 0 0% 98%;
  --border: 240 5.9% 90%;
  --input: 240 5.9% 90%;
  --ring: 240 5.9% 10%;
  --radius: 0.5rem;
  --warning: 38 92% 50%;
  --warning-foreground: 0 0% 100%;
  --success: 142 76% 36%;
  --success-foreground: 0 0% 100%;
  --chart-1: 12 76% 61%;
  --chart-2: 173 58% 39%;
  --chart-3: 197 37% 24%;
  --chart-4: 43 74% 66%;
  --chart-5: 27 87% 67%;
}
.dark {
  --background: 240 10% 3.9%;
  --foreground: 0 0% 98%;
  --card: 240 10% 3.9%;
  --card-foreground: 0 0% 98%;
  --popover: 240 10% 3.9%;
  --popover-foreground: 0 0% 98%;
  --primary: 0 0% 98%;
  --primary-foreground: 240 5.9% 10%;
  --secondary: 240 3.7% 15.9%;
  --secondary-foreground: 0 0% 98%;
  --muted: 240 3.7% 15.9%;
  --muted-foreground: 240 5% 64.9%;
  --accent: 240 3.7% 15.9%;
  --accent-foreground: 0 0% 98%;
  --destructive: 0 62.8% 30.6%;
  --destructive-foreground: 0 0% 98%;
  --border: 240 3.7% 15.9%;
  --input: 240 3.7% 15.9%;
  --ring: 240 4.9% 83.9%;
  --warning: 48 96% 53%;
  --warning-foreground: 240 10% 3.9%;
  --success: 142 76% 36%;
  --success-foreground: 0 0% 100%;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: hsl(var(--background)); color: hsl(var(--foreground)); line-height: 1.5; min-height: 100vh; }
#app { display: flex; flex-direction: column; min-height: 100vh; }
#toolbar { background: hsl(var(--card)); border-bottom: 1px solid hsl(var(--border)); padding: 12px 16px; display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; position: sticky; top: 0; z-index: 30; }
#toolbar .field-group { display: flex; flex-direction: column; gap: 4px; min-width: 100px; }
#toolbar .field-group.wide { min-width: 180px; }
#toolbar label { font-size: 12px; font-weight: 500; color: hsl(var(--muted-foreground)); }
#toolbar input, #toolbar select { height: 36px; padding: 0 12px; font-size: 13px; background: hsl(var(--background)); color: hsl(var(--foreground)); border: 1px solid hsl(var(--input)); border-radius: var(--radius); outline: none; transition: border-color 0.15s, box-shadow 0.15s; }
#toolbar input:focus, #toolbar select:focus { border-color: hsl(var(--ring)); box-shadow: 0 0 0 2px hsl(var(--ring) / 0.1); }
#toolbar input::placeholder { color: hsl(var(--muted-foreground)); }
#toolbar button { height: 36px; padding: 0 16px; font-size: 13px; font-weight: 500; border-radius: var(--radius); cursor: pointer; transition: background 0.15s, color 0.15s, box-shadow 0.15s; display: inline-flex; align-items: center; justify-content: center; white-space: nowrap; gap: 6px; }
.btn-primary { background: hsl(var(--primary)); color: hsl(var(--primary-foreground)); border: none; }
.btn-primary:hover { background: hsl(var(--primary) / 0.9); }
.btn-secondary { background: hsl(var(--secondary)); color: hsl(var(--secondary-foreground)); border: 1px solid hsl(var(--border)); }
.btn-secondary:hover { background: hsl(var(--secondary) / 0.8); }
.btn-outline { background: transparent; color: hsl(var(--foreground)); border: 1px solid hsl(var(--border)); }
.btn-outline:hover { background: hsl(var(--accent)); color: hsl(var(--accent-foreground)); }
.btn-warning { background: hsl(var(--warning)); color: hsl(var(--warning-foreground)); border: none; }
.btn-warning:hover { background: hsl(var(--warning) / 0.9); }
.btn-success { background: hsl(var(--success)); color: hsl(var(--success-foreground)); border: none; }
.btn-success:hover { background: hsl(var(--success) / 0.9); }
#toolbar button:focus-visible { outline: 2px solid hsl(var(--ring)); outline-offset: 2px; }
#advanced-controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; }
#view-controls { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-end; padding-top: 8px; border-top: 1px solid hsl(var(--border)); margin-top: 8px; }
.btn-icon { min-width: 36px; padding: 0 12px; }
#toolbar button svg { width: 18px; height: 18px; stroke: currentColor; fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
#status { font-size: 13px; color: hsl(var(--muted-foreground)); display: flex; align-items: center; padding: 0 8px; height: 36px; }
#content { flex: 1; display: block; min-height: 0; }
#main-view { height: 100%; position: relative; }
#graph-view { width: 100%; height: 100%; background: hsl(var(--muted) / 0.3); }
#table-view { display: none; height: 100%; overflow: auto; background: hsl(var(--background)); }
.table-container { padding: 16px; }
table { border-collapse: collapse; width: 100%; min-width: 900px; font-size: 13px; }
thead { position: sticky; top: 0; z-index: 5; }
th { background: hsl(var(--muted)); color: hsl(var(--muted-foreground)); font-weight: 600; text-align: left; padding: 10px 12px; border-bottom: 1px solid hsl(var(--border)); }
td { padding: 10px 12px; border-bottom: 1px solid hsl(var(--border)); vertical-align: top; }
tbody tr { transition: background 0.15s; }
tbody tr:hover { background: hsl(var(--muted) / 0.5); }
.chip { display: inline-flex; align-items: center; height: 22px; padding: 0 8px; font-size: 11px; font-weight: 500; border-radius: 9999px; background: hsl(var(--secondary)); color: hsl(var(--secondary-foreground)); margin-right: 4px; margin-bottom: 2px; }
.chip.primary { background: hsl(var(--primary)); color: hsl(var(--primary-foreground)); }
.link { stroke: hsl(var(--border)); stroke-opacity: 0.8; }
.node text { pointer-events: none; fill: hsl(var(--muted-foreground)); font-size: 10px; }
#empty { display: none; padding: 48px 24px; text-align: center; color: hsl(var(--muted-foreground)); }
#empty-title { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
#empty-desc { font-size: 14px; }
@media (max-width: 1024px) {
  #toolbar { gap: 8px; }
  #toolbar .field-group { min-width: 80px; }
  #toolbar .field-group.wide { min-width: 140px; }
}
</style>
</head>
<body>
<div id="app">
  <div id="toolbar">
    <div class="field-group">
      <label>View</label>
      <select id="view-select">
        <option value="graph">Graph</option>
        <option value="table">Table</option>
      </select>
    </div>
    <div class="field-group">
      <label>Group</label>
      <select id="group-select">
        <option value="thread">By thread</option>
        <option value="subject">By subject</option>
      </select>
    </div>
    <div class="field-group wide">
      <label>Subject contains</label>
      <input id="subject-filter" type="text" placeholder="Filter by subject...">
    </div>
    <div class="field-group">
      <label>Date from</label>
      <input id="date-from-filter" type="date">
    </div>
    <div class="field-group">
      <label>Date to</label>
      <input id="date-to-filter" type="date">
    </div>
    <div class="field-group">
      <label>Mail kind</label>
      <select id="mail-kind-filter"><option value="">All</option></select>
    </div>
    <div class="field-group">
      <label>Event hint</label>
      <select id="event-kind-filter"><option value="">All</option></select>
    </div>
    <div class="field-group">
      <label>Lifecycle</label>
      <select id="lifecycle-kind-filter"><option value="">All</option></select>
    </div>
    <div id="advanced-controls">
      <div class="field-group">
        <label>Layers</label>
        <select id="layer-select">
          <option value="all">All</option>
          <option value="core">Core only</option>
          <option value="people">People only</option>
        </select>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="apply-btn" class="btn-primary">Apply</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="reset-btn" class="btn-secondary">Reset</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="csv-btn" class="btn-warning">Export CSV</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="excel-btn" class="btn-success">Export Excel</button>
      </div>
    </div>
    <div id="view-controls">
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="zoom-in-btn" class="btn-outline btn-icon" title="Zoom In">+</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="zoom-out-btn" class="btn-outline btn-icon" title="Zoom Out">-</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="reset-view-btn" class="btn-outline" title="Reset View">Reset</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="labels-btn" class="btn-outline" title="Toggle Labels">Labels</button>
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="import-btn" class="btn-secondary" title="Import JSON">Import</button>
        <input type="file" id="file-input" accept=".json" style="display:none">
      </div>
      <div class="field-group">
        <label>&nbsp;</label>
        <button type="button" id="theme-btn" class="btn-outline btn-icon" title="Toggle Theme" aria-label="Toggle Theme">
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9"></path>
          </svg>
        </button>
      </div>
    </div>
    <div><span id="status"></span></div>
  </div>
  <div id="content">
    <div id="main-view">
      <div id="graph-view"></div>
      <div id="table-view">
        <div class="table-container">
          <table id="message-table">
            <thead>
              <tr>
                <th>Date</th><th>Subject</th><th>From</th><th>To</th><th>Mail Kind</th><th>Event</th><th>Lifecycle</th><th>Body</th>
              </tr>
            </thead>
            <tbody id="message-table-body"></tbody>
          </table>
        </div>
        <div id="empty">
          <div id="empty-title">No messages found</div>
          <div id="empty-desc">Try adjusting your filters to see more results.</div>
        </div>
      </div>
    </div>
  </div>
</div>
<script>
"use strict";

const UI_CFG = Object.assign({
  default_view: "graph",
  max_table_rows: 10000,
  enable_advanced: true
}, window.HTML_UI_CONFIG || {});

const COLORS = {
  thread: "#0a0a0a",
  message: "#525252",
  person: "#737373",
  url: "#a3a3a3",
  date: "#171717",
  topic: "#404040"
};
let isDark = false;

let svg = null;
let g = null;
let zoomBehavior = null;
let simulation = null;
let showLabels = true;
let filteredRows = [];
let allRows = [];
let graphNodes = [];
let graphLinks = [];
const THEME_ICON = {
  moon: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9"></path></svg>',
  sun: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2"></path><path d="M12 20v2"></path><path d="m4.93 4.93 1.41 1.41"></path><path d="m17.66 17.66 1.41 1.41"></path><path d="M2 12h2"></path><path d="M20 12h2"></path><path d="m6.34 17.66-1.41 1.41"></path><path d="m19.07 4.93-1.41 1.41"></path></svg>'
};

function init() {
  initControls();
  if (Array.isArray(window.THREAD_DATA) && window.THREAD_DATA.length > 0) {
    rebuildData();
    return;
  }
  if (typeof window.HTML_DATA_URL === "string" && window.HTML_DATA_URL.length > 0) {
    loadThreadDataFromUrl(window.HTML_DATA_URL);
    return;
  }
  rebuildData();
}

function initControls() {
  const view = document.getElementById("view-select");
  view.value = UI_CFG.default_view === "table" ? "table" : "graph";
  document.getElementById("advanced-controls").style.display = UI_CFG.enable_advanced ? "contents" : "none";
  updateThemeButtonIcon();

  document.getElementById("apply-btn").addEventListener("click", applyFiltersAndRender);
  document.getElementById("reset-btn").addEventListener("click", resetFilters);
  document.getElementById("csv-btn").addEventListener("click", exportCsv);
  document.getElementById("excel-btn").addEventListener("click", exportExcel);
  document.getElementById("zoom-in-btn").addEventListener("click", () => zoomGraph(1.25));
  document.getElementById("zoom-out-btn").addEventListener("click", () => zoomGraph(0.8));
  document.getElementById("reset-view-btn").addEventListener("click", resetGraphView);
  document.getElementById("labels-btn").addEventListener("click", () => { showLabels = !showLabels; renderGraph(); });
  document.getElementById("view-select").addEventListener("change", updateViewVisibility);
  document.getElementById("import-btn").addEventListener("click", () => document.getElementById("file-input").click());
  document.getElementById("file-input").addEventListener("change", loadJsonFile);
  document.getElementById("group-select").addEventListener("change", applyFiltersAndRender);
  document.getElementById("layer-select").addEventListener("change", applyFiltersAndRender);
  document.getElementById("theme-btn").addEventListener("click", toggleTheme);
}

function toggleTheme() {
  isDark = !isDark;
  document.documentElement.classList.toggle("dark", isDark);
  updateThemeButtonIcon();
  updateGraphColors();
  renderGraph();
}

function updateThemeButtonIcon() {
  const btn = document.getElementById("theme-btn");
  btn.innerHTML = isDark ? THEME_ICON.sun : THEME_ICON.moon;
}

function updateGraphColors() {
  COLORS.thread = isDark ? "#fafafa" : "#0a0a0a";
  COLORS.message = isDark ? "#a3a3a3" : "#525252";
  COLORS.person = isDark ? "#737373" : "#737373";
  COLORS.url = isDark ? "#525252" : "#a3a3a3";
  COLORS.date = isDark ? "#e5e5e5" : "#171717";
  COLORS.topic = isDark ? "#a3a3a3" : "#404040";
}

function rebuildData() {
  if (Array.isArray(window.PROJECTION_ROWS) && window.PROJECTION_ROWS.length > 0) {
    allRows = window.PROJECTION_ROWS;
  } else {
    const threads = Array.isArray(window.THREAD_DATA) ? window.THREAD_DATA : [];
    allRows = flattenThreads(threads);
    window.PROJECTION_ROWS = allRows;
  }
  hydrateFilterLists(allRows);
  applyFiltersAndRender();
}

async function loadThreadDataFromUrl(url) {
  const statusEl = document.getElementById("status");
  try {
    statusEl.textContent = `Loading ${url}...`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    window.THREAD_DATA = Array.isArray(payload) ? payload : [payload];
    window.PROJECTION_ROWS = flattenThreads(window.THREAD_DATA);
    statusEl.textContent = `Loaded ${url}`;
    rebuildData();
  } catch (err) {
    statusEl.textContent = `Failed to load ${url}: ${err.message}`;
  }
}

function flattenThreads(threads) {
  const rows = [];
  for (const thread of threads) {
    const threadId = thread.thread_id || "unknown";
    for (const msg of (thread.messages || [])) {
      rows.push({
        thread_id: threadId,
        message_key: msg.message_key || "",
        subject: msg.subject || "",
        date: msg.date || "",
        from: msg.from || [],
        to: msg.to || [],
        cc: msg.cc || [],
        reply_text: msg.reply_text || "",
        mail_kinds: (msg.mail_kind_hints || []).map(x => x.kind).filter(Boolean),
        event_kinds: (msg.event_hints || []).map(x => x.kind).filter(Boolean),
        lifecycle_kinds: (msg.service_lifecycle_hints || []).map(x => x.kind).filter(Boolean),
        billing_kinds: (msg.billing_action_hints || []).map(x => x.kind).filter(Boolean)
      });
    }
  }
  return rows;
}

function hydrateFilterLists(rows) {
  fillSelectFromKinds("mail-kind-filter", rows.flatMap(r => r.mail_kinds));
  fillSelectFromKinds("event-kind-filter", rows.flatMap(r => r.event_kinds));
  fillSelectFromKinds("lifecycle-kind-filter", rows.flatMap(r => r.lifecycle_kinds));
}

function fillSelectFromKinds(id, values) {
  const select = document.getElementById(id);
  const existing = new Set([""]);
  for (const option of Array.from(select.options)) existing.add(option.value);
  const uniq = Array.from(new Set(values.filter(Boolean))).sort();
  for (const value of uniq) {
    if (existing.has(value)) continue;
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = value;
    select.appendChild(opt);
  }
}

function resetFilters() {
  document.getElementById("subject-filter").value = "";
  document.getElementById("date-from-filter").value = "";
  document.getElementById("date-to-filter").value = "";
  document.getElementById("mail-kind-filter").value = "";
  document.getElementById("event-kind-filter").value = "";
  document.getElementById("lifecycle-kind-filter").value = "";
  document.getElementById("group-select").value = "thread";
  document.getElementById("layer-select").value = "all";
  applyFiltersAndRender();
}

function applyFiltersAndRender() {
  const subjectNeedle = (document.getElementById("subject-filter").value || "").toLowerCase().trim();
  const dateFrom = document.getElementById("date-from-filter").value || "";
  const dateTo = document.getElementById("date-to-filter").value || "";
  const mailKind = document.getElementById("mail-kind-filter").value || "";
  const eventKind = document.getElementById("event-kind-filter").value || "";
  const lifecycleKind = document.getElementById("lifecycle-kind-filter").value || "";

  filteredRows = allRows.filter((row) => {
    if (subjectNeedle && !row.subject.toLowerCase().includes(subjectNeedle)) return false;
    const datePart = row.date ? row.date.slice(0, 10) : "";
    if (dateFrom && datePart && datePart < dateFrom) return false;
    if (dateTo && datePart && datePart > dateTo) return false;
    if (mailKind && !row.mail_kinds.includes(mailKind)) return false;
    if (eventKind && !row.event_kinds.includes(eventKind)) return false;
    if (lifecycleKind && !row.lifecycle_kinds.includes(lifecycleKind)) return false;
    return true;
  });

  renderTable();
  rebuildGraphFromRows(filteredRows);
  renderGraph();
  updateStatus();
  updateViewVisibility();
}

function updateStatus() {
  const threads = new Set(filteredRows.map(r => r.thread_id));
  document.getElementById("status").textContent = `${filteredRows.length} messages | ${threads.size} threads`;
}

function updateViewVisibility() {
  const view = document.getElementById("view-select").value;
  const graphView = document.getElementById("graph-view");
  const tableView = document.getElementById("table-view");
  graphView.style.display = view === "graph" ? "block" : "none";
  tableView.style.display = view === "table" ? "block" : "none";
}

function rebuildGraphFromRows(rows) {
  const groupBy = document.getElementById("group-select").value;
  const layer = document.getElementById("layer-select").value;
  const nodes = [];
  const links = [];
  const byId = new Map();

  function upsertNode(id, type, extra) {
    if (!byId.has(id)) {
      const node = Object.assign({ id, type }, extra || {});
      byId.set(id, node);
      nodes.push(node);
    }
    return byId.get(id);
  }

  for (const row of rows) {
    const threadKey = groupBy === "subject" ? normalizeSubject(row.subject || row.thread_id) : row.thread_id;
    upsertNode(`T:${threadKey}`, "thread", { subject: row.subject || "(no subject)" });
    upsertNode(`M:${row.message_key}`, "message", {
      subject: row.subject || "(no subject)",
      body: row.reply_text || "",
      date: row.date || "",
      from: row.from,
      to: row.to,
      cc: row.cc
    });
    links.push({ source: `T:${threadKey}`, target: `M:${row.message_key}`, type: "contains" });

    if (layer === "core") continue;

    for (const participant of [...row.from, ...row.to, ...row.cc]) {
      const email = parseAddressEmail(participant);
      if (!email) continue;
      if (layer === "people" || layer === "all") {
        upsertNode(`P:${email}`, "person", { email });
        links.push({ source: `M:${row.message_key}`, target: `P:${email}`, type: "participant" });
      }
    }

    if (layer !== "people") {
      for (const url of extractUrls(row.reply_text)) {
        upsertNode(`U:${url}`, "url", { url });
        links.push({ source: `M:${row.message_key}`, target: `U:${url}`, type: "mentions" });
      }
      if (row.date) {
        const d = row.date.slice(0, 10);
        upsertNode(`D:${d}`, "date", { date: d });
        links.push({ source: `M:${row.message_key}`, target: `D:${d}`, type: "dated" });
      }
    }
  }

  graphNodes = nodes;
  graphLinks = links;
}

function renderGraph() {
  const host = document.getElementById("graph-view");
  if (host.style.display === "none") return;

  if (simulation) {
    simulation.stop();
    simulation = null;
  }
  d3.select("#graph-view").selectAll("*").remove();

  const width = host.clientWidth || window.innerWidth;
  const height = host.clientHeight || (window.innerHeight - 90);
  svg = d3.select("#graph-view").append("svg").attr("width", width).attr("height", height);
  g = svg.append("g");

  zoomBehavior = d3.zoom().scaleExtent([0.15, 5]).on("zoom", (event) => {
    g.attr("transform", event.transform);
  });
  svg.call(zoomBehavior);

  const nodes = graphNodes.map(n => Object.assign({}, n));
  const links = graphLinks.map(l => Object.assign({}, l));
  const radiusByType = { thread: 18, message: 12, person: 9, url: 8, date: 8, topic: 10 };
  for (const n of nodes) n.radius = radiusByType[n.type] || 10;

  simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id).distance((l) => l.type === "contains" ? 70 : 45))
    .force("charge", d3.forceManyBody().strength(-180))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius(d => d.radius + 2))
    .on("tick", ticked);

  const link = g.selectAll(".link")
    .data(links)
    .join("line")
    .attr("class", "link")
    .attr("stroke-width", (d) => d.type === "contains" ? 1.7 : 1);

  const node = g.selectAll(".node")
    .data(nodes)
    .join("g")
    .attr("class", "node")
    .call(d3.drag()
      .on("start", (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on("drag", (event, d) => {
        d.fx = event.x;
        d.fy = event.y;
      })
      .on("end", (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      }));

  node.append("title")
    .text((d) => nodeHoverText(d));

  node.append("circle")
    .attr("r", (d) => d.radius)
    .attr("fill", (d) => COLORS[d.type] || "#525252")
    .attr("stroke", "#e5e5e5")
    .attr("stroke-width", 1.5);

  const labels = node.append("text")
    .attr("dy", (d) => d.radius + 10)
    .attr("text-anchor", "middle")
    .text((d) => shortLabel(d));

  function ticked() {
    link
      .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
    node.attr("transform", d => `translate(${d.x},${d.y})`);
    labels.style("display", showLabels ? null : "none");
  }
}

function renderTable() {
  const tbody = document.getElementById("message-table-body");
  const empty = document.getElementById("empty");
  const rows = filteredRows.slice(0, UI_CFG.max_table_rows);
  if (rows.length === 0) {
    tbody.innerHTML = "";
    empty.style.display = "block";
    return;
  }
  empty.style.display = "none";
  tbody.innerHTML = rows.map((r) => {
    const fromText = formatAddressList(r.from);
    const toText = formatAddressList(r.to);
    return `<tr data-key="${escapeHtml(r.message_key)}">
      <td>${escapeHtml(r.date || "")}</td>
      <td>${escapeHtml(r.subject || "")}</td>
      <td>${escapeHtml(fromText)}</td>
      <td>${escapeHtml(toText)}</td>
      <td>${toChipHtml(r.mail_kinds)}</td>
      <td>${toChipHtml(r.event_kinds)}</td>
      <td>${toChipHtml(r.lifecycle_kinds)}</td>
      <td>${escapeHtml(r.reply_text || "")}</td>
    </tr>`;
  }).join("");
}

function loadJsonFile(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (ev) => {
    try {
      const parsed = JSON.parse(ev.target.result);
      window.THREAD_DATA = Array.isArray(parsed) ? parsed : [parsed];
      rebuildData();
    } catch (err) {
      alert(`Failed to parse JSON: ${err.message}`);
    }
  };
  reader.readAsText(file);
}

function exportCsv() {
  const rows = filteredRows.slice(0, UI_CFG.max_table_rows);
  const header = ["date","thread_id","message_key","subject","from","to","mail_kinds","event_kinds","lifecycle_kinds","reply_text"];
  const lines = [header.join(",")];
  for (const r of rows) {
    const values = [
      r.date || "",
      r.thread_id || "",
      r.message_key || "",
      r.subject || "",
      formatAddressList(r.from),
      formatAddressList(r.to),
      (r.mail_kinds || []).join("|"),
      (r.event_kinds || []).join("|"),
      (r.lifecycle_kinds || []).join("|"),
      (r.reply_text || "").replace(/\s+/g, " ").trim()
    ];
    lines.push(values.map(csvEscape).join(","));
  }
  downloadBlob(new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" }), "mailbox_export.csv");
}

function exportExcel() {
  const rows = filteredRows.slice(0, UI_CFG.max_table_rows);
  const body = rows.map((r) => `<tr>
    <td>${escapeHtml(r.date || "")}</td>
    <td>${escapeHtml(r.thread_id || "")}</td>
    <td>${escapeHtml(r.message_key || "")}</td>
    <td>${escapeHtml(r.subject || "")}</td>
    <td>${escapeHtml(formatAddressList(r.from))}</td>
    <td>${escapeHtml(formatAddressList(r.to))}</td>
    <td>${escapeHtml((r.mail_kinds || []).join("|"))}</td>
    <td>${escapeHtml((r.event_kinds || []).join("|"))}</td>
    <td>${escapeHtml((r.lifecycle_kinds || []).join("|"))}</td>
    <td>${escapeHtml(r.reply_text || "")}</td>
  </tr>`).join("");
  const html = `<html><head><meta charset="utf-8"></head><body><table border="1">
    <tr><th>Date</th><th>Thread</th><th>Message key</th><th>Subject</th><th>From</th><th>To</th><th>Mail kind</th><th>Event</th><th>Lifecycle</th><th>Reply text</th></tr>
    ${body}</table></body></html>`;
  downloadBlob(new Blob([html], { type: "application/vnd.ms-excel" }), "mailbox_export.xls");
}

function downloadBlob(blob, fileName) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function zoomGraph(scaleBy) {
  if (!svg || !zoomBehavior) return;
  svg.transition().duration(250).call(zoomBehavior.scaleBy, scaleBy);
}

function resetGraphView() {
  if (!svg || !zoomBehavior) return;
  svg.transition().duration(300).call(zoomBehavior.transform, d3.zoomIdentity);
}

function shortLabel(node) {
  if (node.type === "thread") return (node.subject || "").slice(0, 18);
  if (node.type === "message") return (node.subject || "").slice(0, 16);
  if (node.type === "person") return (node.email || "").split("@")[0];
  if (node.type === "url") return (node.url || "").replace(/^https?:\/\//, "").slice(0, 14);
  if (node.type === "date") return node.date || "";
  return node.id;
}

function nodeHoverText(node) {
  if (node.type === "message") {
    const subject = node.subject || "(no subject)";
    const fullBody = (node.body || "").replace(/\s+/g, " ").trim();
    if (!fullBody) return subject;
    return `${subject}\n${fullBody}`;
  }
  if (node.type === "thread") return `Thread: ${node.subject || ""}`;
  if (node.type === "person") return node.email || "";
  if (node.type === "url") return node.url || "";
  if (node.type === "date") return node.date || "";
  return node.id || "";
}

function normalizeSubject(subject) {
  return (subject || "")
    .toLowerCase()
    .replace(/^\s*((re|fw|fwd)\s*:\s*)+/g, "")
    .trim() || "(no-subject)";
}

function extractUrls(text) {
  if (!text) return [];
  const matches = text.match(/https?:\/\/[^\s<>"{}|\\^`\[\]]+/gi) || [];
  return matches.map(s => s.replace(/[.,;:!?)\]]+$/, ""));
}

function parseAddressEmail(addr) {
  if (!addr) return "";
  if (typeof addr === "object" && addr.address) return String(addr.address).toLowerCase();
  if (typeof addr === "string") {
    const m = addr.match(/<([^>]+)>/) || addr.match(/([^\s<>]+@[^\s<>]+)/);
    return m ? String(m[1] || m[0]).toLowerCase() : "";
  }
  return "";
}

function formatAddress(addr) {
  if (!addr) return "";
  if (typeof addr === "object" && addr.address) {
    return addr.name ? `${addr.name} <${addr.address}>` : String(addr.address);
  }
  return String(addr);
}

function formatAddressList(list) {
  return (list || []).map(formatAddress).filter(Boolean).join(", ");
}

function toChipHtml(list) {
  const items = (list || []);
  if (!items.length) return "";
  return items.map((x) => `<span class="chip">${escapeHtml(x)}</span>`).join("");
}

function csvEscape(v) {
  const s = String(v || "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

window.addEventListener("resize", () => {
  if (document.getElementById("view-select").value === "graph") renderGraph();
});
</script>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_template_injects_thread_data_and_ui_config() {
        let t = JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: mailbox_parser::CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mk_canonical_msg(
                    "k1",
                    Some("m1@example.com"),
                    None,
                    &[],
                    "Hello",
                )],
            },
        };
        let html = render_html_template(
            &[t],
            &HtmlUiConfig {
                default_view: HtmlDefaultView::Table,
                data_mode: HtmlDataMode::Inline,
                max_table_rows: 123,
                enable_advanced: false,
            },
            None,
        )
        .expect("render html");
        assert!(html.contains("window.THREAD_DATA ="));
        assert!(html.contains("window.HTML_UI_CONFIG ="));
        assert!(html.contains("\"default_view\":\"table\""));
        assert!(html.contains("\"max_table_rows\":123"));
        assert!(html.contains("\"enable_advanced\":false"));
    }

    #[test]
    fn html_template_contains_advanced_controls_and_exports() {
        assert!(HTML_TEMPLATE_START.contains("id=\"view-select\""));
        assert!(HTML_TEMPLATE_START.contains("id=\"group-select\""));
        assert!(HTML_TEMPLATE_START.contains("id=\"csv-btn\""));
        assert!(HTML_TEMPLATE_START.contains("id=\"excel-btn\""));
    }

    #[test]
    fn html_template_view_controls_have_builtin_labels_without_lucide() {
        assert!(!HTML_TEMPLATE_START.contains("unpkg.com/lucide"));
        assert!(HTML_TEMPLATE_START.contains("id=\"zoom-in-btn\""));
        assert!(HTML_TEMPLATE_START.contains(">+</button>"));
        assert!(HTML_TEMPLATE_START.contains("id=\"zoom-out-btn\""));
        assert!(HTML_TEMPLATE_START.contains(">-</button>"));
        assert!(HTML_TEMPLATE_START.contains("id=\"reset-view-btn\""));
        assert!(HTML_TEMPLATE_START.contains(">Reset</button>"));
        assert!(HTML_TEMPLATE_START.contains("id=\"theme-btn\""));
        assert!(HTML_TEMPLATE_START.contains("const THEME_ICON = {"));
        assert!(HTML_TEMPLATE_START.contains("updateThemeButtonIcon()"));
        assert!(HTML_TEMPLATE_START.contains("<svg viewBox=\"0 0 24 24\" aria-hidden=\"true\">"));
        assert!(!HTML_TEMPLATE_START.contains("lucide.createIcons()"));
    }

    #[test]
    fn html_template_uses_full_reply_text_in_table_and_hover() {
        assert!(!HTML_TEMPLATE_START.contains(".slice(0, 200)"));
        assert!(!HTML_TEMPLATE_START.contains(".slice(0, 220)"));
        assert!(!HTML_TEMPLATE_START.contains(".slice(0, 500)"));
        assert!(HTML_TEMPLATE_START.contains("const fullBody = (node.body || \"\")"));
    }

    #[test]
    fn html_template_keeps_export_excel_template_literal_intact() {
        let t = JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: mailbox_parser::CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mk_canonical_msg(
                    "k1",
                    Some("m1@example.com"),
                    None,
                    &[],
                    "Hello",
                )],
            },
        };
        let html = render_html_template(
            &[t],
            &HtmlUiConfig {
                default_view: HtmlDefaultView::Graph,
                data_mode: HtmlDataMode::Inline,
                max_table_rows: 10,
                enable_advanced: true,
            },
            None,
        )
        .expect("render html");

        let literal = "${body}</table></body></html>`;";
        let pos_literal = html.find(literal).expect("export excel template literal");
        let pos_data = html
            .rfind("<script>window.THREAD_DATA =")
            .expect("injected thread data");
        assert!(
            pos_data > pos_literal,
            "thread payload must be injected after script source, not inside template literal"
        );
    }

    #[test]
    fn escape_inline_script_json_escapes_html_breakers() {
        let got = escape_inline_script_json("</script>&<tag>\u{2028}\u{2029}");
        assert_eq!(
            got,
            "\\u003c/script\\u003e\\u0026\\u003ctag\\u003e\\u2028\\u2029"
        );
    }

    #[test]
    fn html_template_external_mode_sets_data_url_loader() {
        let t = JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: mailbox_parser::CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mk_canonical_msg(
                    "k1",
                    Some("m1@example.com"),
                    None,
                    &[],
                    "Hello",
                )],
            },
        };
        let html = render_html_template(
            &[t],
            &HtmlUiConfig {
                default_view: HtmlDefaultView::Graph,
                data_mode: HtmlDataMode::External,
                max_table_rows: 10,
                enable_advanced: true,
            },
            Some("report.data.json"),
        )
        .expect("render html");

        assert!(html.contains("window.HTML_DATA_URL = \"report.data.json\""));
        assert!(html.contains("window.THREAD_DATA = []"));
    }

    fn mk_canonical_msg(
        key: &str,
        message_id: Option<&str>,
        in_reply_to: Option<&str>,
        references: &[&str],
        subject: &str,
    ) -> mailbox_parser::CanonicalMessage {
        mailbox_parser::CanonicalMessage {
            message_key: key.to_string(),
            uid: None,
            internal_date: None,
            message_id: message_id.map(|s| s.to_string()),
            in_reply_to: in_reply_to.map(|s| s.to_string()),
            references: references.iter().map(|s| s.to_string()).collect(),
            subject: Some(subject.to_string()),
            date: None,
            date_raw: None,
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            reply_text: subject.to_string(),
            quoted_blocks: vec![],
            forwarded_blocks: vec![],
            disclaimer_blocks: vec![],
            salutation: None,
            signature: None,
            attachments: vec![],
            contact_hints: vec![],
            signature_entities: Default::default(),
            attachment_hints: vec![],
            event_hints: vec![],
            mail_kind_hints: vec![],
            direction_hint: None,
            unsubscribe_hints: vec![],
            service_lifecycle_hints: vec![],
            billing_action_hints: vec![],
            sender_domain_hint: None,
            participant_domain_hints: vec![],
            forwarded_messages: vec![],
            forwarded_segments: vec![],
        }
    }

    #[test]
    fn tree_builds_nested_children_by_in_reply_to() {
        let messages = vec![
            mk_canonical_msg("k1", Some("m1@example.com"), None, &[], "root"),
            mk_canonical_msg(
                "k2",
                Some("m2@example.com"),
                Some("m1@example.com"),
                &["m1@example.com"],
                "child",
            ),
            mk_canonical_msg(
                "k3",
                Some("m3@example.com"),
                Some("m2@example.com"),
                &["m1@example.com", "m2@example.com"],
                "grandchild",
            ),
        ];
        let (root, orphans) = build_message_tree(&messages).expect("tree");
        assert_eq!(root.message.message_key, "k1");
        assert!(orphans.is_empty());
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].message.message_key, "k2");
        assert_eq!(root.children[0].children.len(), 1);
        assert_eq!(root.children[0].children[0].message.message_key, "k3");
    }

    #[test]
    fn tree_uses_references_when_in_reply_to_missing() {
        let messages = vec![
            mk_canonical_msg("k1", Some("m1@example.com"), None, &[], "root"),
            mk_canonical_msg(
                "k2",
                Some("m2@example.com"),
                None,
                &["m1@example.com"],
                "child-via-ref",
            ),
        ];
        let (root, orphans) = build_message_tree(&messages).expect("tree");
        assert_eq!(root.message.message_key, "k1");
        assert!(orphans.is_empty());
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].message.message_key, "k2");
    }

    #[test]
    fn tree_keeps_unlinked_messages_as_orphans() {
        let messages = vec![
            mk_canonical_msg("k1", Some("m1@example.com"), None, &[], "root"),
            mk_canonical_msg(
                "k2",
                Some("m2@example.com"),
                Some("missing@example.com"),
                &["missing@example.com"],
                "orphan",
            ),
        ];
        let (root, orphans) = build_message_tree(&messages).expect("tree");
        assert_eq!(root.message.message_key, "k1");
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].message.message_key, "k2");
    }

    #[test]
    fn tree_breaks_parent_reference_cycles_without_recursion_loop() {
        let messages = vec![
            mk_canonical_msg(
                "k1",
                Some("m1@example.com"),
                Some("m2@example.com"),
                &["m2@example.com"],
                "a",
            ),
            mk_canonical_msg(
                "k2",
                Some("m2@example.com"),
                Some("m1@example.com"),
                &["m1@example.com"],
                "b",
            ),
        ];
        let (root, orphans) = build_message_tree(&messages).expect("tree");
        assert!(orphans.is_empty());
        assert!(root.message.message_key == "k1" || root.message.message_key == "k2");
        let total_children = root.children.len()
            + root
                .children
                .iter()
                .map(|c| c.children.len())
                .sum::<usize>();
        assert!(total_children <= 1);
    }

    #[test]
    fn compact_json_does_not_duplicate_body_fields() {
        let t = JsonThreadOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: ParsedThread {
                thread_id: "tid".to_string(),
                messages: vec![ParsedThreadMessage {
                    message_key: "k".to_string(),
                    uid: Some(1),
                    internal_date: Some("2026-01-01T00:00:00Z".to_string()),
                    email: mailbox_parser::ParsedEmail {
                        message_id: Some("mid".to_string()),
                        in_reply_to: None,
                        references: vec![],
                        subject: Some("subj".to_string()),
                        date: Some("2026-01-01T00:00:00Z".to_string()),
                        date_raw: None,
                        from: vec![],
                        to: vec![],
                        cc: vec![],
                        bcc: vec![],
                        reply_to: vec![],
                        body_text: Some("TEXT".to_string()),
                        body_html: Some("<p>HTML</p>".to_string()),
                        body_canonical: "CANON".to_string(),
                        attachments: vec![],
                        forwarded_messages: vec![],
                        forwarded_segments: vec![],
                        contact_hints: vec![],
                        signature_entities: Default::default(),
                        attachment_hints: vec![],
                        event_hints: vec![],
                        mail_kind_hints: vec![],
                        direction_hint: None,
                        unsubscribe_hints: vec![],
                        service_lifecycle_hints: vec![],
                        billing_action_hints: vec![],
                        raw_headers: Default::default(),
                    },
                }],
            },
        };

        let ap = HashMap::new();
        let c = to_compact_thread(&t, JsonBody::Canonical, &ap);
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("\"body\":\"CANON\""));
        assert!(s.contains("\"contact_hints_count\":0"));
        assert!(s.contains("\"attachment_hints_count\":0"));
        assert!(s.contains("\"event_hints_count\":0"));
        assert!(s.contains("\"mail_kind_hints_count\":0"));
        assert!(s.contains("\"unsubscribe_hints_count\":0"));
        assert!(s.contains("\"service_lifecycle_hints_count\":0"));
        assert!(s.contains("\"billing_action_hints_count\":0"));
        assert!(!s.contains("body_text"));
        assert!(!s.contains("body_canonical"));
        assert!(!s.contains("body_html"));
        assert!(!s.contains("raw_headers"));
    }

    #[test]
    fn attachments_export_writes_files_and_includes_paths_in_compact_json() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let base_dir = std::env::temp_dir().join(format!("mailbox-parser-cli-test-{nonce}"));
        let attachments_dir = base_dir.join("attachments");
        fs::create_dir_all(&base_dir).unwrap();

        let att = ParsedAttachment {
            filename: Some("hello.txt".to_string()),
            mime_type: "text/plain".to_string(),
            size: 3,
            sha256: "deadbeef".to_string(),
            content_id: None,
            content_disposition: None,
            bytes: vec![0x61, 0x62, 0x63],
        };

        let t = JsonThreadOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: ParsedThread {
                thread_id: "tid".to_string(),
                messages: vec![ParsedThreadMessage {
                    message_key: "k".to_string(),
                    uid: Some(1),
                    internal_date: Some("2026-01-01T00:00:00Z".to_string()),
                    email: mailbox_parser::ParsedEmail {
                        message_id: Some("mid".to_string()),
                        in_reply_to: None,
                        references: vec![],
                        subject: Some("subj".to_string()),
                        date: Some("2026-01-01T00:00:00Z".to_string()),
                        date_raw: None,
                        from: vec![],
                        to: vec![],
                        cc: vec![],
                        bcc: vec![],
                        reply_to: vec![],
                        body_text: Some("TEXT".to_string()),
                        body_html: None,
                        body_canonical: "CANON".to_string(),
                        attachments: vec![att],
                        forwarded_messages: vec![],
                        forwarded_segments: vec![],
                        contact_hints: vec![],
                        signature_entities: Default::default(),
                        attachment_hints: vec![],
                        event_hints: vec![],
                        mail_kind_hints: vec![],
                        direction_hint: None,
                        unsubscribe_hints: vec![],
                        service_lifecycle_hints: vec![],
                        billing_action_hints: vec![],
                        raw_headers: Default::default(),
                    },
                }],
            },
        };

        let ap = export_attachments(&[t.clone()], &base_dir, &attachments_dir).unwrap();
        let p = ap.get("deadbeef").cloned().unwrap();
        assert!(p.starts_with("attachments/"));

        let on_disk = base_dir.join(&p);
        let bytes = fs::read(&on_disk).unwrap();
        assert_eq!(bytes, vec![0x61, 0x62, 0x63]);

        let c = to_compact_thread(&t, JsonBody::Canonical, &ap);
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("\"path\":\"attachments/"));

        let _ = fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn canonical_json_includes_attachment_paths_when_exported() {
        let mut threads = vec![JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: mailbox_parser::CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mailbox_parser::CanonicalMessage {
                    message_key: "k".to_string(),
                    uid: Some(1),
                    internal_date: None,
                    message_id: Some("mid".to_string()),
                    in_reply_to: None,
                    references: vec![],
                    subject: Some("subj".to_string()),
                    date: Some("2026-01-01T00:00:00Z".to_string()),
                    date_raw: None,
                    from: vec![],
                    to: vec![],
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    reply_text: "hi".to_string(),
                    quoted_blocks: vec![],
                    forwarded_blocks: vec![],
                    disclaimer_blocks: vec![],
                    salutation: None,
                    signature: None,
                    attachments: vec![mailbox_parser::CanonicalAttachment {
                        filename: Some("hello.txt".to_string()),
                        mime_type: "text/plain".to_string(),
                        size: 3,
                        sha256: "deadbeef".to_string(),
                        content_id: None,
                        content_disposition: None,
                        path: None,
                    }],
                    contact_hints: vec![],
                    signature_entities: Default::default(),
                    attachment_hints: vec![],
                    event_hints: vec![],
                    mail_kind_hints: vec![],
                    direction_hint: None,
                    unsubscribe_hints: vec![],
                    service_lifecycle_hints: vec![],
                    billing_action_hints: vec![],
                    sender_domain_hint: None,
                    participant_domain_hints: vec![],
                    forwarded_messages: vec![],
                    forwarded_segments: vec![],
                }],
            },
        }];

        let mut ap = HashMap::new();
        ap.insert(
            "deadbeef".to_string(),
            "attachments/deadbeef_hello.txt".to_string(),
        );

        inject_canonical_attachment_paths(&mut threads, &ap);

        assert_eq!(
            threads[0].thread.messages[0].attachments[0].path.as_deref(),
            Some("attachments/deadbeef_hello.txt")
        );
    }

    #[test]
    fn markdown_is_pure_view_over_canonical_threads() {
        use std::collections::BTreeMap;

        let att = ParsedAttachment {
            filename: Some("file.txt".to_string()),
            mime_type: "text/plain".to_string(),
            size: 3,
            sha256: "deadbeef".to_string(),
            content_id: None,
            content_disposition: Some("attachment".to_string()),
            bytes: vec![0x61, 0x62, 0x63],
        };

        let make_addr = |name: &str, address: &str| mailbox_parser::EmailAddress {
            name: Some(name.to_string()),
            address: address.to_string(),
        };

        let root = mailbox_parser::ParsedEmail {
            message_id: Some("root1".to_string()),
            in_reply_to: None,
            references: vec![],
            subject: Some("Root Subject".to_string()),
            date: Some("2026-01-01T00:00:00Z".to_string()),
            date_raw: Some("Wed, 01 Jan 2026 00:00:00 +0000".to_string()),
            from: vec![make_addr("Alice", "alice@example.com")],
            to: vec![make_addr("Bob", "bob@example.com")],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            body_text: Some(
                "Hi Bob,\n\nSounds good.\n\nOn Tue, someone wrote:\n> previous".to_string(),
            ),
            body_html: None,
            body_canonical: "Hi Bob,\n\nSounds good.\n\nOn Tue, someone wrote:\n> previous"
                .to_string(),
            attachments: vec![att],
            forwarded_messages: vec![],
            forwarded_segments: vec![],
            contact_hints: vec![],
            signature_entities: Default::default(),
            attachment_hints: vec![],
            event_hints: vec![],
            mail_kind_hints: vec![],
            direction_hint: None,
            unsubscribe_hints: vec![],
            service_lifecycle_hints: vec![],
            billing_action_hints: vec![],
            raw_headers: BTreeMap::new(),
        };

        let reply = mailbox_parser::ParsedEmail {
            message_id: Some("reply1".to_string()),
            in_reply_to: Some("root1".to_string()),
            references: vec!["root1".to_string()],
            subject: Some("Re: Root Subject".to_string()),
            date: Some("2026-01-01T01:00:00Z".to_string()),
            date_raw: Some("Wed, 01 Jan 2026 01:00:00 +0000".to_string()),
            from: vec![make_addr("Bob", "bob@example.com")],
            to: vec![make_addr("Alice", "alice@example.com")],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            body_text: Some("Thanks!\n\nOn Tue, someone wrote:\n> previous".to_string()),
            body_html: None,
            body_canonical: "Thanks!\n\nOn Tue, someone wrote:\n> previous".to_string(),
            attachments: vec![],
            forwarded_messages: vec![],
            forwarded_segments: vec![],
            contact_hints: vec![],
            signature_entities: Default::default(),
            attachment_hints: vec![],
            event_hints: vec![],
            mail_kind_hints: vec![],
            direction_hint: None,
            unsubscribe_hints: vec![],
            service_lifecycle_hints: vec![],
            billing_action_hints: vec![],
            raw_headers: BTreeMap::new(),
        };

        let parsed_thread = ParsedThread {
            thread_id: "t1".to_string(),
            messages: vec![
                ParsedThreadMessage {
                    message_key: "k1".to_string(),
                    uid: Some(1),
                    internal_date: None,
                    email: root,
                },
                ParsedThreadMessage {
                    message_key: "k2".to_string(),
                    uid: Some(2),
                    internal_date: None,
                    email: reply,
                },
            ],
        };

        let thread = canonicalize_threads(std::slice::from_ref(&parsed_thread))
            .into_iter()
            .next()
            .unwrap();
        let t = JsonThreadCanonicalOut {
            account_id: "test".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread,
        };

        let mut attachment_paths = HashMap::new();
        attachment_paths.insert(
            "deadbeef".to_string(),
            "attachments/deadbeef_file.txt".to_string(),
        );

        let mut buf = Vec::new();
        render_thread_markdown(&mut buf, &t, &attachment_paths).unwrap();
        let got = String::from_utf8(buf).unwrap();

        let expected = "# [Thread - re t1] from Alice <alice@example.com>: Sounds good.\n\nAccount: test\n\nMailboxes: INBOX\n\nSubject: Root Subject\nFrom: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nDate: 2026-01-01T00:00:00Z\nMessage-ID: root1\nUID: 1\n\nSounds good.\n\n### Quoted 1\n\nOn Tue, someone wrote:\n> previous\n\nAttachments:\n- file.txt (text/plain, 3 bytes) [attachments/deadbeef_file.txt]\n\n## from Bob <bob@example.com>: Thanks!\n\nSubject: Re: Root Subject\nFrom: Bob <bob@example.com>\nTo: Alice <alice@example.com>\nDate: 2026-01-01T01:00:00Z\nMessage-ID: reply1\nUID: 2\n\nThanks!\n\n### Quoted 1\n\nOn Tue, someone wrote:\n> previous\n\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn markdown_renders_forwarded_blocks_and_labels_thread_fwd() {
        let email = mailbox_parser::EmailAddress::parse("A <a@example.com>").unwrap();
        let t = JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mailbox_parser::CanonicalMessage {
                    message_key: "k".to_string(),
                    uid: Some(1),
                    internal_date: None,
                    message_id: None,
                    in_reply_to: None,
                    references: vec![],
                    subject: Some("Fwd: hello".to_string()),
                    date: None,
                    date_raw: None,
                    from: vec![email],
                    to: vec![],
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    reply_text: "top".to_string(),
                    quoted_blocks: vec![],
                    forwarded_blocks: vec!["Forwarded content".to_string()],
                    disclaimer_blocks: vec![],
                    salutation: None,
                    signature: None,
                    attachments: vec![],
                    contact_hints: vec![],
                    signature_entities: Default::default(),
                    attachment_hints: vec![],
                    event_hints: vec![],
                    mail_kind_hints: vec![],
                    direction_hint: None,
                    unsubscribe_hints: vec![],
                    service_lifecycle_hints: vec![],
                    billing_action_hints: vec![],
                    sender_domain_hint: None,
                    participant_domain_hints: vec![],
                    forwarded_messages: vec![],
                    forwarded_segments: vec![],
                }],
            },
        };

        let mut buf: Vec<u8> = Vec::new();
        let ap = HashMap::new();
        render_thread_markdown(&mut buf, &t, &ap).unwrap();
        let s = String::from_utf8_lossy(&buf);
        assert!(s.contains("# [Thread - fwd tid]"));
        assert!(s.contains("### Forwarded 1"));
        assert!(s.contains("Forwarded content"));
    }

    #[test]
    fn markdown_labels_reply_threads_re_when_multiple_messages() {
        fn mk_msg(
            k: &str,
            email: &mailbox_parser::EmailAddress,
        ) -> mailbox_parser::CanonicalMessage {
            mailbox_parser::CanonicalMessage {
                message_key: k.to_string(),
                uid: Some(1),
                internal_date: None,
                message_id: None,
                in_reply_to: None,
                references: vec![],
                subject: Some("hello".to_string()),
                date: None,
                date_raw: None,
                from: vec![email.clone()],
                to: vec![],
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                reply_text: "top".to_string(),
                quoted_blocks: vec![],
                forwarded_blocks: vec![],
                disclaimer_blocks: vec![],
                salutation: None,
                signature: None,
                attachments: vec![],
                contact_hints: vec![],
                signature_entities: Default::default(),
                attachment_hints: vec![],
                event_hints: vec![],
                mail_kind_hints: vec![],
                direction_hint: None,
                unsubscribe_hints: vec![],
                service_lifecycle_hints: vec![],
                billing_action_hints: vec![],
                sender_domain_hint: None,
                participant_domain_hints: vec![],
                forwarded_messages: vec![],
                forwarded_segments: vec![],
            }
        }

        let email = mailbox_parser::EmailAddress::parse("A <a@example.com>").unwrap();

        let t = JsonThreadCanonicalOut {
            account_id: "acc".to_string(),
            mailboxes: vec!["INBOX".to_string()],
            thread: CanonicalThread {
                thread_id: "tid".to_string(),
                messages: vec![mk_msg("k1", &email), mk_msg("k2", &email)],
            },
        };

        let mut buf: Vec<u8> = Vec::new();
        let ap = HashMap::new();
        render_thread_markdown(&mut buf, &t, &ap).unwrap();
        let s = String::from_utf8_lossy(&buf);
        assert!(s.contains("# [Thread - re tid]"));
    }

    #[test]
    fn dedupe_messages_by_message_id_keeps_first_normalized_match() {
        fn mk_message(mid: Option<&str>, subject: &str) -> MailMessage {
            MailMessage {
                uid: None,
                internal_date: Some("2026-02-01T00:00:00Z".to_string()),
                flags: Vec::new(),
                parsed: mailbox_parser::ParsedEmail {
                    message_id: mid.map(|s| s.to_string()),
                    in_reply_to: None,
                    references: vec![],
                    subject: Some(subject.to_string()),
                    date: Some("2026-02-01T00:00:00Z".to_string()),
                    date_raw: None,
                    from: vec![],
                    to: vec![],
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    body_text: Some("body".to_string()),
                    body_html: None,
                    body_canonical: "body".to_string(),
                    attachments: vec![],
                    forwarded_messages: vec![],
                    forwarded_segments: vec![],
                    contact_hints: vec![],
                    signature_entities: Default::default(),
                    attachment_hints: vec![],
                    event_hints: vec![],
                    mail_kind_hints: vec![],
                    direction_hint: None,
                    unsubscribe_hints: vec![],
                    service_lifecycle_hints: vec![],
                    billing_action_hints: vec![],
                    raw_headers: Default::default(),
                },
                raw: Vec::new(),
            }
        }

        let input = vec![
            mk_message(Some("<MID-1@EXAMPLE.COM>"), "first"),
            mk_message(Some("mid-1@example.com"), "duplicate"),
            mk_message(Some("<mid-2@example.com>"), "second"),
            mk_message(None, "no-mid"),
        ];

        let (out, deduped) = dedupe_messages_by_message_id(input);

        assert_eq!(deduped, 1);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].parsed.subject.as_deref(), Some("first"));
        assert_eq!(out[1].parsed.subject.as_deref(), Some("second"));
        assert_eq!(out[2].parsed.subject.as_deref(), Some("no-mid"));
    }
}

fn format_email(addr: &mailbox_parser::EmailAddress) -> String {
    if let Some(name) = addr.name.as_deref() {
        format!("{} <{}>", name, addr.address)
    } else {
        addr.address.clone()
    }
}
