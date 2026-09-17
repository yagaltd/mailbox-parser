//! Optional TypeSafe AI refinement (`typesafe` cargo feature).
//!
//! `mailbox-parser-cli typesafe refine` — the teacher-vs-parser signature
//! probe as a first-class command so users can run their own refinement
//! rounds on their own batches (docs/typesafe-signature-probe.md in the
//! repo root). Parses the mbox in-process (streaming, stride-sampled),
//! asks one batched TypeSafe call per message, and prints the agreement
//! matrix plus per-message verdicts.
//!
//! The feature is OFF by default: `cargo build --features typesafe`.
//! The API key is read from `--key-file` (default /tmp/typesafe.key) or
//! `TYPESAFEAI_API_KEY`; it is never logged or written to output files.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

use mailbox_parser::{
    MboxReadOptions, ParseRfc822Options, canonicalize_threads, iter_mbox_messages,
    parse_rfc822_with_options, thread_messages_from_mail_messages,
};

const API_URL: &str = "https://api.typesafe.ai/v1/systemone";
const SHOW_LINES: usize = 60;
const OPTION_WINDOW: usize = 35;
const NEAR_TOLERANCE: isize = 2;
const MAX_RETRIES: u32 = 3;

const MAIL_KINDS: &[(&str, &str)] = &[
    ("human_correspondence", "written by a person to another person (reply, discussion, personal note)"),
    ("newsletter", "periodic digest or mailing-list content with articles/links"),
    ("notification", "service alert about an event or account (security, social, job alert, shipping)"),
    ("transactional", "receipt, invoice, booking confirmation, statement about a transaction"),
    ("automated_report", "scheduled machine-generated report or monitoring output"),
    ("marketing", "promotional offer, campaign, discount announcement"),
];

#[derive(Clone, Debug, Parser)]
pub struct TypesafeArgs {
    #[command(subcommand)]
    pub command: TypesafeCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum TypesafeCommand {
    /// Diff TypeSafe's signature split against the parser's over an mbox sample.
    Refine(RefineArgs),
}

#[derive(Clone, Debug, Parser)]
pub struct RefineArgs {
    /// Mbox file to sample
    #[arg(long)]
    pub path: PathBuf,
    /// Keep every Nth message (stride sampling for large mboxes)
    #[arg(long, default_value_t = 1)]
    pub stride: usize,
    /// Max messages to judge
    #[arg(long, default_value_t = 30)]
    pub limit: usize,
    /// API key file (never committed; chmod 600)
    #[arg(long, default_value = "/tmp/typesafe.key")]
    pub key_file: PathBuf,
    /// Model id
    #[arg(long, default_value = "jev-latest")]
    pub model: String,
    /// Write per-message verdicts as JSONL
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub fn run(args: TypesafeArgs) -> Result<()> {
    match args.command {
        TypesafeCommand::Refine(a) => refine(a),
    }
}

fn load_key(path: &Path) -> Result<String> {
    if path.exists() {
        let key = std::fs::read_to_string(path)?.trim().to_string();
        if !key.is_empty() {
            return Ok(key);
        }
    }
    if let Ok(k) = std::env::var("TYPESAFEAI_API_KEY") {
        if !k.trim().is_empty() {
            return Ok(k.trim().to_string());
        }
    }
    bail!(
        "no API key: write it to {} (chmod 600) or set TYPESAFEAI_API_KEY",
        path.display()
    );
}

/// Mirror of the probe reconstruction: (shown text with line numbers, parser
/// boundary 0-based within shown text, absolute first-shown line number).
fn reconstruct(m: &Value) -> Option<(String, Option<usize>, usize)> {
    let sal = m.get("salutation").and_then(|v| v.as_str()).unwrap_or("");
    let reply = m.get("reply_text").and_then(|v| v.as_str()).unwrap_or("");
    let sig = m.get("signature").and_then(|v| v.as_str()).unwrap_or("");
    if reply.trim().is_empty() {
        return None;
    }
    let mut pre = String::new();
    if !sal.is_empty() {
        pre.push_str(sal);
        pre.push('\n');
    }
    pre.push_str(reply);
    let full = if sig.is_empty() {
        pre.clone()
    } else {
        format!("{pre}\n{sig}")
    };
    let mut pre_lines: Vec<&str> = pre.split('\n').collect();
    while pre_lines.last().is_some_and(|l| l.trim().is_empty()) {
        pre_lines.pop();
    }
    let boundary = if sig.trim().is_empty() {
        None
    } else {
        Some(pre_lines.len())
    };

    let mut lines: Vec<&str> = full.split('\n').collect();
    while lines.first().is_some_and(|l| l.trim().is_empty()) {
        lines.remove(0);
    }
    let n = lines.len();
    let mut boundary = boundary.filter(|b| *b < n);
    let shown_from = n.saturating_sub(SHOW_LINES);
    if boundary.is_some_and(|b| b < shown_from) {
        return None; // boundary outside visible window
    }
    let text = lines
        .iter()
        .enumerate()
        .map(|(i, l)| format!("{}: {}", i + 1, l))
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(b) = boundary.as_mut() {
        *b -= shown_from;
    }
    Some((text, boundary, shown_from))
}

fn build_payload(m: &Value, text: &str, shown_from: usize, model: &str) -> Value {
    let n_lines = text.matches('\n').count() + 1;
    let n = shown_from + n_lines;
    let first_opt = n.saturating_sub(OPTION_WINDOW) + 1;
    let mut options = serde_json::Map::new();
    for i in first_opt..=n {
        options.insert(i.to_string(), Value::String(format!("the personal signature block begins at line {i}")));
    }
    options.insert(
        "none".into(),
        Value::String(
            "this message has no personal signature block (automated mail, or it simply ends without a sign-off)".into(),
        ),
    );
    let mut kind_criteria = serde_json::Map::new();
    for (k, d) in MAIL_KINDS {
        kind_criteria.insert((*k).into(), Value::String((*d).into()));
    }
    json!({
        "state": {
            "subject": m.get("subject").and_then(|v| v.as_str()).unwrap_or(""),
            "from": m.get("from").and_then(|v| v.as_array()).map(|a| a.iter().take(3).map(|f| {
                if f.is_string() { f.clone() } else {
                    json!(format!("{} <{}>",
                        f.get("name").and_then(|x| x.as_str()).unwrap_or(""),
                        f.get("address").and_then(|x| x.as_str()).unwrap_or("")))
                }
            }).collect::<Vec<_>>()).unwrap_or_default(),
            "text": text,
        },
        "model": model,
        "questions": {
            "signature_start": {
                "type": "choice",
                "instructions": "The text is an email message body with each line prefixed by its line number. A personal signature block is the sender's trailing sign-off: greeting word ('Best regards', 'Thanks'), name, and optionally title/company/phone/quote. It is NOT part of the message content and NOT a legal disclaimer or unsubscribe footer. Choose the line where the signature block begins, or 'none' if there is no personal signature.",
                "criteria": Value::Object(options),
            },
            "mail_kind": {
                "type": "choice",
                "instructions": "Classify this email message by its true kind, regardless of language. Judge from sender, subject, and text.",
                "criteria": Value::Object(kind_criteria),
            },
        }
    })
}

fn post(payload: &Value, key: &str) -> Result<Value> {
    let mut delay = Duration::from_secs(1);
    for attempt in 0..MAX_RETRIES {
        let resp = ureq::post(API_URL)
            .set("Authorization", &format!("Bearer {key}"))
            .timeout(Duration::from_secs(60))
            .send_json(payload.clone())?;
        let status = resp.status();
        if status == 429 || status == 529 {
            if attempt + 1 == MAX_RETRIES {
                bail!("API overloaded (HTTP {status}) after {MAX_RETRIES} attempts");
            }
            std::thread::sleep(delay);
            delay *= 2;
            continue;
        }
        if !(200..300).contains(&status) {
            bail!("API HTTP {status}: {}", resp.into_string().unwrap_or_default());
        }
        return Ok(resp.into_json()?);
    }
    unreachable!()
}

fn refine(args: RefineArgs) -> Result<()> {
    let key = load_key(&args.key_file)?;
    let stride = args.stride.max(1);

    // Streaming stride sample: parse only kept messages, bound memory.
    let mut messages = Vec::new();
    let iter = iter_mbox_messages(&args.path, MboxReadOptions { strict: false, max_messages: None })
        .with_context(|| format!("open mbox {}", args.path.display()))?;
    for (idx, item) in iter.enumerate() {
        if idx % stride != 0 {
            continue;
        }
        if messages.len() >= args.limit {
            break;
        }
        let msg = item.with_context(|| format!("read message {idx}"))?;
        let parsed = parse_rfc822_with_options(
            &msg.raw,
            &ParseRfc822Options {
                owner_emails: Vec::new(),
                lifecycle_lexicon: None,
                keep_body_html: false,
                keep_attachment_bytes: false,
            },
        )
        .with_context(|| format!("parse message {idx}"))?;
        messages.push(mailbox_parser::MailMessage {
            uid: None,
            internal_date: parsed.date.clone().or(msg.separator_date.clone()),
            flags: Vec::new(),
            parsed,
            raw: Vec::new(),
        });
    }
    eprintln!("sampled {} messages (stride {stride})", messages.len());

    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msgs: Vec<Value> = canonical
        .iter()
        .flat_map(|t| t.messages.iter())
        .map(|m| serde_json::to_value(m).unwrap_or(Value::Null))
        .filter(|v| v.get("reply_text").and_then(|r| r.as_str()).is_some_and(|s| !s.trim().is_empty()))
        .collect();
    eprintln!("{} messages with reply text; judging…", msgs.len());

    let mut counts = std::collections::BTreeMap::new();
    let mut kinds = std::collections::BTreeMap::new();
    let mut out_file = match &args.out {
        Some(p) => Some(std::io::BufWriter::new(
            std::fs::File::create(p).with_context(|| format!("create {}", p.display()))?,
        )),
        None => None,
    };
    let mut judged = 0usize;
    let mut skipped = 0usize;

    for (i, m) in msgs.iter().enumerate() {
        let Some((text, parser_boundary, shown_from)) = reconstruct(m) else {
            skipped += 1;
            continue;
        };
        let payload = build_payload(m, &text, shown_from, &args.model);
        let resp = match post(&payload, &key) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  {i:3} API error: {e}");
                *counts.entry("error".to_string()).or_insert(0usize) += 1;
                continue;
            }
        };
        judged += 1;
        let sig = &resp["answers"]["signature_start"];
        let kind = &resp["answers"]["mail_kind"];
        let choice = sig["choice"].as_str().unwrap_or("none");
        let t_line = choice.parse::<usize>().ok().map(|v| v.saturating_sub(1));
        let conf = sig["confidence"].as_f64();
        let kind_name = kind["choice"].as_str().unwrap_or("?").to_string();
        *kinds.entry(kind_name.clone()).or_insert(0usize) += 1;

        let bucket = match (parser_boundary, t_line) {
            (Some(p), Some(t)) => {
                if (t as isize - p as isize).abs() <= NEAR_TOLERANCE {
                    "agree"
                } else {
                    "both-present-far"
                }
            }
            (None, None) => "agree-both-none",
            (None, Some(_)) => "teacher-only",
            (Some(_), None) => "parser-only",
        };
        *counts.entry(bucket.to_string()).or_insert(0usize) += 1;
        println!(
            "  {i:3} {bucket:16} parser={parser_boundary:?} teacher={t_line:?} conf={conf:?} kind={kind_name}"
        );
        let excerpt = {
            let line = t_line
                .or(parser_boundary)
                .map(|l| l + shown_from)
                .unwrap_or(0);
            let raw_lines: Vec<&str> = text.split('\n').collect();
            let from = line.saturating_sub(1);
            let to = (line + 3).min(raw_lines.len());
            raw_lines
                .get(from..to)
                .map(|ls| ls.join(" | "))
                .unwrap_or_default()
        };
        if let Some(w) = out_file.as_mut() {
            use std::io::Write;
            serde_json::to_writer(
                &mut *w,
                &json!({
                    "i": i,
                    "subject": m.get("subject").and_then(|v| v.as_str()).unwrap_or("").chars().take(60).collect::<String>(),
                    "parser_sig": parser_boundary,
                    "teacher_line": t_line,
                    "teacher_conf": conf,
                    "mail_kind": kind_name,
                    "excerpt": excerpt,
                }),
            )?;
            writeln!(&mut *w)?;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    if let Some(w) = out_file.as_mut() {
        use std::io::Write;
        w.flush()?;
    }

    println!("\n== signature split ({judged} judged, {skipped} skipped) ==");
    for (k, v) in &counts {
        println!("  {k:18} {v:3}");
    }
    let agree = counts.get("agree").copied().unwrap_or(0)
        + counts.get("agree-both-none").copied().unwrap_or(0);
    if judged > 0 {
        println!("  agreement: {agree}/{judged} = {}%", 100 * agree / judged);
    }
    println!("== teacher mail_kind ==");
    let mut kinds: Vec<_> = kinds.into_iter().collect();
    kinds.sort_by(|a, b| b.1.cmp(&a.1));
    for (k, v) in kinds {
        println!("  {k:22} {v:3}");
    }
    Ok(())
}
