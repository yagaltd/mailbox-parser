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
    MailMessage, MboxReadOptions, ParseRfc822Options, canonicalize_threads, iter_mbox_messages,
    parse_rfc822_with_options, thread_messages_from_mail_messages,
};

/// Streaming stride sample: parse only kept messages, bound memory
/// (shared by `refine` and `signals`).
fn sample_messages(path: &Path, stride: usize, limit: usize) -> Result<Vec<MailMessage>> {
    let mut messages = Vec::new();
    let iter = iter_mbox_messages(path, MboxReadOptions { strict: false, max_messages: None })
        .with_context(|| format!("open mbox {}", path.display()))?;
    for (idx, item) in iter.enumerate() {
        if idx % stride != 0 {
            continue;
        }
        if messages.len() >= limit {
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
        messages.push(MailMessage {
            uid: None,
            internal_date: parsed.date.clone().or(msg.separator_date.clone()),
            flags: Vec::new(),
            parsed,
            raw: Vec::new(),
        });
    }
    Ok(messages)
}

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
    /// Full signal battery per message (one batched call): embed-worthiness,
    /// kind, topics, sentiment, urgency, action, commitment, churn risk,
    /// language. Writes signals JSONL + summary + thread timelines.
    Signals(SignalsArgs),
}

#[derive(Clone, Debug, Parser)]
pub struct SignalsArgs {
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
    /// Write per-message signals as JSONL
    #[arg(long, default_value = "/tmp/typesafe-signals.jsonl")]
    pub out: PathBuf,
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
        TypesafeCommand::Signals(a) => signals(a),
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
    let messages = sample_messages(&args.path, stride, args.limit)?;
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

// ─── Signal battery (one batched call per message) ────────────────────

const TOPIC_LABELS: &[(&str, &str)] = &[
    ("billing_payment", "billing, payments, refunds, subscriptions, pricing"),
    ("technical_issue", "a technical problem, bug, error, or troubleshooting"),
    ("account_access", "login, password, account access, account settings"),
    ("orders_shipping", "orders, deliveries, shipping, returns, product logistics"),
    ("product_feedback", "feedback, requests, opinions about a product or service"),
    ("scheduling_appointments", "scheduling, appointments, meetings, class or session times"),
    ("content_education", "substantive content: articles, lessons, instructions, newsletters"),
    ("general_social", "personal or social conversation with no concrete topic"),
];

const LANGUAGES: &[&str] = &[
    "english", "french", "german", "spanish", "italian", "greek", "turkish", "indonesian", "other",
];

fn build_signals_payload(m: &Value, model: &str) -> Value {
    let body: String = m
        .get("reply_text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .chars()
        .take(4000)
        .collect();
    let from: Vec<Value> = m
        .get("from")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .take(2)
                .map(|f| {
                    if f.is_string() {
                        f.clone()
                    } else {
                        json!(format!(
                            "{} <{}>",
                            f.get("name").and_then(|x| x.as_str()).unwrap_or(""),
                            f.get("address").and_then(|x| x.as_str()).unwrap_or("")
                        ))
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let mut kind_criteria = serde_json::Map::new();
    for (k, d) in MAIL_KINDS {
        kind_criteria.insert((*k).into(), Value::String((*d).into()));
    }
    let actions = [
        ("none", "no action is requested"),
        ("reply_needed", "a reply or answer is requested"),
        ("meeting_or_call", "a meeting, call, or appointment is requested"),
        ("payment_or_purchase", "a payment, renewal, or purchase is requested"),
        ("approval_or_signoff", "an approval, sign-off, or confirmation is requested"),
        ("deadline_or_submission", "something must be submitted or delivered by a deadline"),
    ];
    let mut action_criteria = serde_json::Map::new();
    for (k, d) in actions {
        action_criteria.insert(k.into(), Value::String(d.into()));
    }

    let mut questions = serde_json::Map::new();
    questions.insert(
        "embed_worthy".into(),
        json!({
            "type": "score",
            "instructions": "Would embedding this message's body text in a retrieval index help someone answer a future question? Judge the body text given as state.",
            "criteria": [
                "not worth embedding: boilerplate, automated notification, quoted noise, or no reusable information",
                "borderline: some substance but mostly redundant or trivial",
                "worth embedding: contains information a person would want to retrieve later (decisions, facts, answers, instructions)"
            ]
        }),
    );
    questions.insert(
        "mail_kind".into(),
        json!({
            "type": "choice",
            "instructions": "Classify this email message by its true kind, regardless of language.",
            "criteria": Value::Object(kind_criteria)
        }),
    );
    questions.insert(
        "sentiment".into(),
        json!({
            "type": "score",
            "instructions": "What is the emotional tone of the message body toward the product/service or its recipient?",
            "criteria": [
                "angry or frustrated: complaints, threats to cancel, harsh tone",
                "mildly negative: dissatisfaction, concern, problems reported",
                "neutral: factual or transactional, no emotional tone",
                "positive: satisfied, thankful, enthusiastic"
            ]
        }),
    );
    questions.insert(
        "urgency".into(),
        json!({
            "type": "score",
            "instructions": "How much time pressure does this message carry?",
            "criteria": [
                "low: no time pressure",
                "medium: a response or action is expected soon",
                "high: explicit deadline, urgent language, or time-critical problem"
            ]
        }),
    );
    questions.insert(
        "action_requested".into(),
        json!({
            "type": "choice",
            "instructions": "What, if anything, does this message ask its recipient to do?",
            "criteria": Value::Object(action_criteria)
        }),
    );
    questions.insert(
        "commitment_made".into(),
        json!({
            "type": "noul",
            "instructions": "Does the sender commit to doing something specific in this message (deliver, send, fix, follow up)?",
            "criteria": {
                "true": "the sender makes a concrete commitment to a future action",
                "false": "no commitment is made"
            }
        }),
    );
    questions.insert(
        "churn_risk".into(),
        json!({
            "type": "noul",
            "instructions": "Does this message express intent to cancel, dissatisfaction with the product, or frustration that could lead to losing the customer?",
            "criteria": {
                "true": "cancellation intent or serious frustration is expressed",
                "false": "no churn signal"
            }
        }),
    );
    questions.insert(
        "language".into(),
        json!({
            "type": "choice",
            "instructions": "What language is the message body written in?",
            "criteria": LANGUAGES
                .iter()
                .map(|l| ((*l).to_string(), Value::String(format!("the body is primarily {l}"))))
                .collect::<serde_json::Map<String, Value>>()
        }),
    );
    for (label, desc) in TOPIC_LABELS {
        questions.insert(
            format!("topic_{label}"),
            json!({
                "type": "noul",
                "instructions": format!("Does this email substantively discuss {desc}? Judge only from the message content."),
                "criteria": {
                    "true": "the message genuinely discusses this topic beyond a passing mention",
                    "false": "the topic is absent or only mentioned in passing"
                }
            }),
        );
    }

    json!({
        "state": {
            "subject": m.get("subject").and_then(|v| v.as_str()).unwrap_or(""),
            "from": from,
            "date": m.get("date").and_then(|v| v.as_str()).unwrap_or(""),
            "body": body,
        },
        "model": model,
        "questions": Value::Object(questions)
    })
}

fn signals(args: SignalsArgs) -> Result<()> {
    let key = load_key(&args.key_file)?;
    let messages = sample_messages(&args.path, args.stride.max(1), args.limit)?;
    eprintln!("sampled {} messages", messages.len());

    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msgs: Vec<Value> = canonical
        .iter()
        .flat_map(|t| {
            let tid = json!(t.thread_id);
            t.messages.iter().map(move |m| {
                let mut v = serde_json::to_value(m).unwrap_or(Value::Null);
                if let Some(o) = v.as_object_mut() {
                    o.insert("thread_id".into(), tid.clone());
                }
                v
            })
        })
        .filter(|v| {
            v.get("reply_text")
                .and_then(|r| r.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        })
        .collect();
    eprintln!("{} messages with body; running signal battery…", msgs.len());

    let mut out = std::io::BufWriter::new(
        std::fs::File::create(&args.out).with_context(|| format!("create {}", args.out.display()))?,
    );
    use std::io::Write;

    let mut records: Vec<Value> = Vec::new();
    for (i, m) in msgs.iter().enumerate() {
        let payload = build_signals_payload(m, &args.model);
        let resp = match post(&payload, &key) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  {i:3} API error: {e}");
                continue;
            }
        };
        let answers = &resp["answers"];
        let get_choice = |k: &str| -> (String, f64) {
            (
                answers[k]["choice"].as_str().unwrap_or("?").to_string(),
                answers[k]["confidence"].as_f64().unwrap_or(0.0),
            )
        };
        let get_score = |k: &str| -> (f64, f64) {
            (
                answers[k]["score"].as_f64().unwrap_or(0.0),
                answers[k]["confidence"].as_f64().unwrap_or(0.0),
            )
        };
        let get_noul = |k: &str| -> f64 { answers[k]["noul"].as_f64().unwrap_or(0.0) };

        let mut topics = serde_json::Map::new();
        let mut top_topics: Vec<String> = Vec::new();
        for (label, _) in TOPIC_LABELS {
            let q = format!("topic_{label}");
            let n = get_noul(&q);
            topics.insert(label.to_string(), json!(n));
            if n >= 0.6 {
                top_topics.push(label.to_string());
            }
        }
        let (kind, kind_conf) = get_choice("mail_kind");
        let (sent, sent_conf) = get_score("sentiment");
        let (urg, _) = get_score("urgency");
        let (action, _) = get_choice("action_requested");
        let churn = get_noul("churn_risk");
        let commit = get_noul("commitment_made");
        let (lang, _) = get_choice("language");
        let (embed, _) = get_score("embed_worthy");

        println!(
            "  {i:3} {kind:20} sent={sent:.1} urg={urg:.1} embed={embed:.1} act={action:20} commit={commit:.2} churn={churn:.2} lang={lang:9} topics={}",
            top_topics.join(",")
        );

        let rec = json!({
            "i": i,
            "thread_id": m.get("thread_id").and_then(|v| v.as_str()).unwrap_or(""),
            "date": m.get("date").and_then(|v| v.as_str()).unwrap_or(""),
            "subject": m.get("subject").and_then(|v| v.as_str()).unwrap_or("").chars().take(60).collect::<String>(),
            "embed_worthy": embed,
            "mail_kind": {"value": kind, "confidence": kind_conf},
            "sentiment": {"score": sent, "confidence": sent_conf},
            "urgency": urg,
            "action_requested": action,
            "commitment_made": commit,
            "churn_risk": churn,
            "language": lang,
            "topics": Value::Object(topics),
        });
        records.push(rec.clone());
        serde_json::to_writer(&mut out, &rec)?;
        writeln!(&mut out)?;
        std::thread::sleep(Duration::from_millis(300));
    }
    out.flush()?;

    // ── Summary rollups (pure code, no API) ──
    let n = records.len();
    println!("\n== signal summary ({n} messages) ==");
    let mut hist = |key: &str| {
        let mut c = std::collections::BTreeMap::new();
        for r in &records {
            let v = r[key].clone();
            let v = if v.is_object() {
                v["value"].clone().is_null().then(|| v["score"].clone()).unwrap_or(v["value"].clone())
            } else {
                v
            };
            let v = if v.is_string() {
                v
            } else {
                json!(format!("{:.1}", v.as_f64().unwrap_or(0.0)))
            };
            *c.entry(v.as_str().unwrap_or("?").to_string()).or_insert(0usize) += 1;
        }
        let mut c: Vec<_> = c.into_iter().collect();
        c.sort_by(|a, b| b.1.cmp(&a.1));
        let total: usize = c.iter().map(|(_, v)| v).sum();
        for (k, v) in c.iter().take(6) {
            println!("  {key:16} {k:22} {v:3} ({}%)", 100 * v / total.max(1));
        }
    };
    for k in ["mail_kind", "sentiment", "urgency", "action_requested", "language", "embed_worthy"] {
        hist(k);
    }
    let churny = records.iter().filter(|r| r["churn_risk"].as_f64().unwrap_or(0.0) >= 0.6).count();
    let committing = records.iter().filter(|r| r["commitment_made"].as_f64().unwrap_or(0.0) >= 0.6).count();
    let embed_worthy = records.iter().filter(|r| r["embed_worthy"].as_f64().unwrap_or(0.0) >= 1.5).count();
    println!("  churn_risk>=0.6      {churny:3}");
    println!("  commitment>=0.6      {committing:3}");
    println!("  embed_worthy>=1.5    {embed_worthy:3} ({}/{n} = {}% of index)", embed_worthy, 100 * embed_worthy / n.max(1));

    // ── Thread timelines: sentiment/topic evolution, computed locally ──
    let mut by_thread: std::collections::HashMap<String, Vec<&Value>> = std::collections::HashMap::new();
    for r in &records {
        let t = r["thread_id"].as_str().unwrap_or("").to_string();
        if !t.is_empty() {
            by_thread.entry(t).or_default().push(r);
        }
    }
    let mut long_threads: Vec<_> = by_thread.into_values().filter(|v| v.len() >= 3).collect();
    long_threads.sort_by_key(|v| std::cmp::Reverse(v.len()));
    println!("\n== thread timelines ({} threads with >=3 sampled messages) ==", long_threads.len());
    for t in long_threads.iter().take(2) {
        let mut t: Vec<&&Value> = t.iter().collect();
        t.sort_by(|a, b| a["date"].as_str().unwrap_or("").cmp(b["date"].as_str().unwrap_or("")));
        println!("  thread {} ({} msgs)", t[0]["thread_id"].as_str().unwrap_or("?"), t.len());
        for r in t {
            let topics: Vec<String> = r["topics"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter(|(_, v)| v.as_f64().unwrap_or(0.0) >= 0.6)
                        .map(|(k, _)| k.clone())
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "    {} sent={:.1} urg={:.1} churn={:.2} kind={:18} topics={}",
                r["date"].as_str().unwrap_or("?").chars().take(10).collect::<String>(),
                r["sentiment"]["score"].as_f64().unwrap_or(0.0),
                r["urgency"].as_f64().unwrap_or(0.0),
                r["churn_risk"].as_f64().unwrap_or(0.0),
                r["mail_kind"]["value"].as_str().unwrap_or("?"),
                topics.join(",")
            );
        }
    }
    println!("\nsignals JSONL: {}", args.out.display());
    Ok(())
}
