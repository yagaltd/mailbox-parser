//! Local E2E canonical JSON tests — run manually with:
//!   MAILBOX_PARSER_E2E_FIXTURES=/home/aurel/Documents/current/CognitiveOS/test-files \
//!     cargo test --all-features -- --ignored
//!
//! Requires local fixture files: sample.mbox, sample.pst, simple.eml, FWM-CS.mbox

use mailbox_parser::{
    MailMessage, MboxParseOptions, ParseRfc822Options, canonicalize_threads, parse_mbox_file,
    parse_rfc822_with_options, thread_messages_from_mail_messages,
};
use std::path::Path;

fn fixture_dir() -> Option<&'static Path> {
    let val = std::env::var("MAILBOX_PARSER_E2E_FIXTURES").ok()?;
    let leaked: &'static str = Box::leak(val.into_boxed_str());
    Some(Path::new(leaked))
}

#[test]
#[ignore]
fn e2e_mbox_canonical_has_body_and_headers() {
    let base = fixture_dir().expect("MAILBOX_PARSER_E2E_FIXTURES not set");
    let mbox_path = base.join("sample.mbox");
    if !mbox_path.exists() {
        eprintln!("Skipping: sample.mbox not found at {}", mbox_path.display());
        return;
    }

    let report = parse_mbox_file(
        &mbox_path,
        MboxParseOptions {
            strict: false,
            max_messages: Some(10),
            fail_fast: false,
            owner_emails: vec![],
            lifecycle_lexicon: None,
            keep_raw: false,
            keep_body_html: true,
            keep_attachment_bytes: false,
        },
    )
    .expect("parse sample.mbox");

    eprintln!("Parsed {} messages from sample.mbox", report.messages.len());

    let messages: Vec<MailMessage> = report
        .messages
        .into_iter()
        .map(|m| MailMessage {
            uid: None,
            internal_date: None,
            flags: vec![],
            parsed: m.parsed,
            raw: vec![],
        })
        .collect();

    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);

    let mut total = 0;
    let mut with_headers = 0;
    let mut with_canonical = 0;

    for thread in &canonical {
        for msg in &thread.messages {
            total += 1;
            if msg.raw_headers.is_some() {
                with_headers += 1;
            }
            if msg.body_canonical.is_some() {
                with_canonical += 1;
            }
        }
    }

    eprintln!("mbox: total={total} with_headers={with_headers} with_canonical={with_canonical}");
    assert!(
        with_headers > 0,
        "at least one message should have raw_headers"
    );
    assert!(
        with_canonical > 0,
        "at least one message should have body_canonical"
    );
}

#[test]
#[ignore]
fn e2e_eml_canonical_has_headers() {
    let base = fixture_dir().expect("MAILBOX_PARSER_E2E_FIXTURES not set");
    let eml_path = base.join("simple.eml");
    if !eml_path.exists() {
        eprintln!("Skipping: simple.eml not found at {}", eml_path.display());
        return;
    }

    let bytes = std::fs::read(&eml_path).expect("read simple.eml");
    let parsed = parse_rfc822_with_options(
        &bytes,
        &ParseRfc822Options {
            keep_body_html: true,
            ..Default::default()
        },
    )
    .expect("parse simple.eml");

    let messages = vec![MailMessage {
        uid: None,
        internal_date: parsed.date.clone(),
        flags: vec![],
        parsed,
        raw: vec![],
    }];
    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msg = &canonical[0].messages[0];

    assert!(
        msg.raw_headers.is_some(),
        "EML: raw_headers should be present"
    );
    assert!(
        msg.body_canonical.is_some(),
        "EML: body_canonical should be present"
    );
    assert!(
        msg.raw_headers
            .as_ref()
            .is_some_and(|h| h.contains_key("from") || h.contains_key("subject")),
        "EML: raw_headers should contain standard headers"
    );
}

#[test]
#[ignore]
fn e2e_large_mbox_stress() {
    let base = fixture_dir().expect("MAILBOX_PARSER_E2E_FIXTURES not set");
    let mbox_path = base.join("FWM-CS.mbox");
    if !mbox_path.exists() {
        eprintln!("Skipping: FWM-CS.mbox not found");
        return;
    }

    eprintln!("Parsing large mbox (may take a while)...");
    let report = parse_mbox_file(
        &mbox_path,
        MboxParseOptions {
            strict: false,
            max_messages: Some(100),
            fail_fast: false,
            owner_emails: vec![],
            lifecycle_lexicon: None,
            keep_raw: false,
            keep_body_html: true,
            keep_attachment_bytes: false,
        },
    )
    .expect("parse FWM-CS.mbox");

    eprintln!("Parsed {} messages", report.messages.len());

    let messages: Vec<MailMessage> = report
        .messages
        .into_iter()
        .map(|m| MailMessage {
            uid: None,
            internal_date: None,
            flags: vec![],
            parsed: m.parsed,
            raw: vec![],
        })
        .collect();

    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);

    let mut with_html = 0usize;
    let mut with_headers = 0usize;
    for thread in &canonical {
        for msg in &thread.messages {
            if msg.body_html.is_some() {
                with_html += 1;
            }
            if msg.raw_headers.is_some() {
                with_headers += 1;
            }
        }
    }

    eprintln!("with_body_html={with_html} with_raw_headers={with_headers}");
    assert!(with_headers > 0);
    // body_html may be 0 if all messages are text/plain
}

#[cfg(feature = "msg-parser")]
#[test]
#[ignore]
fn e2e_msg_canonical_has_body_and_headers() {
    use mailbox_parser::parse_msg;

    let bytes = include_bytes!("fixtures/ascii.msg");
    let result = parse_msg(bytes).expect("parse ascii.msg");
    let parsed = parse_rfc822_with_options(&result.rfc822, &ParseRfc822Options::default())
        .expect("re-parse");

    let messages = vec![MailMessage {
        uid: None,
        internal_date: None,
        flags: vec![],
        parsed,
        raw: vec![],
    }];
    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msg = &canonical[0].messages[0];

    assert!(
        msg.raw_headers.is_some(),
        "MSG: raw_headers should be present"
    );
    assert!(
        msg.body_canonical.is_some(),
        "MSG: body_canonical should be present"
    );
}
