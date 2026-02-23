use mailbox_parser::{MboxParseOptions, parse_mbox_file, thread_messages_from_mail_messages};
use pretty_assertions::assert_eq;

fn parse_fixture(path: &str, options: MboxParseOptions) -> mailbox_parser::MboxParseReport {
    parse_mbox_file(std::path::Path::new(path), options).expect("parse mbox")
}

#[test]
fn parse_mbox_splits_messages_and_threads() {
    let report = parse_fixture(
        "tests/fixtures/two_messages_simple.mbox",
        MboxParseOptions::default(),
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.messages.len(), 2);
    assert_eq!(
        report.messages[0].parsed.subject.as_deref(),
        Some("Hello Bob")
    );
    assert_eq!(
        report.messages[1].parsed.in_reply_to.as_deref(),
        Some("msg-1@example.com")
    );

    let threads = thread_messages_from_mail_messages(&report.messages);
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].messages.len(), 2);
}

#[test]
fn mboxrd_unescapes_from_lines_in_body() {
    let report = parse_fixture(
        "tests/fixtures/mboxrd_escaped_from.mbox",
        MboxParseOptions::default(),
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.messages.len(), 1);

    let body = report.messages[0].parsed.body_text.as_deref().unwrap_or("");
    assert!(body.contains("From escaped line"));
    assert!(body.contains(">From double escaped"));
}

#[test]
fn strict_mode_avoids_false_splits() {
    let report = parse_fixture(
        "tests/fixtures/false_positive_from_line.mbox",
        MboxParseOptions {
            strict: true,
            max_messages: None,
            fail_fast: false,
            owner_emails: Vec::new(),
            lifecycle_lexicon: None,
        },
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.messages.len(), 1);

    let body = report.messages[0].parsed.body_text.as_deref().unwrap_or("");
    assert!(body.contains("From not a separator line"));
}

#[test]
fn mbox_attachments_survive_split_and_parse() {
    let report = parse_fixture(
        "tests/fixtures/multipart_with_attachment.mbox",
        MboxParseOptions::default(),
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.messages.len(), 1);

    let msg = &report.messages[0];
    assert_eq!(msg.parsed.attachments.len(), 1);
    assert_eq!(
        msg.parsed.attachments[0].filename.as_deref(),
        Some("notes.txt")
    );
    assert!(msg.parsed.attachments[0].size > 0);
}
