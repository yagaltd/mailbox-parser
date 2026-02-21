use mailbox_parser::{parse_rfc822, reply_text, segment_email_body};
use pretty_assertions::assert_eq;

fn fixture(path: &str) -> Vec<u8> {
    std::fs::read(path).expect("read fixture")
}

#[test]
fn parse_basic_email() {
    let bytes = fixture("tests/fixtures/basic.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    assert_eq!(parsed.message_id.as_deref(), Some("msg-1@example.com"));
    assert_eq!(parsed.in_reply_to.as_deref(), Some("msg-0@example.com"));
    assert_eq!(parsed.references.len(), 2);
    assert_eq!(parsed.subject.as_deref(), Some("Hello Bob"));
    assert_eq!(parsed.from.len(), 1);
    assert_eq!(parsed.from[0].address, "alice@example.com");
    assert_eq!(parsed.from[0].name.as_deref(), Some("Alice"));
    assert!(parsed.body_canonical.contains("Hello Bob"));
}

#[test]
fn parse_forward_and_attachment() {
    let bytes = fixture("tests/fixtures/with_forward.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    assert_eq!(parsed.forwarded_messages.len(), 1);
    assert_eq!(
        parsed.forwarded_messages[0].message_id.as_deref(),
        Some("inner-1@example.com")
    );
    assert_eq!(parsed.attachments.len(), 1);
    assert_eq!(parsed.attachments[0].filename.as_deref(), Some("notes.txt"));
    assert_eq!(parsed.attachments[0].size, 16);
}

#[test]
fn parse_reply_strips_quoted_history() {
    let bytes = fixture("tests/fixtures/with_quote.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    let blocks = segment_email_body(&parsed.body_canonical);
    let reply = reply_text(&parsed.body_canonical, &blocks);
    assert!(reply.contains("Sure, sounds good."));
    assert!(!reply.contains("QUOTED_TOKEN_123"));
}

#[test]
fn thread_fallback_groups_by_subject_and_participants_when_headers_missing() {
    use mailbox_parser::{SyncedEmail, thread_messages};

    let msg1 = b"Subject: Hello Bob\nFrom: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nDate: Tue, 20 Jan 2026 12:34:56 +0000\nContent-Type: text/plain; charset=utf-8\n\nFirst\n";
    let msg2 = b"Subject: Re: Hello Bob\nFrom: Bob <bob@example.com>\nTo: Alice <alice@example.com>\nDate: Tue, 21 Jan 2026 12:34:56 +0000\nContent-Type: text/plain; charset=utf-8\n\nSecond\n";

    let parsed1 = parse_rfc822(msg1).expect("parse1");
    let parsed2 = parse_rfc822(msg2).expect("parse2");

    assert!(parsed1.message_id.is_none());
    assert!(parsed2.message_id.is_none());

    let synced = vec![
        SyncedEmail {
            uid: 1,
            internal_date: None,
            flags: vec![],
            modseq: None,
            rfc822_size: None,
            parsed: parsed1,
            raw: msg1.to_vec(),
        },
        SyncedEmail {
            uid: 2,
            internal_date: None,
            flags: vec![],
            modseq: None,
            rfc822_size: None,
            parsed: parsed2,
            raw: msg2.to_vec(),
        },
    ];

    let threads = thread_messages(&synced);
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].messages.len(), 2);
}

#[test]
fn parse_reply_strips_outlook_from_sent_quote_block() {
    let bytes = fixture("tests/fixtures/with_from_sent_quote.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    let blocks = segment_email_body(&parsed.body_canonical);
    let reply = reply_text(&parsed.body_canonical, &blocks);
    assert!(reply.contains("REPLY_ONLY_TOKEN_XYZ"));
    assert!(!reply.contains("QUOTED_TOKEN_FROM_SENT_456"));
}

#[test]
fn parse_inline_cid_attachment() {
    let bytes = fixture("tests/fixtures/inline_cid.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    let image = parsed
        .attachments
        .iter()
        .find(|att| att.mime_type == "image/png")
        .expect("expected inline image attachment");
    assert_eq!(image.content_id.as_deref(), Some("image1@cid"));
    assert!(image.size > 0);
}

#[test]
fn parse_inline_forwarded_message_id_from_body() {
    let bytes = fixture("tests/fixtures/inline_forward_body.eml");
    let parsed = parse_rfc822(&bytes).expect("parse");

    assert!(
        parsed
            .forwarded_messages
            .iter()
            .any(|f| f.message_id.as_deref() == Some("inner-forward-123@example.com"))
    );
}

#[test]
fn normalize_email_text_strips_common_unicode_junk() {
    let msg = "Subject: Hi\nFrom: A <a@x>\nTo: B <b@x>\nDate: Tue, 20 Jan 2026 12:34:56 +0000\nContent-Type: text/plain; charset=utf-8\n\nHello\u{034F}\u{200C}\u{2007}\u{FEFF}\u{00A0}World\n";
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.body_canonical.contains("Hello World"));
    assert!(!parsed.body_canonical.contains('\u{034F}'));
    assert!(!parsed.body_canonical.contains('\u{200C}'));
    assert!(!parsed.body_canonical.contains('\u{FEFF}'));
}
