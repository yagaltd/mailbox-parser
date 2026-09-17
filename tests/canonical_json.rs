//! Canonical JSON pass-through tests — verify that body fields and raw_headers
//! survive the `ParsedEmail` → `CanonicalMessage` round-trip.

use mailbox_parser::{
    MailMessage, ParseRfc822Options, canonicalize_threads, parse_rfc822_with_options,
    thread_messages_from_mail_messages,
};

/// Automated mail must not produce a `signature` field: its trailing blocks
/// are template boilerplate (probe round 1, bucket A — LinkedIn blurbs, bank
/// footers). The text stays in reply_text.
#[test]
fn canonical_automated_mail_has_no_signature() {
    let raw = b"From: Job Alerts <jobalerts-noreply@linkedin.example.com>\nTo: A <a@example.org>\nSubject: Your job alert\nDate: Tue, 20 Jan 2026 12:34:56 +0000\nList-Unsubscribe: <https://example.com/unsub>\nContent-Type: text/plain\n\nYour job alert for chief operating officer in Jakarta\n\nHead of Stunting Program and Operations\nEdufarmers International\nJakarta\n\nThis company is actively hiring\nApply with resume & profile\nView job: https://www.example.com/jobs/view/4XG2552\n";
    let parsed = parse_rfc822_with_options(raw, &ParseRfc822Options::default()).expect("parse");
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
        msg.signature.is_none(),
        "automated mail must not yield a signature, got: {:?}",
        msg.signature
    );
    assert!(
        msg.reply_text.contains("Edufarmers International"),
        "demoted signature text must stay in reply_text"
    );
}

/// Verify all four new canonical fields are populated from a basic .eml fixture.
#[test]
fn canonical_message_passes_body_and_headers() {
    let bytes = include_bytes!("fixtures/basic.eml");
    let parsed = parse_rfc822_with_options(
        bytes,
        &ParseRfc822Options {
            keep_body_html: true,
            ..Default::default()
        },
    )
    .expect("parse basic.eml");

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

    // body_text or body_html should be present (basic.eml is text/plain)
    assert!(
        msg.body_text.is_some() || msg.body_html.is_some(),
        "either body_text or body_html should be present"
    );
    // body_canonical is always populated
    assert!(
        msg.body_canonical.as_deref().is_some_and(|s| !s.is_empty()),
        "body_canonical should be non-empty"
    );
    // raw_headers should contain standard headers
    assert!(
        msg.raw_headers
            .as_ref()
            .is_some_and(|h| h.contains_key("message-id") || h.contains_key("from")),
        "raw_headers should contain message-id or from"
    );
}

/// Verify that body_html is absent by default and retained when keep_body_html is true.
#[test]
fn canonical_body_html_opt_in() {
    let bytes = include_bytes!("fixtures/inline_cid.eml");

    // Parse WITHOUT keep_body_html.
    let parsed_no_html = parse_rfc822_with_options(bytes, &ParseRfc822Options::default())
        .expect("parse without keep_body_html");
    let messages = vec![MailMessage {
        uid: None,
        internal_date: parsed_no_html.date.clone(),
        flags: vec![],
        parsed: parsed_no_html,
        raw: vec![],
    }];
    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msg = &canonical[0].messages[0];
    assert!(
        msg.body_html.is_none(),
        "body_html should be None when keep_body_html is false"
    );

    // Parse WITH keep_body_html using an actual HTML fixture.
    let parsed_with_html = parse_rfc822_with_options(
        bytes,
        &ParseRfc822Options {
            keep_body_html: true,
            ..Default::default()
        },
    )
    .expect("parse with keep_body_html");
    let messages = vec![MailMessage {
        uid: None,
        internal_date: parsed_with_html.date.clone(),
        flags: vec![],
        parsed: parsed_with_html,
        raw: vec![],
    }];
    let threads = thread_messages_from_mail_messages(&messages);
    let canonical = canonicalize_threads(&threads);
    let msg = &canonical[0].messages[0];
    assert!(
        msg.body_html
            .as_deref()
            .is_some_and(|html| html.contains("<html") && html.contains("cid:image1@cid")),
        "body_html should preserve the original HTML body when keep_body_html is true"
    );
}

/// Verify that old JSON (without new fields) deserializes correctly.
#[test]
fn canonical_message_backward_compat_deserialize() {
    let old_json = r#"{
        "message_key": "k1",
        "uid": null,
        "internal_date": null,
        "flags": [],
        "x_gm_thrid": null,
        "x_gm_labels": [],
        "message_id": "m1@example.com",
        "in_reply_to": null,
        "references": [],
        "subject": "Hello",
        "date": "2026-01-01T00:00:00Z",
        "date_raw": null,
        "from": [{"name": "Alice", "address": "alice@example.com"}],
        "to": [],
        "cc": [],
        "bcc": [],
        "reply_to": [],
        "reply_text": "Hello Bob",
        "quoted_blocks": [],
        "forwarded_blocks": [],
        "disclaimer_blocks": [],
        "salutation": null,
        "signature": null,
        "attachments": [],
        "contact_hints": [],
        "signature_entities": {"emails": [], "phones": [], "urls": [], "org": null, "title": null, "address_lines": [], "is_partial": false},
        "attachment_hints": [],
        "event_hints": [],
        "mail_kind_hints": [],
        "direction_hint": null,
        "unsubscribe_hints": [],
        "service_lifecycle_hints": [],
        "billing_action_hints": [],
        "sender_domain_hint": null,
        "participant_domain_hints": [],
        "forwarded_messages": [],
        "forwarded_segments": []
    }"#;

    let msg: mailbox_parser::CanonicalMessage =
        serde_json::from_str(old_json).expect("old JSON should deserialize");
    assert_eq!(msg.message_key, "k1");
    assert_eq!(msg.subject.as_deref(), Some("Hello"));
    // New fields should default to None
    assert!(msg.body_text.is_none());
    assert!(msg.body_html.is_none());
    assert!(msg.body_canonical.is_none());
    assert!(msg.raw_headers.is_none());
}

/// Verify --strip-raw-headers behavior: raw_headers removed, other fields preserved.
#[test]
fn canonical_strip_raw_headers_removes_map() {
    let bytes = include_bytes!("fixtures/basic.eml");
    let parsed = parse_rfc822_with_options(bytes, &ParseRfc822Options::default()).expect("parse");

    let messages = vec![MailMessage {
        uid: None,
        internal_date: parsed.date.clone(),
        flags: vec![],
        parsed,
        raw: vec![],
    }];
    let threads = thread_messages_from_mail_messages(&messages);
    let mut canonical = canonicalize_threads(&threads);

    // Before stripping: raw_headers should be present
    assert!(canonical[0].messages[0].raw_headers.is_some());
    assert!(canonical[0].messages[0].body_canonical.is_some());

    // Apply strip
    for m in &mut canonical[0].messages {
        m.raw_headers = None;
    }

    // After stripping: raw_headers is None, other data preserved
    assert!(canonical[0].messages[0].raw_headers.is_none());
    assert!(canonical[0].messages[0].body_canonical.is_some());
    assert!(canonical[0].messages[0].message_id.is_some());
}
