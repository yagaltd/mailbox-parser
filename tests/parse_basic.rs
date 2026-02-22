use mailbox_parser::{EmailBlockKind, parse_rfc822, reply_text, segment_email_body};
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

#[test]
fn parse_multilingual_quote_headers() {
    let cases = [
        "On Tue, Jan 2, 2026 at 10:00 AM Alice <a@x> wrote:\n> quoted",
        "Le mar. 2 janv. 2026 à 10:00, Alice <a@x> a écrit :\n> cité",
        "El mar, 2 ene 2026 a las 10:00, Alice <a@x> escribió:\n> citado",
        "Am Di., 2. Jan. 2026 um 10:00 schrieb Alice <a@x>:\n> zitiert",
        "Il giorno 2 gen 2026, Alice <a@x> ha scritto:\n> citato",
        "Op di 2 jan 2026 schreef Alice <a@x>:\n> geciteerd",
        "W dniu 2 sty 2026 Alice <a@x> napisał:\n> cytat",
        "Den 2 jan 2026 skrev Alice <a@x>:\n> citerat",
    ];

    for case in cases {
        let text = format!("Reply body\n\n{}", case);
        let blocks = segment_email_body(&text);
        assert!(
            blocks
                .iter()
                .any(|b| b.kind == EmailBlockKind::Quoted || b.kind == EmailBlockKind::Forwarded),
            "expected quote/forward block for case: {case}"
        );
        let reply = reply_text(&text, &blocks);
        assert!(reply.contains("Reply body"));
        assert!(!reply.contains("quoted"));
    }
}

#[test]
fn parse_salutation_signature_and_disclaimer_blocks() {
    let text = "Hello Bob,\n\nPlease find update below.\n\nBest regards,\nAlice\n\nDisclaimer: This email is confidential and intended only for the recipient.\n\nOn Tue, Jan 2, 2026 at 10:00 AM Bob <bob@x> wrote:\n> old";
    let blocks = segment_email_body(text);

    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Salutation));
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Disclaimer));

    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Please find update below."));
    assert!(!reply.contains("Best regards"));
    assert!(!reply.contains("Disclaimer:"));
    assert!(!reply.contains("On Tue"));
}

#[test]
fn does_not_treat_hyphen_bullets_as_signature() {
    let text = "Hi team,\n\nAgenda:\n- item one\n- item two\n- item three\n\nLet me know if we should reorder.\n";
    let blocks = segment_email_body(text);
    assert!(!blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Agenda:"));
    assert!(reply.contains("- item one"));
}

#[test]
fn parse_reply_strips_de_outlook_header_bundle() {
    let text = "Hi Leonardo,\n\nDanke fuer das Update.\n\nFreundliche Grüße / Best regards\nBenedikt\n\nVon: EDGE TECHNOLOGIES <support@example.com>\nGesendet: Dienstag, 10. Februar 2026 07:16\nAn: Thomas <thomas@example.com>\nCc: Support <support@example.com>\nBetreff: Re: Thema\n\nHi Benedikt,\nold";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Danke fuer das Update."));
    assert!(!reply.contains("Von: EDGE TECHNOLOGIES"));
    assert!(!reply.contains("Betreff: Re: Thema"));
}

#[test]
fn parse_reply_strips_fr_outlook_header_bundle_with_colon_space() {
    let text = "Bonjour,\n\nMerci pour votre retour.\n\nCordialement,\nHerve\n\nDe : EDGE TECHNOLOGIES <support@example.com>\nEnvoyé : mardi 10 février 2026 02:55\nÀ : Thomas <thomas@example.com>\nCc : support@example.com\nObjet : Re: TR: VARIABLES DATE ET HEURE\n\nBonjour Hervé,\nancien";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Merci pour votre retour."));
    assert!(!reply.contains("De : EDGE TECHNOLOGIES"));
    assert!(!reply.contains("Objet : Re: TR: VARIABLES DATE ET HEURE"));
}

#[test]
fn parse_reply_strips_dashed_on_wrote_marker() {
    let text = "Reply body\n\n---- on Thu, 18 Dec 2025 16:40:44 +0700 Thomas <t@x> wrote ----\n> quoted";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Reply body"));
    assert!(!reply.contains("quoted"));
    assert!(!reply.contains("wrote ----"));
}

#[test]
fn parse_signature_detects_mixed_de_en_signoff() {
    let text = "Main message body.\n\nFreundliche Grüße / Best regards\nBenedikt Wilhelm\nPharma Technology Expert";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "Main message body.");
}

#[test]
fn parse_signature_detects_rgds_signoff() {
    let text = "Can we schedule this next week?\n\nRgds\nThomas";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "Can we schedule this next week?");
}

#[test]
fn parse_extracts_contact_hints_and_signature_entities() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Follow up\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Hi Bob,\n",
        "\n",
        "Please review the last update.\n",
        "\n",
        "Best regards,\n",
        "Anna Mueller\n",
        "Sales Manager\n",
        "Acme GmbH\n",
        "anna.mueller@example.de\n",
        "+49 30 1234 5678\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");

    assert!(
        parsed
            .contact_hints
            .iter()
            .any(|h| h.email.as_deref() == Some("alice@example.com"))
    );
    assert!(
        parsed
            .contact_hints
            .iter()
            .any(|h| h.email.as_deref() == Some("anna.mueller@example.de"))
    );
    assert!(
        parsed
            .signature_entities
            .emails
            .iter()
            .any(|e| e == "anna.mueller@example.de")
    );
    assert!(!parsed.signature_entities.is_partial);
}

#[test]
fn parse_attachment_hints_detect_inline_logo() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: logo\n",
        "MIME-Version: 1.0\n",
        "Content-Type: multipart/related; boundary=\"b\"\n",
        "\n",
        "--b\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "See logo.\n",
        "--b\n",
        "Content-Type: image/png\n",
        "Content-Disposition: inline; filename=\"company-logo.png\"\n",
        "Content-ID: <logo1@cid>\n",
        "Content-Transfer-Encoding: base64\n",
        "\n",
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/",
        "PjB6+QAAAABJRU5ErkJggg==\n",
        "--b--\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.attachment_hints.len(), 1);
    let hint = &parsed.attachment_hints[0];
    assert!(hint.is_inline);
    assert!(hint.is_probable_logo);
}

#[test]
fn parse_event_hints_detect_complete_meeting() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Meeting planning\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Let's meet on 2026-03-01 10:30 CET.\n",
        "Join via https://zoom.us/j/123456\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.event_hints.len(), 1);
    let event = &parsed.event_hints[0];
    assert!(event.is_complete);
    assert!(event.missing_fields.is_empty());
}

#[test]
fn parse_signature_moves_kind_regards_out_of_reply_before_dash_separator() {
    let text = "We have conducted a training session.\nKind regards\n--\nNicolas Guillou\nCEO\nnicolas@example.com";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "We have conducted a training session.");
}

#[test]
fn parse_signature_detects_long_tail_contact_card() {
    let text = "Thanks for the update.\n\nBest regards,\nParsa\nAlways here to help,\n[image]\nParsa Zali\nInstrumentation Technician- Shop & Logistics\nMobile +1 905 699-9703\nEmail parsa@currentinstrument.com\nwww.currentinstrument.com\nCurrent Instrumentation & Automation Inc.\n680 Tradewind Dr., Unit 11, Hamilton, ON, Canada L9G 4V5";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "Thanks for the update.");
}

#[test]
fn parse_event_hints_ignores_plain_teams_word_without_meeting_url() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Questions related to Sensa devices\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "few questions came in from the BSP technical teams.\n",
        "We need official feedback from vendor.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.event_hints.is_empty());
}

#[test]
fn parse_event_hints_detects_date_range_with_month() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Site visit planning\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Does 16-18 April or 23-25 April work for you?\n",
        "We can meet on site in Athens.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.event_hints.len(), 1);
    let event = &parsed.event_hints[0];
    assert!(
        event
            .datetime_candidates
            .iter()
            .any(|d| d.raw.contains("16-18 April"))
    );
}
