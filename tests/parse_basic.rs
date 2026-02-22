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
