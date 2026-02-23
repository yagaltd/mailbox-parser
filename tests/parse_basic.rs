use mailbox_parser::{
    BillingActionKind, EmailBlockKind, MailDirection, MailKind, ParseRfc822Options,
    ServiceLifecycleKind, UnsubscribeKind, UnsubscribeSource, parse_rfc822,
    parse_rfc822_with_options, reply_text, segment_email_body,
};
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
fn parse_forwarded_segment_extracts_headers_and_clean_reply() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Fwd: Reporting Interval\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "FYI\n\n",
        "---- Forwarded message ----\n",
        "From: Henry Lynam <henry@minfarmtech.com>\n",
        "To: Thomas GUILLET <thomas.guillet@edgetech.fr>\n",
        "Date: Fri, 18 Apr 2025 18:40:53 +0800\n",
        "Subject: Re: Reporting Interval\n",
        "\n",
        "Hi Thomas,\n\n",
        "Thanks for the reply.\n\n",
        "Kind regards,\n",
        "Henry\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.forwarded_segments.len(), 1);
    let seg = &parsed.forwarded_segments[0];
    assert_eq!(seg.headers.subject.as_deref(), Some("Re: Reporting Interval"));
    assert!(
        seg.headers
            .from
            .iter()
            .any(|a| a.address == "henry@minfarmtech.com")
    );
    assert!(seg.reply_text.contains("Thanks for the reply."));
    assert!(!seg.reply_text.contains("Kind regards"));
    assert!(seg.signature.is_some());
}

#[test]
fn parse_forwarded_segment_recurses_nested_forward() {
    let msg = concat!(
        "From: A <a@example.com>\n",
        "To: B <b@example.com>\n",
        "Subject: Fwd: Outer\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "---- Forwarded message ----\n",
        "From: Outer <outer@example.com>\n",
        "Subject: Outer layer\n",
        "\n",
        "---- Forwarded message ----\n",
        "From: Inner <inner@example.com>\n",
        "Subject: Inner layer\n",
        "\n",
        "Inner body\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.forwarded_segments.len(), 1);
    assert_eq!(parsed.forwarded_segments[0].nested.len(), 1);
    assert_eq!(
        parsed.forwarded_segments[0].nested[0]
            .headers
            .subject
            .as_deref(),
        Some("Inner layer")
    );
}

#[test]
fn parse_forwarded_segment_unfolds_cc_and_strips_embedded_header_bundle() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Fwd: Device question\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "FYI\n\n",
        "---------- Forwarded message ---------\n",
        "From: Haji Yakob <mohd-khiari.yakob@bsp-shell.bn>\n",
        "Date: Wed, Oct 15, 2025 at 8:55 AM\n",
        "Subject: RE: Sensa.io max pressure\n",
        "To: Nicolas Guillou <nicolas.guillou@anian.co>\n",
        "Cc: Lester Teo <lester.teo@anian.co>, Atiqah Fauzi <atiqah.fauzi@anian.co>,\n",
        "Aktas, Ahmet Ufuk <ahmet.aktas@bsp-shell.bn>\n",
        "\n",
        "Nicolas,\n\n",
        "Many thanks for responding to this query.\n\n",
        "Regards,\n",
        "Khiari\n\n",
        "*From:* Nicolas Guillou <nicolas.guillou@anian.co>\n",
        "*Sent:* Wednesday, October 15, 2025 8:46 AM\n",
        "*To:* Wong <wong@bsp-shell.bn>\n",
        "*Cc:* Lester Teo <lester.teo@anian.co>; Atiqah Fauzi <atiqah.fauzi@anian.co>\n",
        "*Subject:* Sensa.io max pressure\n",
        "\n",
        "Hello,\n",
        "Quoted body.\n",
    );

    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.forwarded_segments.len(), 1);
    let seg = &parsed.forwarded_segments[0];
    assert!(
        seg.headers
            .cc
            .iter()
            .any(|a| a.address == "lester.teo@anian.co")
    );
    assert!(
        seg.headers
            .cc
            .iter()
            .any(|a| a.address == "atiqah.fauzi@anian.co")
    );
    assert!(
        seg.headers
            .cc
            .iter()
            .any(|a| a.address == "ahmet.aktas@bsp-shell.bn")
    );
    assert!(!seg.reply_text.contains("*From:*"));
    assert!(!seg.reply_text.contains("*Sent:*"));
    assert!(seg.quoted_blocks.iter().any(|b| b.contains("*From:*")));
}

#[test]
fn parse_forwarded_headers_matrix_current_languages() {
    let cases = [
        (
            "From",
            "To",
            "Date",
            "Subject",
            "From: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nDate: Tue, 20 Jan 2026 12:34:56 +0000\nSubject: Test EN",
        ),
        (
            "De",
            "A",
            "Envoyé",
            "Objet",
            "De: Alice <alice@example.com>\nA: Bob <bob@example.com>\nEnvoyé: mardi 20 janvier 2026 12:34\nObjet: Test FR",
        ),
        (
            "De",
            "Para",
            "Enviado el",
            "Asunto",
            "De: Alice <alice@example.com>\nPara: Bob <bob@example.com>\nEnviado el: martes 20 enero 2026 12:34\nAsunto: Test ES",
        ),
        (
            "Von",
            "An",
            "Gesendet",
            "Betreff",
            "Von: Alice <alice@example.com>\nAn: Bob <bob@example.com>\nGesendet: Dienstag, 20 Januar 2026 12:34\nBetreff: Test DE",
        ),
        (
            "Da",
            "A",
            "Inviato",
            "Oggetto",
            "Da: Alice <alice@example.com>\nA: Bob <bob@example.com>\nInviato: martedi 20 gennaio 2026 12:34\nOggetto: Test IT",
        ),
        (
            "Van",
            "Aan",
            "Verzonden",
            "Onderwerp",
            "Van: Alice <alice@example.com>\nAan: Bob <bob@example.com>\nVerzonden: dinsdag 20 januari 2026 12:34\nOnderwerp: Test NL",
        ),
        (
            "Od",
            "Do",
            "Wysłano",
            "Temat",
            "Od: Alice <alice@example.com>\nDo: Bob <bob@example.com>\nWysłano: wtorek 20 stycznia 2026 12:34\nTemat: Test PL",
        ),
    ];

    for (from_key, to_key, date_key, subject_key, headers) in cases {
        let msg = format!(
            "From: Top <top@example.com>\nTo: Root <root@example.com>\nSubject: Fwd case\nContent-Type: text/plain; charset=utf-8\n\n---- Forwarded message ----\n{headers}\n\nBody line.\n\n*From:* Legacy <legacy@example.com>\n*Sent:* Tue\n*To:* Root <root@example.com>\n*Subject:* Legacy Subject\n\nQuoted line.\n"
        );
        let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
        assert_eq!(parsed.forwarded_segments.len(), 1);
        let seg = &parsed.forwarded_segments[0];
        assert!(
            seg.headers
                .from
                .iter()
                .any(|a| a.address == "alice@example.com"),
            "missing from for language keys {from_key}/{to_key}/{date_key}/{subject_key}"
        );
        assert!(
            seg.headers.to.iter().any(|a| a.address == "bob@example.com"),
            "missing to for language keys {from_key}/{to_key}/{date_key}/{subject_key}"
        );
        assert!(seg.headers.date.is_some(), "missing date for {date_key}");
        assert!(seg.headers.subject.is_some(), "missing subject for {subject_key}");
        assert!(!seg.reply_text.contains("*From:*"));
        assert!(!seg.reply_text.contains("*Sent:*"));
    }
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
fn parse_reply_strips_es_outlook_header_bundle() {
    let text = "Hola,\n\nGracias por el seguimiento.\n\nSaludos,\nCarlos\n\nDe: EDGE TECHNOLOGIES <support@example.com>\nEnviado el: jueves, 24 de octubre de 2024 20:28\nPara: Carlos <carlos@example.com>\nAsunto: Re: Contact Form Request\n\nMensaje anterior";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Gracias por el seguimiento."));
    assert!(!reply.contains("Enviado el: jueves"));
    assert!(!reply.contains("Asunto: Re: Contact Form Request"));
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
fn parse_signature_detects_thank_you_merci_and_a_plus_signoffs() {
    let cases = [
        "Body line.\n\nThank you,\nAlice",
        "Corps du message.\n\nMerci,\nAlice",
        "Message body.\n\nA+\nAlice",
    ];
    for text in cases {
        let blocks = segment_email_body(text);
        assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
        let reply = reply_text(text, &blocks);
        assert_eq!(reply, text.lines().next().unwrap_or_default());
    }
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
        "linkedin.com/in/annamueller\n",
        "https://www.acme.example/company-page\n",
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
    assert!(
        parsed
            .signature_entities
            .urls
            .iter()
            .any(|u| u.contains("linkedin.com/in/annamueller"))
    );
    assert!(
        parsed.contact_hints.iter().any(|h| {
            h.url.as_deref().is_some_and(|u| u.contains("linkedin.com/in/annamueller"))
                && h.profile_type.is_some()
        })
    );
    assert!(!parsed.signature_entities.is_partial);
}

#[test]
fn parse_contact_hints_extracts_bare_social_domains() {
    let msg = concat!(
        "From: Nicolas Guillou <nicolas.guillou@anian.co>\n",
        "To: Support <support@sensa.io>\n",
        "Subject: signature links\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Best regards,\n",
        "Nicolas Guillou\n",
        "linkedin.com/company/anian-co\n",
        "x.com/nicolasg\n",
        "instagram.com/anian.co\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.contact_hints.iter().any(|h| {
        h.url.as_deref()
            .is_some_and(|u| u.contains("linkedin.com/company/anian-co"))
            && h
                .profile_type
                .as_ref()
                .is_some_and(|t| t == &mailbox_parser::ContactProfileType::LinkedinCompany)
    }));
    assert!(parsed.contact_hints.iter().any(|h| {
        h.url.as_deref().is_some_and(|u| u.contains("x.com/nicolasg"))
            && h
                .profile_type
                .as_ref()
                .is_some_and(|t| t == &mailbox_parser::ContactProfileType::TwitterX)
    }));
}

#[test]
fn parse_contact_hints_links_url_to_sender_when_domain_matches() {
    let msg = concat!(
        "From: Nicolas Guillou <nicolas.guillou@anian.co>\n",
        "To: Support <support@sensa.io>\n",
        "Subject: signature links\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Best regards,\n",
        "Nicolas Guillou\n",
        "https://linkedin.com/company/anian-co\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    let linked = parsed
        .contact_hints
        .iter()
        .find(|h| {
            h.url.as_deref()
                .is_some_and(|u| u.contains("linkedin.com/company/anian-co"))
        })
        .expect("expected linkedin hint");
    assert!(linked.linked_entity_key.is_some());
    assert!(linked.link_reason.is_some());
}

#[test]
fn parse_contact_hints_keeps_ambiguous_urls_unlinked() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: signature links\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Best regards,\n",
        "Alice\n",
        "https://tiktok.com/@randomvendor\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    let hint = parsed
        .contact_hints
        .iter()
        .find(|h| h.url.as_deref().is_some_and(|u| u.contains("tiktok.com")))
        .expect("expected tiktok hint");
    assert!(hint.linked_entity_key.is_none());
}

#[test]
fn parse_contact_hints_normalizes_wrapped_signature_urls() {
    let msg = concat!(
        "From: Thomas GUILLET <thomas.guillet@edgetech.fr>\n",
        "To: Support <support@sensa.io>\n",
        "Subject: signature wrapped urls\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Best regards,\n",
        "Thomas GUILLET\n",
        "[Logo]<https://www.wi6labs.com/>\n",
        "Assistance/Support<https://support.wi6labs.net/>\n",
        "www.tcb.gr<http://www.tcb.gr/>\n",
        "[2]https://otrs.example.com/customer.pl?Action=CustomerTicketZoom&TicketID=31017\n",
        "[Portal](https://book.sensa.io/#/customer/thomas)\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    let urls: Vec<String> = parsed
        .contact_hints
        .iter()
        .filter_map(|h| h.url.clone())
        .collect();
    assert!(
        urls.iter()
            .any(|u| u.starts_with("https://www.wi6labs.com"))
    );
    assert!(
        urls.iter()
            .any(|u| u.starts_with("https://support.wi6labs.net"))
    );
    assert!(urls.iter().any(|u| u.starts_with("http://www.tcb.gr")));
    assert!(
        urls.iter()
            .any(|u| u == "https://otrs.example.com/customer.pl?Action=CustomerTicketZoom&TicketID=31017")
    );
    assert!(
        urls.iter()
            .any(|u| u == "https://book.sensa.io/#/customer/thomas")
    );
    assert!(
        !urls
            .iter()
            .any(|u| u.contains('[') || u.contains(']') || u.contains('<') || u.contains('>'))
    );
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
    println!("REPLY={}", parsed.body_canonical);
    println!("EVENTS={:?}", parsed.event_hints);
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
fn parse_signature_detects_cdlt_after_double_blank_line() {
    let text = "Message principal sur la configuration.\n\nPouvez-vous confirmer ?\n\n\ncdlt\nThomas GUILLET\nhttps://book.sensa.io/#/customer/thomas\nmailto:thomas.guillet@edgetech.fr";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Message principal"));
    assert!(!reply.contains("cdlt"));
    assert!(!reply.contains("book.sensa.io"));
}

#[test]
fn parse_signature_detects_contact_card_with_blank_gap_no_signoff() {
    let text = "Merci pour votre retour.\n\nNous validons l'etape suivante.\n\n\nThomas GUILLET\nEDGE TECHNOLOGIES SAS\n1480 Avenue D'Armenie\nhttp://www.edgetech.fr/\n+33 1 23 45 67 89";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Nous validons l'etape suivante."));
    assert!(!reply.contains("EDGE TECHNOLOGIES"));
    assert!(!reply.contains("edgetech.fr"));
}

#[test]
fn parse_signature_does_not_cut_normal_paragraphs_with_blank_lines() {
    let text = "Bonjour,\n\nVoici un premier paragraphe explicatif.\n\n\nVoici un second paragraphe qui continue la demande et contient suffisamment de texte pour rester dans le corps du message.\n\nMerci.";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(!blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    assert!(reply.contains("second paragraphe"));
}

#[test]
fn parse_signature_keeps_business_url_in_reply_when_not_tail_card() {
    let text = "Please review https://book.sensa.io/#/customer/thomas before tomorrow.\nIt is part of the requested troubleshooting steps.\nCan you confirm once done?";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(!blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    assert!(reply.contains("book.sensa.io"));
}

#[test]
fn parse_signature_detects_bien_cordialement_variant() {
    let text = "Nous avons applique la correction.\n\nBien cordialement,\nThomas GUILLET\nEDGE TECHNOLOGIES SAS\nhttp://www.edgetech.fr/";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "Nous avons applique la correction.");
}

#[test]
fn parse_signature_prefers_explicit_signoff_over_late_contact_tail() {
    let text = "We confirm device Dev EUI is 1CA8520000004734.\n\nPlease see how to address it.\n\nBest regards,\nZakaria Syed\nI&C Dept-Manager\nAl Barakat Golden General Trading & Contracting Co.\n\n---- older chain ----\nMobile: 94468437\nEmail: s.zakaria@barkaat-golden.com\nwww.barkaat-golden.com";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Please see how to address it."));
    assert!(!reply.contains("Best regards"));
    assert!(!reply.contains("barkaat-golden.com"));
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

#[test]
fn parse_event_hints_ignores_header_bundle_lines() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Re: Contact Form Request\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Enviado el: jueves, 24 de octubre de 2024 20:28\n",
        "Asunto: Re: Contact Form Request\n",
        "From: support@example.com\n",
        "Thanks for your note.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.event_hints.is_empty());
}

#[test]
fn parse_event_hints_prefers_shipping_when_pickup_waybill_and_call_present() {
    let msg = concat!(
        "From: Ops <ops@example.com>\n",
        "To: Vendor <vendor@example.com>\n",
        "Subject: Re: RTD specification pickup\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Hi team,\n",
        "\n",
        "Please schedule pickup on 2026-02-28 10:30 CET.\n",
        "Please call the courier in the morning and confirm waybill number 2880126981.\n",
        "\n",
        "Best regards,\n",
        "Ops\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.event_hints.len(), 1);
    let event = &parsed.event_hints[0];
    assert_eq!(event.kind, mailbox_parser::EventHintKind::Shipping);
}

#[test]
fn parse_event_hints_ignores_measurement_unit_noise_lines() {
    let msg = concat!(
        "From: Alice <alice@example.com>\n",
        "To: Bob <bob@example.com>\n",
        "Subject: Site visit planning\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "We have design new mounting bracket 3mm thickness to make is more rigid and 2 differents formats.\n",
        "Does 16-18 April work for you?\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.event_hints.len(), 1);
    let event = &parsed.event_hints[0];
    assert!(
        !event
            .datetime_candidates
            .iter()
            .any(|d| d.raw.contains("3mm thickness"))
    );
    assert!(
        event
            .datetime_candidates
            .iter()
            .any(|d| d.raw.contains("16-18 April"))
    );
}

#[test]
fn signature_extracts_terminal_best_regards_without_contact_card() {
    let text = "Please check this issue with LoRa decoding.\n\nI will test again after lunch.\n\nBest regards,";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("Please check this issue"));
    assert!(!reply.contains("Best regards"));
}

#[test]
fn signature_extracts_terminal_kind_regards_with_name_and_title_tail() {
    let text = "Thanks for your support.\n\nThe command is now applied.\n\nKind regards,\nAhmed Elkrewi\nCOO";
    let blocks = segment_email_body(text);
    assert!(blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert_eq!(reply, "Thanks for your support.\n\nThe command is now applied.");
}

#[test]
fn signature_does_not_extract_when_only_long_prose_tail() {
    let text = "Hello team,\n\nPlease keep this paragraph in reply text even if it is near the end and looks like a conclusion because it is still part of the functional request and not a signature block.\n\nThis second paragraph is also body text and should not be moved.";
    let blocks = segment_email_body(text);
    assert!(!blocks.iter().any(|b| b.kind == EmailBlockKind::Signature));
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("functional request"));
    assert!(reply.contains("second paragraph"));
}

#[test]
fn signature_does_not_swallow_reply_prose_before_contact_markers() {
    let text = "The gateway sends valid payloads and we confirmed this in production.\n\nPlease keep this explanatory sentence in reply text because it is not signature data.\n\nBest regards,\nThomas GUILLET\nEDGE TECHNOLOGIES SAS\nthomas.guillet@edgetech.fr";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("explanatory sentence"));
    assert!(!reply.contains("Best regards"));
    assert!(!reply.contains("EDGE TECHNOLOGIES"));
}

#[test]
fn forwarded_segment_strips_embedded_header_bundle_from_reply_text_regression() {
    let msg = concat!(
        "From: A <a@example.com>\n",
        "To: B <b@example.com>\n",
        "Subject: Fwd: test\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "---- Forwarded message ----\n",
        "From: Sender <sender@example.com>\n",
        "To: Receiver <receiver@example.com>\n",
        "Date: Tue, 20 Jan 2026 12:34:56 +0000\n",
        "Subject: Re: status\n",
        "\n",
        "Top forwarded reply line.\n",
        "Second reply line.\n",
        "\n",
        "From: Legacy <legacy@example.com>\n",
        "Sent: Monday, January 19, 2026 10:00 AM\n",
        "To: Sender <sender@example.com>\n",
        "Subject: previous\n",
        "\n",
        "Old quoted body.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.forwarded_segments.len(), 1);
    let seg = &parsed.forwarded_segments[0];
    assert!(seg.reply_text.contains("Top forwarded reply line."));
    assert!(!seg.reply_text.contains("From: Legacy"));
    assert!(!seg.reply_text.contains("Sent: Monday"));
    assert!(!seg.reply_text.contains("Subject: previous"));
}

#[test]
fn signature_keeps_plain_sentence_before_best_regards_in_reply() {
    let text = "I have a temperature sensor and I would like to disable Bluetooth.\n\nTherefore, I kindly ask you to provide me with guidance as soon as possible\nso I can solve this issue.\n\nBest regards,";
    let blocks = segment_email_body(text);
    let reply = reply_text(text, &blocks);
    assert!(reply.contains("so I can solve this issue."));
    assert!(!reply.contains("Best regards,"));
    let sig = blocks
        .iter()
        .find(|b| b.kind == EmailBlockKind::Signature)
        .and_then(|b| text.get(b.byte_start..b.byte_end))
        .unwrap_or("");
    assert_eq!(sig.trim(), "Best regards,");
}

#[test]
fn parse_mail_kind_hints_detects_newsletter_headers() {
    let msg = concat!(
        "From: Reddit <noreply@redditmail.com>\n",
        "To: Aurel <fitchefaurel@gmail.com>\n",
        "Subject: Weekly recap and recommendations\n",
        "List-Unsubscribe: <https://reddit.com/unsubscribe>\n",
        "List-Id: <news.reddit.com>\n",
        "Precedence: bulk\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "View in browser\n",
        "Manage preferences\n",
        "Unsubscribe\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    let primary = parsed
        .mail_kind_hints
        .iter()
        .find(|h| h.is_primary)
        .expect("primary hint");
    assert_eq!(primary.kind, MailKind::Newsletter);
}

#[test]
fn parse_mail_kind_hints_detects_promotion_tokens() {
    let msg = concat!(
        "From: Shop <offers@example.com>\n",
        "To: User <user@example.com>\n",
        "Subject: Limited time sale - 30% off coupon\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Use this coupon for a discount deal today.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    let primary = parsed
        .mail_kind_hints
        .iter()
        .find(|h| h.is_primary)
        .expect("primary hint");
    assert_eq!(primary.kind, MailKind::Promotion);
}

#[test]
fn parse_direction_hint_detects_outbound_with_owner_email() {
    let msg = concat!(
        "From: Aurel <fitchefaurel@gmail.com>\n",
        "To: Team <team@example.com>\n",
        "Subject: Follow up\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Please review.\n",
    );
    let parsed = parse_rfc822_with_options(
        msg.as_bytes(),
        &ParseRfc822Options {
            owner_emails: vec!["fitchefaurel@gmail.com".to_string()],
        },
    )
    .expect("parse");
    let direction = parsed.direction_hint.expect("direction");
    assert_eq!(direction.direction, MailDirection::Outbound);
}

#[test]
fn parse_direction_hint_detects_inbound_with_owner_email() {
    let msg = concat!(
        "From: Sender <sender@example.com>\n",
        "To: Aurel <fitchefaurel@gmail.com>\n",
        "Subject: Re: Follow up\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Received.\n",
    );
    let parsed = parse_rfc822_with_options(
        msg.as_bytes(),
        &ParseRfc822Options {
            owner_emails: vec!["fitchefaurel@gmail.com".to_string()],
        },
    )
    .expect("parse");
    let direction = parsed.direction_hint.expect("direction");
    assert_eq!(direction.direction, MailDirection::Inbound);
}

#[test]
fn parse_html_newsletter_cleanup_removes_tail_footer_noise() {
    let msg = concat!(
        "From: News <noreply@example.com>\n",
        "To: User <user@example.com>\n",
        "Subject: Digest\n",
        "Content-Type: text/html; charset=utf-8\n",
        "\n",
        "<html><body>",
        "<p>Hello there, this is the useful update.</p>",
        "<p>Actionable line for users.</p>",
        "<p>View in browser</p>",
        "<p>Manage preferences</p>",
        "<p>Unsubscribe</p>",
        "<p>https://facebook.com/brand</p>",
        "</body></html>",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.body_canonical.contains("useful update"));
    assert!(!parsed.body_canonical.to_ascii_lowercase().contains("unsubscribe"));
    assert!(
        !parsed
            .body_canonical
            .to_ascii_lowercase()
            .contains("manage preferences")
    );
}

#[test]
fn parse_unsubscribe_hints_from_list_headers_and_body() {
    let msg = concat!(
        "From: Newsletter <news@example.com>\n",
        "To: User <user@example.com>\n",
        "Subject: Weekly digest\n",
        "List-Unsubscribe: <https://example.com/unsub>, <mailto:unsubscribe@example.com>\n",
        "List-Unsubscribe-Post: List-Unsubscribe=One-Click\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "If you prefer, manage preferences here: https://example.com/preferences\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.unsubscribe_hints.iter().any(|h| {
        h.source == UnsubscribeSource::HeaderListUnsubscribe
            && h.kind == UnsubscribeKind::Url
            && h.url.as_deref() == Some("https://example.com/unsub")
    }));
    assert!(parsed.unsubscribe_hints.iter().any(|h| {
        h.source == UnsubscribeSource::HeaderListUnsubscribe
            && h.kind == UnsubscribeKind::MailTo
            && h.email.as_deref() == Some("unsubscribe@example.com")
    }));
    assert!(parsed.unsubscribe_hints.iter().any(|h| {
        h.source == UnsubscribeSource::HeaderListUnsubscribePost
            && h.kind == UnsubscribeKind::OneClick
    }));
}

#[test]
fn parse_service_lifecycle_hint_detects_subscription_cancellation() {
    let msg = concat!(
        "From: Kajabi <notifications@kajabi.com>\n",
        "To: Owner <owner@example.com>\n",
        "Subject: [NOTIFICATION] Subscription cancellation\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "We're sending you an email to let you know that the following subscription has been canceled.\n",
        "Customer name: Julia Samokhvalova\n",
        "Customer email: julia.samx@gmail.com\n",
        "Offer: 6 month Flow with Mira Membership\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert_eq!(parsed.service_lifecycle_hints.len(), 1);
    let hint = &parsed.service_lifecycle_hints[0];
    assert_eq!(hint.kind, ServiceLifecycleKind::SubscriptionCanceled);
    assert_eq!(hint.customer_email.as_deref(), Some("julia.samx@gmail.com"));
    assert!(
        hint.plan_name
            .as_deref()
            .is_some_and(|p| p.contains("Mira Membership"))
    );
}

#[test]
fn parse_cleanup_strips_reddit_digest_footer_tail() {
    let msg = concat!(
        "From: Reddit <noreply@redditmail.com>\n",
        "To: Aurel <fitchefaurel@gmail.com>\n",
        "Subject: digest\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Here are your posts.\n",
        "Read More\n",
        "This email was intended for u/fitchefaurel.\n",
        "Unsubscribefrom daily digest messages, or visit your settings to manage\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.body_canonical.contains("Here are your posts."));
    assert!(!parsed.body_canonical.contains("This email was intended for"));
    assert!(!parsed.body_canonical.contains("Unsubscribefrom daily digest messages"));
}

#[test]
fn lifecycle_gate_blocks_newsletter_billing_content_false_positive() {
    let msg = concat!(
        "From: News <news@daily.example>\n",
        "To: User <user@example.com>\n",
        "Subject: Market newsletter\n",
        "List-Unsubscribe: <https://daily.example/unsub>\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "Today we discuss billing strategy and invoice optimization in SaaS.\n",
        "This is editorial content, not a notification.\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.service_lifecycle_hints.is_empty());
}

#[test]
fn parse_billing_action_hints_extracts_url_without_lifecycle() {
    let msg = concat!(
        "From: Newsletter <news@example.com>\n",
        "To: User <user@example.com>\n",
        "Subject: Weekly digest\n",
        "List-Unsubscribe: <https://example.com/unsub>\n",
        "Content-Type: text/plain; charset=utf-8\n",
        "\n",
        "To view invoice details visit https://example.com/billing/invoice/123\n",
    );
    let parsed = parse_rfc822(msg.as_bytes()).expect("parse");
    assert!(parsed.service_lifecycle_hints.is_empty());
    assert!(parsed.billing_action_hints.iter().any(|h| {
        h.kind == BillingActionKind::ViewInvoice
            && h.url
                .as_deref()
                .is_some_and(|u| u.contains("/billing/invoice/123"))
    }));
}
