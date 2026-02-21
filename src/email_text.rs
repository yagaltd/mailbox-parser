use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmailBlockKind {
    Salutation,
    Reply,
    Quoted,
    Forwarded,
    Signature,
    Disclaimer,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmailBlock {
    pub kind: EmailBlockKind,
    pub byte_start: usize,
    pub byte_end: usize,
}

pub fn normalize_email_text(s: &str) -> String {
    // Normalize line terminators and remove problematic Unicode formatting chars that often
    // appear after HTML->text conversion.
    fn normalize_char(ch: char) -> Option<char> {
        match ch {
            // Line terminators.
            '\r' => Some('\n'),
            '\u{2028}' | '\u{2029}' => Some('\n'),

            // Common invisible/format chars (Cf-ish) and friends.
            '\u{034F}' // COMBINING GRAPHEME JOINER (shows as "͏")
            | '\u{00AD}' // soft hyphen
            | '\u{FEFF}' // BOM / zero width no-break space
            | '\u{200B}' // ZWSP
            | '\u{200C}' // ZWNJ
            | '\u{200D}' // ZWJ
            | '\u{2060}' // WORD JOINER
            | '\u{180E}' // MONGOLIAN VOWEL SEPARATOR (deprecated)
            | '\u{200E}' // LRM
            | '\u{200F}' // RLM
            | '\u{202A}' | '\u{202B}' | '\u{202C}' | '\u{202D}' | '\u{202E}'
            | '\u{2066}' | '\u{2067}' | '\u{2068}' | '\u{2069}' => None,

            // Non-breaking/figure/thin spaces.
            '\u{00A0}' | '\u{2007}' | '\u{202F}' | '\u{2009}' | '\u{200A}' => Some(' '),

            _ => Some(ch),
        }
    }

    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    let mut prev_nl = false;
    let mut blank_run = 0usize;

    let mut it = s.chars().peekable();
    while let Some(mut ch) = it.next() {
        if ch == '\r' {
            if matches!(it.peek(), Some('\n')) {
                let _ = it.next();
            }
            ch = '\n';
        }
        let ch = match normalize_char(ch) {
            Some(c) => c,
            None => continue,
        };

        // Handle CRLF which we normalize to a single \n.
        if ch == '\n' {
            if matches!(it.peek(), Some('\n')) {
                // Keep as-is; double newlines handled below.
            }

            if prev_nl {
                blank_run += 1;
            } else {
                blank_run = 0;
            }

            // Allow up to 2 consecutive blank lines (i.e. at most 3 '\n' in a row).
            if blank_run <= 1 {
                out.push('\n');
            }
            prev_space = false;
            prev_nl = true;
            continue;
        }

        prev_nl = false;
        blank_run = 0;

        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }

    // Trim trailing whitespace per line.
    let mut cleaned = String::with_capacity(out.len());
    for (i, line) in out.split('\n').enumerate() {
        if i > 0 {
            cleaned.push('\n');
        }
        cleaned.push_str(line.trim_end());
    }
    cleaned.trim().to_string()
}

pub fn segment_email_body(text: &str) -> Vec<EmailBlock> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    let bytes = text.as_bytes();
    let mut lines: Vec<(usize, usize)> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let start = i;
        let mut end = i;
        while end < bytes.len() && bytes[end] != b'\n' {
            end += 1;
        }
        lines.push((start, end));
        i = (end + 1).min(bytes.len());
    }

    let get_line = |(s, e): (usize, usize)| -> &str { text.get(s..e).unwrap_or("") };

    let line_core = |line: &str| -> String {
        let mut s = line.trim_start();
        while let Some(rest) = s.strip_prefix('>') {
            s = rest.trim_start();
        }
        s.trim().to_ascii_lowercase()
    };

    let starts_with_any =
        |s: &str, patterns: &[&str]| -> bool { patterns.iter().any(|p| s.starts_with(p)) };

    const FORWARD_MARKERS: &[&str] = &[
        "---- forwarded message ----",
        "-----forwarded message-----",
        "begin forwarded message",
        "mensaje reenviado",
        "nachricht weitergeleitet",
        "messaggio inoltrato",
        "bericht doorgestuurd",
        "wiadomość dalej",
    ];
    const QUOTE_SEPARATORS: &[&str] = &[
        "-----original message-----",
        "----original message----",
        "original message",
        "mensaje original",
        "message d'origine",
        "ursprüngliche nachricht",
        "messaggio originale",
        "oorspronkelijk bericht",
        "wiadomość oryginalna",
    ];
    const HEADER_KEYS: &[&str] = &[
        "from:",
        "sent:",
        "date:",
        "to:",
        "cc:",
        "subject:",
        "message-id:",
        "de:",
        "envoyé:",
        "objet:",
        "von:",
        "gesendet:",
        "betreff:",
        "da:",
        "inviato:",
        "oggetto:",
        "van:",
        "verzonden:",
        "onderwerp:",
        "od:",
        "wysłano:",
        "temat:",
    ];
    const WROTE_TOKENS: &[&str] = &[
        "wrote",
        "a écrit",
        "escribió",
        "schrieb",
        "ha scritto",
        "schreef",
        "napisał",
        "skrev",
    ];
    const SALUTATION_PREFIXES: &[&str] = &[
        "hi",
        "hello",
        "dear",
        "bonjour",
        "salut",
        "hola",
        "buenos",
        "hallo",
        "hej",
        "ciao",
        "dzień dobry",
        "witam",
    ];
    const SIGNATURE_CUES: &[&str] = &[
        "best regards",
        "kind regards",
        "regards",
        "thanks",
        "cheers",
        "sincerely",
        "mit freundlichen grüßen",
        "viele grüße",
        "cordialement",
        "bien à vous",
        "saludos",
        "un saludo",
        "distinti saluti",
        "vriendelijke groet",
        "pozdrawiam",
        "med vänlig hälsning",
    ];
    const MOBILE_SIGNATURE_CUES: &[&str] = &[
        "sent from my",
        "get outlook for",
        "sent from outlook",
        "envoyé depuis mon",
        "enviado desde mi",
        "gesendet von meinem",
        "inviato da",
        "wysłane z",
    ];
    const DISCLAIMER_CUES: &[&str] = &[
        "disclaimer:",
        "confidentiality notice",
        "confidentiality:",
        "this email",
        "ce message",
        "este correo",
        "diese e-mail",
        "questo messaggio",
        "ten e-mail",
        "ta wiadomość",
    ];

    let is_forward_marker_line = |t: &str| {
        let tl = line_core(t);
        starts_with_any(&tl, FORWARD_MARKERS) || tl.contains("forwarded message")
    };

    let is_quote_marker_line = |t: &str| {
        let tl = line_core(t);
        let locale_on_prefix = ["on ", "le ", "el ", "am ", "il ", "op ", "w dniu ", "den "];
        starts_with_any(&tl, QUOTE_SEPARATORS)
            || (tl.starts_with("on ")
                && tl.ends_with(':')
                && WROTE_TOKENS.iter().any(|w| tl.contains(w)))
            || (locale_on_prefix.iter().any(|p| tl.starts_with(p))
                && tl.ends_with(':')
                && WROTE_TOKENS.iter().any(|w| tl.contains(w)))
            || tl.starts_with("from:") && tl.contains("sent:")
    };

    let looks_like_header_bundle = |idx: usize| -> bool {
        let mut hits = 0usize;
        let end = (idx + 10).min(lines.len());
        for j in idx..end {
            let line = get_line(lines[j]);
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            let tl = line_core(t);
            if starts_with_any(&tl, HEADER_KEYS) {
                hits += 1;
            }
        }
        hits >= 3
    };

    let header_bundle_kind = |idx: usize| -> EmailBlockKind {
        // If a nearby line indicates forwarding, treat this as Forwarded; otherwise Quoted.
        let mut saw_forward_marker = false;
        for back in 1..=4 {
            if idx < back {
                break;
            }
            let t = get_line(lines[idx - back]).trim();
            if t.is_empty() {
                continue;
            }
            if is_forward_marker_line(t) {
                saw_forward_marker = true;
                break;
            }
        }
        if saw_forward_marker {
            return EmailBlockKind::Forwarded;
        }
        let end = (idx + 10).min(lines.len());
        for j in idx..end {
            let t = get_line(lines[j]).trim();
            if t.is_empty() {
                continue;
            }
            let tl = line_core(t);
            if let Some(rest) = tl.strip_prefix("subject:") {
                if rest.trim_start().starts_with("fwd:") || rest.trim_start().starts_with("fw:") {
                    return EmailBlockKind::Forwarded;
                }
                break;
            }
        }
        EmailBlockKind::Quoted
    };

    let mut quote_start: Option<(usize, EmailBlockKind)> = None;
    let mut consecutive_gt = 0usize;
    let mut first_gt_line_start = 0usize;

    for (idx, (s, e)) in lines.iter().copied().enumerate() {
        let line = text.get(s..e).unwrap_or("");
        let t = line.trim();
        if t.is_empty() {
            consecutive_gt = 0;
            continue;
        }

        if is_forward_marker_line(t) {
            quote_start = Some((s, EmailBlockKind::Forwarded));
            break;
        }
        if is_quote_marker_line(t) {
            quote_start = Some((s, EmailBlockKind::Quoted));
            break;
        }

        let tl = line_core(t);
        if (tl.starts_with("from:") || tl.starts_with("sent:") || tl.starts_with("date:"))
            && looks_like_header_bundle(idx)
        {
            quote_start = Some((s, header_bundle_kind(idx)));
            break;
        }

        if t.starts_with('>') {
            if consecutive_gt == 0 {
                first_gt_line_start = s;
            }
            consecutive_gt += 1;
            if consecutive_gt >= 2 {
                quote_start = Some((first_gt_line_start, EmailBlockKind::Quoted));
                break;
            }
        } else {
            consecutive_gt = 0;
        }
    }

    // Signature/salutation/disclaimer detection.
    let mut signature_start: Option<usize> = None;
    let mut disclaimer_start: Option<usize> = None;
    let mut salutation_line: Option<(usize, usize)> = None;
    let quote_byte_start = quote_start.as_ref().map(|(s, _)| *s).unwrap_or(bytes.len());
    let mut non_empty_before_quote = Vec::new();
    for (idx, (s, e)) in lines.iter().copied().enumerate() {
        if s >= quote_byte_start {
            break;
        }
        let t = get_line((s, e)).trim();
        if !t.is_empty() {
            non_empty_before_quote.push((idx, s, e));
        }
    }

    for (_idx, s, e) in non_empty_before_quote.iter().copied().take(6) {
        let core = line_core(get_line((s, e)));
        if SALUTATION_PREFIXES.iter().any(|p| {
            core == *p || core.starts_with(&format!("{p} ")) || core.starts_with(&format!("{p},"))
        }) {
            salutation_line = Some((s, e));
            break;
        }
    }

    for (rev_pos, (_idx, s, e)) in non_empty_before_quote
        .iter()
        .copied()
        .rev()
        .take(20)
        .enumerate()
    {
        let t = get_line((s, e)).trim();
        let core = line_core(t);
        if DISCLAIMER_CUES
            .iter()
            .any(|c| core.starts_with(c) || core.contains(c))
        {
            disclaimer_start = Some(s);
        }
        if t == "--" || t == "-- " {
            signature_start = Some(s);
            break;
        }
        if MOBILE_SIGNATURE_CUES.iter().any(|c| core.starts_with(c)) {
            signature_start = Some(s);
            break;
        }
        if SIGNATURE_CUES
            .iter()
            .any(|c| core == *c || core.starts_with(&format!("{c},")) || core.starts_with(c))
            && rev_pos <= 12
        {
            signature_start = Some(s);
            break;
        }
    }

    if let (Some(sig), Some(dis)) = (signature_start, disclaimer_start)
        && dis < sig
    {
        disclaimer_start = None;
    }

    let mut out = Vec::new();

    let body_end = bytes.len();
    let quoted_kind = quote_start.as_ref().map(|(_, k)| k.clone());
    let quote_byte_start = quote_start.as_ref().map(|(s, _)| *s);

    let reply_start = salutation_line.map(|(_, e)| e).unwrap_or(0);
    let reply_end = signature_start
        .or(disclaimer_start)
        .or(quote_byte_start)
        .unwrap_or(body_end)
        .min(body_end);

    if let Some((s, e)) = salutation_line {
        out.push(EmailBlock {
            kind: EmailBlockKind::Salutation,
            byte_start: s,
            byte_end: e,
        });
    }

    if reply_end > reply_start {
        out.push(EmailBlock {
            kind: EmailBlockKind::Reply,
            byte_start: reply_start,
            byte_end: reply_end,
        });
    }

    if let Some(sig_start) = signature_start {
        let sig_end = disclaimer_start.unwrap_or(quote_byte_start.unwrap_or(body_end));
        if sig_end > sig_start {
            out.push(EmailBlock {
                kind: EmailBlockKind::Signature,
                byte_start: sig_start,
                byte_end: sig_end,
            });
        }
    }

    if let Some(dis_start) = disclaimer_start {
        let dis_end = quote_byte_start.unwrap_or(body_end);
        if dis_end > dis_start {
            out.push(EmailBlock {
                kind: EmailBlockKind::Disclaimer,
                byte_start: dis_start,
                byte_end: dis_end,
            });
        }
    }

    if let Some(qs) = quote_byte_start {
        if body_end > qs {
            out.push(EmailBlock {
                kind: quoted_kind.unwrap_or(EmailBlockKind::Quoted),
                byte_start: qs,
                byte_end: body_end,
            });
        }
    }

    // Trim block boundaries.
    for b in &mut out {
        while b.byte_start < b.byte_end {
            let ch = text[b.byte_start..b.byte_end].chars().next().unwrap_or(' ');
            if ch.is_whitespace() {
                b.byte_start += ch.len_utf8();
            } else {
                break;
            }
        }
        while b.byte_end > b.byte_start {
            let ch = text[..b.byte_end].chars().rev().next().unwrap_or(' ');
            if ch.is_whitespace() {
                b.byte_end -= ch.len_utf8();
            } else {
                break;
            }
        }
    }
    out.retain(|b| b.byte_end > b.byte_start);
    out
}

pub fn reply_text(text: &str, blocks: &[EmailBlock]) -> String {
    let mut parts = Vec::new();
    for b in blocks {
        if b.kind == EmailBlockKind::Reply {
            if let Some(s) = text.get(b.byte_start..b.byte_end) {
                let t = s.trim();
                if !t.is_empty() {
                    parts.push(t);
                }
            }
        }
    }
    parts.join("\n\n")
}

pub fn forwarded_message_ids(text: &str, blocks: &[EmailBlock]) -> Vec<String> {
    let mut out = Vec::new();
    for b in blocks {
        if b.kind != EmailBlockKind::Forwarded {
            continue;
        }
        let Some(s) = text.get(b.byte_start..b.byte_end) else {
            continue;
        };
        for line in s.lines() {
            let tl = line.trim();
            if tl.len() < 10 {
                continue;
            }
            let lower = tl.to_ascii_lowercase();
            if let Some(rest) = lower.strip_prefix("message-id:") {
                let id = rest.trim();
                if !id.is_empty() {
                    out.push(id.to_string());
                }
            }
        }
    }
    out
}
