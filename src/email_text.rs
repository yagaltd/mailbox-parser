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
    let normalize_space_before_colon = |s: &str| -> String {
        let mut out = String::with_capacity(s.len());
        let mut it = s.chars().peekable();
        while let Some(ch) = it.next() {
            if ch == ' ' {
                while matches!(it.peek(), Some(' ')) {
                    let _ = it.next();
                }
                if matches!(it.peek(), Some(':')) {
                    continue;
                }
            }
            out.push(ch);
        }
        out
    };
    let line_core_header = |line: &str| {
        let raw = normalize_space_before_colon(&line_core(line));
        let normalize_emphasis = |input: &str, marker: char| -> Option<String> {
            if !input.starts_with(marker) {
                return None;
            }
            let colon = input.find(':')?;
            let key_raw = input.get(..colon)?.trim();
            let key = key_raw.trim_matches(marker).trim();
            if key.is_empty() {
                return None;
            }
            let mut after = input.get(colon + 1..)?.trim_start();
            if let Some(rest) = after.strip_prefix(marker) {
                after = rest.trim_start();
            }
            Some(format!("{key}:{after}"))
        };
        normalize_emphasis(&raw, '*')
            .or_else(|| normalize_emphasis(&raw, '_'))
            .unwrap_or(raw)
    };
    let has_email_like = |line: &str| {
        line.split_whitespace().any(|tok| {
            let t = tok.trim_matches(|c: char| ",;:.()<>[]\"'".contains(c));
            let Some((_, domain)) = t.split_once('@') else {
                return false;
            };
            !domain.is_empty() && domain.contains('.')
        })
    };
    let has_url_like = |line: &str| {
        let lower = line.to_ascii_lowercase();
        lower.contains("http://") || lower.contains("https://") || lower.contains("www.")
    };
    let has_phone_like = |line: &str| {
        let digits = line.chars().filter(|c| c.is_ascii_digit()).count();
        digits >= 8 && (line.contains('+') || line.contains('-') || line.contains(' '))
    };
    let has_cid_or_image_marker = |line: &str| {
        let lower = line.to_ascii_lowercase();
        lower.contains("cid:") || lower.contains("[image") || lower.contains("logo")
    };
    let has_address_marker = |line: &str| {
        let lower = line.to_ascii_lowercase();
        [
            "avenue",
            "street",
            "road",
            "blvd",
            "boulevard",
            "unit ",
            "p.o.box",
            "po box",
            "city",
            "france",
            "germany",
            "canada",
            "ksa",
            "usa",
        ]
        .iter()
        .any(|t| lower.contains(t))
    };
    let has_org_or_title_marker = |line: &str| {
        let lower = line.to_ascii_lowercase();
        [
            "inc",
            "llc",
            "gmbh",
            "sas",
            "ltd",
            "company",
            "manager",
            "director",
            "engineer",
            "technician",
            "support",
        ]
        .iter()
        .any(|t| lower.contains(t))
    };
    let looks_like_name_line = |line: &str| {
        let t = line.trim();
        if t.is_empty() || t.len() > 60 {
            return false;
        }
        let words: Vec<&str> = t.split_whitespace().collect();
        if words.is_empty() || words.len() > 4 {
            return false;
        }
        if words.len() == 1 {
            let w = words[0].trim_matches(|c: char| ",.;:()<>[]\"'".contains(c));
            return w.len() >= 2
                && w.chars()
                    .all(|c| c.is_ascii_alphabetic() || c == '-' || c == '\'')
                && w.chars()
                    .next()
                    .map(|c| c.is_ascii_uppercase())
                    .unwrap_or(false);
        }
        words.iter().all(|w| {
            let w = w.trim_matches(|c: char| ",.;:()<>[]\"'".contains(c));
            !w.is_empty()
                && w.chars()
                    .all(|c| c.is_ascii_alphabetic() || c == '-' || c == '\'')
                && w.chars()
                    .next()
                    .map(|c| c.is_ascii_uppercase())
                    .unwrap_or(false)
        })
    };
    let looks_like_title_only_line = |line: &str| {
        let t = line.trim();
        if t.is_empty() || t.len() > 48 {
            return false;
        }
        let words: Vec<&str> = t.split_whitespace().collect();
        if words.is_empty() || words.len() > 6 {
            return false;
        }
        let upper_words = words
            .iter()
            .filter(|w| {
                let token = w.trim_matches(|c: char| ",.;:()<>[]\"'".contains(c));
                token.len() >= 2
                    && token
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c == '&' || c == '-' || c == '/')
            })
            .count();
        if upper_words >= 1 {
            return true;
        }
        let lower = t.to_ascii_lowercase();
        [
            "manager",
            "director",
            "engineer",
            "technician",
            "consultant",
            "coo",
            "ceo",
            "cto",
            "cfo",
            "vp",
            "head of",
            "lead",
        ]
        .iter()
        .any(|k| lower.contains(k))
    };
    let signature_contact_score = |line: &str| -> usize {
        let mut score = 0usize;
        if has_email_like(line) {
            score += 2;
        }
        if has_phone_like(line) {
            score += 2;
        }
        if has_url_like(line) {
            score += 2;
        }
        if has_cid_or_image_marker(line) {
            score += 2;
        }
        if has_address_marker(line) {
            score += 1;
        }
        if has_org_or_title_marker(line) {
            score += 1;
        }
        score
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
        "para:",
        "asunto:",
        "enviado:",
        "enviado el:",
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
        "napsal",  // Czech "wrote"
        "napísal", // Slovak "wrote"
    ];
    const SALUTATION_PREFIXES: &[&str] = &[
        // English
        "hi",
        "hello",
        "dear",
        "hey",
        "good morning",
        "good afternoon",
        "good evening",
        "good day",
        "greetings",
        "howdy",
        // French
        "bonjour",
        "salut",
        "coucou",
        // Spanish
        "hola",
        "buenos",
        "buenas",
        "qué tal",
        // German
        "hallo",
        "guten morgen",
        "guten tag",
        "guten abend",
        // Italian
        "ciao",
        "buongiorno",
        "buonasera",
        // Scandinavian
        "hej",
        "hei",
        "god dag",
        "god morgen",
        // Polish
        "dzień dobry",
        "witam",
        "cześć",
        // Portuguese
        "olá",
        "bom dia",
        "boa tarde",
        // Dutch
        "hallo",
        "goedemorgen",
        "goede dag",
        // Czech/Slovak
        "dobrý den",
        "ahoj",
        // Hungarian
        "kedves",
        "szia",
        // Romanian
        "bună",
        "salut",
    ];
    const SIGNATURE_CUES: &[&str] = &[
        // English
        "best regards",
        "kind regards",
        "regards",
        "regards,",
        "rgds",
        "your best",
        "thanks",
        "thank you",
        "many thanks",
        "thanks and regards",
        "cheers",
        "sincerely",
        "yours truly",
        "yours sincerely",
        "best wishes",
        "warm regards",
        "with thanks",
        // French
        "merci",
        "merci beaucoup",
        "cordialement",
        "bien cordialement",
        "bien à vous",
        "à bientôt",
        "sincèrement",
        "amicalement",
        "cdlt",
        "a+",
        // Spanish
        "saludos",
        "un saludo",
        "saludos cordiales",
        "atte.",
        "atentamente",
        "muchas gracias",
        "un abrazo",
        // German
        "mit freundlichen grüßen",
        "freundliche grüße / best regards",
        "viele grüße",
        "herzliche grüße",
        "mfg",
        "beste grüße",
        // Italian
        "distinti saluti",
        "cordiali saluti",
        "grazie",
        "tantissimi saluti",
        // Dutch
        "vriendelijke groet",
        "met vriendelijke groet",
        "groet",
        "hartelijke groet",
        // Polish
        "pozdrawiam",
        "z poważaniem",
        "z wyrazami szacunku",
        // Scandinavian
        "med vänlig hälsning",
        "med vennlig hilsen",
        "mvh",
        "vh",
        "venlig hilsen",
        // Portuguese
        "com melhores cumprimentos",
        "cumprimentos",
        "obrigado",
        "obrigada",
        // Czech/Slovak
        "s pozdravem",
        "s úctou",
        // Romanian
        "cu respect",
        "toate cele bune",
    ];
    const MOBILE_SIGNATURE_CUES: &[&str] = &[
        // English
        "sent from my",
        "get outlook for",
        "sent from outlook",
        "sent from iphone",
        "sent from android",
        "sent from mobile",
        // French
        "envoyé depuis mon",
        "envoyé de mon",
        // Spanish
        "enviado desde mi",
        "enviado desde mi iphone",
        "enviado desde mi android",
        // German
        "gesendet von meinem",
        "gesendet von meinem iphone",
        "gesendet von meinem android",
        // Italian
        "inviato da",
        "inviato dal mio",
        // Polish
        "wysłane z",
        "wysłane z mojego",
        // Portuguese
        "enviado do meu",
        "enviado do iphone",
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
    const FOOTER_CUES: &[&str] = &[
        // English
        "unsubscribe",
        "update your preferences",
        "manage your subscription",
        "update email settings",
        "email updates",
        "click here to",
        "manage subscriptions",
        "update your email",
        "this email was sent to",
        "you received this email because",
        "if you no longer wish to",
        "to stop receiving",
        // French
        "se désabonner",
        "mettre à jour vos préférences",
        "gérer votre abonnement",
        "cliquez ici pour",
        // Spanish
        "darse de baja",
        "actualizar sus preferencias",
        "gestionar su suscripción",
        // German
        "abbestellen",
        "abo verwalten",
        "einstellungen aktualisieren",
        // Italian
        "annulla iscrizione",
        "gestisci abbonamento",
        // Portuguese
        "cancelar inscrição",
        "atualizar preferências",
        // Polish
        "wypisać się",
        "zaktualizuj preferencje",
        // Platform/Newsletter patterns
        "the kajabi team",
        "contact kajabi",
        "official help site",
        "kajabi support",
        "all rights reserved",
        // Newsletter patterns
        "founders & ai",
        "built an ai tool",
    ];

    let is_forward_marker_line = |t: &str| {
        let tl = line_core(t);
        starts_with_any(&tl, FORWARD_MARKERS) || tl.contains("forwarded message")
    };

    let is_quote_marker_line = |t: &str| {
        let tl_raw = line_core(t);
        let tl = tl_raw.trim_matches('-').trim().to_string();
        let tl_header = normalize_space_before_colon(&tl);
        let locale_on_prefix = [
            "on ", "le ", "el ", "am ", "il ", "op ", "w dniu ", "den ", "v ",
        ];
        let ends_like_wrote = WROTE_TOKENS
            .iter()
            .any(|w| tl.ends_with(':') || tl.ends_with(w));
        starts_with_any(&tl, QUOTE_SEPARATORS)
            || (tl.starts_with("on ")
                && ends_like_wrote
                && WROTE_TOKENS.iter().any(|w| tl.contains(w)))
            || (locale_on_prefix.iter().any(|p| tl.starts_with(p))
                && ends_like_wrote
                && WROTE_TOKENS.iter().any(|w| tl.contains(w)))
            || tl_header.starts_with("from:") && tl_header.contains("sent:")
    };
    let is_header_key_line = |t: &str| starts_with_any(&line_core_header(t), HEADER_KEYS);

    let looks_like_header_bundle = |idx: usize| -> bool {
        let mut hits = 0usize;
        let end = (idx + 10).min(lines.len());
        for j in idx..end {
            let line = get_line(lines[j]);
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            if is_header_key_line(t) {
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
            let tl = line_core_header(t);
            let rest = [
                "subject:",
                "betreff:",
                "objet:",
                "oggetto:",
                "onderwerp:",
                "temat:",
            ]
            .iter()
            .find_map(|p| tl.strip_prefix(p));
            if let Some(rest) = rest {
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

    // Define Zendesk quote marker and datetime detection here for use in quote detection
    let is_zendesk_quote_marker_outer = |t: &str| -> bool {
        let trimmed = t.trim();
        trimmed.starts_with('*')
            && trimmed.ends_with('*')
            && trimmed.len() > 2
            && trimmed[1..trimmed.len() - 1]
                .chars()
                .all(|c| c.is_alphabetic() || c.is_whitespace() || c == '\'')
    };

    let is_datetime_line_outer = |t: &str| -> bool {
        let trimmed = t.trim();
        if trimmed.len() > 50 {
            return false;
        }
        let mut has_day = false;
        let mut has_month = false;
        let mut has_year = false;
        let words: Vec<&str> = trimmed.split_whitespace().collect();
        for word in &words {
            let clean_word = word.trim_matches(|c| c == ',' || c == ':' || c == '.' || c == ';');
            if clean_word.len() == 4 && clean_word.chars().all(|c| c.is_ascii_digit()) {
                has_year = true;
            } else if clean_word.parse::<u32>().is_ok() {
                has_day = true;
            } else {
                let month_names = [
                    "jan",
                    "feb",
                    "mar",
                    "apr",
                    "may",
                    "jun",
                    "jul",
                    "aug",
                    "sep",
                    "oct",
                    "nov",
                    "dec",
                    "january",
                    "february",
                    "march",
                    "april",
                    "june",
                    "july",
                    "august",
                    "september",
                    "october",
                    "november",
                    "december",
                ];
                let lower = word
                    .trim_matches(|c| c == ',' || c == ':' || c == '.')
                    .to_lowercase();
                if month_names.iter().any(|m| lower.contains(m)) {
                    has_month = true;
                }
            }
        }
        let has_time = words.iter().any(|w| {
            let parts: Vec<&str> = w.split(':').collect();
            parts.len() == 2
                && parts
                    .iter()
                    .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
        });
        (has_day && has_month && has_year && (has_time || trimmed.len() < 30))
            || (has_time && has_day && has_month && has_year)
    };

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

        // NEW: Detect Zendesk quote markers (*Name*) as quote start
        if is_zendesk_quote_marker_outer(t) {
            quote_start = Some((s, EmailBlockKind::Quoted));
            break;
        }

        // NEW: Detect datetime lines as quote start (indicates quoted content)
        if is_datetime_line_outer(t) {
            quote_start = Some((s, EmailBlockKind::Quoted));
            break;
        }

        if is_header_key_line(t) && looks_like_header_bundle(idx) {
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

    let is_salutation_line = |line: &str| {
        let core = line_core(line);
        let raw = line.trim();
        SALUTATION_PREFIXES.iter().any(|p| {
            core == *p
                || core.starts_with(&format!("{p} "))
                || core.starts_with(&format!("{p},"))
                || core.starts_with(&format!("{p}:"))
                || (core.starts_with(p)
                    && raw
                        .chars()
                        .nth(p.len())
                        .map(|ch| ch.is_ascii_uppercase())
                        .unwrap_or(false))
        })
    };

    for (_idx, s, e) in non_empty_before_quote.iter().copied().take(6) {
        let line = get_line((s, e));
        if is_salutation_line(line) {
            salutation_line = Some((s, e));
            break;
        }
    }

    let tail_scan_take = non_empty_before_quote.len().min(60);
    let tail_start_idx = non_empty_before_quote.len().saturating_sub(tail_scan_take);
    let is_signature_cue_line = |core: &str| {
        let core = core
            .trim_end_matches(|c: char| c == ',' || c == '.' || c == ';' || c == ':' || c == '!')
            .trim();
        const STRICT_SIGNOFF_CUES: &[&str] = &[
            "thanks",
            "thank you",
            "many thanks",
            "merci",
            "merci beaucoup",
            "a+",
            "cheers",
            "cdlt",
        ];
        SIGNATURE_CUES.iter().any(|c| {
            if STRICT_SIGNOFF_CUES.iter().any(|s| s == c) {
                return core == *c
                    || core == format!("{c},")
                    || core == format!("{c}.")
                    || core == format!("{c}!");
            }
            if *c == "your best" {
                return core == *c
                    || core.starts_with("your best,")
                    || core.starts_with("your best.");
            }
            core == *c
                || core.starts_with(&format!("{c},"))
                || core.starts_with(&format!("{c}."))
                || core.starts_with(c)
        })
    };
    let inline_signature_start_offset = |line: &str| -> Option<usize> {
        let lower = line.to_ascii_lowercase();
        for marker in [" your best,", " your best.", " your best!", " your best"] {
            if let Some(pos) = lower.find(marker) {
                let start = pos + 1; // skip leading space in the marker
                return Some(start);
            }
        }
        None
    };
    let is_ambiguous_short_signoff = |core: &str| {
        let core = core
            .trim_end_matches(|c: char| c == ',' || c == '.' || c == ';' || c == ':' || c == '!')
            .trim();
        matches!(
            core,
            "thanks"
                | "thank you"
                | "many thanks"
                | "merci"
                | "merci beaucoup"
                | "a+"
                | "cheers"
                | "cdlt"
        )
    };
    let mut signature_pos: Option<usize> = None;
    let mut fallback_signature_pos: Option<usize> = None;

    let has_strong_contact_marker =
        |line: &str| has_email_like(line) || has_phone_like(line) || has_url_like(line);
    let has_soft_contact_marker = |line: &str| {
        has_address_marker(line) || has_org_or_title_marker(line) || has_cid_or_image_marker(line)
    };
    let blank_gap_before_pos = |pos: usize| -> usize {
        if pos == 0 {
            return 0;
        }
        let (idx, _, _) = non_empty_before_quote[pos];
        let (prev_idx, _, _) = non_empty_before_quote[pos - 1];
        idx.saturating_sub(prev_idx + 1)
    };
    let looks_like_body_continuation = |line: &str| -> bool {
        let t = line.trim();
        if t.len() < 48 {
            return false;
        }
        let ends_sentence =
            t.ends_with('.') || t.ends_with('!') || t.ends_with('?') || t.ends_with(':');
        let has_many_words = t.split_whitespace().count() >= 8;
        let starts_upper = t
            .chars()
            .next()
            .map(|c| c.is_ascii_uppercase())
            .unwrap_or(false);
        ends_sentence && has_many_words && starts_upper
    };
    let looks_like_long_body_sentence = |line: &str| -> bool {
        let t = line.trim();
        if t.len() < 64 {
            return false;
        }
        t.split_whitespace().count() >= 10
            && (t.ends_with('.') || t.ends_with('!') || t.ends_with('?') || t.ends_with(':'))
    };

    // Detect Zendesk-style quote markers like *David*, *John Smith*
    let is_zendesk_quote_marker = |t: &str| -> bool {
        let trimmed = t.trim();
        // Must be: starts with *, ends with *, contains text in between
        // Content should be mostly alphabetic (name)
        trimmed.starts_with('*')
            && trimmed.ends_with('*')
            && trimmed.len() > 2
            && trimmed[1..trimmed.len() - 1]
                .chars()
                .all(|c| c.is_alphabetic() || c.is_whitespace() || c == '\'')
    };

    // Detect lines that are primarily date/time stamps (indicating quoted content)
    let is_datetime_line = |t: &str| -> bool {
        let trimmed = t.trim();
        // Must be relatively short to be a datetime line
        if trimmed.len() > 50 {
            return false;
        }
        // Simple pattern matching without regex
        let has_day_month_year = {
            let mut has_day = false;
            let mut has_month = false;
            let mut has_year = false;
            let words: Vec<&str> = trimmed.split_whitespace().collect();
            for word in &words {
                // Strip trailing punctuation for checking
                let clean_word =
                    word.trim_matches(|c| c == ',' || c == ':' || c == '.' || c == ';');
                // Check for year first (4-digit number like 2025)
                if clean_word.len() == 4 && clean_word.chars().all(|c| c.is_ascii_digit()) {
                    has_year = true;
                } else if clean_word.parse::<u32>().is_ok() {
                    // Could be a day (1-31), but also might be a year in different formats
                    // Keep as day for now
                    has_day = true;
                } else {
                    let month_names = [
                        "jan",
                        "feb",
                        "mar",
                        "apr",
                        "may",
                        "jun",
                        "jul",
                        "aug",
                        "sep",
                        "oct",
                        "nov",
                        "dec",
                        "january",
                        "february",
                        "march",
                        "april",
                        "june",
                        "july",
                        "august",
                        "september",
                        "october",
                        "november",
                        "december",
                    ];
                    let lower = word
                        .trim_matches(|c| c == ',' || c == ':' || c == '.')
                        .to_lowercase();
                    if month_names.iter().any(|m| lower.contains(m)) {
                        has_month = true;
                    }
                }
            }
            // Check for patterns like "21 Jan 2025" or "Jan 21, 2025"
            (has_day && has_month && has_year)
                // Or ISO date format 2025-01-21
                || (trimmed.len() == 10
                    && trimmed.chars().nth(4) == Some('-')
                    && trimmed.chars().nth(7) == Some('-'))
        };
        // Also match time patterns like "23:53 NZDT" or "11:30 PM EST"
        let has_time = trimmed.split_whitespace().any(|w| {
            // Must contain a colon, but not be just a time like "15:23"
            // Check if it's a valid time pattern: digits:digits
            let parts: Vec<&str> = w.split(':').collect();
            parts.len() == 2
                && parts
                    .iter()
                    .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
        });
        // A datetime line has both date and time, or is primarily just a date
        (has_day_month_year && (has_time || trimmed.len() < 30)) || (has_time && has_day_month_year)
    };

    for pos in (tail_start_idx..non_empty_before_quote.len()).rev() {
        let (_idx, s, e) = non_empty_before_quote[pos];
        let t = get_line((s, e)).trim();
        let core = line_core(t);
        let has_inline_signoff = inline_signature_start_offset(t).is_some();

        // Step 1: Stop if we've entered quoted section (lines starting with >)
        if t.starts_with('>') {
            break;
        }

        // Step 1b: Stop at Zendesk quote markers (*Name*)
        if is_zendesk_quote_marker(t) {
            break;
        }

        // Step 1c: Stop at datetime lines (indicating quoted content from original email)
        if is_datetime_line(t) {
            break;
        }

        // Step 1d: Stop at quote marker lines (On ... wrote:, De: ..., Von: ..., etc.)
        if is_quote_marker_line(t) {
            break;
        }

        // Step 3: Stop if we've entered footer content
        if FOOTER_CUES.iter().any(|c| core.to_lowercase().contains(c)) {
            break;
        }

        if DISCLAIMER_CUES
            .iter()
            .any(|c| core.starts_with(c) || core.contains(c))
        {
            disclaimer_start = Some(s);
        }
        let mut score = 0i32;
        let mut strong_evidence = false;
        let mut has_contact_marker = false;

        if t == "--" || t == "-- " {
            score += 3;
            strong_evidence = true;
        }
        if MOBILE_SIGNATURE_CUES.iter().any(|c| core.starts_with(c)) {
            score += 3;
            strong_evidence = true;
        }
        if is_signature_cue_line(&core) || has_inline_signoff {
            score += 3;
            strong_evidence = true;
        }
        if blank_gap_before_pos(pos) >= 2 {
            score += 2;
        }

        let end = (pos + 8).min(non_empty_before_quote.len());
        let mut next_has_name = false;
        for next in (pos + 1)..end {
            let (_j, ns, ne) = non_empty_before_quote[next];
            let nt = get_line((ns, ne)).trim();
            if nt.is_empty() {
                continue;
            }
            if has_strong_contact_marker(nt) {
                has_contact_marker = true;
                strong_evidence = true;
                score += 2;
            } else if has_soft_contact_marker(nt) {
                score += 1;
            }
            if next <= pos + 2 && looks_like_name_line(nt) {
                next_has_name = true;
                score += 1;
            }
        }

        if !has_contact_marker && looks_like_body_continuation(t) {
            score -= 3;
        }
        if !strong_evidence && !next_has_name {
            score -= 2;
        }

        // Avoid clipping short tails that look like body text.
        let tail_lines = non_empty_before_quote.len().saturating_sub(pos);
        if tail_lines < 3 && !is_signature_cue_line(&core) {
            score -= 2;
        }

        let tail_lines = non_empty_before_quote.len().saturating_sub(pos);
        let signoff_tail_small = tail_lines <= 4;
        let ambiguous_short = is_ambiguous_short_signoff(&core);
        let explicit_signoff_candidate = (is_signature_cue_line(&core) || has_inline_signoff)
            && (next_has_name
                || has_contact_marker
                || (!ambiguous_short
                    && blank_gap_before_pos(pos) >= 1
                    && signoff_tail_small
                    && !looks_like_body_continuation(t)));
        if explicit_signoff_candidate {
            signature_pos = Some(pos);
            break;
        }
        if score >= 4 && strong_evidence && fallback_signature_pos.is_none() {
            fallback_signature_pos = Some(pos);
        }
    }
    if signature_pos.is_none() {
        signature_pos = fallback_signature_pos;
    }

    if signature_pos.is_none() {
        // Terminal sign-off fallback for compact tails (e.g. "Best regards," with optional name/title).
        for pos in (tail_start_idx..non_empty_before_quote.len()).rev() {
            let (_idx, s, e) = non_empty_before_quote[pos];
            let t = get_line((s, e)).trim();
            if t.is_empty() {
                continue;
            }

            // Stop at quoted content markers
            if t.starts_with('>')
                || is_zendesk_quote_marker(t)
                || is_datetime_line(t)
                || is_quote_marker_line(t)
            {
                break;
            }

            let core = line_core(t);
            if !is_signature_cue_line(&core) {
                continue;
            }
            let ambiguous_short = is_ambiguous_short_signoff(&core);
            let gap = blank_gap_before_pos(pos);
            if gap < 1 {
                continue;
            }
            let tail_lines = non_empty_before_quote.len().saturating_sub(pos);
            if tail_lines > 4 {
                continue;
            }
            let mut continuation_ok = true;
            for next in (pos + 1)..non_empty_before_quote.len() {
                let (_j, ns, ne) = non_empty_before_quote[next];
                let nt = get_line((ns, ne)).trim();
                if nt.is_empty() {
                    continue;
                }
                let acceptable = looks_like_name_line(nt)
                    || looks_like_title_only_line(nt)
                    || has_soft_contact_marker(nt)
                    || has_strong_contact_marker(nt);
                if !acceptable {
                    continuation_ok = false;
                    break;
                }
            }
            let has_continuation_lines = non_empty_before_quote.len().saturating_sub(pos) > 1;
            if continuation_ok
                && !looks_like_long_body_sentence(t)
                && (!ambiguous_short || has_continuation_lines)
            {
                signature_pos = Some(pos);
                break;
            }
        }
    }

    if signature_pos.is_none() {
        // Fallback: detect contact-card tails without explicit sign-off cue.
        let mut block_start_pos: Option<usize> = None;
        let mut block_score = 0usize;
        let mut block_lines = 0usize;
        let mut has_strong_marker = false;
        let mut prose_lines = 0usize;
        let mut has_signoff_or_marker = false;
        for pos in (tail_start_idx..non_empty_before_quote.len()).rev() {
            let (_idx, s, e) = non_empty_before_quote[pos];
            let t = get_line((s, e)).trim();

            // Stop at quoted content markers
            if t.starts_with('>')
                || is_zendesk_quote_marker(t)
                || is_datetime_line(t)
                || is_quote_marker_line(t)
            {
                break;
            }

            let core = line_core(t);
            let score = signature_contact_score(t);
            let supportive = score > 0
                || looks_like_name_line(t)
                || looks_like_title_only_line(t)
                || is_signature_cue_line(&core);
            if supportive {
                block_start_pos = Some(pos);
                block_score += score;
                block_lines += 1;
                if has_strong_contact_marker(t) {
                    has_strong_marker = true;
                }
                if is_signature_cue_line(&core) || score > 0 {
                    has_signoff_or_marker = true;
                }
                if looks_like_long_body_sentence(t) {
                    prose_lines += 1;
                }
                continue;
            }
            if block_start_pos.is_some() {
                break;
            }
        }
        if block_score >= 3 && block_lines >= 3 && prose_lines <= 1 && has_signoff_or_marker {
            if let Some(start_pos) = block_start_pos {
                let has_gap = blank_gap_before_pos(start_pos) >= 1;
                if has_gap || has_strong_marker {
                    signature_pos = Some(start_pos);
                }
            }
        }
    }

    if let Some(mut pos) = signature_pos {
        // Guard: keep plain request/prose lines in reply text when they sit just before a sign-off.
        while pos + 1 < non_empty_before_quote.len() {
            let (_idx, s, e) = non_empty_before_quote[pos];
            let cur = get_line((s, e)).trim();
            let cur_core = line_core(cur);
            let cur_is_plain_body = !cur_core.is_empty()
                && !is_signature_cue_line(&cur_core)
                && !looks_like_name_line(cur)
                && !looks_like_title_only_line(cur)
                && !has_soft_contact_marker(cur)
                && !has_strong_contact_marker(cur)
                && cur.split_whitespace().count() >= 4
                && (cur.ends_with('.') || cur.ends_with('!') || cur.ends_with('?'));
            if !cur_is_plain_body {
                break;
            }
            let look_end = (pos + 4).min(non_empty_before_quote.len());
            let mut shifted = false;
            for next_pos in (pos + 1)..look_end {
                let (_nidx, ns, ne) = non_empty_before_quote[next_pos];
                let next = get_line((ns, ne)).trim();
                let next_core = line_core(next);
                if !is_signature_cue_line(&next_core) {
                    let next_is_plain = !looks_like_name_line(next)
                        && !looks_like_title_only_line(next)
                        && !has_soft_contact_marker(next)
                        && !has_strong_contact_marker(next);
                    if !next_is_plain {
                        break;
                    }
                    continue;
                }
                if blank_gap_before_pos(next_pos) >= 1 {
                    pos = next_pos;
                    shifted = true;
                }
                break;
            }
            if shifted {
                continue;
            }
            break;
        }

        // Pull signature start upward for sign-off/name lines immediately above explicit markers.
        while pos > 0 {
            let (_pidx, ps, pe) = non_empty_before_quote[pos - 1];
            let prev = get_line((ps, pe)).trim();
            let prev_core = line_core(prev);
            let prev_supportive = prev_core == "--"
                || prev_core.contains("[image]")
                || is_signature_cue_line(&prev_core)
                || looks_like_name_line(prev)
                || looks_like_title_only_line(prev)
                || has_soft_contact_marker(prev)
                || has_strong_contact_marker(prev);
            if looks_like_long_body_sentence(prev) {
                break;
            }
            if is_signature_cue_line(&prev_core)
                || looks_like_name_line(prev)
                || prev_supportive
                || prev_core.starts_with("always here to help")
            {
                pos -= 1;
                continue;
            }
            break;
        }
        let (line_start, line_end) = (non_empty_before_quote[pos].1, non_empty_before_quote[pos].2);
        let line = get_line((line_start, line_end));
        let inline_start = inline_signature_start_offset(line).map(|off| line_start + off);
        signature_start = Some(inline_start.unwrap_or(line_start));
    }

    if let (Some(sig), Some(dis)) = (signature_start, disclaimer_start)
        && dis < sig
    {
        disclaimer_start = None;
    }

    if signature_start.is_none() {
        let footer_scan_end = disclaimer_start.unwrap_or(quote_byte_start);
        let prequote = text.get(0..footer_scan_end).unwrap_or("");
        let lower_full = prequote.to_ascii_lowercase();
        let footer_markers = [
            "all rights reserved",
            "manage your notification settings",
            "unsubscribe",
            "do not reply",
            "please do not reply",
            "notification settings",
            "copyright",
        ];
        let marker_hits = |s: &str| footer_markers.iter().filter(|m| s.contains(**m)).count();

        if let Some(cut) = footer_markers
            .iter()
            .filter_map(|m| lower_full.find(m))
            .filter(|idx| *idx > prequote.len() / 3)
            .min()
        {
            let suffix_lower = &lower_full[cut..];
            let tail = prequote[cut..].trim();
            let head = prequote[..cut].trim();
            if (marker_hits(suffix_lower) >= 2
                || (marker_hits(suffix_lower) >= 1
                    && (suffix_lower.contains("http://") || suffix_lower.contains("https://"))))
                && head.len() >= 24
                && tail.len() >= 24
            {
                signature_start = Some(cut);
            }
        }
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
