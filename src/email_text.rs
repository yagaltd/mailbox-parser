use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmailBlockKind {
    Reply,
    Quoted,
    Forwarded,
    Signature,
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

    let is_forward_marker_line = |t: &str| {
        let tl = t.trim().to_ascii_lowercase();
        tl.starts_with("---- forwarded message ----")
            || tl.starts_with("-----forwarded message-----")
            || tl.starts_with("begin forwarded message")
            || tl.contains("forwarded message")
    };

    let is_quote_marker_line = |t: &str| {
        let tl = t.trim().to_ascii_lowercase();
        tl.starts_with("-----original message-----")
            || tl.starts_with("----original message----")
            || (tl.starts_with("on ") && tl.contains(" wrote:"))
            || tl.starts_with("from:") && tl.contains("sent:")
    };

    let looks_like_header_bundle = |idx: usize| -> bool {
        // Detect Outlook/Gmail-style inline headers in the body.
        // From: ...
        // Sent: ...
        // To: ...
        // Subject: ...
        let mut hits = 0usize;
        let end = (idx + 10).min(lines.len());
        for j in idx..end {
            let line = get_line(lines[j]);
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            let tl = t.to_ascii_lowercase();
            if tl.starts_with("from:")
                || tl.starts_with("sent:")
                || tl.starts_with("date:")
                || tl.starts_with("to:")
                || tl.starts_with("cc:")
                || tl.starts_with("subject:")
                || tl.starts_with("message-id:")
            {
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
            let tl = t.to_ascii_lowercase();
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

        let tl = t.to_ascii_lowercase();
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

    // Signature delimiter before quoted history.
    let mut signature_start: Option<usize> = None;
    let quote_byte_start = quote_start.as_ref().map(|(s, _)| *s).unwrap_or(bytes.len());
    for (idx, (s, e)) in lines.iter().copied().enumerate() {
        if s >= quote_byte_start {
            break;
        }
        let t = get_line((s, e)).trim();
        if t == "--" || t == "-- " {
            // Only treat it as signature if it is near the end of the reply.
            let remaining = lines.len().saturating_sub(idx);
            if remaining <= 25 {
                signature_start = Some(s);
                break;
            }
        }
    }

    let mut out = Vec::new();

    let body_end = bytes.len();
    let quoted_kind = quote_start.as_ref().map(|(_, k)| k.clone());
    let quote_byte_start = quote_start.as_ref().map(|(s, _)| *s);

    let reply_end = signature_start.or(quote_byte_start).unwrap_or(body_end);

    let reply_end = reply_end.min(body_end);
    if reply_end > 0 {
        out.push(EmailBlock {
            kind: EmailBlockKind::Reply,
            byte_start: 0,
            byte_end: reply_end,
        });
    }

    if let Some(sig_start) = signature_start {
        let sig_end = quote_byte_start.unwrap_or(body_end);
        if sig_end > sig_start {
            out.push(EmailBlock {
                kind: EmailBlockKind::Signature,
                byte_start: sig_start,
                byte_end: sig_end,
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
