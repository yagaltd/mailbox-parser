//! Universal text cleanups applied at the canonicalization boundary so every
//! consumer (JSON, HTML, markdown export, downstream SDKs) sees the same
//! clean text. Source-faithful content is otherwise preserved; only noise
//! that helps nobody is removed: `<mailto:addr>` autolink wrappers (HTML
//! conversion leak) and `&nbsp;` entities.

/// Clean one line: strip mailto wrappers, then decode HTML entities
/// (reuses the crate's full entity decoder).
pub fn clean_text_line(line: &str) -> String {
    crate::decode_html_entities(&strip_mailto_wrappers(line))
}

/// Clean a multi-line text field (per line).
pub fn clean_text(s: &str) -> String {
    s.split('\n')
        .map(clean_text_line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// `<mailto:addr>` autolink wrappers (Outlook style) leak into text bodies.
/// `addr<mailto:addr>` → `addr` (dedup); bare `<mailto:addr>` → `addr`;
/// space-padded `< mailto:addr >` (Apple Mail) → `addr`; bracket-less
/// `mailto:addr` (forwarded headers) → `addr`.
pub fn strip_mailto_wrappers(line: &str) -> String {
    let mut out = line.to_string();
    loop {
        // an '<' whose next non-space chars are 'mailto:' (ASCII-ci match on
        // the original bytes — never lowercase the haystack: İ (U+0130)
        // lowercases to TWO chars and shifts every byte offset)
        let mut probe = 0;
        let open = loop {
            let lt = out.as_bytes()[probe..]
                .iter()
                .position(|b| *b == b'<')
                .map(|p| probe + p);
            let Some(lt) = lt else {
                break None;
            };
            let after = lt + 1;
            let skip = out[after..].len() - out[after..].trim_start_matches(' ').len();
            if ascii_ci_starts_with(&out[after + skip..], "mailto:") {
                break Some(lt);
            }
            probe = after;
        };
        let Some(i) = open else { break };
        let scheme = ascii_ci_find(&out[i..], "mailto:").unwrap() + i + "mailto:".len();
        let Some(close_rel) = out[scheme..].find('>') else {
            break;
        };
        let close = scheme + close_rel;
        let addr = out[scheme..close].trim().to_string();
        let before = &out[..i];
        if before.to_lowercase().ends_with(&addr.to_lowercase()) {
            // duplicated wrapper: drop `<mailto:addr>` entirely
            out = format!("{before}{}", &out[close + 1..]);
        } else {
            // bare wrapper: keep the address, drop the brackets
            out = format!("{before}{addr}{}", &out[close + 1..]);
        }
    }
    // bracket-less leaked scheme prefix (forwarded header lines)
    replace_ci(&out, "mailto:", "")
}

/// ASCII-case-insensitive prefix test on the ORIGINAL bytes — safe for
/// Unicode text (no lowercase remapping, so byte offsets never shift).
fn ascii_ci_starts_with(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// ASCII-case-insensitive find; returns a byte offset at a char boundary.
fn ascii_ci_find(s: &str, needle: &str) -> Option<usize> {
    let mut i = 0;
    while i + needle.len() <= s.len() {
        if ascii_ci_starts_with(&s[i..], needle) {
            return Some(i);
        }
        i += s[i..].chars().next().unwrap().len_utf8();
    }
    None
}

/// `&nbsp;` and other HTML entities leak from HTML conversion and
/// text/plain parts; the crate's full decoder (lib.rs) handles them at the
/// canonical boundary via `clean_text_line`.
pub(crate) fn replace_ci(s: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if ascii_ci_starts_with(&s[i..], from) {
            out.push_str(to);
            i += from.len();
        } else {
            let ch = s[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailto_wrappers_all_forms() {
        assert_eq!(
            strip_mailto_wrappers("support@example.com<mailto:support@example.com>. Learn why"),
            "support@example.com. Learn why"
        );
        assert_eq!(
            strip_mailto_wrappers("Zakaria <z@example.com<mailto:z@example.com>> wrote"),
            "Zakaria <z@example.com> wrote"
        );
        assert_eq!(
            strip_mailto_wrappers("write to <MAILTO:help@example.org> today"),
            "write to help@example.org today"
        );
        assert_eq!(
            strip_mailto_wrappers("From: N G < mailto:nicolas@example.co >"),
            "From: N G nicolas@example.co"
        );
        assert_eq!(
            strip_mailto_wrappers("Email: mailto:dgrivas@example.org – website: x"),
            "Email: dgrivas@example.org – website: x"
        );
        assert_eq!(strip_mailto_wrappers("no wrappers"), "no wrappers");
    }

    /// Turkish İ (U+0130) lowercases to TWO chars — any scanner that slices
    /// a lowercased haystack with original byte offsets panics mid-char.
    /// Found by the 13.6 GB gmail corpus ("İbrahim mutlay replied…").
    #[test]
    fn mailto_scan_survives_unicode_case_expansion() {
        let s = "İ̇brahim mutlay replied to your comment. mailto:İ̇@example.com end";
        let cleaned = strip_mailto_wrappers(s);
        assert!(!cleaned.contains("mailto:"), "{cleaned}");
        assert!(cleaned.starts_with('İ') || cleaned.starts_with('i'));
    }

    #[test]
    fn entities_decode() {
        assert_eq!(crate::decode_html_entities("a&nbsp;b"), "a b");
        assert_eq!(clean_text_line("a&nbsp;b"), "a b");
        assert_eq!(
            clean_text_line("---&nbsp;原始邮件&nbsp;---"),
            "--- 原始邮件 ---"
        );
    }
}
