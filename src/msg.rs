//! Outlook MSG (.msg) file parser.
//!
//! Converts MSG (OLE Compound Document) files into the canonical dual output:
//!   - `ParsedEmail` for the canonical JSON / threading pipeline
//!   - RFC 822 bytes for agent-mailbox filesystem storage
//!
//! Uses the `msg_parser` crate under the hood.

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};

use crate::{EmailAddress, ParsedAttachment, ParsedEmail};

/// Error that occurred during MSG parsing.
#[derive(Clone, Debug)]
pub struct MsgParseError {
    /// Path or source description.
    pub source: String,
    /// Error description.
    pub error: String,
}

/// Result of parsing an MSG file — dual output.
pub struct MsgParseResult {
    /// Structured email data for the canonical pipeline.
    pub parsed: ParsedEmail,
    /// RFC 822 serialized bytes for filesystem storage.
    pub rfc822: Vec<u8>,
}

/// Parse an Outlook .msg file from a byte slice.
pub fn parse_msg(bytes: &[u8]) -> Result<MsgParseResult> {
    let msg =
        msg_parser::Outlook::from_slice(bytes).map_err(|e| anyhow!("msg_parser failed: {e}"))?;
    let parsed = msg_to_parsed_email(&msg)?;
    let rfc822 = msg_to_rfc822(&msg)?;
    Ok(MsgParseResult { parsed, rfc822 })
}

/// Parse an Outlook .msg file from a filesystem path.
pub fn parse_msg_file(path: &std::path::Path) -> Result<MsgParseResult> {
    let bytes = std::fs::read(path).map_err(|e| anyhow!("read {}: {e}", path.display()))?;
    parse_msg(&bytes)
}

/// Direct field mapping: `msg_parser::Outlook` → `ParsedEmail`.
fn msg_to_parsed_email(msg: &msg_parser::Outlook) -> Result<ParsedEmail> {
    let subject = if msg.subject.is_empty() {
        None
    } else {
        Some(msg.subject.clone())
    };

    let date = if msg.message_delivery_time.is_empty() {
        if msg.client_submit_time.is_empty() {
            None
        } else {
            Some(msg.client_submit_time.clone())
        }
    } else {
        Some(msg.message_delivery_time.clone())
    };

    let from = if msg.sender.email.is_empty() && msg.sender.name.is_empty() {
        // Fallback: check transport headers for From
        let from_header = msg
            .headers
            .raw
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("from:"))
            .map(|l| {
                let val = l.splitn(2, ':').nth(1).unwrap_or("").trim().to_string();
                parse_address_header(&val)
            })
            .unwrap_or_default();
        from_header
    } else {
        let name = if msg.sender.name.is_empty() || msg.sender.name == msg.sender.email {
            None
        } else {
            Some(msg.sender.name.clone())
        };
        EmailAddress::new(&msg.sender.email, name)
            .into_iter()
            .collect()
    };

    let person_to_email = |p: &msg_parser::Person| -> Option<EmailAddress> {
        if p.email.is_empty() && p.name.is_empty() {
            return None;
        }
        let name = if p.name.is_empty() || p.name == p.email {
            None
        } else {
            Some(p.name.clone())
        };
        EmailAddress::new(&p.email, name)
    };

    let to: Vec<EmailAddress> = msg.to.iter().filter_map(person_to_email).collect();
    let cc: Vec<EmailAddress> = msg.cc.iter().filter_map(person_to_email).collect();
    let bcc: Vec<EmailAddress> = msg.bcc.iter().filter_map(person_to_email).collect();

    let body_text = if msg.body.is_empty() {
        None
    } else {
        Some(msg.body.clone())
    };

    let body_html = if msg.html.is_empty() {
        None
    } else {
        Some(msg.html.clone())
    };

    // Build raw_headers from transport headers
    let raw_headers = parse_raw_headers_text(&msg.headers.raw);

    // Build body_canonical from body text or html
    let body_canonical = crate::build_canonical_body(body_text.as_deref(), body_html.as_deref());

    // Map attachments
    let attachments = msg
        .attachments
        .iter()
        .filter(|a| a.attach_method == 1) // by_value only; skip embedded messages and OLE objects
        .map(|a| {
            let filename = if !a.long_file_name.is_empty() {
                Some(a.long_file_name.clone())
            } else if !a.file_name.is_empty() {
                Some(a.file_name.clone())
            } else if !a.display_name.is_empty() {
                Some(a.display_name.clone())
            } else {
                None
            };

            let mime_type = if !a.mime_tag.is_empty() {
                a.mime_tag.clone()
            } else {
                crate::mime_from_extension(&a.extension)
            };

            let content_id = if a.content_id.is_empty() {
                None
            } else {
                Some(a.content_id.clone())
            };

            let cd = if a.content_id.is_empty() {
                Some("attachment".to_string())
            } else {
                Some("inline".to_string())
            };
            ParsedAttachment {
                filename,
                mime_type,
                size: a.payload_bytes.len(),
                sha256: crate::sha256_hex(&a.payload_bytes),
                content_disposition: cd,
                content_id,
                _bytes: None, // MSG embeds bytes in the RFC 822 MIME output
            }
        })
        .collect();

    // Extract message_id and in_reply_to from transport headers
    let message_id = raw_headers
        .get("message-id")
        .map(|s| crate::normalize_message_id(s))
        .filter(|s| !s.is_empty());
    let in_reply_to = raw_headers
        .get("in-reply-to")
        .map(|s| crate::normalize_message_id(s))
        .filter(|s| !s.is_empty());
    let references = if let Some(raw) = raw_headers.get("references") {
        raw.split(|c: char| c.is_whitespace() || c == ',')
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(|t| crate::normalize_message_id(t))
            .filter(|t| !t.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    Ok(ParsedEmail {
        message_id,
        in_reply_to,
        references,
        subject,
        date,
        date_raw: raw_headers.get("date").cloned(),
        from,
        to,
        cc,
        bcc,
        reply_to: Vec::new(),
        body_text,
        body_html,
        body_canonical,
        attachments,
        forwarded_messages: Vec::new(),
        forwarded_segments: Vec::new(),
        contact_hints: Vec::new(),
        signature_entities: Default::default(),
        attachment_hints: Vec::new(),
        event_hints: Vec::new(),
        mail_kind_hints: Vec::new(),
        direction_hint: None,
        unsubscribe_hints: Vec::new(),
        service_lifecycle_hints: Vec::new(),
        billing_action_hints: Vec::new(),
        raw_headers,
    })
}

/// Assemble RFC 822 bytes from the parsed MSG data.
///
/// Uses `msg_parser`'s transport headers as the header section, then adds
/// body and attachments as MIME parts.
fn msg_to_rfc822(msg: &msg_parser::Outlook) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    // Determine if we have attachments to include
    let attachments: Vec<&msg_parser::Attachment> = msg
        .attachments
        .iter()
        .filter(|a| a.attach_method == 1 && !a.payload_bytes.is_empty())
        .collect();
    let has_attachments = !attachments.is_empty();
    let has_html = !msg.html.is_empty();
    let has_text = !msg.body.is_empty();
    let is_multipart = has_attachments || (has_text && has_html);

    // --- Headers ---
    if !msg.headers.raw.is_empty() {
        // Use transport headers as-is, but ensure they end with \r\n
        let raw = msg.headers.raw.trim_end();
        buf.extend_from_slice(raw.as_bytes());
        if !raw.ends_with('\n') {
            buf.extend_from_slice(b"\r\n");
        } else if !raw.ends_with("\r\n") {
            // Replace trailing \n with \r\n
            buf.pop();
            buf.extend_from_slice(b"\r\n");
        }
    } else {
        // Build minimal headers from MSG properties
        write_header(&mut buf, "From", &format_sender(msg));
        write_header(&mut buf, "To", &format_recipients(&msg.to));
        if !msg.cc.is_empty() {
            write_header(&mut buf, "Cc", &format_recipients(&msg.cc));
        }
        write_header(&mut buf, "Subject", &msg.subject);
        if !msg.message_delivery_time.is_empty() {
            write_header(&mut buf, "Date", &msg.message_delivery_time);
        } else if !msg.client_submit_time.is_empty() {
            write_header(&mut buf, "Date", &msg.client_submit_time);
        }
        if !msg.headers.message_id.is_empty() {
            write_header(&mut buf, "Message-ID", &msg.headers.message_id);
        }
        buf.extend_from_slice(b"MIME-Version: 1.0\r\n");
    };

    // --- Content-Type & Body ---
    if is_multipart {
        let boundary = format!("=_msg_parser_{:016x}", rand_boundary());
        if !msg.headers.raw.is_empty() {
            // Replace or add Content-Type header
            write_header(
                &mut buf,
                "Content-Type",
                &format!("multipart/mixed; boundary=\"{boundary}\""),
            );
        } else {
            write_header(
                &mut buf,
                "Content-Type",
                &format!("multipart/mixed; boundary=\"{boundary}\""),
            );
        }
        buf.extend_from_slice(b"\r\n"); // end of headers

        // -- text/plain part
        if has_text {
            append_boundary_delimiter(&mut buf, &boundary);
            buf.extend_from_slice(b"Content-Type: text/plain; charset=\"utf-8\"\r\n");
            buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
            write_base64_wrapped(&mut buf, msg.body.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        // -- text/html part
        if has_html {
            append_boundary_delimiter(&mut buf, &boundary);
            buf.extend_from_slice(b"Content-Type: text/html; charset=\"utf-8\"\r\n");
            buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
            write_base64_wrapped(&mut buf, msg.html.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        // -- attachment parts
        for att in &attachments {
            append_boundary_delimiter(&mut buf, &boundary);
            let filename = if !att.long_file_name.is_empty() {
                &att.long_file_name
            } else if !att.file_name.is_empty() {
                &att.file_name
            } else {
                &att.display_name
            };
            let mime = if !att.mime_tag.is_empty() {
                &att.mime_tag
            } else {
                "application/octet-stream"
            };

            write_header(
                &mut buf,
                "Content-Type",
                &format!("{mime}; name=\"{filename}\""),
            );
            write_header(
                &mut buf,
                "Content-Disposition",
                &format!("attachment; filename=\"{filename}\""),
            );
            if !att.content_id.is_empty() {
                write_header(&mut buf, "Content-ID", &format!("<{}>", att.content_id));
            }
            buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
            write_base64_wrapped(&mut buf, &att.payload_bytes);
            buf.extend_from_slice(b"\r\n");
        }

        append_closing_boundary(&mut buf, &boundary);
    } else {
        // Single part
        if has_html && !has_text {
            write_header(&mut buf, "Content-Type", "text/html; charset=\"utf-8\"");
            buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
            write_base64_wrapped(&mut buf, msg.html.as_bytes());
        } else {
            // Text only (or empty)
            if has_text {
                write_header(&mut buf, "Content-Type", "text/plain; charset=\"utf-8\"");
                buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
                write_base64_wrapped(&mut buf, msg.body.as_bytes());
            } else {
                buf.extend_from_slice(b"Content-Type: text/plain; charset=\"utf-8\"\r\n");
                buf.extend_from_slice(b"Content-Transfer-Encoding: 7bit\r\n\r\n");
            }
        }
    }

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn write_header(buf: &mut Vec<u8>, name: &str, value: &str) {
    buf.extend_from_slice(name.as_bytes());
    buf.extend_from_slice(b": ");
    buf.extend_from_slice(value.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

fn format_sender(msg: &msg_parser::Outlook) -> String {
    if msg.sender.email.is_empty() && msg.sender.name.is_empty() {
        String::new()
    } else if msg.sender.name.is_empty() || msg.sender.name == msg.sender.email {
        msg.sender.email.clone()
    } else {
        format!("\"{}\" <{}>", msg.sender.name, msg.sender.email)
    }
}

fn format_recipients(recipients: &[msg_parser::Person]) -> String {
    recipients
        .iter()
        .filter(|p| !p.email.is_empty() || !p.name.is_empty())
        .map(|p| {
            if p.name.is_empty() || p.name == p.email {
                p.email.clone()
            } else {
                format!("\"{}\" <{}>", p.name, p.email)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_raw_headers_text(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let normalized = text.replace("\r\n", "\n");
    let mut cur_key = String::new();
    let mut cur_val = String::new();

    let flush = |k: &str, v: &str, out: &mut BTreeMap<String, String>| {
        let k = k.trim().to_ascii_lowercase();
        let v = v.trim();
        if k.is_empty() || v.is_empty() {
            return;
        }
        out.insert(k, v.to_string());
    };

    for line in normalized.lines() {
        let line = line.trim_end();
        if line.is_empty() {
            break; // end of headers
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if !cur_key.is_empty() {
                cur_val.push(' ');
                cur_val.push_str(line.trim());
            }
            continue;
        }
        if !cur_key.is_empty() {
            flush(&cur_key, &cur_val, &mut out);
        }
        cur_key.clear();
        cur_val.clear();
        if let Some((k, v)) = line.split_once(':') {
            cur_key = k.to_string();
            cur_val = v.trim().to_string();
        }
    }
    if !cur_key.is_empty() {
        flush(&cur_key, &cur_val, &mut out);
    }
    out
}

fn parse_address_header(header_value: &str) -> Vec<EmailAddress> {
    let mut out = Vec::new();
    // Split by comma (simple parsing)
    for part in header_value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Try "Name" <email> format
        if let Some(open) = part.rfind('<') {
            if let Some(close) = part[open..].find('>') {
                let email = part[open + 1..open + close].trim();
                let name = part[..open].trim().trim_matches('"').trim();
                if !email.is_empty() {
                    if let Some(addr) = EmailAddress::new(
                        email,
                        if name.is_empty() {
                            None
                        } else {
                            Some(name.to_string())
                        },
                    ) {
                        out.push(addr);
                    }
                    continue;
                }
            }
        }
        // Simple email or name
        let trimmed = part.trim();
        if trimmed.contains('@') {
            if let Some(addr) = EmailAddress::new(trimmed, None) {
                out.push(addr);
            }
        }
    }
    out
}

fn append_boundary_delimiter(buf: &mut Vec<u8>, boundary: &str) {
    buf.extend_from_slice(b"--");
    buf.extend_from_slice(boundary.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

fn append_closing_boundary(buf: &mut Vec<u8>, boundary: &str) {
    buf.extend_from_slice(b"--");
    buf.extend_from_slice(boundary.as_bytes());
    buf.extend_from_slice(b"--\r\n");
}

fn write_base64_wrapped(buf: &mut Vec<u8>, data: &[u8]) {
    let b64 = crate::base64_encode_bytes(data);
    let mut offset = 0;
    while offset < b64.len() {
        let end = std::cmp::min(offset + 76, b64.len());
        buf.extend_from_slice(b64[offset..end].as_bytes());
        buf.extend_from_slice(b"\r\n");
        offset = end;
    }
}

fn rand_boundary() -> u64 {
    // Simple deterministic boundary based on some of the data
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
