//! Outlook PST (.pst) archive parser.
//!
//! Iterates PST messages directly, yielding dual output per message:
//!   - `ParsedEmail` for the canonical pipeline
//!   - RFC 822 bytes for agent-mailbox storage
//!   - Folder name for mailbox tagging
//!
//! Uses the `outlook-pst` crate (Microsoft-authored, MIT licensed).
//!
//! ## Attachment limitation
//!
//! The `outlook-pst` crate's public API returns `Rc<dyn Message>` from
//! `store.open_message()`, which gives access to the attachment *table*
//! (metadata: filename, MIME type, size) but not attachment binary data.
//! Binary extraction requires `Attachment::read()` which needs the concrete
//! `Rc<UnicodeMessage>` type — but `UnicodeStore`, `UnicodeMessage`, and the
//! `read_write` module are private in the crate.
//!
//! Result: PST attachments have metadata in `ParsedEmail` but `sha256` is
//! empty and no MIME parts are embedded in the RFC 822 output. A future
//! upstream change exposing `open_attachment()` on the `Store` trait or
//! making the concrete types public would fix this.

use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;

use anyhow::{Context, Result, anyhow};

use outlook_pst::ltp::prop_context::PropertyValue;
use outlook_pst::ltp::table_context::{
    TableColumnDescriptor, TableContext, TableRowColumnValue, TableRowData,
};
use outlook_pst::messaging::message::Message;
use outlook_pst::messaging::store::{EntryId, Store};
use outlook_pst::ndb::node_id::NodeIdType;

use crate::{EmailAddress, ParsedAttachment, ParsedEmail};

/// Error that occurred during PST parsing.
#[derive(Clone, Debug)]
pub struct PstParseError {
    /// Folder name where the error occurred.
    pub folder: String,
    /// Error description.
    pub error: String,
}

/// A single message extracted from a PST file.
pub struct PstMessage {
    /// Structured email data for the canonical pipeline.
    pub parsed: ParsedEmail,
    /// RFC 822 serialized bytes for filesystem storage.
    pub rfc822: Vec<u8>,
    /// PST folder name (e.g. "Inbox", "Sent Items", "Custom Folder").
    pub folder: String,
}

/// Parse all email messages from a PST file.
///
/// Walks the folder hierarchy, extracts each email message's properties,
/// and yields `PstMessage` per message.
pub fn parse_pst_messages(path: &Path) -> Result<Vec<PstMessage>> {
    let store = outlook_pst::open_store(path)
        .with_context(|| format!("failed to open PST file: {}", path.display()))?;

    let ipm_subtree = store
        .properties()
        .ipm_sub_tree_entry_id()
        .with_context(|| "PST store has no IPM_SUBTREE entry ID")?;

    let mut messages = Vec::new();
    walk_folder_hierarchy(&store, &ipm_subtree, &mut messages)?;

    Ok(messages)
}

/// Recursively walk the folder hierarchy and extract messages from each folder.
fn walk_folder_hierarchy(
    store: &Rc<dyn Store>,
    folder_entry_id: &EntryId,
    messages: &mut Vec<PstMessage>,
) -> Result<()> {
    let folder = store
        .open_folder(folder_entry_id)
        .with_context(|| "failed to open PST folder")?;

    let folder_name = folder
        .properties()
        .display_name()
        .unwrap_or_else(|_| "Unknown".to_string());

    // Read messages in this folder
    if let Some(contents_table) = folder.contents_table() {
        let columns = contents_table.context().columns().to_vec();
        let rows: Vec<&TableRowData> = contents_table.rows_matrix().collect();

        for row in &rows {
            if let Ok(Some(email)) = extract_message_from_row(
                store,
                row,
                &columns,
                contents_table.as_ref(),
                &folder_name,
            ) {
                messages.push(email);
            }
        }
    }

    // Recurse into sub-folders
    if let Some(hierarchy_table) = folder.hierarchy_table() {
        let hcols = hierarchy_table.context().columns().to_vec();
        for hrow in hierarchy_table.rows_matrix() {
            if let Ok(Some(child_entry_id)) =
                row_to_entry_id(hrow, &hcols, hierarchy_table.as_ref())
            {
                let node_type = child_entry_id.node_id().id_type().ok();
                if node_type == Some(NodeIdType::NormalFolder)
                    || node_type == Some(NodeIdType::SearchFolder)
                {
                    if let Err(e) = walk_folder_hierarchy(store, &child_entry_id, messages) {
                        log::warn!("Error walking sub-folder: {e}");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Extract a message from a contents table row.
fn extract_message_from_row(
    store: &Rc<dyn Store>,
    row: &TableRowData,
    columns: &[TableColumnDescriptor],
    tctx: &dyn TableContext,
    folder_name: &str,
) -> Result<Option<PstMessage>> {
    let row_values = row
        .columns(tctx.context())
        .with_context(|| "failed to read table row columns")?;

    let entry_id = extract_entry_id_from_row(&row_values, columns, tctx)?;

    let Some(entry_id) = entry_id else {
        return Ok(None);
    };

    // Verify this is a message entry
    let node_type = entry_id.node_id().id_type()?;
    if node_type != NodeIdType::NormalMessage && node_type != NodeIdType::AssociatedMessage {
        return Ok(None);
    }

    let prop_ids: &[u16] = &[
        0x001A, // MessageClass
        0x0037, // Subject
        0x003E, // InReplyTo
        0x007D, // TransportMessageHeaders
        0x0C1E, // SenderName
        0x0C1F, // SentRepresentingName
        0x0E04, // SenderEmailAddress
        0x1000, // Body
        0x1013, // Html
        0x0E06, // MessageDeliveryTime
        0x3007, // CreationTime
    ];

    let message = match store.open_message(&entry_id, Some(prop_ids)) {
        Ok(msg) => msg,
        Err(e) => {
            log::warn!("Error opening message in folder '{folder_name}': {e}");
            return Ok(None);
        }
    };

    let parsed = pst_msg_to_parsed_email(&message)?;
    let rfc822 = pst_msg_to_rfc822(&message)?;

    Ok(Some(PstMessage {
        parsed,
        rfc822,
        folder: folder_name.to_string(),
    }))
}

/// Build `ParsedEmail` from a PST message's MAPI properties.
fn pst_msg_to_parsed_email(message: &Rc<dyn Message>) -> Result<ParsedEmail> {
    let props = message.properties();
    let raw_headers = read_prop_string(props, 0x007D).unwrap_or_default();
    let parsed_raw_headers = if raw_headers.is_empty() {
        BTreeMap::new()
    } else {
        parse_raw_headers_string(&raw_headers)
    };

    let subject = read_prop_string(props, 0x0037);
    let message_id = parsed_raw_headers
        .get("message-id")
        .map(|s| crate::normalize_message_id(s))
        .filter(|s| !s.is_empty());
    let in_reply_to = parsed_raw_headers
        .get("in-reply-to")
        .map(|s| crate::normalize_message_id(s))
        .filter(|s| !s.is_empty());
    let references = if let Some(raw) = parsed_raw_headers.get("references") {
        raw.split(|c: char| c.is_whitespace() || c == ',')
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(|t| crate::normalize_message_id(t))
            .filter(|t| !t.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    let date =
        read_prop_time_string(props, 0x0E06).or_else(|| read_prop_time_string(props, 0x3007));
    let date_raw = date
        .clone()
        .or_else(|| parsed_raw_headers.get("date").cloned());

    let sender_name = read_prop_string(props, 0x0C1F).or_else(|| read_prop_string(props, 0x0C1E));
    let sender_email = read_prop_string(props, 0x0E04).unwrap_or_default();

    let from = if !sender_email.is_empty() {
        let name = sender_name.filter(|n| !n.is_empty() && *n != sender_email);
        EmailAddress::new(&sender_email, name).into_iter().collect()
    } else if let Some(from_val) = parsed_raw_headers.get("from") {
        parse_address_header(from_val)
    } else {
        Vec::new()
    };

    let (to, cc, bcc) = extract_recipients(message);

    let body_text = read_prop_string(props, 0x1000);
    let body_html = read_prop_string(props, 0x1013);
    let body_canonical = crate::build_canonical_body(body_text.as_deref(), body_html.as_deref());

    // Attachments: metadata only — the outlook-pst crate's public API does not
    // expose binary attachment data via the dyn Message trait object.
    // See module-level doc comment for details.
    let attachments = extract_attachments_metadata(message);

    Ok(ParsedEmail {
        message_id,
        in_reply_to,
        references,
        subject,
        date,
        date_raw,
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
        raw_headers: parsed_raw_headers,
    })
}

/// Assemble RFC 822 bytes from PST message properties.
fn pst_msg_to_rfc822(message: &Rc<dyn Message>) -> Result<Vec<u8>> {
    let props = message.properties();
    let mut buf = Vec::new();

    let transport_headers = read_prop_string(props, 0x007D).unwrap_or_default();
    let subject = read_prop_string(props, 0x0037).unwrap_or_default();
    let body_text = read_prop_string(props, 0x1000).unwrap_or_default();
    let body_html = read_prop_string(props, 0x1013).unwrap_or_default();
    let sender_email = read_prop_string(props, 0x0E04).unwrap_or_default();
    let sender_name = read_prop_string(props, 0x0C1F)
        .or_else(|| read_prop_string(props, 0x0C1E))
        .unwrap_or_default();
    let (to, cc, _bcc) = extract_recipients(message);

    let has_text = !body_text.is_empty();
    let has_html = !body_html.is_empty();
    let is_multipart = has_text && has_html;

    // --- Headers ---
    if !transport_headers.is_empty() {
        let raw = transport_headers.trim_end();
        buf.extend_from_slice(raw.as_bytes());
        if !raw.ends_with('\n') {
            buf.extend_from_slice(b"\r\n");
        } else if !raw.ends_with("\r\n") {
            buf.pop();
            buf.extend_from_slice(b"\r\n");
        }
    }

    ensure_header(
        &mut buf,
        "From",
        &format_sender_pst(&sender_name, &sender_email),
    );
    ensure_header(&mut buf, "To", &format_emails(&to));
    if !cc.is_empty() {
        ensure_header(&mut buf, "Cc", &format_emails(&cc));
    }
    ensure_header(&mut buf, "Subject", &subject);

    let date = read_prop_time_string(props, 0x0E06)
        .or_else(|| read_prop_time_string(props, 0x3007))
        .unwrap_or_default();
    if !date.is_empty() {
        ensure_header(&mut buf, "Date", &date);
    }

    ensure_header(&mut buf, "MIME-Version", "1.0");

    // --- Content-Type & Body ---
    if is_multipart {
        let boundary = format!("=_pst_{:016x}", rand_boundary());
        write_header_pst(
            &mut buf,
            "Content-Type",
            &format!("multipart/mixed; boundary=\"{boundary}\""),
        );
        ensure_blank_line(&mut buf);

        // text/plain part
        append_boundary_delimiter(&mut buf, &boundary);
        buf.extend_from_slice(b"Content-Type: text/plain; charset=\"utf-8\"\r\n");
        buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
        write_base64_wrapped(&mut buf, body_text.as_bytes());
        buf.extend_from_slice(b"\r\n");

        // text/html part
        append_boundary_delimiter(&mut buf, &boundary);
        buf.extend_from_slice(b"Content-Type: text/html; charset=\"utf-8\"\r\n");
        buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
        write_base64_wrapped(&mut buf, body_html.as_bytes());
        buf.extend_from_slice(b"\r\n");

        append_closing_boundary(&mut buf, &boundary);
    } else if has_html && !has_text {
        write_header_pst(&mut buf, "Content-Type", "text/html; charset=\"utf-8\"");
        ensure_blank_line(&mut buf);
        buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
        write_base64_wrapped(&mut buf, body_html.as_bytes());
    } else if has_text {
        write_header_pst(&mut buf, "Content-Type", "text/plain; charset=\"utf-8\"");
        ensure_blank_line(&mut buf);
        buf.extend_from_slice(b"Content-Transfer-Encoding: base64\r\n\r\n");
        write_base64_wrapped(&mut buf, body_text.as_bytes());
    } else {
        ensure_blank_line(&mut buf);
    }

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn read_prop_string(
    props: &outlook_pst::messaging::message::MessageProperties,
    id: u16,
) -> Option<String> {
    let value = props.get(id)?;
    match value {
        PropertyValue::String8(s) => {
            let s = String::from_utf8_lossy(s.buffer());
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        PropertyValue::Unicode(s) => {
            let trimmed = s.to_string().trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }
        _ => None,
    }
}

fn read_prop_time_string(
    props: &outlook_pst::messaging::message::MessageProperties,
    id: u16,
) -> Option<String> {
    let value = props.get(id)?;
    match value {
        PropertyValue::Time(ts) => Some(filetime_to_rfc3339(*ts)),
        PropertyValue::String8(s) => {
            let s = String::from_utf8_lossy(s.buffer()).trim().to_string();
            if s.is_empty() { None } else { Some(s) }
        }
        PropertyValue::Unicode(s) => {
            let s = s.to_string().trim().to_string();
            if s.is_empty() { None } else { Some(s) }
        }
        _ => None,
    }
}

fn filetime_to_rfc3339(filetime: i64) -> String {
    let unix_secs = (filetime / 10_000_000) - 11_644_473_600_i64;
    match chrono::DateTime::from_timestamp(unix_secs, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => format!("FILETIME({filetime})"),
    }
}

fn extract_recipients(
    message: &Rc<dyn Message>,
) -> (Vec<EmailAddress>, Vec<EmailAddress>, Vec<EmailAddress>) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();

    if let Some(recipient_table) = message.recipient_table() {
        for row in recipient_table.rows_matrix() {
            if let Ok(row_values) = row.columns(recipient_table.context()) {
                let email = get_string(&row_values, recipient_table.as_ref(), 0x39FE)
                    .or_else(|| get_string(&row_values, recipient_table.as_ref(), 0x0C1F))
                    .unwrap_or_default();
                let name =
                    get_string(&row_values, recipient_table.as_ref(), 0x3001).unwrap_or_default();
                let recipient_type =
                    get_i32(&row_values, recipient_table.as_ref(), 0x0C15).unwrap_or(1);

                let addr = if email.contains('@') || email.is_empty() {
                    email
                } else {
                    get_string(&row_values, recipient_table.as_ref(), 0x39FE).unwrap_or_default()
                };

                if let Some(parsed) = EmailAddress::new(
                    &addr,
                    if name.is_empty() || name == addr {
                        None
                    } else {
                        Some(name.clone())
                    },
                ) {
                    match recipient_type {
                        2 => cc.push(parsed),
                        3 => bcc.push(parsed),
                        _ => to.push(parsed),
                    }
                }
            }
        }
    }

    (to, cc, bcc)
}

fn extract_attachments_metadata(message: &Rc<dyn Message>) -> Vec<ParsedAttachment> {
    let mut attachments = Vec::new();

    if let Some(attachment_table) = message.attachment_table() {
        for row in attachment_table.rows_matrix() {
            if let Ok(row_values) = row.columns(attachment_table.context()) {
                let filename =
                    get_string(&row_values, attachment_table.as_ref(), 0x3707).unwrap_or_default();
                let long_filename =
                    get_string(&row_values, attachment_table.as_ref(), 0x370E).unwrap_or_default();
                let mime_tag =
                    get_string(&row_values, attachment_table.as_ref(), 0x3704).unwrap_or_default();
                let ext =
                    get_string(&row_values, attachment_table.as_ref(), 0x3714).unwrap_or_default();
                let content_id = get_string(&row_values, attachment_table.as_ref(), 0x3712);

                let display_name = if !long_filename.is_empty() {
                    Some(long_filename.clone())
                } else if !filename.is_empty() {
                    Some(filename.clone())
                } else {
                    None
                };

                let mime = if !mime_tag.is_empty() {
                    mime_tag.clone()
                } else {
                    crate::mime_from_extension(&ext)
                };

                let size =
                    get_i32(&row_values, attachment_table.as_ref(), 0x0E20).unwrap_or(0) as usize;

                attachments.push(ParsedAttachment {
                    filename: display_name,
                    mime_type: mime,
                    size,
                    sha256: String::new(), // No binary access via dyn Message trait
                    content_id,
                    content_disposition: None,
                    _bytes: None,
                });
            }
        }
    }

    attachments
}

fn get_string(
    row_values: &[Option<TableRowColumnValue>],
    tctx: &dyn TableContext,
    prop_id: u16,
) -> Option<String> {
    for (i, col) in tctx.context().columns().iter().enumerate() {
        if col.prop_id() == prop_id {
            let val = row_values.get(i)?;
            if let Some(v) = val {
                if let Ok(pv) = tctx.read_column(v, col.prop_type()) {
                    return match pv {
                        PropertyValue::String8(s) => {
                            let s = String::from_utf8_lossy(s.buffer()).trim().to_string();
                            if s.is_empty() { None } else { Some(s) }
                        }
                        PropertyValue::Unicode(s) => {
                            let s = s.to_string().trim().to_string();
                            if s.is_empty() { None } else { Some(s) }
                        }
                        _ => None,
                    };
                }
            }
            return None;
        }
    }
    None
}

fn get_i32(
    row_values: &[Option<TableRowColumnValue>],
    tctx: &dyn TableContext,
    prop_id: u16,
) -> Option<i32> {
    for (i, col) in tctx.context().columns().iter().enumerate() {
        if col.prop_id() == prop_id {
            let val = row_values.get(i)?;
            if let Some(v) = val {
                if let Ok(pv) = tctx.read_column(v, col.prop_type()) {
                    if let PropertyValue::Integer32(val) = pv {
                        return Some(val);
                    }
                }
            }
            return None;
        }
    }
    None
}

fn extract_entry_id_from_row(
    row_values: &[Option<TableRowColumnValue>],
    columns: &[TableColumnDescriptor],
    tctx: &dyn TableContext,
) -> Result<Option<EntryId>> {
    for (i, col) in columns.iter().enumerate() {
        if col.prop_id() == 0x0FFF {
            let cell = match row_values.get(i) {
                Some(Some(v)) => v,
                _ => continue,
            };
            if let Ok(pv) = tctx.read_column(cell, col.prop_type()) {
                if let PropertyValue::Binary(bin) = pv {
                    let entry_id = EntryId::try_from(bin.buffer())
                        .map_err(|e: std::io::Error| anyhow!("{e}"))?;
                    return Ok(Some(entry_id));
                }
            }
        }
    }
    Ok(None)
}

fn row_to_entry_id(
    row: &TableRowData,
    columns: &[TableColumnDescriptor],
    tctx: &dyn TableContext,
) -> Result<Option<EntryId>> {
    let row_values = row
        .columns(tctx.context())
        .with_context(|| "failed to read hierarchy table row")?;
    extract_entry_id_from_row(&row_values, columns, tctx)
}

// ---------------------------------------------------------------------------
// RFC 822 assembly helpers
// ---------------------------------------------------------------------------

fn ensure_blank_line(buf: &mut Vec<u8>) {
    if !buf.ends_with(b"\r\n\r\n") {
        if buf.ends_with(b"\r\n") {
            buf.extend_from_slice(b"\r\n");
        } else {
            buf.extend_from_slice(b"\r\n\r\n");
        }
    }
}

fn write_header_pst(buf: &mut Vec<u8>, name: &str, value: &str) {
    let search = format!("{}:", name.to_ascii_lowercase());
    if !has_header(buf, &search) {
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(value.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
}

fn ensure_header(buf: &mut Vec<u8>, name: &str, value: &str) {
    let search = format!("{}:", name.to_ascii_lowercase());
    if !has_header(buf, &search) && !value.is_empty() {
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(value.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
}

fn has_header(buf: &[u8], lowercase_name_colon: &str) -> bool {
    let text = String::from_utf8_lossy(buf);
    for line in text.lines() {
        if line
            .trim_start()
            .to_ascii_lowercase()
            .starts_with(lowercase_name_colon)
        {
            return true;
        }
    }
    false
}

fn format_sender_pst(name: &str, email: &str) -> String {
    if email.is_empty() && name.is_empty() {
        String::new()
    } else if name.is_empty() || name == email {
        email.to_string()
    } else {
        format!("\"{}\" <{}>", name, email)
    }
}

fn format_emails(addrs: &[EmailAddress]) -> String {
    addrs
        .iter()
        .map(|a| {
            if let Some(ref name) = a.name {
                if !name.is_empty() && *name != a.address {
                    format!("\"{}\" <{}>", name, a.address)
                } else {
                    a.address.clone()
                }
            } else {
                a.address.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_raw_headers_string(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let normalized = text.replace("\r\n", "\n");
    let mut cur_key = String::new();
    let mut cur_val = String::new();

    for line in normalized.lines() {
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if !cur_key.is_empty() {
                cur_val.push(' ');
                cur_val.push_str(line.trim());
            }
            continue;
        }
        if !cur_key.is_empty() {
            let k = cur_key.trim().to_ascii_lowercase();
            let v = cur_val.trim();
            if !k.is_empty() && !v.is_empty() {
                out.insert(k, v.to_string());
            }
        }
        cur_key.clear();
        cur_val.clear();
        if let Some((k, v)) = line.split_once(':') {
            cur_key = k.to_string();
            cur_val = v.trim().to_string();
        }
    }
    if !cur_key.is_empty() {
        let k = cur_key.trim().to_ascii_lowercase();
        let v = cur_val.trim();
        if !k.is_empty() && !v.is_empty() {
            out.insert(k, v.to_string());
        }
    }
    out
}

fn parse_address_header(header_value: &str) -> Vec<EmailAddress> {
    let mut out = Vec::new();
    for part in header_value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
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
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
