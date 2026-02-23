use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result, anyhow};

use crate::{
    MailMessage, MailboxScanError, MailboxScanMessage, MailboxScanReport, ParseRfc822Options,
    parse_rfc822_headers, parse_rfc822_with_options,
};

#[derive(Clone, Debug)]
pub struct MboxReadOptions {
    pub strict: bool,
    pub max_messages: Option<usize>,
}

impl Default for MboxReadOptions {
    fn default() -> Self {
        Self {
            strict: false,
            max_messages: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MboxParseOptions {
    pub strict: bool,
    pub max_messages: Option<usize>,
    pub fail_fast: bool,
    pub owner_emails: Vec<String>,
}

impl Default for MboxParseOptions {
    fn default() -> Self {
        Self {
            strict: false,
            max_messages: None,
            fail_fast: false,
            owner_emails: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MboxMessage {
    pub raw: Vec<u8>,
    pub from_line: Option<String>,
    pub separator_date: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MboxParseError {
    pub index: usize,
    pub error: String,
    pub from_line: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MboxParseReport {
    pub messages: Vec<MailMessage>,
    pub errors: Vec<MboxParseError>,
}

pub fn iter_mbox_messages(
    path: &Path,
    options: MboxReadOptions,
) -> Result<MboxMessageIter<BufReader<File>>> {
    let file = File::open(path).with_context(|| format!("open mbox {}", path.display()))?;
    let reader = BufReader::new(file);
    Ok(MboxMessageIter::new(reader, options))
}

pub fn parse_mbox_file(path: &Path, options: MboxParseOptions) -> Result<MboxParseReport> {
    let mut report = MboxParseReport {
        messages: Vec::new(),
        errors: Vec::new(),
    };

    let iter = iter_mbox_messages(
        path,
        MboxReadOptions {
            strict: options.strict,
            max_messages: options.max_messages,
        },
    )?;

    for (idx, item) in iter.enumerate() {
        match item {
            Ok(msg) => match parse_rfc822_with_options(
                &msg.raw,
                &ParseRfc822Options {
                    owner_emails: options.owner_emails.clone(),
                },
            ) {
                Ok(parsed) => {
                    let internal_date = parsed.date.clone().or(msg.separator_date.clone());
                    report.messages.push(MailMessage {
                        uid: None,
                        internal_date,
                        flags: Vec::new(),
                        parsed,
                        raw: msg.raw,
                    });
                }
                Err(err) => {
                    if options.fail_fast {
                        return Err(err.context(format!("parse rfc822 at message {idx}")));
                    }
                    report.errors.push(MboxParseError {
                        index: idx,
                        error: err.to_string(),
                        from_line: msg.from_line.clone(),
                    });
                }
            },
            Err(err) => {
                if options.fail_fast {
                    return Err(err.context(format!("read mbox message {idx}")));
                }
                report.errors.push(MboxParseError {
                    index: idx,
                    error: err.to_string(),
                    from_line: None,
                });
            }
        }
    }

    Ok(report)
}

pub fn scan_mbox_headers(path: &Path, options: MboxReadOptions) -> Result<MailboxScanReport> {
    scan_mbox_headers_inner(path, options, None)
}

pub fn scan_mbox_headers_with_progress<F: FnMut(usize, f64)>(
    path: &Path,
    options: MboxReadOptions,
    progress_every: usize,
    mut on_progress: F,
) -> Result<MailboxScanReport> {
    scan_mbox_headers_inner(
        path,
        options,
        Some((&mut on_progress, progress_every.max(1))),
    )
}

pub fn scan_mbox_file_headers_only(
    path: &Path,
    options: MboxReadOptions,
) -> Result<MailboxScanReport> {
    scan_mbox_headers(path, options)
}

fn scan_mbox_headers_inner(
    path: &Path,
    options: MboxReadOptions,
    mut progress: Option<(&mut dyn FnMut(usize, f64), usize)>,
) -> Result<MailboxScanReport> {
    let mut report = MailboxScanReport::default();

    let t0 = std::time::Instant::now();

    let iter = iter_mbox_messages(path, options)?;

    for (idx, item) in iter.enumerate() {
        match item {
            Ok(msg) => match parse_rfc822_headers(&msg.raw) {
                Ok(headers) => {
                    let size = msg.raw.len().min(u32::MAX as usize) as u32;
                    report.messages.push(MailboxScanMessage {
                        uid: None,
                        internal_date: msg.separator_date.clone(),
                        rfc822_size: Some(size),
                        mailbox: None,
                        headers,
                    });

                    if let Some((cb, every)) = progress.as_mut() {
                        if report.messages.len() % *every == 0 {
                            let elapsed = t0.elapsed().as_secs_f64().max(0.001);
                            let rate = (report.messages.len() as f64) / elapsed;
                            (cb)(report.messages.len(), rate);
                        }
                    }
                }
                Err(err) => {
                    report.errors.push(MailboxScanError {
                        source: format!("mbox:{}#{}", path.display(), idx),
                        error: err.to_string(),
                    });
                }
            },
            Err(err) => {
                report.errors.push(MailboxScanError {
                    source: format!("mbox:{}#{}", path.display(), idx),
                    error: err.to_string(),
                });
            }
        }
    }

    if let Some((cb, _every)) = progress.as_mut() {
        let elapsed = t0.elapsed().as_secs_f64().max(0.001);
        let rate = (report.messages.len() as f64) / elapsed;
        (cb)(report.messages.len(), rate);
    }

    Ok(report)
}

pub struct MboxMessageIter<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    current: Vec<u8>,
    current_from_line: Option<String>,
    current_separator_date: Option<String>,
    strict: bool,
    max_messages: Option<usize>,
    yielded: usize,
    in_headers: bool,
    done: bool,
}

impl<R: BufRead> MboxMessageIter<R> {
    fn new(reader: R, options: MboxReadOptions) -> Self {
        Self {
            reader,
            buf: Vec::with_capacity(1024),
            current: Vec::new(),
            current_from_line: None,
            current_separator_date: None,
            strict: options.strict,
            max_messages: options.max_messages,
            yielded: 0,
            in_headers: true,
            done: false,
        }
    }

    fn start_message_from_separator(&mut self, line: &[u8]) {
        let raw = String::from_utf8_lossy(line);
        let trimmed = raw.trim_end_matches(['\r', '\n']).to_string();
        self.current_from_line = Some(trimmed.clone());
        self.current_separator_date = parse_mbox_separator_date(&trimmed);
        self.current.clear();
        self.in_headers = true;
    }

    fn finish_message(&mut self) -> MboxMessage {
        let raw = std::mem::take(&mut self.current);
        let from_line = self.current_from_line.take();
        let separator_date = self.current_separator_date.take();
        self.in_headers = true;
        MboxMessage {
            raw,
            from_line,
            separator_date,
        }
    }

    fn should_stop(&self) -> bool {
        if let Some(max) = self.max_messages {
            return self.yielded >= max;
        }
        false
    }
}

impl<R: BufRead> Iterator for MboxMessageIter<R> {
    type Item = Result<MboxMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.should_stop() {
            self.done = true;
            return None;
        }

        loop {
            self.buf.clear();
            match self.reader.read_until(b'\n', &mut self.buf) {
                Ok(0) => {
                    self.done = true;
                    if self.current_from_line.is_some() && !self.should_stop() {
                        self.yielded += 1;
                        return Some(Ok(self.finish_message()));
                    }
                    return None;
                }
                Ok(_) => {
                    if is_mbox_separator_line(&self.buf, self.strict) {
                        if self.current_from_line.is_some() {
                            let msg = self.finish_message();
                            let line = self.buf.clone();
                            self.start_message_from_separator(&line);
                            if self.should_stop() {
                                self.done = true;
                                return None;
                            }
                            self.yielded += 1;
                            return Some(Ok(msg));
                        }
                        let line = self.buf.clone();
                        self.start_message_from_separator(&line);
                        continue;
                    }

                    if self.current_from_line.is_none() {
                        continue;
                    }

                    if self.in_headers && is_blank_line(&self.buf) {
                        self.in_headers = false;
                    }

                    if !self.in_headers && self.buf.starts_with(b">From ") {
                        self.current.extend_from_slice(&self.buf[1..]);
                    } else {
                        self.current.extend_from_slice(&self.buf);
                    }
                }
                Err(err) => {
                    self.done = true;
                    return Some(Err(anyhow!(err).context("read mbox line")));
                }
            }
        }
    }
}

fn is_blank_line(line: &[u8]) -> bool {
    let trimmed = line
        .iter()
        .copied()
        .filter(|b| !matches!(b, b'\r' | b'\n'))
        .collect::<Vec<u8>>();
    trimmed.is_empty()
}

fn is_mbox_separator_line(line: &[u8], strict: bool) -> bool {
    if !line.starts_with(b"From ") {
        return false;
    }
    if !strict {
        return true;
    }
    let raw = String::from_utf8_lossy(line);
    let trimmed = raw.trim_end_matches(['\r', '\n']);
    parse_mbox_separator_date(trimmed).is_some()
}

fn parse_mbox_separator_date(line: &str) -> Option<String> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 7 {
        return None;
    }
    if tokens[0] != "From" {
        return None;
    }
    let date_tokens = &tokens[2..];
    parse_mbox_date_tokens(date_tokens)
}

fn parse_mbox_date_tokens(tokens: &[&str]) -> Option<String> {
    if tokens.len() < 5 {
        return None;
    }
    if let Some(parsed) = parse_mbox_date_tokens_manual(tokens) {
        return Some(parsed);
    }

    let with_tz = if tokens.len() >= 6 {
        Some(tokens[..6].join(" "))
    } else {
        None
    };
    if let Some(s) = with_tz.as_deref() {
        if let Some(parsed) = parse_mbox_date_with_tz(s) {
            return Some(parsed);
        }
    }

    let no_tz = tokens[..5].join(" ");
    parse_mbox_date_no_tz(&no_tz)
}

fn parse_mbox_date_tokens_manual(tokens: &[&str]) -> Option<String> {
    use chrono::{FixedOffset, NaiveDate, TimeZone, Utc};

    let month = parse_month(tokens[1])?;
    let day: u32 = tokens[2].parse().ok()?;
    let (hour, minute, second) = parse_time(tokens[3])?;
    let year: i32 = tokens[4].parse().ok()?;

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let naive = date.and_hms_opt(hour, minute, second)?;

    if tokens.len() >= 6 {
        if let Some(offset_seconds) = parse_tz_offset(tokens[5]) {
            let offset = if offset_seconds >= 0 {
                FixedOffset::east_opt(offset_seconds)?
            } else {
                FixedOffset::west_opt(-offset_seconds)?
            };
            let dt = offset.from_local_datetime(&naive).single()?;
            return Some(dt.to_rfc3339());
        }
    }

    Some(Utc.from_utc_datetime(&naive).to_rfc3339())
}

fn parse_month(token: &str) -> Option<u32> {
    match token.to_ascii_lowercase().as_str() {
        "jan" => Some(1),
        "feb" => Some(2),
        "mar" => Some(3),
        "apr" => Some(4),
        "may" => Some(5),
        "jun" => Some(6),
        "jul" => Some(7),
        "aug" => Some(8),
        "sep" => Some(9),
        "oct" => Some(10),
        "nov" => Some(11),
        "dec" => Some(12),
        _ => None,
    }
}

fn parse_time(token: &str) -> Option<(u32, u32, u32)> {
    let mut parts = token.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    let second = parts.next()?.parse().ok()?;
    Some((hour, minute, second))
}

fn parse_tz_offset(token: &str) -> Option<i32> {
    let token = token.trim();
    if token.eq_ignore_ascii_case("utc") || token.eq_ignore_ascii_case("gmt") {
        return Some(0);
    }
    let (sign, rest) = token.split_at(1);
    let sign = match sign {
        "+" => 1,
        "-" => -1,
        _ => return None,
    };
    let rest = rest.replace(':', "");
    if rest.len() != 4 {
        return None;
    }
    let hours: i32 = rest[0..2].parse().ok()?;
    let minutes: i32 = rest[2..4].parse().ok()?;
    let total = hours * 3600 + minutes * 60;
    Some(sign * total)
}

fn parse_mbox_date_with_tz(s: &str) -> Option<String> {
    use chrono::DateTime;

    const FORMATS: [&str; 4] = [
        "%a %b %e %H:%M:%S %Y %z",
        "%a %b %d %H:%M:%S %Y %z",
        "%a %b %e %H:%M:%S %Y %:z",
        "%a %b %d %H:%M:%S %Y %:z",
    ];
    for fmt in FORMATS {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.to_rfc3339());
        }
    }
    None
}

fn parse_mbox_date_no_tz(s: &str) -> Option<String> {
    use chrono::{NaiveDateTime, TimeZone, Utc};

    const FORMATS: [&str; 2] = ["%a %b %e %H:%M:%S %Y", "%a %b %d %H:%M:%S %Y"];
    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(Utc.from_utc_datetime(&dt).to_rfc3339());
        }
    }
    None
}
