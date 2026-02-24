use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{CanonicalThread, EmailAddress};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionRow {
    pub thread_id: String,
    pub message_key: String,
    pub subject: String,
    pub date: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub reply_text: String,
    pub mail_kinds: Vec<String>,
    pub event_kinds: Vec<String>,
    pub lifecycle_kinds: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionNode {
    pub id: String,
    pub node_type: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionLink {
    pub source: String,
    pub target: String,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionFacets {
    pub mail_kinds: Vec<String>,
    pub event_kinds: Vec<String>,
    pub lifecycle_kinds: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionStats {
    pub messages: usize,
    pub threads: usize,
    pub nodes: usize,
    pub links: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionDataset {
    pub rows: Vec<ProjectionRow>,
    pub nodes: Vec<ProjectionNode>,
    pub links: Vec<ProjectionLink>,
    pub facets: ProjectionFacets,
    pub stats: ProjectionStats,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ProjectionQuery {
    pub group: String,
    pub layer: String,
    pub subject_contains: String,
    pub date_from: String,
    pub date_to: String,
    pub mail_kind: String,
    pub event_kind: String,
    pub lifecycle_kind: String,
    pub sort: String,
    pub limit: Option<usize>,
}

pub fn rows_from_canonical_threads(threads: &[CanonicalThread]) -> Vec<ProjectionRow> {
    let mut rows = Vec::new();
    for thread in threads {
        for msg in &thread.messages {
            rows.push(ProjectionRow {
                thread_id: thread.thread_id.clone(),
                message_key: msg.message_key.clone(),
                subject: msg.subject.clone().unwrap_or_default(),
                date: msg.date.clone().unwrap_or_default(),
                from: msg.from.iter().map(format_email_address).collect(),
                to: msg.to.iter().map(format_email_address).collect(),
                cc: msg.cc.iter().map(format_email_address).collect(),
                reply_text: msg.reply_text.clone(),
                mail_kinds: msg.mail_kind_hints.iter().map(|h| format!("{:?}", h.kind).to_lowercase()).collect(),
                event_kinds: msg.event_hints.iter().map(|h| format!("{:?}", h.kind).to_lowercase()).collect(),
                lifecycle_kinds: msg
                    .service_lifecycle_hints
                    .iter()
                    .map(|h| format!("{:?}", h.kind).to_lowercase())
                    .collect(),
            });
        }
    }
    rows
}

pub fn apply_query(mut rows: Vec<ProjectionRow>, query: &ProjectionQuery) -> Vec<ProjectionRow> {
    let subject_filter = query.subject_contains.to_ascii_lowercase();
    rows.retain(|row| {
        if !subject_filter.is_empty() && !row.subject.to_ascii_lowercase().contains(&subject_filter)
        {
            return false;
        }
        let date_part = row.date.get(..10).unwrap_or_default();
        if !query.date_from.is_empty()
            && !date_part.is_empty()
            && date_part < query.date_from.as_str()
        {
            return false;
        }
        if !query.date_to.is_empty()
            && !date_part.is_empty()
            && date_part > query.date_to.as_str()
        {
            return false;
        }
        if !query.mail_kind.is_empty() && !row.mail_kinds.iter().any(|v| v == &query.mail_kind) {
            return false;
        }
        if !query.event_kind.is_empty() && !row.event_kinds.iter().any(|v| v == &query.event_kind) {
            return false;
        }
        if !query.lifecycle_kind.is_empty()
            && !row.lifecycle_kinds.iter().any(|v| v == &query.lifecycle_kind)
        {
            return false;
        }
        true
    });

    rows.sort_by(|a, b| match query.sort.as_str() {
        "date_asc" => a.date.cmp(&b.date),
        "subject_asc" => a.subject.cmp(&b.subject),
        "subject_desc" => b.subject.cmp(&a.subject),
        _ => b.date.cmp(&a.date),
    });
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    rows
}

pub fn build_graph(rows: &[ProjectionRow], group: &str, layer: &str) -> (Vec<ProjectionNode>, Vec<ProjectionLink>) {
    let mut nodes = Vec::new();
    let mut links = Vec::new();
    let mut by_node: HashSet<String> = HashSet::new();
    let mut by_link: HashSet<(String, String, String)> = HashSet::new();

    for row in rows {
        let group_key = if group == "subject" {
            normalize_subject(&row.subject)
        } else {
            row.thread_id.clone()
        };
        let thread_id = format!("T:{}", group_key);
        let msg_id = format!("M:{}", row.message_key);
        if by_node.insert(thread_id.clone()) {
            nodes.push(ProjectionNode {
                id: thread_id.clone(),
                node_type: "thread".to_string(),
                label: row.subject.clone(),
                body: None,
                snippet: None,
            });
        }
        if by_node.insert(msg_id.clone()) {
            nodes.push(ProjectionNode {
                id: msg_id.clone(),
                node_type: "message".to_string(),
                label: row.subject.clone(),
                body: Some(row.reply_text.clone()),
                snippet: Some(row.reply_text.chars().take(240).collect()),
            });
        }
        if by_link.insert((thread_id.clone(), msg_id.clone(), "contains".to_string())) {
            links.push(ProjectionLink {
                source: thread_id,
                target: msg_id.clone(),
                kind: "contains".to_string(),
            });
        }
        if layer == "core" {
            continue;
        }
        for participant in row.from.iter().chain(row.to.iter()).chain(row.cc.iter()) {
            let email = parse_address_email(participant);
            if email.is_empty() {
                continue;
            }
            let pid = format!("P:{}", email);
            if by_node.insert(pid.clone()) {
                nodes.push(ProjectionNode {
                    id: pid.clone(),
                    node_type: "person".to_string(),
                    label: email.clone(),
                    body: None,
                    snippet: None,
                });
            }
            if by_link.insert((msg_id.clone(), pid.clone(), "participant".to_string())) {
                links.push(ProjectionLink {
                    source: msg_id.clone(),
                    target: pid,
                    kind: "participant".to_string(),
                });
            }
        }
        if layer != "people" {
            for url in extract_urls(&row.reply_text) {
                let uid = format!("U:{}", url);
                if by_node.insert(uid.clone()) {
                    nodes.push(ProjectionNode {
                        id: uid.clone(),
                        node_type: "url".to_string(),
                        label: url.clone(),
                        body: None,
                        snippet: None,
                    });
                }
                if by_link.insert((msg_id.clone(), uid.clone(), "mentions".to_string())) {
                    links.push(ProjectionLink {
                        source: msg_id.clone(),
                        target: uid,
                        kind: "mentions".to_string(),
                    });
                }
            }
            if !row.date.is_empty() {
                let d = row.date.get(..10).unwrap_or(&row.date).to_string();
                let did = format!("D:{}", d);
                if by_node.insert(did.clone()) {
                    nodes.push(ProjectionNode {
                        id: did.clone(),
                        node_type: "date".to_string(),
                        label: d,
                        body: None,
                        snippet: None,
                    });
                }
                if by_link.insert((msg_id.clone(), did.clone(), "dated".to_string())) {
                    links.push(ProjectionLink {
                        source: msg_id.clone(),
                        target: did,
                        kind: "dated".to_string(),
                    });
                }
            }
        }
    }
    (nodes, links)
}

pub fn facets(rows: &[ProjectionRow]) -> ProjectionFacets {
    let mut mail = BTreeSet::new();
    let mut event = BTreeSet::new();
    let mut lifecycle = BTreeSet::new();
    for row in rows {
        for v in &row.mail_kinds {
            if !v.is_empty() {
                mail.insert(v.clone());
            }
        }
        for v in &row.event_kinds {
            if !v.is_empty() {
                event.insert(v.clone());
            }
        }
        for v in &row.lifecycle_kinds {
            if !v.is_empty() {
                lifecycle.insert(v.clone());
            }
        }
    }
    ProjectionFacets {
        mail_kinds: mail.into_iter().collect(),
        event_kinds: event.into_iter().collect(),
        lifecycle_kinds: lifecycle.into_iter().collect(),
    }
}

pub fn dataset(rows: Vec<ProjectionRow>, query: &ProjectionQuery) -> ProjectionDataset {
    let rows = apply_query(rows, query);
    let (nodes, links) = build_graph(&rows, &query.group, &query.layer);
    let mut thread_ids = HashSet::new();
    for row in &rows {
        thread_ids.insert(row.thread_id.clone());
    }
    ProjectionDataset {
        facets: facets(&rows),
        stats: ProjectionStats {
            messages: rows.len(),
            threads: thread_ids.len(),
            nodes: nodes.len(),
            links: links.len(),
        },
        rows,
        nodes,
        links,
    }
}

fn format_email_address(addr: &EmailAddress) -> String {
    let name = addr.name.as_deref().unwrap_or("").trim();
    if name.is_empty() {
        addr.address.to_string()
    } else {
        format!("{} <{}>", name, addr.address)
    }
}

fn normalize_subject(subject: &str) -> String {
    let mut s = subject.trim().to_string();
    loop {
        let lower = s.to_ascii_lowercase();
        if lower.starts_with("re:") {
            s = s[3..].trim().to_string();
            continue;
        }
        if lower.starts_with("fw:") {
            s = s[3..].trim().to_string();
            continue;
        }
        if lower.starts_with("fwd:") {
            s = s[4..].trim().to_string();
            continue;
        }
        break;
    }
    if s.is_empty() {
        "(no-subject)".to_string()
    } else {
        s
    }
}

fn parse_address_email(input: &str) -> String {
    let trimmed = input.trim();
    if let Some(start) = trimmed.find('<') {
        if let Some(end) = trimmed[start + 1..].find('>') {
            return trimmed[start + 1..start + 1 + end].to_ascii_lowercase();
        }
    }
    trimmed.to_ascii_lowercase()
}

fn extract_urls(text: &str) -> Vec<String> {
    let mut uniq = HashMap::new();
    for token in text.split_whitespace() {
        if token.starts_with("http://") || token.starts_with("https://") {
            let cleaned = token
                .trim_end_matches([',', '.', ';', ':', ')', ']'])
                .to_string();
            uniq.entry(cleaned.clone()).or_insert(cleaned);
        }
    }
    uniq.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_subject_filter_and_sort() {
        let rows = vec![
            ProjectionRow {
                thread_id: "t1".into(),
                message_key: "m1".into(),
                subject: "Re: Invoice".into(),
                date: "2026-01-02T00:00:00Z".into(),
                ..Default::default()
            },
            ProjectionRow {
                thread_id: "t2".into(),
                message_key: "m2".into(),
                subject: "Newsletter".into(),
                date: "2026-01-03T00:00:00Z".into(),
                ..Default::default()
            },
        ];
        let q = ProjectionQuery {
            subject_contains: "invoice".into(),
            sort: "date_desc".into(),
            group: "thread".into(),
            layer: "all".into(),
            ..Default::default()
        };
        let out = apply_query(rows, &q);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].message_key, "m1");
    }

    #[test]
    fn graph_message_node_carries_full_body_and_snippet() {
        let rows = vec![ProjectionRow {
            thread_id: "t1".into(),
            message_key: "m1".into(),
            subject: "Subject".into(),
            date: "2026-01-02T00:00:00Z".into(),
            reply_text: "Line 1\nLine 2".into(),
            ..Default::default()
        }];
        let (nodes, _links) = build_graph(&rows, "thread", "all");
        let message_node = nodes
            .into_iter()
            .find(|n| n.id == "M:m1")
            .expect("message node");
        assert_eq!(message_node.body.as_deref(), Some("Line 1\nLine 2"));
        assert_eq!(message_node.snippet.as_deref(), Some("Line 1\nLine 2"));
    }
}
