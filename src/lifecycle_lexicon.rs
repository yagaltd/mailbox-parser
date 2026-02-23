use std::collections::HashSet;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Deserialize;

use crate::{BillingActionKind, ReservationType, ServiceLifecycleKind};

const DEFAULT_LEXICON_YAML: &str = include_str!("../config/lifecycle_lexicon.yaml");

#[derive(Clone, Debug)]
pub struct LifecycleRuleMatch {
    pub kind: ServiceLifecycleKind,
    pub signal: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LifecycleLexicon {
    lifecycle_keywords: Vec<PatternMatcher>,
    confirmation_gate_patterns: Vec<PatternMatcher>,
    known_billing_senders: Vec<String>,
    lifecycle_rules: Vec<CompiledLifecycleRule>,
    billing_action_rules: Vec<CompiledBillingActionRule>,
    event_shipping_intent_patterns: Vec<PatternMatcher>,
    event_shipping_structure_patterns: Vec<PatternMatcher>,
    event_shipping_hard_structure_patterns: Vec<PatternMatcher>,
    event_meeting_intent_patterns: Vec<PatternMatcher>,
    event_meeting_invite_verb_patterns: Vec<PatternMatcher>,
    event_deadline_patterns: Vec<PatternMatcher>,
    event_availability_patterns: Vec<PatternMatcher>,
    event_reservation_intent_patterns: Vec<PatternMatcher>,
    event_reservation_restaurant_patterns: Vec<PatternMatcher>,
    event_reservation_hotel_patterns: Vec<PatternMatcher>,
    event_reservation_spa_patterns: Vec<PatternMatcher>,
    event_reservation_salon_patterns: Vec<PatternMatcher>,
    event_reservation_bar_patterns: Vec<PatternMatcher>,
    event_marketing_list_noise_patterns: Vec<PatternMatcher>,
}

#[derive(Clone, Debug)]
enum PatternMatcher {
    Literal(String),
    Regex(Regex),
}

impl PatternMatcher {
    fn matches(&self, text: &str) -> bool {
        match self {
            Self::Literal(s) => contains_literal_with_boundaries(text, s),
            Self::Regex(r) => r.is_match(text),
        }
    }

    fn matches_substring(&self, text: &str) -> bool {
        match self {
            Self::Literal(s) => text.contains(s),
            Self::Regex(r) => r.is_match(text),
        }
    }
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn contains_literal_with_boundaries(text: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let mut start = 0usize;
    while let Some(found) = text[start..].find(needle) {
        let abs = start + found;
        let end = abs + needle.len();
        let left_ok = text[..abs]
            .chars()
            .next_back()
            .is_none_or(|c| !is_word_char(c));
        let right_ok = text[end..].chars().next().is_none_or(|c| !is_word_char(c));
        if left_ok && right_ok {
            return true;
        }
        start = abs + needle.len();
        if start >= text.len() {
            break;
        }
    }
    false
}

#[derive(Clone, Debug)]
struct CompiledLifecycleRule {
    id: String,
    kind: ServiceLifecycleKind,
    priority: i32,
    signal: Option<String>,
    any: Vec<PatternMatcher>,
    all_groups: Vec<Vec<PatternMatcher>>,
}

impl CompiledLifecycleRule {
    fn matches(&self, text: &str) -> bool {
        let any_ok = self.any.is_empty() || self.any.iter().any(|m| m.matches(text));
        if !any_ok {
            return false;
        }
        self.all_groups
            .iter()
            .all(|g| g.iter().any(|m| m.matches(text)))
    }
}

#[derive(Clone, Debug)]
struct CompiledBillingActionRule {
    kind: BillingActionKind,
    patterns: Vec<PatternMatcher>,
}

#[derive(Clone, Debug, Deserialize)]
struct LexiconConfig {
    version: u32,
    #[allow(dead_code)]
    #[serde(default)]
    languages: Vec<String>,
    #[serde(default)]
    known_billing_senders: Vec<String>,
    #[serde(default)]
    lifecycle_keyword_patterns: Vec<PatternEntry>,
    #[serde(default)]
    confirmation_gate_patterns: Vec<PatternEntry>,
    #[serde(default)]
    lifecycle_rules: Vec<LifecycleRuleEntry>,
    #[serde(default)]
    billing_action_rules: Vec<BillingActionRuleEntry>,
    #[serde(default)]
    event_shipping_intent_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_shipping_structure_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_shipping_hard_structure_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_meeting_intent_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_meeting_invite_verb_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_deadline_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_availability_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_intent_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_restaurant_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_hotel_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_spa_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_salon_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_reservation_bar_patterns: Vec<PatternEntry>,
    #[serde(default)]
    event_marketing_list_noise_patterns: Vec<PatternEntry>,
}

#[derive(Clone, Debug, Deserialize)]
struct LifecycleRuleEntry {
    id: String,
    kind: String,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    signal: Option<String>,
    #[serde(default)]
    any: Vec<PatternEntry>,
    #[serde(default)]
    all_groups: Vec<Vec<PatternEntry>>,
}

#[derive(Clone, Debug, Deserialize)]
struct BillingActionRuleEntry {
    id: String,
    action_kind: String,
    #[serde(default)]
    patterns: Vec<PatternEntry>,
}

#[derive(Clone, Debug, Deserialize)]
struct PatternEntry {
    pattern: String,
    #[serde(default)]
    match_mode: MatchMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum MatchMode {
    #[default]
    Literal,
    Regex,
}

pub fn default_lifecycle_lexicon() -> &'static LifecycleLexicon {
    static DEFAULT: OnceLock<LifecycleLexicon> = OnceLock::new();
    DEFAULT.get_or_init(|| {
        LifecycleLexicon::from_yaml_str(DEFAULT_LEXICON_YAML)
            .expect("embedded lifecycle lexicon must be valid")
    })
}

pub fn load_lifecycle_lexicon_from_yaml(path: &Path) -> Result<LifecycleLexicon> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read lifecycle lexicon {}", path.display()))?;
    LifecycleLexicon::from_yaml_str(&raw)
        .with_context(|| format!("parse lifecycle lexicon {}", path.display()))
}

impl LifecycleLexicon {
    pub fn from_yaml_str(raw: &str) -> Result<Self> {
        let cfg: LexiconConfig = serde_yaml::from_str(raw).context("deserialize yaml")?;
        if cfg.version != 1 {
            bail!("unsupported lifecycle lexicon version {}", cfg.version);
        }

        let lifecycle_keywords = compile_patterns(
            "lifecycle_keyword_patterns",
            &cfg.lifecycle_keyword_patterns,
        )?;
        if lifecycle_keywords.is_empty() {
            bail!("lifecycle_keyword_patterns must not be empty");
        }

        let confirmation_gate_patterns = compile_patterns(
            "confirmation_gate_patterns",
            &cfg.confirmation_gate_patterns,
        )?;

        let mut lifecycle_rules = Vec::new();
        let mut lifecycle_ids = HashSet::new();
        for r in cfg.lifecycle_rules {
            if !lifecycle_ids.insert(r.id.clone()) {
                bail!("duplicate lifecycle rule id '{}'", r.id);
            }
            if r.any.is_empty() && r.all_groups.is_empty() {
                bail!("lifecycle rule '{}' must define any or all_groups", r.id);
            }
            let any = compile_patterns(&format!("lifecycle rule '{}' any", r.id), &r.any)?;
            let mut all_groups = Vec::new();
            for (idx, group) in r.all_groups.iter().enumerate() {
                let compiled = compile_patterns(
                    &format!("lifecycle rule '{}' all_groups[{}]", r.id, idx),
                    group,
                )?;
                if compiled.is_empty() {
                    bail!("lifecycle rule '{}' has empty all_groups[{}]", r.id, idx);
                }
                all_groups.push(compiled);
            }
            lifecycle_rules.push(CompiledLifecycleRule {
                id: r.id,
                kind: parse_lifecycle_kind(&r.kind)?,
                priority: r.priority,
                signal: r.signal,
                any,
                all_groups,
            });
        }
        if lifecycle_rules.is_empty() {
            bail!("lifecycle_rules must not be empty");
        }
        lifecycle_rules.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));

        let mut billing_action_rules = Vec::new();
        let mut action_ids = HashSet::new();
        for r in cfg.billing_action_rules {
            if !action_ids.insert(r.id.clone()) {
                bail!("duplicate billing action rule id '{}'", r.id);
            }
            let patterns = compile_patterns(
                &format!("billing action rule '{}' patterns", r.id),
                &r.patterns,
            )?;
            if patterns.is_empty() {
                bail!("billing action rule '{}' must define patterns", r.id);
            }
            billing_action_rules.push(CompiledBillingActionRule {
                kind: parse_billing_action_kind(&r.action_kind)?,
                patterns,
            });
        }
        if billing_action_rules.is_empty() {
            bail!("billing_action_rules must not be empty");
        }

        let event_shipping_intent_patterns = compile_patterns_or_default(
            "event_shipping_intent_patterns",
            &cfg.event_shipping_intent_patterns,
            &[
                "ship",
                "shipment",
                "delivery",
                "pickup",
                "pick up",
                "waybill",
                "air waybill",
                "courier",
                "tracking",
                "label",
                "dhl",
                "fedex",
                "ups",
            ],
        )?;
        let event_shipping_structure_patterns = compile_patterns_or_default(
            "event_shipping_structure_patterns",
            &cfg.event_shipping_structure_patterns,
            &[
                "tracking",
                "track your order",
                "order id",
                "shipment id",
                "waybill",
                "air waybill",
                "awb",
                "delivered",
                "eta",
            ],
        )?;
        let event_shipping_hard_structure_patterns = compile_patterns_or_default(
            "event_shipping_hard_structure_patterns",
            &cfg.event_shipping_hard_structure_patterns,
            &[
                "tracking",
                "track your order",
                "order id",
                "shipment id",
                "waybill",
                "air waybill",
                "awb",
            ],
        )?;
        let event_meeting_intent_patterns = compile_patterns_or_default(
            "event_meeting_intent_patterns",
            &cfg.event_meeting_intent_patterns,
            &[
                "meeting",
                "meet ",
                "visit",
                "onboarding",
                "training",
                "zoom",
                "teams",
                "webex",
                "calendly",
            ],
        )?;
        let event_meeting_invite_verb_patterns = compile_patterns_or_default(
            "event_meeting_invite_verb_patterns",
            &cfg.event_meeting_invite_verb_patterns,
            &["join", "invited", "invite", "scheduled", "appointment", "call"],
        )?;
        let event_deadline_patterns = compile_patterns_or_default(
            "event_deadline_patterns",
            &cfg.event_deadline_patterns,
            &["deadline", "due"],
        )?;
        let event_availability_patterns = compile_patterns_or_default(
            "event_availability_patterns",
            &cfg.event_availability_patterns,
            &[
                "available",
                "availability",
                "now live",
                "now available",
                "disponible",
                "maintenant disponible",
                "ya disponible",
                "jetzt verfügbar",
                "ora disponibile",
                "nu beschikbaar",
                "dostępne",
                "juz dostępne",
            ],
        )?;
        let event_reservation_intent_patterns = compile_patterns_or_default(
            "event_reservation_intent_patterns",
            &cfg.event_reservation_intent_patterns,
            &[
                "reservation",
                "booking",
                "booked",
                "book your",
                "check-in",
                "check in",
                "check-out",
                "check out",
                "table for",
                "covers",
                "confirmation id",
                "réservation",
                "reserva",
                "reservierung",
                "prenotazione",
                "reservering",
                "rezerwac",
            ],
        )?;
        let event_reservation_restaurant_patterns = compile_patterns_or_default(
            "event_reservation_restaurant_patterns",
            &cfg.event_reservation_restaurant_patterns,
            &[
                "restaurant",
                "cafe",
                "dinner",
                "lunch",
                "table",
                "covers",
                "reservation at",
            ],
        )?;
        let event_reservation_hotel_patterns = compile_patterns_or_default(
            "event_reservation_hotel_patterns",
            &cfg.event_reservation_hotel_patterns,
            &[
                "hotel",
                "check-in",
                "check in",
                "check-out",
                "check out",
                "room",
                "suite",
                "stay",
            ],
        )?;
        let event_reservation_spa_patterns = compile_patterns_or_default(
            "event_reservation_spa_patterns",
            &cfg.event_reservation_spa_patterns,
            &["spa", "massage", "wellness", "facial"],
        )?;
        let event_reservation_salon_patterns = compile_patterns_or_default(
            "event_reservation_salon_patterns",
            &cfg.event_reservation_salon_patterns,
            &["salon", "haircut", "barber", "stylist", "appointment with"],
        )?;
        let event_reservation_bar_patterns = compile_patterns_or_default(
            "event_reservation_bar_patterns",
            &cfg.event_reservation_bar_patterns,
            &[" bar ", "cocktail", "happy hour", "pub"],
        )?;
        let event_marketing_list_noise_patterns = compile_patterns_or_default(
            "event_marketing_list_noise_patterns",
            &cfg.event_marketing_list_noise_patterns,
            &[
                "trick",
                "tips",
                "ways",
                "reasons",
                "mistakes",
                "lessons",
                "benefits",
                "strategy",
                "strategies",
                "guide",
                "trending",
                "what's trending",
                "now live",
                "launch",
                "launches",
                "upgrade",
                "conseils",
                "astuces",
                "tendances",
                "disponible maintenant",
                "lanzamiento",
                "consejos",
                "tendencias",
                "ya disponible",
                "tipps",
                "trends",
                "jetzt verfügbar",
                "consigli",
                "tendenze",
                "ora disponibile",
                "nu beschikbaar",
                "porady",
                "trendy",
                "juz dostępne",
            ],
        )?;

        Ok(Self {
            lifecycle_keywords,
            confirmation_gate_patterns,
            known_billing_senders: cfg
                .known_billing_senders
                .into_iter()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect(),
            lifecycle_rules,
            billing_action_rules,
            event_shipping_intent_patterns,
            event_shipping_structure_patterns,
            event_shipping_hard_structure_patterns,
            event_meeting_intent_patterns,
            event_meeting_invite_verb_patterns,
            event_deadline_patterns,
            event_availability_patterns,
            event_reservation_intent_patterns,
            event_reservation_restaurant_patterns,
            event_reservation_hotel_patterns,
            event_reservation_spa_patterns,
            event_reservation_salon_patterns,
            event_reservation_bar_patterns,
            event_marketing_list_noise_patterns,
        })
    }

    pub fn has_lifecycle_keyword(&self, text: &str) -> bool {
        self.lifecycle_keywords.iter().any(|p| p.matches(text))
    }

    pub fn has_confirmation_gate_match(&self, text: &str) -> bool {
        self.confirmation_gate_patterns
            .iter()
            .any(|p| p.matches(text))
    }

    pub fn is_known_billing_sender(&self, sender: &str) -> bool {
        let lower = sender.to_ascii_lowercase();
        self.known_billing_senders.iter().any(|s| lower.contains(s))
    }

    pub fn classify_lifecycle(&self, text: &str) -> Option<LifecycleRuleMatch> {
        self.lifecycle_rules.iter().find_map(|rule| {
            if rule.matches(text) {
                Some(LifecycleRuleMatch {
                    kind: rule.kind.clone(),
                    signal: rule.signal.clone(),
                })
            } else {
                None
            }
        })
    }

    pub fn classify_billing_action_line(&self, lower_line: &str) -> Option<BillingActionKind> {
        self.billing_action_rules.iter().find_map(|rule| {
            if rule.patterns.iter().any(|p| p.matches(lower_line)) {
                Some(rule.kind.clone())
            } else {
                None
            }
        })
    }

    fn any_substring_match(patterns: &[PatternMatcher], lower_text: &str) -> bool {
        patterns.iter().any(|p| p.matches_substring(lower_text))
    }

    pub fn has_event_shipping_intent(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_shipping_intent_patterns, lower_text)
    }

    pub fn has_event_shipping_structure(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_shipping_structure_patterns, lower_text)
    }

    pub fn has_event_shipping_hard_structure(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_shipping_hard_structure_patterns, lower_text)
    }

    pub fn has_event_meeting_intent(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_meeting_intent_patterns, lower_text)
    }

    pub fn has_event_meeting_invite_verb(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_meeting_invite_verb_patterns, lower_text)
    }

    pub fn has_event_deadline_signal(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_deadline_patterns, lower_text)
    }

    pub fn has_event_availability_signal(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_availability_patterns, lower_text)
    }

    pub fn has_event_reservation_intent(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_reservation_intent_patterns, lower_text)
    }

    pub fn classify_reservation_type(&self, lower_text: &str) -> Option<ReservationType> {
        if Self::any_substring_match(&self.event_reservation_restaurant_patterns, lower_text) {
            return Some(ReservationType::Restaurant);
        }
        if Self::any_substring_match(&self.event_reservation_hotel_patterns, lower_text) {
            return Some(ReservationType::Hotel);
        }
        if Self::any_substring_match(&self.event_reservation_spa_patterns, lower_text) {
            return Some(ReservationType::Spa);
        }
        if Self::any_substring_match(&self.event_reservation_salon_patterns, lower_text) {
            return Some(ReservationType::Salon);
        }
        if Self::any_substring_match(&self.event_reservation_bar_patterns, lower_text) {
            return Some(ReservationType::Bar);
        }
        if self.has_event_reservation_intent(lower_text) {
            return Some(ReservationType::Other);
        }
        None
    }

    pub fn has_event_marketing_list_noise(&self, lower_text: &str) -> bool {
        Self::any_substring_match(&self.event_marketing_list_noise_patterns, lower_text)
    }

    #[cfg(test)]
    fn billing_action_rule_kinds(&self) -> Vec<BillingActionKind> {
        self.billing_action_rules
            .iter()
            .map(|r| r.kind.clone())
            .collect()
    }
}

fn compile_patterns(ctx: &str, entries: &[PatternEntry]) -> Result<Vec<PatternMatcher>> {
    let mut out = Vec::new();
    for (idx, entry) in entries.iter().enumerate() {
        let raw = entry.pattern.trim();
        if raw.is_empty() {
            bail!("{} pattern[{}] is empty", ctx, idx);
        }
        let matcher = match entry.match_mode {
            MatchMode::Literal => PatternMatcher::Literal(raw.to_ascii_lowercase()),
            MatchMode::Regex => {
                let re = Regex::new(raw)
                    .with_context(|| format!("{} pattern[{}] invalid regex", ctx, idx))?;
                PatternMatcher::Regex(re)
            }
        };
        out.push(matcher);
    }
    Ok(out)
}

fn compile_patterns_or_default(
    ctx: &str,
    entries: &[PatternEntry],
    defaults: &[&str],
) -> Result<Vec<PatternMatcher>> {
    if entries.is_empty() {
        return Ok(defaults
            .iter()
            .map(|d| PatternMatcher::Literal(d.to_ascii_lowercase()))
            .collect());
    }
    compile_patterns(ctx, entries)
}

fn parse_lifecycle_kind(kind: &str) -> Result<ServiceLifecycleKind> {
    match kind.trim() {
        "subscription_created" => Ok(ServiceLifecycleKind::SubscriptionCreated),
        "subscription_renewed" => Ok(ServiceLifecycleKind::SubscriptionRenewed),
        "subscription_canceled" => Ok(ServiceLifecycleKind::SubscriptionCanceled),
        "membership_updated" => Ok(ServiceLifecycleKind::MembershipUpdated),
        "order_confirmation" => Ok(ServiceLifecycleKind::OrderConfirmation),
        "ticket_confirmation" => Ok(ServiceLifecycleKind::TicketConfirmation),
        "billing_notice" => Ok(ServiceLifecycleKind::BillingNotice),
        "unknown" => Ok(ServiceLifecycleKind::Unknown),
        other => Err(anyhow!("unknown lifecycle kind '{}'", other)),
    }
}

fn parse_billing_action_kind(kind: &str) -> Result<BillingActionKind> {
    match kind.trim() {
        "view_invoice" => Ok(BillingActionKind::ViewInvoice),
        "pay_now" => Ok(BillingActionKind::PayNow),
        "manage_subscription" => Ok(BillingActionKind::ManageSubscription),
        "update_payment_method" => Ok(BillingActionKind::UpdatePaymentMethod),
        "billing_portal" => Ok(BillingActionKind::BillingPortal),
        "unknown" => Ok(BillingActionKind::Unknown),
        other => Err(anyhow!("unknown billing action kind '{}'", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_lexicon_classifies_order_confirmation() {
        let lx = default_lifecycle_lexicon();
        let m = lx.classify_lifecycle("order confirmation for your event ticket");
        assert!(m.is_some());
        assert_eq!(
            m.expect("rule").kind,
            ServiceLifecycleKind::OrderConfirmation
        );
    }

    #[test]
    fn lexicon_rejects_duplicate_rule_ids() {
        let raw = r#"
version: 1
lifecycle_keyword_patterns:
  - pattern: invoice
lifecycle_rules:
  - id: dupe
    kind: billing_notice
    priority: 10
    any:
      - pattern: invoice
  - id: dupe
    kind: billing_notice
    priority: 9
    any:
      - pattern: billing
billing_action_rules:
  - id: a1
    action_kind: view_invoice
    patterns:
      - pattern: invoice
"#;
        let err = LifecycleLexicon::from_yaml_str(raw)
            .expect_err("duplicate IDs must fail")
            .to_string();
        assert!(err.contains("duplicate lifecycle rule id"));
    }

    #[test]
    fn lexicon_rejects_invalid_regex() {
        let raw = r#"
version: 1
lifecycle_keyword_patterns:
  - pattern: invoice
lifecycle_rules:
  - id: r1
    kind: billing_notice
    priority: 10
    any:
      - pattern: "(invoice"
        match_mode: regex
billing_action_rules:
  - id: a1
    action_kind: view_invoice
    patterns:
      - pattern: invoice
"#;
        let err = LifecycleLexicon::from_yaml_str(raw)
            .expect_err("invalid regex must fail")
            .to_string();
        assert!(err.contains("invalid regex"));
    }

    #[test]
    fn default_lexicon_billing_actions_are_loaded() {
        let lx = default_lifecycle_lexicon();
        let kinds = lx.billing_action_rule_kinds();
        assert!(kinds.iter().any(|k| *k == BillingActionKind::ViewInvoice));
        assert!(kinds.iter().any(|k| *k == BillingActionKind::PayNow));
        assert!(
            lx.classify_billing_action_line("see invoice details at https://x")
                .is_some()
        );
    }

    #[test]
    fn literal_match_uses_word_boundaries() {
        assert!(contains_literal_with_boundaries("pay now please", "pay"));
        assert!(!contains_literal_with_boundaries("galapagos", "pago"));
    }
}
