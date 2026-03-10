# Lifecycle Lexicon

The lifecycle lexicon (`config/lifecycle_lexicon.yaml`) provides data-driven detection for service lifecycle events, billing actions, and event hints. It is embedded at build time but can be overridden at runtime.

## Supported Languages

The lexicon supports multilingual token matching:
- English (`en`)
- French (`fr`)
- Spanish (`es`)
- German (`de`)
- Italian (`it`)
- Dutch (`nl`)
- Polish (`pl`)

## Lexicon Sections

### `known_billing_senders`

Domain/keyword signals for known billing providers:
- `stripe`, `kajabi`, `paddle`, `chargebee`, `paypal`, `xero`, `quickbooks`, `invoice`, `billing`

Used to boost confidence for billing-related emails.

### `lifecycle_keyword_patterns`

General keyword patterns for lifecycle events across languages. These are used as fallback signals when specific rules don't match.

### `lifecycle_rules`

Structured rules for classifying lifecycle events. Each rule has:

- `id`: Unique identifier
- `kind`: Event type (e.g., `subscription_canceled`, `subscription_renewed`, `order_confirmation`)
- `priority`: Higher priority rules are evaluated first
- `signal`: Token signal type
- `any` or `all_groups`: Pattern matching logic

**Supported lifecycle kinds:**

| Kind | Description |
|------|-------------|
| `subscription_canceled` | Subscription was canceled |
| `subscription_renewed` | Subscription renewed (auto-renew) |
| `subscription_created` | New subscription / welcome |
| `membership_updated` | Membership plan changed |
| `ticket_confirmation` | Event/booking ticket confirmation |
| `order_confirmation` | Order/purchase confirmation |
| `billing_notice` | General billing notice |

### `billing_action_rules`

Actionable billing URLs and labels detected in emails:

| Action Kind | Description |
|-------------|-------------|
| `view_invoice` | Link to view invoice |
| `pay_now` | Payment link |
| `manage_subscription` | Subscription management |
| `update_payment_method` | Update payment details |
| `billing_portal` | Billing portal link |

### Event Pattern Families

The lexicon also configures event hint detection:

#### Shipping Events
- `event_shipping_intent_patterns`: Shipping-related keywords
- `event_shipping_structure_patterns`: Order/shipment tracking structure
- `event_shipping_hard_structure_patterns`: Strong shipping signals

#### Meeting Events
- `event_meeting_intent_patterns`: Meeting-related keywords
- `event_meeting_invite_verb_patterns`: Invitation verbs

#### Deadline Events
- `event_deadline_patterns`: Deadline keywords (`deadline`, `due`)

#### Availability Events
- `event_availability_patterns`: Availability/new release keywords

#### Reservation Events
- `event_reservation_intent_patterns`: General reservation keywords
- `event_reservation_restaurant_patterns`: Restaurant-specific signals
- `event_reservation_hotel_patterns`: Hotel-specific signals
- `event_reservation_spa_patterns`: Spa/salon signals
- `event_reservation_bar_patterns`: Bar/happy hour signals

#### Marketing Noise Suppression
- `event_marketing_list_noise_patterns`: Patterns to suppress in newsletters (numbered lists like "5 tips", "3 mistakes")

## Runtime Override

### Load Custom Lexicon

```rust
use std::sync::Arc;
use mailbox_parser::{
    load_lifecycle_lexicon_from_yaml, parse_rfc822_with_options, ParseRfc822Options
};

let lex = load_lifecycle_lexicon_from_yaml(std::path::Path::new("custom_lexicon.yaml"))?;
let parsed = parse_rfc822_with_options(
    &std::fs::read("message.eml")?,
    &ParseRfc822Options {
        owner_emails: vec!["owner@example.com".to_string()],
        lifecycle_lexicon: Some(Arc::new(lex)),
    },
)?;
```

### JSONL Overrides

Append-only JSONL allows additive updates without modifying the base YAML:

```rust
let lex = load_lifecycle_lexicon_with_overrides(
    Some(std::path::Path::new("lifecycle_lexicon.yaml")),
    std::path::Path::new("lifecycle_override_ops.jsonl"),
)?;
```

**JSONL line format:**

```json
{"op":"add_pattern","target":"event_meeting_intent_patterns","pattern":"new_pattern","match_mode":"literal"}
```

**Override rules:**
- `op`: Currently only `add_pattern` (add-only)
- `target`: Must match an existing YAML key
- `pattern`: Pattern to add
- `match_mode`: `literal` or `regex`

**Valid targets for overrides:**
- `lifecycle_keyword_patterns`
- `lifecycle_rules` (add new rules)
- `billing_action_rules` (add new actions)
- `event_shipping_intent_patterns`
- `event_meeting_intent_patterns`
- `event_deadline_patterns`
- `event_availability_patterns`
- `event_reservation_intent_patterns`
- `event_marketing_list_noise_patterns`
- And other pattern families

## Service Lifecycle Hints

The parser emits `service_lifecycle_hints` with:
- `kind`: The lifecycle event type
- `confidence`: `low`, `medium`, or `high`
- `extracted_entities`: Key-value pairs extracted (customer, plan, amount, etc.)

## Billing Action Hints

The parser emits `billing_action_hints` even when lifecycle classification is gated (e.g., for newsletter/promo emails):
- `action_kind`: Type of action
- `url`: Action URL
- `label`: Link text/label
