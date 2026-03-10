# Local LLM Review Roadmap

## Goal

Add an offline local-LLM review workflow around `mailbox-parser` without turning the parser itself into a probabilistic system.

The parser remains the source of truth for:

- `reply_text`
- `quoted_blocks`
- `forwarded_blocks`
- `salutation`
- `signature`
- `contact_hints`
- `event_hints`
- `mail_kind_hints`
- `service_lifecycle_hints`

The local LLM is used only for:

- reviewing medium/low-confidence parser outputs
- producing structured issue/topic/entity summaries from extracted `reply_text`
- proposing candidate lexicon additions for human review

The local LLM must never write production YAML directly.

## Non-Goals

- replacing deterministic segmentation in `src/email_text.rs`
- replacing lifecycle/event/mail-kind heuristics in `src/lib.rs`
- auto-appending regex or YAML rules without review
- using unstructured free-text LLM output in the main parse path

## High-Level Architecture

1. `mailbox-parser` parses mailboxes deterministically.
2. Canonical JSON output is written as usual.
3. A separate offline reviewer selects only messages worth inspecting.
4. A local LLM receives a strict JSON prompt payload.
5. The LLM returns strict JSON only.
6. Reviewer code validates the JSON against schema rules.
7. Accepted suggestions are written to review artifacts, not production config.
8. A human approves recurring candidate patterns.
9. Approved patterns are converted into YAML or JSONL overrides and backed by regression tests.

## Selection Strategy

Do not send every message to the LLM. Send only a review subset.

### Review candidates

- `reply_text` is empty but `body_canonical` is non-empty
- `reply_text` is very short and `body_canonical` is much longer
- `reply_text` still contains obvious quote/header/footer leakage
- `mail_kind_hints` contains only `low` or no primary kind
- `service_lifecycle_hints` contains only `low` or is empty for messages with billing/order/ticket cues
- `event_hints` is empty for messages with schedule-like tokens
- sender/domain clusters produce repeated parser misses in `.test/` review

### Skip candidates

- high-confidence deterministic outputs
- obvious newsletters/promotions already classified correctly
- giant bodies with no downstream business value
- messages already reviewed with the same parser fingerprint

## Data Model

All review artifacts should be line-delimited JSON for append-only auditing.

Recommended files:

- `.review/jobs.jsonl`
- `.review/results.jsonl`
- `.review/pattern_candidates.jsonl`
- `.review/approved_override_ops.jsonl`

## JSON Schema

### 1. Review Job

One input item sent to the offline local-LLM reviewer.

```json
{
  "schema_version": "1.0",
  "job_id": "sha256:...",
  "created_at": "2026-03-10T00:00:00Z",
  "parser_version": "mailbox-parser@0.1.0",
  "source": {
    "kind": "imap|mbox|dir",
    "mailbox": "INBOX",
    "path": "/abs/path/to/file.mbox"
  },
  "message": {
    "thread_id": "tid",
    "message_key": "k1",
    "message_id": "<abc@example.com>",
    "subject": "Re: Billing issue",
    "date": "2026-03-09T12:00:00Z",
    "from": [{"name": "Alice", "address": "alice@example.com"}],
    "to": [{"name": "Support", "address": "support@example.com"}],
    "sender_domain_hint": {
      "role": "from",
      "email": "alice@example.com",
      "domain": "example.com",
      "bucket": "company"
    }
  },
  "parser_output": {
    "body_canonical": "full cleaned body",
    "reply_text": "top-level extracted reply",
    "quoted_blocks": ["..."],
    "forwarded_blocks": [],
    "disclaimer_blocks": [],
    "salutation": "Hi team,",
    "signature": "Alice",
    "contact_hints": [],
    "signature_entities": {
      "emails": [],
      "phones": [],
      "urls": [],
      "org": null,
      "title": null,
      "address_lines": []
    },
    "event_hints": [],
    "mail_kind_hints": [
      {"kind": "transactional", "confidence": "low"}
    ],
    "service_lifecycle_hints": [],
    "billing_action_hints": []
  },
  "review_reasons": [
    "mail_kind_low_confidence",
    "possible_billing_message_without_lifecycle_hint",
    "reply_text_contains_possible_footer_leakage"
  ],
  "review_policy": {
    "allow_reply_text_repair": true,
    "allow_issue_classification": true,
    "allow_entity_extraction": true,
    "allow_pattern_suggestions": true,
    "max_suggested_patterns": 8
  }
}
```

### 2. Review Result

Strict LLM output. This is advisory. It does not modify parser output directly.

```json
{
  "schema_version": "1.0",
  "job_id": "sha256:...",
  "model": {
    "name": "qwen2.5-7b-instruct",
    "runtime": "llama.cpp",
    "prompt_version": "review-v1"
  },
  "reply_text_review": {
    "status": "keep|suspect|repair_candidate",
    "reason_codes": [
      "signature_leakage",
      "quoted_header_leakage"
    ],
    "candidate_reply_text": "Please cancel the renewal for next month.",
    "evidence_spans": [
      {
        "text": "Best regards,\nAlice\nAcme Corp",
        "label": "signature",
        "start": 42,
        "end": 70
      }
    ]
  },
  "issue_annotation": {
    "issue_kind": "billing_problem|cancel_request|refund_request|technical_problem|account_access|feature_request|general_question|unknown",
    "issue_topic": "subscription renewal",
    "summary": "Customer asks support to stop the next subscription renewal.",
    "sentiment": "negative|neutral|positive|mixed",
    "urgency": "low|medium|high|unknown",
    "confidence": "low|medium|high"
  },
  "entities": [
    {
      "label": "product_or_service",
      "text": "Pro Annual Plan",
      "normalized": "pro annual plan",
      "confidence": "medium"
    },
    {
      "label": "billing_term",
      "text": "renewal",
      "normalized": "renewal",
      "confidence": "high"
    }
  ],
  "parser_assessment": {
    "mail_kind_override_candidate": {
      "kind": "transactional",
      "confidence": "medium",
      "reason": "Billing/account message with explicit support request"
    },
    "lifecycle_override_candidate": {
      "kind": "billing_notice|subscription_canceled|subscription_renewed|subscription_created|membership_updated|ticket_confirmation|order_confirmation|unknown",
      "confidence": "low|medium|high",
      "reason": "LLM explanation tied to body evidence"
    }
  },
  "pattern_suggestions": [
    {
      "target": "lifecycle_keyword_patterns",
      "pattern_type": "literal",
      "pattern": "stop the renewal",
      "evidence": [
        "Please stop the renewal for next month"
      ],
      "rationale": "Recurring cancel intent phrase not covered by current lexicon"
    }
  ],
  "review_notes": [
    "No automatic parser change requested"
  ]
}
```

### 3. Pattern Candidate Artifact

Produced by reviewer code after validation and normalization. This is not yet an approved parser override.

```json
{
  "schema_version": "1.0",
  "candidate_id": "sha256:...",
  "created_at": "2026-03-10T00:00:00Z",
  "source_job_id": "sha256:...",
  "target": "lifecycle_keyword_patterns",
  "pattern": "stop the renewal",
  "match_mode": "literal",
  "review_status": "pending|accepted|rejected",
  "support_count": 3,
  "examples": [
    "Please stop the renewal for next month",
    "I want to stop the renewal on this account"
  ],
  "related_issue_kinds": [
    "cancel_request",
    "billing_problem"
  ],
  "notes": "Promote only after corpus repetition and regression coverage"
}
```

### 4. Approved Override Operation

This is the only format allowed to feed parser config.

It should match the repo's existing add-only override model.

```json
{
  "op": "add_pattern",
  "target": "lifecycle_keyword_patterns",
  "pattern": "stop the renewal",
  "match_mode": "literal"
}
```

## Validation Rules

Reviewer code must reject or quarantine results when:

- output is not valid JSON
- required fields are missing
- enum values are outside the allowed set
- `candidate_reply_text` is longer than `body_canonical`
- pattern suggestion is too short, too generic, or obviously unsafe
- pattern suggestion duplicates an existing YAML/JSONL rule
- summary includes hallucinated facts not present in message text

## Acceptance Policy

### Reply-text review

The LLM may suggest a repaired `candidate_reply_text`, but it must not replace `reply_text` in canonical exports automatically.

Allowed uses:

- analyst review UI
- offline quality reports
- future heuristic design

Not allowed:

- mutating historical exports in place
- changing parser output at ingestion time without deterministic logic

### Issue annotation

LLM issue labels and summaries are allowed as downstream enrichment because they are additive.

Store them separately from parser output:

- `issue_kind`
- `issue_topic`
- `summary`
- `sentiment`
- `urgency`
- extracted domain entities

### Pattern suggestions

Promote a pattern only when all checks pass:

1. The same or equivalent phrase appears across multiple messages.
2. The phrase is explainable as a deterministic rule.
3. The phrase is not just one customer's prose.
4. The phrase improves a real parser miss category.
5. A regression test is added before approval.

## Offline Review Pipeline

### Phase 0. Baseline

- Continue current `.test/` mailbox parsing workflow.
- Keep manual verification as the baseline truth source.
- Track recurring miss categories:
  - reply-text leakage
  - missing lifecycle hint
  - wrong mail kind
  - missing issue/topic extraction

Deliverable:

- `.review/baseline_findings.md`

### Phase 1. Review Job Export

Build a small offline exporter that reads canonical output and emits `.review/jobs.jsonl`.

Inputs:

- canonical thread/message JSON
- optional mailbox source metadata

Logic:

- select review candidates only
- embed current parser outputs into the job payload
- include stable `job_id`

Deliverable:

- `cli` subcommand or offline script that produces `.review/jobs.jsonl`

### Phase 2. Local LLM Runner

Add a separate offline runner, not part of the parser core.

Requirements:

- consume `jobs.jsonl`
- prompt local LLM with strict schema instructions
- require JSON-only output
- write raw outputs plus validated outputs separately

Recommended files:

- `.review/raw_results.jsonl`
- `.review/results.jsonl`
- `.review/rejected_results.jsonl`

### Phase 3. Candidate Aggregator

Aggregate repeated LLM suggestions across many messages.

Tasks:

- normalize phrases
- dedupe equivalent candidates
- count support
- group by target family
- reject low-support generic phrases

Deliverable:

- `.review/pattern_candidates.jsonl`

### Phase 4. Human Review

Review only aggregated candidates, not raw LLM noise.

Decision outcomes:

- reject
- keep as annotation only
- convert into override op

Deliverable:

- `.review/approved_override_ops.jsonl`

### Phase 5. Deterministic Promotion

Load approved override ops with existing lexicon override support and add regression tests.

Required actions for every accepted candidate:

- add or update tests under `tests/` or module `mod tests`
- verify no regressions in known `.test/` problem cases
- document the new pattern family if the behavior changes materially

### Phase 6. Product Enrichment

Only after the offline review loop is stable, expose the additive LLM annotations to an app layer:

- issue summaries
- issue type/topic
- routing labels
- searchable entities
- support analytics

These stay outside the parser's canonical contract unless explicitly promoted later.

## Recommended File Layout

```text
mailbox-parser/
  roadmap.md
  .review/
    jobs.jsonl
    raw_results.jsonl
    results.jsonl
    rejected_results.jsonl
    pattern_candidates.jsonl
    approved_override_ops.jsonl
    baseline_findings.md
```

## Metrics

Measure value before committing to the pipeline.

### Parser-quality metrics

- percent of reviewed messages with true `reply_text` extraction issues
- percent of LLM suggestions that identify a real parser miss
- percent of accepted pattern candidates that become deterministic rules
- regression pass rate after promoting candidates

### Product-value metrics

- issue classification coverage
- issue-topic usefulness for search/routing
- summary usefulness in analyst workflow
- entity usefulness for filters, facets, and BM25 recall

## Decision Gate

Proceed beyond Phase 2 only if all are true:

- the LLM repeatedly finds misses not already obvious in manual review
- the suggestions cluster into deterministic patterns
- accepted candidates improve parser behavior on future mailboxes
- the reviewer workload is lower than the value gained

If those are not true, keep the current workflow:

- parse more mailboxes
- inspect `.test/` outputs
- tighten deterministic heuristics directly

## Practical Recommendation

Build this only as an offline reviewer and lexicon-suggestion system.

Do not:

- run the LLM inside the main parse loop
- auto-rewrite canonical parser output
- auto-append YAML

Do:

- keep parser behavior deterministic
- use the LLM for additive summaries and issue annotations
- use the LLM to propose repeated candidate phrases
- promote only human-reviewed candidates into add-only override files
