# TypeSafe signature-split probe — round 1 findings

Branch: `typesafe/signature-probe` · date: 2026-09-17 · 40 messages, 0 API errors.

First run of the teacher-vs-parser diff loop: TypeSafe (`jev-latest`) judges the
signature boundary on the same text the parser segmented; disagreements become
rule-fix candidates. Key resolution: `/tmp/typesafe.key` or `TYPESAFEAI_API_KEY`
env. Sample: every 600th message of the 13 GB Gmail mbox (65 msgs, 60 with
reply text; probe = first 40).

## Headline

**Agreement 17/40 (42%).** Failure mode is one-directional: the parser invents
signatures; it almost never misses real ones (2 teacher-only).

| Bucket | Count | Meaning |
|---|---|---|
| agree (±2 lines) | 8 | both find the same boundary |
| agree-both-none | 9 | both say no signature |
| **parser-only** | **20** | parser "signature" that the teacher rejects |
| teacher-only | 2 | real signature the parser missed |
| both-present-far | 1 | both see one, boundaries >2 lines apart |

Teacher mail_kind over the sample: human 14 · transactional 12 · notification
11 · marketing 3.

## The 20 parser-only misses, by cause

### A. Automated-mail boilerplate — 16/20 (fix: rule file)

LinkedIn job-alert company blurbs (`Head of Operations / Clear. / APAC`),
Airwallex "please do not reply" + link lists, Envato/CodeCanyon postal
addresses, UptimeRobot footer, Google unsubscribe links, Indonesian bank
transaction footers. `SIGNATURE_CUES` (address block / company / URL cues)
fire on footers endemic to notification/transactional mail. Teacher confidence
≥0.9 on 14 of 16 — trustworthy signal.

**Candidate rule:** suppress signature capture when the message is automated
(sender matches noreply/do-not-reply patterns, List-Unsubscribe present,
Auto-Submitted ≠ no — the `mail_kind_hints` machinery already exists).

### B. Quote/attribution bleed — 2/20 (fix: structural guard)

- `Re: [ThemeForest]…`: reply text (`Anyway, we can try something.`) **and** the
  following `On Fri, Apr 20, 2018 at 2:01 PM, …` attribution were both binned
  as *signature* — quoted-history split failed, signature absorbed the reply.
- `Re: Gelato List`: `cheers,` + the `2015-10-19 11:21 GMT+07:00 …` attribution
  line captured as one signature block.

**Candidate rule:** a captured signature may not contain an attribution line
(`WROTE_TOKENS`/date-header patterns); if it does, the split is wrong upstream.

### C. Machine sign-off, human shape — 2/20 (policy decision, not a bug)

`Thank you, / Bank CIMB Niaga` and `Sincerely, / Arvixe, LLC` — machine
sign-offs formatted exactly like personal ones. Teacher says "none" per the
probe criteria ("personal signature"). Either the loop's criteria accept
machine sign-offs (taxonomy call) or these stay as-is. Verify-step territory:
do not distill without deciding.

## The 2 teacher-only misses

Both low/near confidence (0.69) — exactly the answers the loop should re-verify
(self-consistency or reasoning pass) before treating as parser bugs.

## Costs & mechanics

One call per message, two questions batched (`signature_start` Choice over the
last 35 line indices + `mail_kind`) — 40 calls, sub-minute wall time, no
retries needed. Raw answers: `/tmp/typesafe-probe-results.json` (local only —
contains real subjects; not committed).

## Reproduce

```sh
awk '/^From /{c++; sel=(c%600==0)} sel{print}' "$MBOX" > /tmp/probe.mbox
cli/target/release/mailbox-parser-cli mbox threads --path /tmp/probe.mbox \
  --out /tmp/probe.jsonl --format jsonl --json-profile canonical
python3 tools/typesafe_signature_probe.py /tmp/probe.jsonl --limit 40
```

## Next round

1. Decide C (machine sign-offs count as signatures or not) — one criteria line.
2. Distill bucket A into the first `suppress_signature_when` rules.
3. Bucket B needs a structural guard in Rust (Tier 3), not a lexicon entry.
