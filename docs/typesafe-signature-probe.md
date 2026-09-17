# TypeSafe signature-split probe — rounds 1–4

Branch: `typesafe/signature-probe` · date: 2026-09-17 · teacher: `jev-latest`.

First run of the teacher-vs-parser diff loop: TypeSafe (`jev-latest`) judges the
signature boundary on the same text the parser segmented; disagreements become
rule-fix candidates. Key resolution: `/tmp/typesafe.key` or `TYPESAFEAI_API_KEY`
env. Sample: every 600th message of the 13 GB Gmail mbox (65 msgs, 60 with
reply text; probe = first 40).

## Headline

| Round | Sample | Agreement | Notes |
|---|---|---|---|
| 1 | 40 msgs, gmail (automated-heavy) | 42% | baseline; 20 parser-only |
| 2 | same 40, after fixes | 72% | bucket A gated, attribution fixed |
| 3 | 68 msgs, 4 mboxes (human-heavy) | 71% | fixes hold on fresh corpus |
| 4 | same 68, after lexicon round | 76% | distilled cues applied |
| 5 | fresh strides, 4 mboxes (176 judged) | gmail 75 / fitch 73 / **FWM 45** | FWM teacher-only 26/57 |
| 6 | +English cues, team rule v1 | FWM 61 | fallback re-split bug |
| 7 | +fallback suppression | fitch 91 | team rule regressed Mira |
| 8 | team rule v2 + footer-token guard + no-cue fallback guard | **FWM 68 / fitch 92 / gmail 82** | garbage signatures eliminated |

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

## Open residual (round 4)

- **Policy C, machine sign-offs in human shape** (`Thank you, / Bank CIMB
  Niaga`, `Sincerely, / UptimeRobot`): teacher says none, parser keeps them on
  non-gated custom-domain senders. Decision pending — treat as sender-identity
  noise or legitimate signatures.
- Custom-domain transactional footers (Indonesian e-commerce) from senders
  outside the noreply family: bounded set; extend gate by domain evidence only
  if it matters downstream.
- Teacher variance zone: several disagreements flipped between rounds at
  confidence <0.7 — the loop's verify step (re-ask or reasoning pass) before
  distilling any single low-confidence disagreement.

## Mechanism answer (from review)

Hardcoded consts + tests remain the refinement mechanism for this repo:
contributors compile anyway, goldens pin behavior, diffs are reviewable Rust.
External JSON lexicons earn their keep only when non-developer users refine
rules without a build step (agent-mailbox/Rhai pattern).

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

## Fixes distilled and shipped on this branch

1. **Wrapped/date-first attribution recognition** (`src/email_text.rs`):
   Gmail wraps long attributions so the address lands on the next line
   (`On Fri, Apr 20, 2018 … Name <\naddr@x> wrote:`); some locales put the date
   first (`2015-10-19 11:21 GMT+07:00 Name <`). Neither carried the `:`/`wrote`
   suffix on its first line, so both escaped quote detection — attribution bled
   into reply/signature (round-1 bucket B, msgs #28/#37).
2. **Automated-mail signature gate** (`src/canonical.rs::is_automated_mail`):
   List-Unsubscribe / Auto-Submitted≠no / Precedence bulk,list,junk / noreply-family
   sender → signature blocks demote to body (round-1 bucket A: 16/20).
3. **Team sign-off demotion** (`is_team_signoff`, rounds 5–8): an org line
   ("The Kajabi Team") directly after a comma-terminated sign-off, or opening
   the block, is a template sign-off — demoted. A person name between greeting
   and team line ("Warmly, / David / The Flow with Mira Team") keeps the
   signature. Demotion suppresses the footer-fallback re-split.
4. **Footer-fallback guards** (round 8): URL-only tails no longer qualify as
   signatures (needs ≥1 real footer token); the block engine's no-cue
   fallback requires the line itself to carry sign-off evidence (killed the
   URL/email instruction-list false positive).
5. **Lexicon entries**: `kindest regards`, `warmest regards`, `all the best`,
   `wishing you all the best`, `with much love and gratitude`, `see you on
   screen`, `warmly` (strict), `bises`, French forward markers
   (`début du message transmis/transféré`), `notification` gate pattern.

Tests: `tests/parse_basic.rs` (wrapped attribution, date-first, prose guard),
`tests/canonical_json.rs` (automated gate). All suites green.
