#!/usr/bin/env python3
"""Signature-split probe: teacher (TypeSafe AI) vs. mailbox-parser's heuristic split.

Minimum distillation-loop experiment (docs/typesafe-signature-probe.md):
  1. parse a message sample with --json-profile canonical (jsonl)
  2. reconstruct the text the parser segmented:
       full = [salutation] + reply_text + [signature]
     parser's implied signature boundary = line count of the body part
  3. one TypeSafe call per message, two questions (batched, same state):
       signature_start — Choice over trailing line indices + "none"
       mail_kind       — Choice over a 6-way taxonomy
  4. diff teacher vs parser; print agreement matrix + miss buckets

No corpus data, no API key is ever written by this script. Key resolution
order: --key-file (default /tmp/typesafe.key), then TYPESAFEAI_API_KEY env.
The key is never printed, logged, or included in saved results.

Usage:
  python3 tools/typesafe_signature_probe.py /tmp/probe.jsonl --limit 30
  python3 tools/typesafe_signature_probe.py /tmp/probe.jsonl --limit 1 --dry-run
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

API_URL = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-latest"
MAX_CALL_RETRIES = 3
SHOW_LINES = 60          # max trailing lines sent as state
OPTION_WINDOW = 35       # trailing lines offered as signature_start options
NEAR_TOLERANCE = 2       # |teacher - parser| within this => "agree (near)"

MAIL_KINDS = [
    "human_correspondence",
    "newsletter",
    "notification",
    "transactional",
    "automated_report",
    "marketing",
]


def load_key(args):
    if args.key_file and os.path.exists(args.key_file):
        key = open(args.key_file).read().strip()
        if key:
            return key
    env = os.environ.get("TYPESAFEAI_API_KEY", "").strip()
    return env or None


def reconstruct(m):
    """Return (full_text, parser_boundary_0based_or_None)."""
    sal = m.get("salutation") or ""
    body = m.get("reply_text") or ""
    sig = m.get("signature") or ""
    pre = (sal + "\n" if sal else "") + body
    full = pre + ("\n" + sig if sig else "")
    pre_lines = pre.split("\n")
    while pre_lines and not pre_lines[-1].strip():
        pre_lines.pop()
    boundary = len(pre_lines) if sig.strip() else None
    return full, boundary


def build_request(m):
    full, boundary = reconstruct(m)
    lines = full.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    n = len(lines)
    if boundary is not None and boundary >= n:
        boundary = None  # signature vanished after trimming; treat as none
    shown = lines[-SHOW_LINES:]
    offset = n - len(shown)  # 1-based line numbers keep absolute positions
    if boundary is not None and boundary < offset:
        return None  # boundary outside visible window; skip
    state = {
        "subject": (m.get("subject") or "")[:200],
        "from": [
            f if isinstance(f, str) else f"{f.get('name','')} <{f.get('address','')}>"
            for f in (m.get("from") or [])[:3]
        ],
        "text": "\n".join(f"{i+1}: {ln}" for i, ln in enumerate(shown)),
    }
    first_opt = max(1, n - OPTION_WINDOW + 1)
    options = {str(i): f"the personal signature block begins at line {i}" for i in range(first_opt, n + 1)}
    options["none"] = (
        "this message has no personal signature block "
        "(automated mail, or it simply ends without a sign-off)"
    )
    questions = {
        "signature_start": {
            "type": "choice",
            "instructions": (
                "The text is an email message body with each line prefixed by its "
                "line number. A personal signature block is the sender's trailing "
                "sign-off: greeting word ('Best regards', 'Thanks', 'Σας ευχαριστώ'), "
                "name, and optionally title/company/phone/quote. It is NOT part of "
                "the message content and NOT a legal disclaimer or unsubscribe "
                "footer. Choose the line where the signature block begins, or "
                "'none' if there is no personal signature."
            ),
            "criteria": options,
        },
        "mail_kind": {
            "type": "choice",
            "instructions": (
                "Classify this email message by its true kind, regardless of "
                "language. Judge from sender, subject, and text."
            ),
            "criteria": {
                "human_correspondence": "written by a person to another person (reply, discussion, personal note)",
                "newsletter": "periodic digest or mailing-list content with articles/links",
                "notification": "service alert about an event or account (security, social, job alert, shipping)",
                "transactional": "receipt, invoice, booking confirmation, statement about a transaction",
                "automated_report": "scheduled machine-generated report or monitoring output",
                "marketing": "promotional offer, campaign, discount announcement",
            },
        },
    }
    payload = {"state": state, "model": MODEL, "questions": questions}
    return payload, boundary


def post(payload, key):
    req = urllib.request.Request(
        API_URL,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    for attempt in range(MAX_CALL_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 529) and attempt < MAX_CALL_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < MAX_CALL_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--key-file", default="/tmp/typesafe.key")
    ap.add_argument("--results", default="/tmp/typesafe-probe-results.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    threads = [json.loads(l) for l in open(args.jsonl)]
    msgs = [m for t in threads for m in t["messages"]]
    msgs = [m for m in msgs if (m.get("reply_text") or "").strip()]

    if not args.dry_run:
        key = load_key(args)
        if not key:
            sys.exit("no API key: --key-file or TYPESAFEAI_API_KEY")

    results, skipped = [], 0
    for i, m in enumerate(msgs[: args.limit]):
        built = build_request(m)
        if built is None:
            skipped += 1
            continue
        payload, parser_boundary = built
        if args.dry_run:
            print(json.dumps(payload, ensure_ascii=False)[:1200])
            print(f"[dry-run] parser boundary: {parser_boundary}")
            return
        try:
            resp = post(payload, key)
        except Exception as e:  # noqa: BLE001 - probe: record and continue
            print(f"  {i}: API error {e}", file=sys.stderr)
            results.append({"i": i, "error": str(e)})
            continue
        a = resp.get("answers", {})
        sig = a.get("signature_start", {})
        kind = a.get("mail_kind", {})
        t_line = sig.get("choice")
        t_line = int(t_line) - 1 if isinstance(t_line, str) and t_line.isdigit() else None
        rec = {
            "i": i,
            "subject": (m.get("subject") or "")[:60],
            "parser_sig": parser_boundary,
            "teacher_line": t_line,
            "teacher_conf": sig.get("confidence"),
            "teacher_probabilities": sig.get("probabilities"),
            "mail_kind": kind.get("choice"),
            "mail_kind_conf": kind.get("confidence"),
        }
        results.append(rec)
        both = parser_boundary is not None and t_line is not None
        if both:
            verdict = "AGREE" if abs(t_line - parser_boundary) <= NEAR_TOLERANCE else "FAR"
        elif parser_boundary is None and t_line is None:
            verdict = "AGREE-both-none"
        elif parser_boundary is None:
            verdict = "TEACHER-ONLY"
        else:
            verdict = "PARSER-ONLY"
        print(f"  {i:3d} {verdict:16s} parser={parser_boundary} teacher={t_line} conf={sig.get('confidence')} kind={kind.get('choice')}")
        time.sleep(0.3)

    # summary
    def bucket(r):
        p, t = r.get("parser_sig"), r.get("teacher_line")
        if "error" in r:
            return "error"
        if p is not None and t is not None:
            return "agree" if abs(t - p) <= NEAR_TOLERANCE else "both-present-far"
        if p is None and t is None:
            return "agree-both-none"
        return "teacher-only" if p is None else "parser-only"

    counts, kinds = {}, {}
    for r in results:
        counts[bucket(r)] = counts.get(bucket(r), 0) + 1
        k = r.get("mail_kind")
        if k:
            kinds[k] = kinds.get(k, 0) + 1
    ok = len(results)
    print(f"\n== signature split ({ok} judged, {skipped} skipped-out-of-window) ==")
    for k in sorted(counts):
        print(f"  {k:18s} {counts[k]:3d}")
    agree = counts.get("agree", 0) + counts.get("agree-both-none", 0)
    if ok:
        print(f"  agreement: {agree}/{ok} = {100*agree/ok:.0f}%")
    print("== teacher mail_kind ==")
    for k in sorted(kinds, key=kinds.get, reverse=True):
        print(f"  {k:22s} {kinds[k]:3d}")

    json.dump(results, open(args.results, "w"), ensure_ascii=False, indent=1)
    print(f"raw results: {args.results}")


if __name__ == "__main__":
    main()
