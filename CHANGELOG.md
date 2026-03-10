## 0.1.1 - 2026-03-10

- expanded email-body segmentation coverage for additional multilingual salutations, sign-offs, mobile footer cues, and quote markers to reduce `reply_text` leakage
- tightened canonical salutation/signature truncation to avoid over-capturing long prose and promotional footer content
- added deterministic sender/participant domain projection hints and refreshed related JSON/README contract references
- introduced the `mailbox-parser-cli` crate with JSON, Markdown, CSV, and interactive HTML export flows
- hardened HTML toolbar rendering so zoom/reset/import/labels controls remain visible locally and the theme toggle uses embedded moon/sun SVG icons instead of a remote icon dependency
- added crate-local docs for the JSON contract and lifecycle lexicon, plus an offline local-LLM review roadmap for human-approved parser improvement
