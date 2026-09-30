# CLAUDE.md

**Before anything else, read `AGENTS.md`** — the contract for work in this repo: the five hard rules,
how the page is served, the local preview port, the DOM contract the gates check, the pitfalls
(Tailwind purging, preflight, the two hand-rolled heat scales, the five literal group colours, the
client-side XLSX export) and the handback checklist.

Then read **`BRIEF.md`** — the design brief for this pass.

The shape of it, so you cannot miss it:

- **Static page, published from the repo root.** `index.html` is the site; GitHub Pages serves the
  whole tracked tree on `main`. Presentation only, never the data plane.
- **The design language is already decided and transplanted** from the sibling AEMO credit dashboard:
  tokens in `assets/css/tailwind.src.css` → `./scripts/build-css.sh` → `assets/css/app.css` (committed).
  Adopt it; do not invent a second one. `design/tokens.html` is the proof page — open it first.
- **Work on `design/2026-10`** (already checked out), one commit per step.
- **Stop after BRIEF step 1** and report before scaling to the rest.
- **Never touch** `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`, `.github/**`. Never
  invent data. Verify in a browser, not by grep. Finish with the handback checklist in `AGENTS.md`.
