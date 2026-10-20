---
project: AEMO Renewable Generator Dashboard
agent: claude
branch: docs/drop-aemo-audit-mention
merged: false
status: PR open; one line in AGENTS.md
outstanding: []
---

# Drop the aemo-audit skill mention (2026-10-20)

AGENTS.md pointed at a Hermes skill `aemo-audit` that does not exist (hermes-skills deleted it on
2026-10-14). The line now names the `nas-lane-ops` skill and the lane's row in ARCHITECTURE.md §8.5.
Found by the skills outcome audit (report in the owner's private work folder). Docs only; nothing run.
