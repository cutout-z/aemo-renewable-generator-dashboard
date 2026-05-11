#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/aemo-renewable-generator-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
PIPELINE_ARGS="${PIPELINE_ARGS:---full-refresh}"
RUN_TESTS="${RUN_TESTS:-1}"
PUSH_CHANGES="${PUSH_CHANGES:-1}"
COMMIT_MESSAGE_PREFIX="${COMMIT_MESSAGE_PREFIX:-Update renewable generator dashboard data}"

cd "${APP_DIR}"

git fetch origin main
git checkout main
git pull --ff-only origin main
before_summary="$(mktemp)"
cp outputs/summary.csv "${before_summary}" 2>/dev/null || true

"${PYTHON}" -m src.main ${PIPELINE_ARGS}

if [[ "${RUN_TESTS}" == "1" ]]; then
  "${PYTHON}" tests/validate_outputs.py
fi

git add outputs/ data/*.feather

if [[ -s "${before_summary}" ]] && cmp -s "${before_summary}" outputs/summary.csv; then
  git restore --staged --worktree -- outputs/ data/*.feather
  rm -f "${before_summary}"
  echo "No canonical summary.csv changes; skipping workbook/cache-only publish noise."
  exit 0
fi
rm -f "${before_summary}"

if git diff --cached --quiet; then
  echo "No publishable output changes."
  exit 0
fi

git config user.name "${GIT_AUTHOR_NAME:-aemo-vps-bot}"
git config user.email "${GIT_AUTHOR_EMAIL:-aemo-vps-bot@users.noreply.github.com}"
git commit -m "${COMMIT_MESSAGE_PREFIX} $(date -u +%Y-%m)"

if [[ "${PUSH_CHANGES}" == "1" ]]; then
  git push origin main
else
  echo "PUSH_CHANGES=0; commit created but not pushed."
fi
