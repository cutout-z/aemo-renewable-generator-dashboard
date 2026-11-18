#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/workspace/repos/aemo-renewable-generator-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
PIPELINE_ARGS="${PIPELINE_ARGS:---full-refresh}"
RUN_TESTS="${RUN_TESTS:-1}"
PUSH_CHANGES="${PUSH_CHANGES:-1}"
COMMIT_MESSAGE_PREFIX="${COMMIT_MESSAGE_PREFIX:-Update renewable generator dashboard data}"
# 1 = a failed lane data check holds back the commit and push (default 0: warn-only, the lane ends red but publishes).
LANE_TESTS_BLOCK_PUSH="${LANE_TESTS_BLOCK_PUSH:-0}"
LANE_CHECKS_FAILED=()

cd "${APP_DIR}"

# Every normal exit path ends here (a failed step exits earlier, under set -e). Checks that
# must turn the lane red without holding back this run's publish (a confirmed newer ELI
# edition, decision 6(b)) run after any push.
# A failed lane data check (below) also ends the run non-zero, after the publish.
finish() {
  local rc=0
  "${PYTHON}" -m src.post_publish_check || rc=$?
  if [[ ${#LANE_CHECKS_FAILED[@]} -gt 0 ]]; then
    echo ""
    echo "Lane data checks failed: ${LANE_CHECKS_FAILED[*]}"
    if [[ "${LANE_TESTS_HELD:-0}" == "1" ]]; then
      echo "LANE_TESTS_BLOCK_PUSH=1 held this run back: nothing was committed or pushed; exiting non-zero."
    else
      echo "This run's changes (if any) were still published; exiting non-zero so this stays visible."
    fi
    [[ "${rc}" -ne 0 ]] || rc=1
  fi
  exit "${rc}"
}

git fetch origin main
git checkout main
if ! git pull --ff-only origin main; then
  echo "origin/main is not fast-forwardable (rewritten?) — resetting onto it."
  git reset --hard origin/main
fi
# Start from the COMMITTED outputs and feather caches. A failed or manual run may have left them
# edited or left stray files; the pipeline would read an edited cache as its fallback, the check
# below would compare against an edited summary, and `git add` would publish the strays.
# Gitignored upstream snapshots in data/ (xls, xlsx, zip, json) are left alone.
git checkout HEAD -- outputs/ 'data/*.feather' 2>/dev/null || true
git clean -fdq -- outputs/ 'data/*.feather'
# Baseline = the COMMITTED summary, not the working tree.
before_summary="$(mktemp)"
git show HEAD:outputs/summary.csv > "${before_summary}" 2>/dev/null || : > "${before_summary}"

"${PYTHON}" -m src.main ${PIPELINE_ARGS}

if [[ "${RUN_TESTS}" == "1" ]]; then
  "${PYTHON}" tests/validate_outputs.py
  # Lane data checks (test-suite audit 2026-11, finding R-E7). The validator above checks outputs/; the offline
  # suite also checks the committed reference files against hand-checked values (test_committed_files_agree),
  # which no lane run tested before. It reads the data this run is about to commit, blocks the network
  # (tests/conftest.py) and runs in seconds. A failure ends the lane red; by default (warn-only) the run still
  # publishes. LANE_TESTS_BLOCK_PUSH=1 holds it back instead. pytest and hypothesis are dev dependencies, installed
  # by hand in the NAS lane venv (2026-11-18); a venv without pytest cannot vouch for the data, so that is a failed
  # check too. tests/test_page_smoke.py is a browser test (it needs Playwright, which the lane venv does not have,
  # and raises at import without it) and is left to the Mac. --basetemp keeps pytest's temporary files in the clone.
  if "${PYTHON}" -c 'import pytest' >/dev/null 2>&1; then
    tests_rc=0
    "${PYTHON}" -m pytest -q -p no:cacheprovider --basetemp "${APP_DIR}/.lane-pytest-tmp" \
      --ignore=tests/test_page_smoke.py tests || tests_rc=$?
    rm -rf "${APP_DIR}/.lane-pytest-tmp"
    if [[ "${tests_rc}" -ne 0 ]]; then
      echo "WARN: the offline test suite failed on this run's data (pytest exit ${tests_rc}, output above)."
      LANE_CHECKS_FAILED+=("pytest tests (exit ${tests_rc})")
    fi
  else
    echo "WARN: pytest is not importable by ${PYTHON}; the offline test suite did not run."
    LANE_CHECKS_FAILED+=("pytest missing")
  fi
fi

# A fixed-edition reference (2026 ISP Appendix A3), built by hand with python -m src.isp_rez_appendix: a scheduled
# run must never change it. Compared by content (blob id), so a file-mode change alone is not flagged.
ISP_A3_REF="data/isp_rez_curtailment.feather"
committed_ref="$(git rev-parse -q --verify "HEAD:${ISP_A3_REF}" 2>/dev/null || true)"
current_ref="$(git hash-object -- "${ISP_A3_REF}" 2>/dev/null || true)"
if [[ -n "${committed_ref}" && "${current_ref}" != "${committed_ref}" ]]; then
  echo "WARN: ${ISP_A3_REF} differs from HEAD after the refresh; it is the fixed 2026 ISP A3 reference and a lane run must not change it. Restored from HEAD: not published."
  git checkout HEAD -- "${ISP_A3_REF}"
  LANE_CHECKS_FAILED+=("${ISP_A3_REF} changed")
fi

# outputs/ includes source_status.json (source editions and fetch dates for the page footer).
# It is published only with a summary.csv change (the trigger below), so the footer dates are
# those of the last run that changed values, not of the last run.
git add outputs/ data/*.feather

if [[ -s "${before_summary}" ]] && cmp -s "${before_summary}" outputs/summary.csv; then
  git restore --staged --worktree -- outputs/ data/*.feather
  rm -f "${before_summary}"
  echo "No canonical summary.csv changes; skipping workbook/cache-only publish noise."
  finish
fi
rm -f "${before_summary}"

if git diff --cached --quiet; then
  echo "No publishable output changes."
  finish
fi

# LANE_TESTS_BLOCK_PUSH=1: a failed lane data check publishes nothing (see the checks above).
if [[ "${LANE_TESTS_BLOCK_PUSH}" == "1" && ${#LANE_CHECKS_FAILED[@]} -gt 0 ]]; then
  git restore --staged --worktree -- outputs/ data/*.feather
  echo "LANE_TESTS_BLOCK_PUSH=1 and a lane data check failed: nothing committed or pushed; outputs/ and data/*.feather are back at HEAD."
  LANE_TESTS_HELD=1
  finish
fi

git config user.name "${GIT_AUTHOR_NAME:-aemo-nas-bot}"
git config user.email "${GIT_AUTHOR_EMAIL:-aemo-nas-bot@users.noreply.github.com}"
git commit -m "${COMMIT_MESSAGE_PREFIX} $(date -u +%Y-%m)"

if [[ "${PUSH_CHANGES}" == "1" ]]; then
  git push origin main
else
  echo "PUSH_CHANGES=0; commit created but not pushed."
fi

finish
