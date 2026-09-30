#!/usr/bin/env bash
# Build assets/css/app.css from the page sources. Run after ANY class-name change or any edit to
# assets/css/tailwind.src.css.
#
# Tailwind v3 standalone binary — no Node, no npm, no package.json, no build server.
# The binary is gitignored (tools/); this script fetches it if it is missing.
# app.css is COMMITTED because GitHub Pages serves the repo: there is no build step at deploy time.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="tools/tailwindcss"
VERSION="v3.4.17"
OUT="assets/css/app.css"

if [ ! -x "$BIN" ]; then
  echo "==> fetching Tailwind $VERSION standalone binary"
  mkdir -p tools
  curl -fsSL -o "$BIN" \
    "https://github.com/tailwindlabs/tailwindcss/releases/download/$VERSION/tailwindcss-macos-arm64"
  chmod +x "$BIN"
fi

"$BIN" -c tailwind.config.js -i assets/css/tailwind.src.css -o "$OUT" --minify
echo "==> wrote $OUT ($(wc -c < "$OUT" | tr -d ' ') bytes)"
echo "    remember to commit it: GitHub Pages serves the repo, not a build"
