#!/usr/bin/env bash
# Fail if any tracked, non-vendored file contains an em-dash (U+2014).
# House style bans em-dashes everywhere; use a comma, colon, parentheses, or --.
# Vendored code we do not own (api/edts submodule, api/static/ed3d) is exempt.
set -euo pipefail

matches=$(git grep -nP "\x{2014}" -- \
  ':(exclude)api/edts' \
  ':(exclude)api/static/ed3d' \
  || true)

if [ -n "$matches" ]; then
  echo "Em-dash (U+2014) found. Replace with a comma, colon, parentheses, or --:" >&2
  echo "$matches" >&2
  exit 1
fi

echo "OK: no em-dashes in tracked source."
