#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "Usage: $0 \"SYSTEM NAME\""
  exit 1
fi

SYSTEM_NAME="$*"

ENCODED_NAME=$(printf '%s' "$SYSTEM_NAME" | jq -sRr @uri)

curl -s "https://edgis.elitedangereuse.fr/bodies?name_or_id=${ENCODED_NAME}" |
jq '[.[] | {body_id, body_name, type, parents} + (
      if .type == "Star" and .stellar_mass != null then {mass: .stellar_mass}
      elif .type == "Planet" and .mass_em != null then {mass: .mass_em}
      else {}
      end
    )]' > "${SYSTEM_NAME}".json
