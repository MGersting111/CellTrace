#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:-http://localhost:8081}"
cd "$(dirname "$0")/.."          # vom Repo-Root aus arbeiten

register() {   # usage: register <subject> <datei> <compatibility>
  local subject="$1" file="$2" compat="$3"

  curl -fsS -X PUT "$REGISTRY/config/$subject" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"compatibility\":\"$compat\"}" > /dev/null

  jq -n --arg s "$(cat "$file")" '{schema: $s}' \
    | curl -fsS -X POST "$REGISTRY/subjects/$subject/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" -d @-
  echo " ← $subject"
}

register telemetry.station.v1-value schemas/telemetry_recorded.avsc BACKWARD
register mes.quality-value schemas/mes_quality.avsc BACKWARD
