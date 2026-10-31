#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../infra/compose"

set -a; source .env; set +a
CONNECT="${CONNECT:-http://localhost:8083}"

until curl -fs "$CONNECT/" > /dev/null; do      # warten, bis Connect antwortet
  echo "warte auf Connect..."; sleep 2
done

for f in connect/*-connector.json; do
  name=$(jq -r .name "$f")
  envsubst '$POSTGRES_USER $POSTGRES_PASSWORD $POSTGRES_DB' < "$f" \
    | jq .config \
    | curl -fsS -X PUT "$CONNECT/connectors/$name/config" \
        -H "Content-Type: application/json" -d @- > /dev/null
  echo "ok: $name"
done
