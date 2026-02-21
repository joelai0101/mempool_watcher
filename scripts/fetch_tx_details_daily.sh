#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${DB_PATH:-./data/tx_details.db}"
SOURCE_DB_PATH="${SOURCE_DB_PATH:-./data/mempool.db}"
SLEEP_SEC="${SLEEP_SEC:-0.3}"
LIMIT="${LIMIT:-0}"
LOG_PATH="${LOG_PATH:-./data/tx_details_daily.log}"

SINCE_UTC="$(date -u +%Y-%m-%dT00:00:00+00:00)"

CMD=("./scripts/fetch_tx_details.py" "--db" "$DB_PATH" "--source-db" "$SOURCE_DB_PATH" "--since" "$SINCE_UTC" "--sleep" "$SLEEP_SEC")
if [[ "$LIMIT" -gt 0 ]]; then
  CMD+=("--limit" "$LIMIT")
fi

"${CMD[@]}" >> "$LOG_PATH" 2>&1
