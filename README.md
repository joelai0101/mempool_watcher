# mempool_watcher

Tools for collecting and analyzing mempool replacement events and fee market data from mempool.space.

## What it collects

From mempool.space:
- `/api/mempool` (mempool size + fee histogram)
- `/api/v1/fees/recommended` (recommended fees)
- `/api/v1/fees/precise` (precise recommended fees)
- `/api/v1/fees/mempool-blocks` (fee distribution by blocks)
- `/api/v1/replacements` (replacement events)

## Data storage

SQLite (or Postgres) via `api_snapshots` and `replacement_events` tables.
JSONL snapshots are optional.

## Start collecting

Single run:
```bash
./scripts/fetch_mempool_space.py --db sqlite:///./data/mempool.db --interval 0
```

Run every 60 seconds:
```bash
./scripts/fetch_mempool_space.py --db sqlite:///./data/mempool.db --interval 60
```

If TLS verification fails in your environment:
```bash
./scripts/fetch_mempool_space.py --db sqlite:///./data/mempool.db --interval 60 --insecure
```

Stop collection:
```bash
pkill -f fetch_mempool_space.py
```

## Check latest status

```bash
./scripts/check_latest.py --db ./data/mempool.db
```

## Export CSV

Recommended fees (recommended endpoints):
```bash
./scripts/export_fees_csv.py --db ./data/mempool.db --out ./data/fees.csv
```

Mempool size:
```bash
./scripts/export_mempool_csv.py --db ./data/mempool.db --out ./data/mempool.csv
```

## Plots

Replacement events volume:
```bash
./scripts/plot_replacements.py --db ./data/mempool.db --out ./data/replacements_hourly.png --bucket hour
```

Mempool vsize + precise recommended fee:
```bash
./scripts/plot_mempool_fees.py --db ./data/mempool.db --out ./data/mempool_fees_hourly.png --bucket hour
```

Fee histogram (from `/api/mempool`):
```bash
./scripts/plot_fee_histogram.py --db ./data/mempool.db --out ./data/fee_histogram.png \
  --start 2026-02-13T00:00:00+00:00 --end 2026-02-13T23:59:59+00:00 --style step
```

## Fetch tx details (witness)

Fetch `/api/tx/{txid}` for replacement events and store in `tx_details` table:
```bash
./scripts/fetch_tx_details.py --db ./data/mempool.db --sleep 0.2
```

## Tables (SQLite)

`api_snapshots`
- `observed_at`, `endpoint`, `success`, `latency_ms`, `error`, `data_json`

`replacement_events`
- `observed_at`, `event_time`, `old_txid`, `new_txid`
- `old_fee_sat`, `old_feerate`, `old_vsize`
- `new_fee_sat`, `new_feerate`, `new_vsize`
- `interval_seconds`, `full_rbf`, `mined`

## Notes

- `fees_precise` is stored under endpoint `fees_precise` in `api_snapshots`.
- `/api/mempool` includes `fee_histogram`, so no separate endpoint is required.
