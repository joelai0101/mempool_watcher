#!/usr/bin/env python3
"""Show latest mempool/fees/replacements snapshots from SQLite DB."""

from __future__ import annotations

import argparse
import json
import sqlite3


def main() -> int:
    parser = argparse.ArgumentParser(description="Show latest snapshots.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB (e.g. ./data/mempool.db)")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    print("api_snapshots (latest)")
    cur.execute(
        """
        SELECT observed_at, endpoint, success, latency_ms, data_json
        FROM api_snapshots
        ORDER BY id DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    for observed_at, endpoint, success, latency_ms, data_json in cur.fetchall():
        size = len(data_json) if data_json else 0
        print(f"{observed_at} {endpoint} success={success} latency_ms={latency_ms} data_len={size}")

    print("\nreplacement_events (latest)")
    cur.execute(
        """
        SELECT observed_at, event_time, old_txid, new_txid, old_fee_sat, new_fee_sat
        FROM replacement_events
        ORDER BY id DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    rows = cur.fetchall()
    if not rows:
        print("(none)")
    else:
        for row in rows:
            print(row)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
