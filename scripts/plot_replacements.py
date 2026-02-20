#!/usr/bin/env python3
"""Plot replacement events volume over time."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

import matplotlib.pyplot as plt


def parse_iso(ts: str) -> str:
    if not ts:
        return ""
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"Invalid ISO timestamp: {ts}") from exc
    return ts


def bucket_ts(iso_ts: str, bucket: str) -> str:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    if bucket == "hour":
        dt = dt.replace(minute=0, second=0, microsecond=0)
    elif bucket == "day":
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise SystemExit("bucket must be 'hour' or 'day'")
    return dt.astimezone(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot replacement event metrics.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB (e.g. ./data/mempool.db)")
    parser.add_argument("--out", required=True, help="Output PNG path")
    parser.add_argument("--csv-out", default="", help="Optional CSV export path")
    parser.add_argument("--start", default="", help="ISO start time (inclusive)")
    parser.add_argument("--end", default="", help="ISO end time (inclusive)")
    parser.add_argument("--bucket", default="hour", choices=["hour", "day"])
    args = parser.parse_args()

    start = parse_iso(args.start)
    end = parse_iso(args.end)

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    query = """
        SELECT observed_at
        FROM replacement_events
        WHERE 1=1
    """
    params: list[str] = []
    if start:
        query += " AND observed_at >= ?"
        params.append(start)
    if end:
        query += " AND observed_at <= ?"
        params.append(end)
    query += " ORDER BY observed_at ASC"

    cur.execute(query, params)

    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0})

    for (observed_at,) in cur.fetchall():
        b = bucket_ts(observed_at, args.bucket)
        buckets[b]["count"] += 1

    conn.close()

    if not buckets:
        raise SystemExit("No replacement_events found for the selected range.")

    xs = sorted(buckets.keys())
    counts = [buckets[x]["count"] for x in xs]

    if args.csv_out:
        with open(args.csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["bucket", "count"])
            for x, c in zip(xs, counts):
                writer.writerow([x, c])

    plt.figure(figsize=(12, 7))
    ax = plt.gca()
    ax.bar(xs, counts, color="#4B714D", alpha=0.6, label="replacement count")
    ax.set_ylabel("count")
    ax.tick_params(axis='x', rotation=45)
    ax.legend(loc="upper left")

    plt.tight_layout()
    plt.savefig(args.out, dpi=150)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
