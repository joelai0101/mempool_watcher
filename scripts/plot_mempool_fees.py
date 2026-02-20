#!/usr/bin/env python3
"""Plot mempool vsize and recommended fees over time (separate chart)."""

from __future__ import annotations

import argparse
import csv
import json
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
    parser = argparse.ArgumentParser(description="Plot mempool vsize and recommended fees.")
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
        SELECT observed_at, endpoint, data_json
        FROM api_snapshots
        WHERE endpoint IN ('mempool', 'fees_precise')
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

    buckets: dict[str, dict[str, float | None]] = defaultdict(lambda: {
        "vsize": None,
        "fastestFee": None,
    })

    for observed_at, endpoint, data_json in cur.fetchall():
        if not data_json:
            continue
        data = json.loads(data_json)
        b = bucket_ts(observed_at, args.bucket)
        if endpoint == "mempool":
            buckets[b]["vsize"] = data.get("vsize")
        elif endpoint == "fees_precise":
            if "fastestFee" in data:
                buckets[b]["fastestFee"] = data.get("fastestFee")

    conn.close()

    if not buckets:
        raise SystemExit("No mempool/fees snapshots found for the selected range.")

    xs = sorted(buckets.keys())
    vsize = [buckets[x]["vsize"] or 0 for x in xs]
    fastest = [buckets[x]["fastestFee"] or 0 for x in xs]

    if args.csv_out:
        with open(args.csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["bucket", "vsize", "fastestFee"])
            for row in zip(xs, vsize, fastest):
                writer.writerow(row)

    plt.figure(figsize=(12, 7))
    ax1 = plt.gca()
    ax1.plot(xs, vsize, color="#4B714D", label="mempool vsize")
    ax1.set_ylabel("vsize")
    ax1.tick_params(axis='x', rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(xs, fastest, color="#8A6B4B", label="fastestFee")
    ax2.set_ylabel("recommended fee")

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    plt.tight_layout()
    plt.savefig(args.out, dpi=150)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
