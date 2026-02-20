#!/usr/bin/env python3
"""Plot fee_histogram from latest mempool snapshot."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime

import matplotlib.pyplot as plt


def parse_iso(ts: str) -> str:
    if not ts:
        return ""
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"Invalid ISO timestamp: {ts}") from exc
    return ts


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot mempool fee histogram.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB (e.g. ./data/mempool.db)")
    parser.add_argument("--out", required=True, help="Output PNG path")
    parser.add_argument("--start", default="", help="ISO start time (inclusive)")
    parser.add_argument("--end", default="", help="ISO end time (inclusive)")
    parser.add_argument("--log-x", action="store_true", help="Use log scale for fee rate")
    parser.add_argument("--log-y", action="store_true", help="Use log scale for vsize")
    parser.add_argument(
        "--style",
        default="step",
        choices=["step", "bar"],
        help="Plot style using fee_rate as x (step area or bars).",
    )
    args = parser.parse_args()

    start = parse_iso(args.start)
    end = parse_iso(args.end)

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    query = """
        SELECT observed_at, data_json
        FROM api_snapshots
        WHERE endpoint = 'mempool'
    """
    params: list[str] = []
    if start:
        query += " AND observed_at >= ?"
        params.append(start)
    if end:
        query += " AND observed_at <= ?"
        params.append(end)
    query += " ORDER BY id DESC LIMIT 1"

    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()

    if not row:
        raise SystemExit("No mempool snapshots found for the selected range.")

    observed_at, data_json = row
    data = json.loads(data_json)
    histogram = data.get("fee_histogram") or []
    if not histogram:
        raise SystemExit("fee_histogram missing in snapshot.")

    sorted_pairs = sorted((bucket[0], bucket[1]) for bucket in histogram)
    fees = [p[0] for p in sorted_pairs]
    vsizes = [p[1] for p in sorted_pairs]

    min_fee = min(fees) if fees else 0
    max_fee = max(fees) if fees else 0

    plt.figure(figsize=(12, 6))
    if args.style == "bar":
        widths: list[float] = []
        if len(fees) > 1:
            for i, fee in enumerate(fees):
                left = fees[i - 1] if i > 0 else None
                right = fees[i + 1] if i < len(fees) - 1 else None
                if left is None:
                    width = (right - fee) * 0.9
                elif right is None:
                    width = (fee - left) * 0.9
                else:
                    width = (right - left) * 0.45
                widths.append(max(width, 0.0001))
        else:
            widths = [0.1]
        plt.bar(fees, vsizes, width=widths, align="center", color="#4B5F8A", alpha=0.7)
    else:
        plt.step(fees, vsizes, where="mid", color="#4B5F8A", linewidth=1.5)
        plt.fill_between(fees, vsizes, step="mid", color="#4B5F8A", alpha=0.25)
    plt.xlabel("fee rate (sat/vB)")
    plt.ylabel("vsize")
    plt.title(f"fee_histogram @ {observed_at} (min={min_fee:.3g}, max={max_fee:.3g})")
    if args.log_x:
        plt.xscale("log")
    if args.log_y:
        plt.yscale("log")
    plt.tight_layout()
    plt.savefig(args.out, dpi=150)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
