#!/usr/bin/env python3
"""Fetch mempool.space tx details for replacement events."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import ssl
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://mempool.space"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(base_url: str, path: str, timeout: int, context: ssl.SSLContext | None) -> dict:
    url = base_url.rstrip("/") + path
    req = Request(url, headers={"User-Agent": "mempool-watcher/1.0"})
    if context:
        resp = urlopen(req, timeout=timeout, context=context)
    else:
        resp = urlopen(req, timeout=timeout)
    with resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tx_details (
            txid TEXT PRIMARY KEY,
            fetched_at TEXT NOT NULL,
            success INTEGER NOT NULL,
            status_code INTEGER,
            error TEXT,
            data_json TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_details_fetched_at ON tx_details(fetched_at)")
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch tx details for replacement events.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB (e.g. ./data/mempool.db)")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--outdir", default="./data")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds to sleep between requests")
    parser.add_argument("--limit", type=int, default=0, help="Max txids to fetch (0=all)")
    parser.add_argument("--since", default="", help="ISO time filter on replacement observed_at")
    parser.add_argument("--fetch-all", action="store_true", help="Fetch even if txid already stored")
    parser.add_argument("--no-jsonl", action="store_true")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification (not recommended)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)
    cur = conn.cursor()

    params: list[str] = []
    since_clause = ""
    if args.since:
        since_clause = " WHERE observed_at >= ?"
        params.append(args.since)

    if args.fetch_all:
        query = (
            "SELECT txid FROM ("
            "SELECT old_txid AS txid FROM replacement_events" + since_clause + " "
            "UNION SELECT new_txid AS txid FROM replacement_events" + since_clause +
            ") WHERE txid IS NOT NULL"
        )
    else:
        query = (
            "SELECT txid FROM ("
            "SELECT old_txid AS txid FROM replacement_events" + since_clause + " "
            "UNION SELECT new_txid AS txid FROM replacement_events" + since_clause +
            ") t LEFT JOIN tx_details d ON t.txid = d.txid AND d.success = 1 "
            "WHERE t.txid IS NOT NULL AND d.txid IS NULL"
        )

    if args.limit and args.limit > 0:
        query += " LIMIT ?"
        params.append(str(args.limit))

    cur.execute(query, params)
    txids = [row[0] for row in cur.fetchall()]

    ssl_context = ssl._create_unverified_context() if args.insecure else None

    out_path = os.path.join(args.outdir, "tx_details.jsonl")
    if not args.no_jsonl:
        os.makedirs(args.outdir, exist_ok=True)

    for txid in txids:
        fetched_at = utc_now_iso()
        try:
            data = fetch_json(args.base_url, f"/api/tx/{txid}", args.timeout, ssl_context)
            cur.execute(
                "INSERT OR REPLACE INTO tx_details (txid, fetched_at, success, status_code, error, data_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (txid, fetched_at, 1, 200, None, json.dumps(data, ensure_ascii=True)),
            )
            conn.commit()
            if not args.no_jsonl:
                with open(out_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"fetched_at": fetched_at, "txid": txid, "data": data}, ensure_ascii=True) + "\n")
            print(f"[{fetched_at}] ok {txid}", flush=True)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            cur.execute(
                "INSERT OR REPLACE INTO tx_details (txid, fetched_at, success, status_code, error, data_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (txid, fetched_at, 0, exc.code, body[:1000], None),
            )
            conn.commit()
            print(f"[{fetched_at}] error {txid} status={exc.code}", flush=True)
        except URLError as exc:
            cur.execute(
                "INSERT OR REPLACE INTO tx_details (txid, fetched_at, success, status_code, error, data_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (txid, fetched_at, 0, None, str(exc), None),
            )
            conn.commit()
            print(f"[{fetched_at}] error {txid} {exc}", flush=True)

        if args.sleep > 0:
            time.sleep(args.sleep)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
