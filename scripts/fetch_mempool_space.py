#!/usr/bin/env python3
"""
Fetch mempool.space API data and write JSONL/DB snapshots.

Outputs:
  - mempool.jsonl: /api/mempool snapshots
  - fees.jsonl: /api/v1/fees/recommended snapshots
  - mempool_blocks.jsonl: /api/v1/fees/mempool-blocks snapshots (optional)
  - fees_precise.jsonl: /api/v1/fees/precise snapshots
  - replacements.jsonl: /api/v1/replacements snapshots (optional)
  - api_snapshots + replacement_events tables (SQLite/Postgres) when --db is set
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import ssl
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://mempool.space"
DEFAULT_OUTDIR = "./data"

ENDPOINTS = {
    "mempool": "/api/mempool",
    "fees": "/api/v1/fees/recommended",
    "fees_precise": "/api/v1/fees/precise",
    "mempool_blocks": "/api/v1/fees/mempool-blocks",
    "replacements": "/api/v1/replacements",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(base_url: str, path: str, timeout: int, context: ssl.SSLContext | None) -> dict | list:
    url = base_url.rstrip("/") + path
    req = Request(url, headers={"User-Agent": "mempool-watcher/1.0"})
    if context:
        resp = urlopen(req, timeout=timeout, context=context)
    else:
        resp = urlopen(req, timeout=timeout)
    with resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def fetch_json_with_retry(
    base_url: str,
    path: str,
    timeout: int,
    retries: int,
    backoff_base: float,
    backoff_max: float,
    context: ssl.SSLContext | None,
) -> tuple[dict | list, int]:
    attempt = 0
    while True:
        start = time.perf_counter()
        try:
            data = fetch_json(base_url, path, timeout, context)
            latency_ms = int((time.perf_counter() - start) * 1000)
            return data, latency_ms
        except (HTTPError, URLError, json.JSONDecodeError) as exc:
            attempt += 1
            if attempt > retries:
                raise exc
            delay = min(backoff_base * (2 ** (attempt - 1)), backoff_max)
            time.sleep(delay)


def append_jsonl(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def iter_replacement_edges(node: dict) -> list[dict]:
    edges: list[dict] = []
    new_tx = node.get("tx", {})
    new_time = node.get("time")
    for repl in node.get("replaces", []) or []:
        old_tx = repl.get("tx", {})
        edges.append(
            {
                "new_tx": new_tx,
                "old_tx": old_tx,
                "new_time": new_time,
                "old_time": repl.get("time"),
                "interval": repl.get("interval"),
                "full_rbf": repl.get("fullRbf"),
                "mined": repl.get("mined"),
            }
        )
        edges.extend(iter_replacement_edges(repl))
    return edges


class DbWriter:
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url
        self.kind = "sqlite"
        self.conn = None

        parsed = urlparse(db_url)
        if parsed.scheme in ("postgres", "postgresql"):
            self.kind = "postgres"
        elif parsed.scheme in ("sqlite", ""):
            self.kind = "sqlite"
        else:
            raise ValueError(f"Unsupported db url scheme: {parsed.scheme}")

        if self.kind == "sqlite":
            path = parsed.path if parsed.scheme == "sqlite" else db_url
            if path.startswith("/") and parsed.netloc:
                path = f"/{parsed.netloc}{parsed.path}"
            if path.startswith("/./") or path.startswith("/../"):
                path = path[1:]
            db_dir = os.path.dirname(path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            self.conn = sqlite3.connect(path)
            self.conn.execute("PRAGMA journal_mode=WAL;")
        else:
            try:
                import psycopg2  # type: ignore
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("psycopg2 is required for Postgres support") from exc
            self.conn = psycopg2.connect(db_url)

        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cursor = self.conn.cursor()
        if self.kind == "sqlite":
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS api_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    latency_ms INTEGER,
                    error TEXT,
                    data_json TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS replacement_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at TEXT NOT NULL,
                    event_time INTEGER,
                    old_txid TEXT NOT NULL,
                    new_txid TEXT NOT NULL,
                    old_fee_sat REAL,
                    old_feerate REAL,
                    old_vsize REAL,
                    new_fee_sat REAL,
                    new_feerate REAL,
                    new_vsize REAL,
                    interval_seconds INTEGER,
                    full_rbf INTEGER,
                    mined INTEGER
                )
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_snapshots_time ON api_snapshots(observed_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_snapshots_endpoint ON api_snapshots(endpoint)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_replacement_events_time ON replacement_events(observed_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_replacement_events_txids ON replacement_events(old_txid, new_txid)")
        else:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS api_snapshots (
                    id SERIAL PRIMARY KEY,
                    observed_at TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    latency_ms INTEGER,
                    error TEXT,
                    data_json TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS replacement_events (
                    id SERIAL PRIMARY KEY,
                    observed_at TEXT NOT NULL,
                    event_time BIGINT,
                    old_txid TEXT NOT NULL,
                    new_txid TEXT NOT NULL,
                    old_fee_sat DOUBLE PRECISION,
                    old_feerate DOUBLE PRECISION,
                    old_vsize DOUBLE PRECISION,
                    new_fee_sat DOUBLE PRECISION,
                    new_feerate DOUBLE PRECISION,
                    new_vsize DOUBLE PRECISION,
                    interval_seconds BIGINT,
                    full_rbf INTEGER,
                    mined INTEGER
                )
                """
            )
        self.conn.commit()

    def write_snapshot(
        self,
        observed_at: str,
        endpoint: str,
        success: bool,
        latency_ms: int | None,
        error: str | None,
        data: dict | list | None,
    ) -> None:
        cursor = self.conn.cursor()
        data_json = json.dumps(data, ensure_ascii=True) if data is not None else None
        cursor.execute(
            """
            INSERT INTO api_snapshots (observed_at, endpoint, success, latency_ms, error, data_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            if self.kind == "postgres"
            else """
            INSERT INTO api_snapshots (observed_at, endpoint, success, latency_ms, error, data_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (observed_at, endpoint, int(success), latency_ms, error, data_json),
        )
        self.conn.commit()

    def write_replacement_event(
        self,
        observed_at: str,
        event_time: int | None,
        old_txid: str,
        new_txid: str,
        old_fee_sat: float | None,
        old_feerate: float | None,
        old_vsize: float | None,
        new_fee_sat: float | None,
        new_feerate: float | None,
        new_vsize: float | None,
        interval_seconds: int | None,
        full_rbf: bool | None,
        mined: bool | None,
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO replacement_events (
                observed_at,
                event_time,
                old_txid,
                new_txid,
                old_fee_sat,
                old_feerate,
                old_vsize,
                new_fee_sat,
                new_feerate,
                new_vsize,
                interval_seconds,
                full_rbf,
                mined
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            if self.kind == "postgres"
            else """
            INSERT INTO replacement_events (
                observed_at,
                event_time,
                old_txid,
                new_txid,
                old_fee_sat,
                old_feerate,
                old_vsize,
                new_fee_sat,
                new_feerate,
                new_vsize,
                interval_seconds,
                full_rbf,
                mined
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observed_at,
                event_time,
                old_txid,
                new_txid,
                old_fee_sat,
                old_feerate,
                old_vsize,
                new_fee_sat,
                new_feerate,
                new_vsize,
                interval_seconds,
                int(full_rbf) if full_rbf is not None else None,
                int(mined) if mined is not None else None,
            ),
        )
        self.conn.commit()


class Metrics:
    def __init__(self) -> None:
        self.total_ok = 0
        self.total_err = 0
        self.total_latency_ms = 0

    def record(self, ok: bool, latency_ms: int | None) -> None:
        if ok:
            self.total_ok += 1
            if latency_ms is not None:
                self.total_latency_ms += latency_ms
        else:
            self.total_err += 1

    def summary(self) -> str:
        total = self.total_ok + self.total_err
        avg_latency = int(self.total_latency_ms / self.total_ok) if self.total_ok else 0
        success_rate = (self.total_ok / total * 100) if total else 0.0
        return f"success={self.total_ok} error={self.total_err} rate={success_rate:.1f}% avg_latency_ms={avg_latency}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch mempool.space snapshots.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--interval", type=int, default=0, help="Seconds between polls; 0 for single run.")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--backoff-base", type=float, default=0.5)
    parser.add_argument("--backoff-max", type=float, default=8.0)
    parser.add_argument("--db", default="", help="sqlite:///path/to.db or postgres://user:pass@host/db")
    parser.add_argument("--no-jsonl", action="store_true")
    parser.add_argument("--no-mempool-blocks", action="store_true")
    parser.add_argument("--no-replacements", action="store_true")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification (not recommended).")
    args = parser.parse_args()

    out_mempool = os.path.join(args.outdir, "mempool.jsonl")
    out_fees = os.path.join(args.outdir, "fees.jsonl")
    out_fees_precise = os.path.join(args.outdir, "fees_precise.jsonl")
    out_blocks = os.path.join(args.outdir, "mempool_blocks.jsonl")
    out_replacements = os.path.join(args.outdir, "replacements.jsonl")

    db_writer = DbWriter(args.db) if args.db else None
    metrics = Metrics()

    ssl_context = ssl._create_unverified_context() if args.insecure else None

    while True:
        ts = utc_now_iso()
        for name, path in ENDPOINTS.items():
            if name == "mempool_blocks" and args.no_mempool_blocks:
                continue
            if name == "replacements" and args.no_replacements:
                continue
            try:
                data, latency_ms = fetch_json_with_retry(
                    args.base_url,
                    path,
                    args.timeout,
                    args.retries,
                    args.backoff_base,
                    args.backoff_max,
                    ssl_context,
                )
                if not args.no_jsonl:
                    if name == "mempool":
                        append_jsonl(out_mempool, {"observed_at": ts, "data": data})
                    elif name == "fees":
                        append_jsonl(out_fees, {"observed_at": ts, "data": data})
                    elif name == "fees_precise":
                        append_jsonl(out_fees_precise, {"observed_at": ts, "data": data})
                    elif name == "replacements":
                        append_jsonl(out_replacements, {"observed_at": ts, "data": data})
                    else:
                        append_jsonl(out_blocks, {"observed_at": ts, "data": data})
                if db_writer:
                    db_writer.write_snapshot(ts, name, True, latency_ms, None, data)
                    if name == "replacements":
                        for item in data:
                            for edge in iter_replacement_edges(item):
                                new_tx = edge.get("new_tx", {})
                                old_tx = edge.get("old_tx", {})
                                new_txid = new_tx.get("txid")
                                old_txid = old_tx.get("txid")
                                if not new_txid or not old_txid:
                                    continue
                                db_writer.write_replacement_event(
                                    observed_at=ts,
                                    event_time=edge.get("new_time"),
                                    old_txid=old_txid,
                                    new_txid=new_txid,
                                    old_fee_sat=old_tx.get("fee"),
                                    old_feerate=old_tx.get("rate"),
                                    old_vsize=old_tx.get("vsize"),
                                    new_fee_sat=new_tx.get("fee"),
                                    new_feerate=new_tx.get("rate"),
                                    new_vsize=new_tx.get("vsize"),
                                    interval_seconds=edge.get("interval"),
                                    full_rbf=edge.get("full_rbf"),
                                    mined=edge.get("mined"),
                                )
                metrics.record(True, latency_ms)
            except (HTTPError, URLError, json.JSONDecodeError) as exc:
                if db_writer:
                    db_writer.write_snapshot(ts, name, False, None, str(exc), None)
                metrics.record(False, None)
                print(f"[{ts}] {name} error: {exc}", flush=True)

        print(f"[{ts}] {metrics.summary()}", flush=True)

        if args.interval <= 0:
            break
        time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
