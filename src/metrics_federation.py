#!/usr/bin/env python3
"""BlackRoad OS - Thanos-compatible metrics federation with SQLite backend."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DB_PATH = Path.home() / ".blackroad" / "thanos.db"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class StoreEndpoint:
    id: str
    name: str
    address: str
    store_type: str  # "prometheus", "victoriametrics", "local"
    labels: dict[str, str] = field(default_factory=dict)
    registered_at: float = field(default_factory=time.time)
    healthy: bool = True


@dataclass
class Block:
    id: str
    store_id: str
    min_time: float
    max_time: float
    series_count: int
    samples_count: int
    labels: dict[str, str] = field(default_factory=dict)
    compaction_level: int = 1
    uploaded: bool = False
    created_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# MetricsFederation
# ---------------------------------------------------------------------------

class MetricsFederation:
    """SQLite-backed Thanos-compatible metrics federation layer."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS store_endpoints (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                store_type TEXT NOT NULL,
                labels TEXT NOT NULL DEFAULT '{}',
                registered_at REAL NOT NULL,
                healthy INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS blocks (
                id TEXT PRIMARY KEY,
                store_id TEXT NOT NULL,
                min_time REAL NOT NULL,
                max_time REAL NOT NULL,
                series_count INTEGER NOT NULL DEFAULT 0,
                samples_count INTEGER NOT NULL DEFAULT 0,
                labels TEXT NOT NULL DEFAULT '{}',
                compaction_level INTEGER NOT NULL DEFAULT 1,
                uploaded INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS federated_queries (
                id TEXT PRIMARY KEY,
                expr TEXT NOT NULL,
                stores_queried TEXT NOT NULL DEFAULT '[]',
                result_count INTEGER NOT NULL DEFAULT 0,
                duration_ms REAL,
                queried_at REAL NOT NULL
            );
        """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Store management
    # ------------------------------------------------------------------

    def register_store(self, name: str, address: str, store_type: str = "prometheus",
                       labels: dict | None = None) -> StoreEndpoint:
        store = StoreEndpoint(
            id=str(uuid.uuid4()),
            name=name,
            address=address,
            store_type=store_type,
            labels=labels or {},
            registered_at=time.time(),
        )
        self._conn.execute(
            "INSERT INTO store_endpoints (id, name, address, store_type, labels, registered_at, healthy) "
            "VALUES (?,?,?,?,?,?,?)",
            (store.id, store.name, store.address, store.store_type,
             json.dumps(store.labels), store.registered_at, 1),
        )
        self._conn.commit()
        return store

    def deregister_store(self, store_id: str) -> bool:
        cur = self._conn.execute("DELETE FROM store_endpoints WHERE id=?", (store_id,))
        self._conn.commit()
        return cur.rowcount > 0

    def get_store_status(self) -> list[dict]:
        rows = self._conn.execute("SELECT * FROM store_endpoints ORDER BY registered_at DESC").fetchall()
        result = []
        for r in rows:
            block_count = self._conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE store_id=?", (r["id"],)
            ).fetchone()[0]
            result.append({
                "id": r["id"],
                "name": r["name"],
                "address": r["address"],
                "type": r["store_type"],
                "labels": json.loads(r["labels"]),
                "healthy": bool(r["healthy"]),
                "block_count": block_count,
                "registered_at": r["registered_at"],
            })
        return result

    # ------------------------------------------------------------------
    # Federated query
    # ------------------------------------------------------------------

    def federated_query(self, expr: str, start: float | None = None,
                        end: float | None = None, step: int = 60) -> dict[str, Any]:
        t0 = time.time()
        start = start or (time.time() - 3600)
        end = end or time.time()

        # Find blocks that overlap the time range
        relevant_blocks = self._conn.execute(
            "SELECT DISTINCT store_id FROM blocks WHERE min_time <= ? AND max_time >= ?",
            (end, start),
        ).fetchall()
        stores_queried = [r["store_id"] for r in relevant_blocks]

        # Simulate federated result (in production, would query real stores)
        result = {
            "expr": expr,
            "stores_queried": stores_queried,
            "time_range": {"start": start, "end": end, "step": step},
            "data": {"resultType": "matrix", "result": []},
        }

        # Log the query
        query_id = str(uuid.uuid4())
        duration = (time.time() - t0) * 1000
        self._conn.execute(
            "INSERT INTO federated_queries (id, expr, stores_queried, result_count, duration_ms, queried_at) "
            "VALUES (?,?,?,?,?,?)",
            (query_id, expr, json.dumps(stores_queried), 0, duration, time.time()),
        )
        self._conn.commit()
        result["query_id"] = query_id
        result["duration_ms"] = duration
        return result

    # ------------------------------------------------------------------
    # Block management
    # ------------------------------------------------------------------

    def create_block(self, store_id: str, min_time: float, max_time: float,
                     series_count: int = 0, samples_count: int = 0,
                     labels: dict | None = None) -> Block:
        block = Block(
            id=str(uuid.uuid4()),
            store_id=store_id,
            min_time=min_time,
            max_time=max_time,
            series_count=series_count,
            samples_count=samples_count,
            labels=labels or {},
            created_at=time.time(),
        )
        self._conn.execute(
            "INSERT INTO blocks (id, store_id, min_time, max_time, series_count, "
            "samples_count, labels, compaction_level, uploaded, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (block.id, block.store_id, block.min_time, block.max_time,
             block.series_count, block.samples_count, json.dumps(block.labels),
             block.compaction_level, 0, block.created_at),
        )
        self._conn.commit()
        return block

    def list_blocks(self, store_id: str | None = None, uploaded: bool | None = None) -> list[Block]:
        clauses, params = [], []
        if store_id:
            clauses.append("store_id=?"); params.append(store_id)
        if uploaded is not None:
            clauses.append("uploaded=?"); params.append(1 if uploaded else 0)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM blocks {where} ORDER BY min_time", params
        ).fetchall()
        return [Block(
            id=r["id"], store_id=r["store_id"], min_time=r["min_time"],
            max_time=r["max_time"], series_count=r["series_count"],
            samples_count=r["samples_count"], labels=json.loads(r["labels"]),
            compaction_level=r["compaction_level"], uploaded=bool(r["uploaded"]),
            created_at=r["created_at"],
        ) for r in rows]

    def compact_blocks(self, store_id: str, max_blocks: int = 4) -> dict:
        """Compact multiple small blocks into fewer larger ones (simulated)."""
        blocks = self.list_blocks(store_id=store_id)
        if len(blocks) < 2:
            return {"compacted": 0, "blocks_before": len(blocks), "blocks_after": len(blocks)}

        # Group into groups of max_blocks and merge each group
        groups = [blocks[i:i + max_blocks] for i in range(0, len(blocks), max_blocks)]
        compacted = 0
        for group in groups:
            if len(group) < 2:
                continue
            min_t = min(b.min_time for b in group)
            max_t = max(b.max_time for b in group)
            total_series = sum(b.series_count for b in group)
            total_samples = sum(b.samples_count for b in group)
            max_level = max(b.compaction_level for b in group) + 1
            new_block = self.create_block(store_id, min_t, max_t, total_series, total_samples)
            self._conn.execute("UPDATE blocks SET compaction_level=? WHERE id=?",
                               (max_level, new_block.id))
            ids = [b.id for b in group]
            self._conn.executemany("DELETE FROM blocks WHERE id=?", [(i,) for i in ids])
            compacted += len(ids)
        self._conn.commit()
        after = len(self.list_blocks(store_id=store_id))
        return {"compacted": compacted, "blocks_before": len(blocks), "blocks_after": after}

    def downsample_blocks(self, store_id: str, resolution_seconds: int = 300) -> dict:
        """Mark blocks as downsampled (simulated)."""
        blocks = self.list_blocks(store_id=store_id)
        count = 0
        for b in blocks:
            duration = b.max_time - b.min_time
            if duration > resolution_seconds * 10:
                new_samples = max(1, int(b.samples_count / resolution_seconds))
                self._conn.execute(
                    "UPDATE blocks SET samples_count=? WHERE id=?", (new_samples, b.id)
                )
                count += 1
        self._conn.commit()
        return {"downsampled": count, "resolution_seconds": resolution_seconds}

    def upload_block(self, block_id: str) -> bool:
        cur = self._conn.execute("UPDATE blocks SET uploaded=1 WHERE id=?", (block_id,))
        self._conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Retention
    # ------------------------------------------------------------------

    def get_retention_policy(self) -> dict:
        return {
            "raw": "30d",
            "5m_downsampled": "90d",
            "1h_downsampled": "1y",
            "delete_delay": "48h",
        }

    def apply_retention(self, max_age_seconds: float = 86400 * 30) -> int:
        cutoff = time.time() - max_age_seconds
        cur = self._conn.execute(
            "DELETE FROM blocks WHERE max_time < ? AND uploaded=1", (cutoff,)
        )
        self._conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        stores = self._conn.execute("SELECT COUNT(*) FROM store_endpoints").fetchone()[0]
        blocks = self._conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        queries = self._conn.execute("SELECT COUNT(*) FROM federated_queries").fetchone()[0]
        total_series = self._conn.execute(
            "SELECT COALESCE(SUM(series_count),0) FROM blocks"
        ).fetchone()[0]
        total_samples = self._conn.execute(
            "SELECT COALESCE(SUM(samples_count),0) FROM blocks"
        ).fetchone()[0]
        return {
            "stores": stores,
            "blocks": blocks,
            "total_series": total_series,
            "total_samples": total_samples,
            "queries": queries,
            "db_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="BlackRoad OS Metrics Federation (Thanos)")
    sub = parser.add_subparsers(dest="cmd")

    p_stores = sub.add_parser("stores", help="List registered stores")

    p_blocks = sub.add_parser("blocks", help="List blocks")
    p_blocks.add_argument("--store-id", default=None)

    p_compact = sub.add_parser("compact", help="Compact blocks for a store")
    p_compact.add_argument("store_id")

    p_register = sub.add_parser("register", help="Register a store endpoint")
    p_register.add_argument("name")
    p_register.add_argument("address")
    p_register.add_argument("--type", default="prometheus", dest="store_type")

    args = parser.parse_args()
    fed = MetricsFederation()

    if args.cmd == "stores":
        statuses = fed.get_store_status()
        if not statuses:
            print("No stores registered.")
        for s in statuses:
            health = "✓" if s["healthy"] else "✗"
            print(f"  {health} {s['name']} ({s['type']}) @ {s['address']}  blocks={s['block_count']}")
    elif args.cmd == "blocks":
        blocks = fed.list_blocks(store_id=args.store_id)
        if not blocks:
            print("No blocks found.")
        for b in blocks:
            span = f"{b.min_time:.0f}-{b.max_time:.0f}"
            print(f"  {b.id[:8]}  store={b.store_id[:8]}  time={span}  "
                  f"series={b.series_count}  level={b.compaction_level}")
    elif args.cmd == "compact":
        result = fed.compact_blocks(args.store_id)
        print(json.dumps(result, indent=2))
    elif args.cmd == "register":
        store = fed.register_store(args.name, args.address, store_type=args.store_type)
        print(f"Registered {store.name} with id={store.id}")
    else:
        stats = fed.get_stats()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
