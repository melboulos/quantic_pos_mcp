#!/usr/bin/env python3
"""
purge_hot.py - one-time cleanup of the hot tier (quantic.pos.*) after history is in place.

Deletes closed documents older than the cutoff from the hot collections, ONLY after checking that a
sample of them already exists in quantic.pos_history.*. Open (not yet closed) documents are kept.

BEFORE RUNNING
  1. All six Eventing copy functions show Backlog 0.
  2. Each function's OnDelete is silent (otherwise every delete writes a log line).
  3. Take the "before" snapshot (docs/retention_snapshot.md).

USAGE
  # what would be deleted, plus the history check; changes nothing
  python tools/purge_hot.py --dry_run --create_indexes

  # do it (creates temporary indexes if missing, deletes in batches, drops the temporary indexes)
  python tools/purge_hot.py --create_indexes --drop_indexes

  --cutoff defaults to 2026-07-08T04:00:00Z: 90 days before the end of the demo data (2026-10-06, New York).
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions

COLLECTIONS = ["cart", "check", "orders", "payment", "orderSummary", "transactionDetail"]

p = argparse.ArgumentParser(description="Purge hot-tier documents older than the cutoff (after verifying history)")
p.add_argument("--cutoff", default="2026-07-08T04:00:00Z", help="ISO time (UTC); closed docs created before it are deleted")
p.add_argument("--collections", default=",".join(COLLECTIONS))
p.add_argument("--bucket", default=os.environ.get("CB_BUCKET", "quantic"))
p.add_argument("--batch", type=int, default=50000, help="documents per DELETE statement")
p.add_argument("--verify_sample", type=int, default=2000, help="docs per collection checked in history before deleting")
p.add_argument("--create_indexes", action="store_true", help="create + build temporary purge indexes if missing")
p.add_argument("--drop_indexes", action="store_true", help="drop the temporary purge indexes when done")
p.add_argument("--dry_run", action="store_true", help="count and verify only; delete nothing")
args = p.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("couchbase").setLevel(logging.WARNING)
log = logging.getLogger("purge")

CUTOFF_MS = int(datetime.fromisoformat(args.cutoff.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)
B = args.bucket
# closed docs only; transactionDetail has no isClosed field, so MISSING counts as closed
CLOSED = "(x.isClosed = 1 OR x.isClosed IS MISSING)"


def connect():
    conn, user, pwd = os.environ.get("CB_CONNECTION_STRING"), os.environ.get("CB_USERNAME"), os.environ.get("CB_PASSWORD")
    if not (conn and user and pwd):
        sys.exit("Set CB_CONNECTION_STRING, CB_USERNAME and CB_PASSWORD in .env")
    c = Cluster(conn, ClusterOptions(PasswordAuthenticator(user, pwd), timeout_options=ClusterTimeoutOptions(
        query_timeout=timedelta(minutes=15), connect_timeout=timedelta(seconds=30))))
    c.wait_until_ready(timedelta(seconds=60))
    return c


def q(cluster, stmt, metrics=False):
    res = cluster.query(stmt, QueryOptions(timeout=timedelta(minutes=15), metrics=metrics))
    rows = list(res.rows())
    return rows, (res.metadata().metrics() if metrics else None)


def ensure_index(cluster, coll):
    name = f"ix_tmp_purge_{coll.lower()}"
    rows, _ = q(cluster, f'SELECT RAW i.state FROM system:indexes i WHERE i.bucket_id = "{B}" AND i.scope_id = "pos" '
                         f'AND i.keyspace_id = "{coll}" AND i.name = "{name}"')
    if not rows:
        if not args.create_indexes:
            sys.exit(f"missing {name}; rerun with --create_indexes")
        log.info(f"creating {name}")
        q(cluster, f"CREATE INDEX {name} ON {B}.pos.`{coll}`(dateCreated, isClosed)")
    while True:
        rows, _ = q(cluster, f'SELECT RAW i.state FROM system:indexes i WHERE i.bucket_id = "{B}" AND i.scope_id = "pos" '
                             f'AND i.keyspace_id = "{coll}" AND i.name = "{name}"')
        if rows and rows[0] == "online":
            return name
        log.info(f"waiting for {name} to build ({rows[0] if rows else 'pending'})")
        time.sleep(15)


def main():
    cluster = connect()
    colls = [c.strip() for c in args.collections.split(",") if c.strip()]
    log.info(f"cutoff {args.cutoff} ({CUTOFF_MS}); collections {colls}; dry_run={args.dry_run}")
    total = 0
    for coll in colls:
        idx = ensure_index(cluster, coll)
        where = f"x.dateCreated < {CUTOFF_MS} AND {CLOSED}"
        rows, _ = q(cluster, f"SELECT RAW COUNT(*) FROM {B}.pos.`{coll}` x USE INDEX ({idx}) WHERE {where}")
        n = rows[0]

        # safety: a sample of what would be deleted must already be in history
        rows, _ = q(cluster, f"SELECT RAW COUNT(*) FROM {B}.pos_history.`{coll}` h USE KEYS "
                             f"(SELECT RAW META(x).id FROM {B}.pos.`{coll}` x USE INDEX ({idx}) WHERE {where} "
                             f"LIMIT {args.verify_sample})")
        found = rows[0]
        sample = min(args.verify_sample, n)
        log.info(f"{coll}: {n:,} closed docs before cutoff; history check {found}/{sample}")
        if found < sample:
            sys.exit(f"ABORT {coll}: {sample - found} sampled docs missing from pos_history.{coll}. "
                     f"Wait for the Eventing backlog to reach 0, then rerun.")
        if args.dry_run or n == 0:
            continue

        done, t0 = 0, time.time()
        while True:
            _, m = q(cluster, f"DELETE FROM {B}.pos.`{coll}` x USE INDEX ({idx}) WHERE {where} LIMIT {args.batch}",
                     metrics=True)
            k = int(m.mutation_count() or 0)  # SDK returns UnsignedInt64
            done += k
            rate = done / max(1e-6, time.time() - t0)
            log.info(f"{coll}: deleted {done:,}/{n:,} ({rate:,.0f}/s)")
            if k == 0:
                break
        total += done

        if args.drop_indexes:
            q(cluster, f"DROP INDEX {idx} ON {B}.pos.`{coll}`")
            log.info(f"dropped {idx}")
    log.info(f"DONE deleted {total:,} documents" + (" (dry run: nothing deleted)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
