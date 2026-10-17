# /// script
# requires-python = ">=3.10"
# dependencies = ["mcp>=1.10,<2", "couchbase>=4.1,<5", "python-dotenv>=1.0"]
# ///
"""
analytics_server.py - read-only MCP server for the Couchbase Analytics service.

The Couchbase MCP server only talks to the Query and Data services. This one sends SQL++ for Analytics
to the Analytics nodes, so long-range questions over quantic.pos_history.* (full history, never purged)
run there instead of on the operational Query service.

Safety: every statement runs with read_only=True, so the Analytics service itself rejects DDL/DML.
Results are capped (max_rows) so a careless SELECT * cannot flood the conversation.

Run (Claude Desktop config):
  "quantic-analytics": {
    "command": "uv",
    "args": ["run", "--script", "/Users/melboulos/git/quantic_pos_mcp/mcp/analytics_server.py"],
    "env": { "CB_CONNECTION_STRING": "...", "CB_USERNAME": "...", "CB_PASSWORD": "..." }
  }
Connection settings come from the env block, falling back to the repo's .env.
"""

import logging
import os
import sys
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

import anyio
from mcp.server.fastmcp import FastMCP

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import AnalyticsOptions, ClusterOptions

# stdout carries the MCP protocol, so all logging goes to stderr
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("couchbase").setLevel(logging.WARNING)
log = logging.getLogger("quantic-analytics")

DEFAULT_TIMEOUT_S = 120
MAX_TIMEOUT_S = 600
DEFAULT_MAX_ROWS = 500
MAX_ROWS = 5000

mcp = FastMCP(
    "quantic-analytics",
    instructions=(
        "Runs read-only SQL++ for Analytics on the Couchbase Analytics service. Use it for questions that span "
        "more than ~90 days or the whole platform: quantic.pos_history.* holds the full history (orders, cart, "
        "check, payment, orderSummary, transactionDetail); quantic.pos.location/item/employee are reference data. "
        "Use the Query-service tools (quantic-couchbase) only for recent, selective lookups: the hot tier holds just the "
        "last 90 days, so anything older answered there would be incomplete, not just slow. "
        "Analytics dialect: no GSI indexes needed; timestamps are UTC epoch ms, convert with "
        "datetime_from_unix_time_in_ms(x) and get_year/get_month/get_date_from_datetime; use LET for derived values. "
        "Sales = closeType = \"paid\"; net = subTotal - totalDiscount. Call list_analytics_datasets first if unsure."
    ),
)

_cluster: Cluster | None = None
_lock = threading.Lock()


def _get_cluster() -> Cluster:
    """Connect lazily so the server starts (and lists its tools) even if the cluster is paused."""
    global _cluster
    with _lock:
        if _cluster is None:
            conn = os.environ.get("CB_CONNECTION_STRING")
            user = os.environ.get("CB_USERNAME")
            pwd = os.environ.get("CB_PASSWORD")
            if not (conn and user and pwd):
                raise RuntimeError("Set CB_CONNECTION_STRING, CB_USERNAME and CB_PASSWORD (MCP env block or .env)")
            opts = ClusterOptions(PasswordAuthenticator(user, pwd))
            if conn.startswith("couchbases://") and "cloud.couchbase.com" in conn:
                opts.apply_profile("wan_development")  # Capella from a laptop: longer connect/KV timeouts
            cluster = Cluster(conn, opts)
            cluster.wait_until_ready(timedelta(seconds=30))
            _cluster = cluster
            log.info("connected to %s as %s", conn, user)
        return _cluster


def _run(statement: str, timeout_s: int, max_rows: int) -> dict[str, Any]:
    result = _get_cluster().analytics_query(
        statement, AnalyticsOptions(read_only=True, timeout=timedelta(seconds=timeout_s))
    )
    rows, truncated = [], False
    for row in result.rows():
        if len(rows) >= max_rows:
            truncated = True
            break
        rows.append(row)

    out: dict[str, Any] = {"rows": rows, "returned": len(rows), "truncated": truncated}
    meta = result.metadata()
    if meta is not None:
        m = meta.metrics()
        if m is not None and not truncated:  # metrics arrive after the last row
            out["metrics"] = {
                "elapsed": str(m.elapsed_time()),
                "execution": str(m.execution_time()),
                "result_count": int(m.result_count()),
                "processed_objects": int(m.processed_objects()),
            }
        if meta.warnings():
            out["warnings"] = [str(w) for w in meta.warnings()]
    if truncated:
        out["note"] = f"Stopped at {max_rows} rows. Aggregate in the query or raise max_rows (limit {MAX_ROWS})."
    return out


@mcp.tool()
async def run_analytics_query(statement: str, timeout_seconds: int = DEFAULT_TIMEOUT_S,
                              max_rows: int = DEFAULT_MAX_ROWS) -> dict[str, Any]:
    """Run a read-only SQL++ for Analytics statement on the Analytics service.

    Returns rows plus server metrics (elapsed time, objects processed). Prefer aggregates over raw rows.
    Example: SELECT COUNT(*) AS n FROM quantic.pos_history.orders o WHERE o.closeType = "paid"
    """
    timeout_s = max(1, min(int(timeout_seconds), MAX_TIMEOUT_S))
    rows = max(1, min(int(max_rows), MAX_ROWS))
    log.info("analytics query (timeout %ss): %s", timeout_s, " ".join(statement.split())[:300])
    return await anyio.to_thread.run_sync(_run, statement, timeout_s, rows)


@mcp.tool()
async def list_analytics_datasets() -> dict[str, Any]:
    """List the collections mapped into the Analytics service (what run_analytics_query can read)."""
    stmt = ('SELECT VALUE d.DataverseName || "." || d.DatasetName FROM Metadata.`Dataset` d '
            'WHERE d.DataverseName != "Metadata" ORDER BY d.DataverseName, d.DatasetName')
    return await anyio.to_thread.run_sync(_run, stmt, 30, MAX_ROWS)


if __name__ == "__main__":
    mcp.run()
