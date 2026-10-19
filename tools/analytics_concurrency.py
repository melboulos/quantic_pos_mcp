"""Run the same Analytics query N times concurrently and report each elapsed time.

Usage (from the repo root):
  .venv/bin/python tools/analytics_concurrency.py            # 3 concurrent runs
  .venv/bin/python tools/analytics_concurrency.py -n 1       # single-query baseline
Reads CB_CONNECTION_STRING / CB_USERNAME / CB_PASSWORD from .env (never printed).
"""
import argparse, os, time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import AnalyticsOptions, ClusterOptions, ClusterTimeoutOptions

QUERY = """
SELECT get_year(datetime_from_unix_time_in_ms(c.dateCreated)) AS yr,
       COUNT(*) AS lines, SUM(c.quantity) AS units, ROUND(SUM(c.subTotal), 0) AS sales
FROM quantic.pos_history.cart c
WHERE c.closeType = "paid"
GROUP BY get_year(datetime_from_unix_time_in_ms(c.dateCreated))
ORDER BY yr;
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=3, help="concurrent runs")
    args = ap.parse_args()
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
    opts = ClusterOptions(
        PasswordAuthenticator(os.environ["CB_USERNAME"], os.environ["CB_PASSWORD"]),
        timeout_options=ClusterTimeoutOptions(connect_timeout=timedelta(seconds=30),
                                              analytics_timeout=timedelta(minutes=15)))
    for attempt in range(1, 4):   # bootstrap over a home connection can time out; retry a few times
        try:
            cluster = Cluster(os.environ["CB_CONNECTION_STRING"], opts)
            break
        except Exception as e:
            print(f"connect attempt {attempt} failed: {type(e).__name__}; retrying in 10s")
            time.sleep(10)
    else:
        raise SystemExit("could not connect after 3 attempts - check network / Capella allowed IPs")
    cluster.wait_until_ready(timedelta(seconds=30))

    def run(i):
        t0 = time.time()
        res = cluster.analytics_query(QUERY, AnalyticsOptions(timeout=timedelta(minutes=15)))
        rows = list(res.rows())
        m = res.metadata().metrics()
        return i, time.time() - t0, m.execution_time(), len(rows)

    print(f"{time.strftime('%H:%M:%S')} starting {args.n} concurrent run(s)")
    t_all = time.time()
    with ThreadPoolExecutor(max_workers=args.n) as ex:
        for i, wall, exe, n in ex.map(run, range(1, args.n + 1)):
            print(f"  run {i}: wall {wall:6.1f}s  server execution {exe.total_seconds():6.1f}s  rows {n}")
    print(f"{time.strftime('%H:%M:%S')} all done in {time.time() - t_all:.1f}s")

if __name__ == "__main__":
    main()
