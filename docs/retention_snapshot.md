# Retention test: before / after snapshot

Demo environment = about 10% of Quantic production. Record "Before" after the Eventing backlog reaches 0,
"After" once compaction has settled following the purge (deleted-document markers are purged after the
metadata purge interval, often a few days). Same time of day, no load running.

## Environment
- Cluster / node sizes at "Before": 7 nodes: 3 Data+Eventing (svc-de-020..022), 2 Query+Index+Search (svc-qis-023..024), 2 Analytics (svc-a-025..026)
- Cluster / node sizes at "After":
- Purge cutoff: 2026-07-08T04:00:00Z (90 days before end of data)

## Data service

"Before" 2026-10-16 from the Capella Buckets page: 175,918,771 items in `quantic` (hot tier + history), Magma, 1 replica.
| Metric | Before | After | Change |
| --- | --- | --- | --- |
| Bucket items | 175,918,771 | 122,517,136 (2026-10-17 06:00 ET; expected 122,517,124, +12 unexplained) | -53,401,635 (-30.4%) |
| Bucket disk used (GB) | 457.3 | 452.5 day 1 (463,374 MiB, 2026-10-17 06:00 ET) | -4.8 (-1%): space not reclaimed yet |
| Bucket memory used (GB) | 3.6 | 2.95 (3,021 / 4,500 MiB quota) | |
| Resident ratio (%) | 0.06 | 1.40 | 23x higher |
| pos.cart items | 39,861,608 | | |
| pos.orders items | 9,185,313 | | |
| pos.orderSummary items | 9,688,496 | | |
| pos.payment items | 9,656,417 | | |
| pos.check items | 9,710,384 | | |
| pos.transactionDetail items | 7,961,061 | | |
| pos_history.* items (total) | 86,016,765 (cart 39,830,016; check 9,702,730; orders 9,178,045; orderSummary 9,688,496; payment 9,656,417; transactionDetail 7,961,061) | | |

Day-1 disk note: items dropped 30% but bucket disk only 1%. Magma reclaims space by background compaction once a
bucket's fragmentation passes its threshold (50% by default), and tombstones stay until the metadata purge interval
passes. Re-check disk on day 3. Data-node disk (all buckets, 2026-10-17): 161.80 + 160.23 + 159.39 = 481.4 GB of 900;
node CPU 5-8%.

## Index service

"Before" taken 2026-10-16, after the Eventing functions were redeployed "From now" (backlog < 1K), before any purge
indexes existed. An ad-hoc index (idx_orders_sales_by_loc, 5.94 GiB) was dropped before the snapshot and is excluded. From Capella Data Tools -> Indexes. Size = index data size, primary + replica (each index has 1 replica).
RR = resident ratio, primary / replica. 99.8% of index size sits on the six collections the purge touches.

| Index | Collection | Items before | Size before (GiB, both copies) | RR before | Items after | Size after (GiB) | RR after |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ix_cart_emp | cart | 39,861,608 | 28.05 | 0% / 0% | 14,964,495 | 24.47 (12.21 + 12.26) | 0% / 0% |
| ix_cart_name_date | cart | 39,311,751 | 23.92 | 0% / 0% | 14,738,160 | 22.16 (11.07 + 11.09) | 0% / 0% |
| ix_cart_loc_date | cart | 39,311,751 | 22.30 | 1% / 1% | 14,738,160 | 19.86 (9.93 + 9.93) | 0% / 0% |
| ix_cart_order | cart | 39,861,608 | 22.06 | 0% / 0% | 14,964,495 | 10.88 (5.45 + 5.43) | 0% / 1% |
| ix_payment_settle | payment | 13,975,298 | 6.59 | 1% / 2% | 5,120,282 | 6.25 (3.12 + 3.13) | 0% / 0% |
| ix_payment_txn | payment | 14,116,650 | 6.56 | 0% / 0% | 5,161,542 | 7.09 (3.54 + 3.55) | 0% / 0% |
| ix_orders_loc_date | orders | 9,150,375 | 6.27 | 0% / 0% | 3,532,476 | 4.43 (2.23 + 2.20) | 0% / 0% |
| ix_os_loc_date | orderSummary | 9,688,496 | 6.18 | 1% / 100% | 3,708,806 | 6.13 (3.06 + 3.07) | 24% / 19% |
| ix_orders_date | orders | 9,150,375 | 6.12 | 0% / 0% | 3,532,476 | 4.32 (2.15 + 2.17) | 0% / 0% |
| ix_os_loc_cc_date | orderSummary | 9,656,417 | 4.82 | 0% / 0% | 3,697,029 | 4.93 (2.46 + 2.47) | 30% / 27% |
| ix_cart_void | cart | 518,561 | 0.38 | 0% / 0% | 195,039 | 0.34 (172.1 + 171.1 MiB) | 0% / 0% |
| ix_tm_loc_date | timeManagement | 331,876 | 0.24 | 0% / 0% | 331,876 | 0.24 | 0% / 0% |
| ix_stock_loc_item | stockHistory | 19,448 | 0.01 | 24% / 100% | 19,448 | 0.01 | 0% / 0% |
| ix_employee, ix_item, ix_catalog_loc_date, ix_location, ix_audit_date | reference / logs | 13,921 (total) | 0.01 | - | 13,921 | 0.01 | - |
| **Total (18 indexes x 2 copies)** | | | **133.52** | | | **111.13 day 1 (-16.8%)** | |

"After" day 1 (2026-10-17 morning): item counts on the cart indexes match the purge exactly (-62%), but sizes fell only
7-13% (ix_cart_order -51%). Bytes per entry roughly doubled (ix_cart_emp ~377 B -> ~875 B per copy): the index storage still
holds the deleted entries' space until its own log cleaning / compaction catches up, same as the bucket disk. Re-check day 3.
Reference indexes (employee, item, location, catalog, audit) unchanged, as expected. Total index data 133.52 -> 111.13 GiB
(-17%) while purged-collection entries fell ~62%. Two indexes even GREW (ix_payment_txn +8%, ix_os_loc_cc_date +2%): deletes
are written to the log-structured index store as new records before the old space is cleaned. orderSummary indexes are now
19-30% resident (were 0-1%): orderSummary was the last collection with indexes to be purged, so its pages are the most
recently touched; the cart indexes, purged first, are back to 0%.

Expected after the purge: orders before the 2026-07-08 cutoff are ~62% of the year (later months are larger), so items on
the purged collections should drop ~60% and index size toward ~52 GiB once disk cleanup settles. Resident ratio should rise.

### Temporary purge indexes (built 2026-10-16 by the dry run, dropped by the real run)

On (dateCreated, isClosed), 1 replica each. Not part of "Before" or "After". Note when reading the Index service memory total.

| Index | Items | Size primary / replica (GiB) | RR primary / replica (about 20 min after the builds) |
| --- | ---: | --- | --- |
| ix_tmp_purge_cart | 39,861,608 | 6.50 / 6.49 | 0% / 0% |
| ix_tmp_purge_check | 9,710,384 | 1.56 / 1.56 | 0% / 0% |
| ix_tmp_purge_orders | 9,185,313 | 1.51 / 1.51 | 0% / 0% |
| ix_tmp_purge_payment | 9,656,417 | 1.59 / 1.59 | 61% / 45% |
| ix_tmp_purge_ordersummary | 9,688,496 | 1.72 / 1.72 | 99% / 99% |
| ix_tmp_purge_transactiondetail | 7,961,061 | 1.42 / 1.42 | 100% / 100% |
| **Total** | | **14.30 / 14.29 (28.59 both copies)** | |

Observation: resident ratio follows build order; an index built 15 to 20 minutes earlier was already at 0%. The Index service
was NOT at its memory quota (see below), so this is not simple quota eviction. Cause still to be confirmed (likely the storage
engine trimming cold pages on its own).

### Index service totals (Capella Metrics, 2026-10-16 15:44, includes the 6 temporary purge indexes)

Console totals are CLUSTER-WIDE, not just `quantic`: system:indexes lists 106 GSI index definitions (agent_catalog 9,
agent_memory 25, quantic._default 14, quantic.pos 24, sales 2, supermarket 32 Sync Gateway). The console's 177 likely
counts replica instances too (not confirmed). Other buckets are not touched by the purge, so before/after deltas still hold.

`quantic._default._default` holds an early prototype dataset (358,595 docs) with 14 leftover experiment indexes
(ix_cart_order_v4..v7, ix_payment_precompute_test*, primary, etc.): about 0.3-0.4 GiB in total, both copies, at most
358,595 items each. Negligible next to `pos`, and the purge does not touch them. Plan: drop them after the "After" snapshot
(cleanup, not part of the measurement).

| Metric | Before (incl. temp) | After |
| --- | --- | --- |
| Index nodes | 2 (svc-qis-023, -024; 8 cores, 64 GB each, shared with Query + Search) | |
| Memory quota (GB) | 33.67 | |
| Total memory used (GB) | 7.55 (22% of quota) | |
| Total disk used (GB) | 64.39 (vs ~162 GiB index data size; on-disk is compressed) | |
| Items indexed (console total) | 454,450,260 | |
| Indexes (console count) | 177 | |
| Node memory used / allocated | 023: 8.88 / 64 GB; 024: 10.02 / 64 GB | 13.38% + 13.17% of 64 GB = 17.0 GB (was 18.9) |
| Node disk used / allocated | 023: 34.99 / 100 GB; 024: 34.67 / 100 GB (69.66 total, incl. ~29 GiB logical temp purge indexes) | 26.05% + 28.03% of 100 GB = 54.08 GB day 1 (2026-10-17 morning; -15.6 GB, -22%) |
| CPU (idle) | service 0.63%; nodes 9.8% / 12.9% | |

## Analytics
| Metric | Before | After |
| --- | --- | --- |
| Analytics disk used (GB) | 189.97 node-level (svc-a-025 95.01 + svc-a-026 94.96 of 200 GB each; the "Storage Used" chart reads 0, broken) | 184.9 day 1 (46.22% + 46.21% of 200 GB; history unchanged, small drop = merges/logs) |
| Analytics node memory used (GB) | 31.59 (16.32 + 15.27 of 32 GB each) | |

Analytics replicas = 1 (two copies), same as the bucket. The same ~86M history docs take roughly 228 GB for two copies on
Data (Magma; half of the 457.3 GB bucket, which holds hot + history x 2 copies) and about 190 GB for two copies on Analytics
(node disk, includes logs, spill space, LSM merge headroom and the A4 secondary index). That is about 1.1 KB per copy on
Analytics vs 1.3 KB on Data: comparable, Analytics slightly smaller. Replica already counted; no extra factor for production.
| pos_history.orders items (Analytics) | 9,178,045 | |

## Query timings (Workbench elapsed)

Q1 literal used for both runs: orderID "FC13FD96-9AF8-4071-B6F9-C4095C7C5067", locationID "ED56DDC7-E76B-48A3-9D1D-719007A83CF9"
(King of Prussia), commChannel "847", isPurged 0. "Before" taken Fri 2026-10-16 afternoon; "after" Sat 2026-10-17 morning,
cluster idle both times.
Caveat from the Q2 control (stockHistory, not purged): 22.6 ms -> 6.0 ms with no data change. Friday's "before" timings
were taken while Eventing redeploys / backlog were settling, so small-query differences (a few ms) are within noise.
Only large differences on the broad scans (Demo 1, Demo 3) should be credited to the purge.
| Query | Before | After |
| --- | --- | --- |
| Q1 cart lines of an order | 5.3 ms (9 lines) | 4.3 ms warm (-19%); 17.5 ms first run, cold (9 lines, 16.4 KB) |
| Q2 inventory history, vendor like %test% (stockHistory, not purged) | 22.6 ms (14 rows) | 6.0 ms (14 rows). CONTROL: collection untouched, yet 3.8x faster, so conditions differed |
| Q3 order summaries page | 71.5 ms (100 rows, 500 KB) | 55.4 ms (100 rows, 499.7 KB); within noise (newest-100 page, purge does not change its path) |
| Q4 unsettled transactions | 205 ms (739 rows) | 80.8 ms (739 rows, 199.2 KB); same rows, all recent. Faster but within the control's drift |
| Demo 1 platform month | 4.2 s (1,231,182 orders) | ~4.4 s, consistent (1,231,182 orders; 56 merchants, 152 locations, net $54.37M, avg $44.16). No change: Sept is wholly post-cutoff, so the scanned range is identical; cost is ~1.2M doc fetches at 1.4% RR. Belongs on Analytics |
| Demo 3 churn early warning (Jul vs Sep) | 9.9 s (4 merchants at zero; next tier ~-20%) | ~8.5 s, consistent. **Wrong answer:** Jul 1-7 is purged, so `jul` is ~23% low. The 4 zero merchants still show -1, but the next tier now reads +1.3% to +1.9% (Lucky Harbor, Anderson's, Happy Bird, Wild Oak): the ~20% decline is hidden. No error, same 8 rows. Must run on Analytics |
| Demo 3 on Analytics (`pos_history`, correct) | n/a | 5.8 s. Same 4 at -1; next tier -20.3% to -21.8% (Wild Oak 3,645->2,851; Anderson's 15,078->11,915; Lucky Harbor 45,164->35,840; Happy Bird 11,813->9,412). Sep counts identical to Query (Sept intact); Jul ~29% higher than Query. Faster than the wrong Query answer |
| Demo 4 sandwich launch | 0.77 s | 770.8 ms, identical weekly units (3,729 / 11,286 / 15,771 / 19,947 / partial 7,469). Recent, index-only: untouched by the purge, as expected |
| Demo 6a voids by employee | not timed (tested: Tyler 938/7,887) | 193.4 ms, identical (Tyler Brennan 11.9%, 938 of 7,887; others 1.6-2.1%). Window starts exactly at the Jul 8 cutoff and nothing was lost: purge boundary is exact. Baseline for the TTL test |
| A1 full-year platform (Analytics) | n/a | |

## Purge dry run (2026-10-16, cutoff 2026-07-08T04:00:00Z)

Closed docs older than the cutoff, per hot collection. Each history check looked up 2,000 sampled IDs in `pos_history` and found all of them.
Each line's time includes building the temporary index `ix_tmp_purge_<coll>` on (dateCreated, isClosed).

| Collection | Items now | To delete | % | Remaining | History check | Index build + count |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| cart | 39,861,608 | 24,897,113 | 62.5% | 14,964,495 | 2000/2000 | 7m 36s |
| check | 9,710,384 | 5,988,844 | 61.7% | 3,721,540 | 2000/2000 | 3m 34s |
| orders | 9,185,313 | 5,634,985 | 61.3% | 3,550,328 | 2000/2000 | 3m 39s |
| payment | 9,656,417 | 5,959,388 | 61.7% | 3,697,029 | 2000/2000 | 3m 58s |
| orderSummary | 9,688,496 | 5,979,690 | 61.7% | 3,708,806 | 2000/2000 | 4m 42s |
| transactionDetail | 7,961,061 | 4,941,627 | 62.1% | 3,019,434 | 2000/2000 | 3m 44s |
| **Total** | **86,063,279** | **53,401,647** | **62.0%** | **32,661,632** | | 27m 12s |

Eventing copy functions were undeployed for the purge (nothing loading); redeploy "From now" afterwards.

### Purge run (N1QL DELETE, 50k batches, from Mel's laptop over WAN)

- Started 2026-10-16 16:00 ET, finished 2026-10-17 00:47:53 ET: **8 h 47 m** for 53,401,647 docs (log total 53,351,647
  plus the first 50,000 batch that completed before the UnsignedInt64 crash, since fixed).
- Average about **1,690 docs/s**. Cart (6 indexes per delete) ran at ~1,540/s; transactionDetail (temp index only)
  at ~1,900/s. Five fewer indexes only made it ~23% faster, so index maintenance is not the main limit. Most of the
  cost is per-statement Query-service work (fetch + delete). That points to KV removes for production.
- All temporary purge indexes dropped by the script.
- At this rate a one-off purge at x10 volume would take ~3.7 days. Recommend continuous expiry (TTL, or expiry set by
  the Eventing copy) plus in-region, parallel KV removes for any backlog.

## Production estimate: Quantic at ~1 billion documents (first approximation)

Per-document costs measured on the demo (both copies, 1 replica):
- Data (Magma) disk: 457.3 GB / 175.9M docs = ~2.6 KB per doc.
- GSI index data on the hot tier: 133.5 GiB / 86.1M hot docs = ~1.67 KB per doc (logical; Quantic's real index list will differ).
- Analytics disk: ~190 GB / 86.0M history docs = ~2.2 KB per doc.
- Hot fraction kept by a 90-day TTL: 38% if the data spans 1 year (measured: 62% purged); ~12% if it spans ~3 years.

Assumptions: 1B = Quantic's current operational docs with the same doc mix and sizes as the demo. KEY UNKNOWN: how many
years the 1B spans (sets both the hot fraction and the yearly growth).

| Resource | Today (no retention) | Target: hot 90 d + history + Analytics | Change |
| --- | --- | --- | --- |
| GSI index data (memory-sensitive) | ~1.67 TB | 1-yr span: ~0.63 TB; 3-yr: ~0.20 TB | -1.0 to -1.5 TB |
| Data disk: hot tier | ~2.6 TB | 1-yr: ~1.0 TB; 3-yr: ~0.3 TB | |
| Data disk: history (no GSI; small memory quota is fine) | 0 | ~2.6 TB | NEW |
| Data disk total | ~2.6 TB | 1-yr: ~3.6 TB; 3-yr: ~2.9 TB | +0.3 to +1.0 TB |
| Analytics disk | 0 | ~2.2 TB (+ ~50% headroom for merges) | NEW |
| Eventing | none | dedicated service group (2+ nodes); steady load = write rate | NEW, size from TTL test |
| Write amplification | 1 write (+replica) | 2 writes (+replicas) per closed record | Data-node IOPS up |

Yearly growth after go-live (A = new docs per year): history Data disk +2.6 KB x A, Analytics +2.2 KB x A; hot tier and
GSI stay flat. Example: if A = 330M/yr (1B over 3 years), +0.9 TB Data and +0.7 TB Analytics per year. History needs its
own retention policy eventually (e.g. a 7-year rule), or it grows without limit.

Trade in one line: more total disk (history + Analytics), much less index (the expensive, memory-bound part), and a new
Eventing tier to run. Eventing steady load at 1B/yr is ~32 docs/s on average (~300/s at peak), so it is small; the
heavy events are the backfill of history (1B copies) and catch-up after any pause. Measure both in the TTL reload test.

Migration gotcha: docs written by the XDCR move get a collection maxTTL counted from the migration date, so the whole
hot tier would expire on the same day 90 days later (an expiry storm of tombstones to devices). Set per-doc expiry from
dateCreated after the move (KV touch or N1QL UPDATE ... SET META().expiration), or migrate only the last 90 days into
the main collections and send older docs straight to history.
