# Retention test: before / after snapshot

Demo environment = about 10% of Quantic production. Record "Before" after the Eventing backlog reaches 0,
"After" once compaction has settled following the purge (deleted-document markers are purged after the
metadata purge interval, often a few days). Same time of day, no load running.

## Environment
- Cluster / node sizes at "Before":
- Cluster / node sizes at "After":
- Purge cutoff: 2026-07-08T04:00:00Z (90 days before end of data)

## Data service
| Metric | Before | After | Change |
| --- | --- | --- | --- |
| Bucket disk used (GB) | | | |
| Bucket memory used (GB) | | | |
| Resident ratio (%) | | | |
| pos.cart items | | | |
| pos.orders items | | | |
| pos.orderSummary items | | | |
| pos.payment items | | | |
| pos.check items | | | |
| pos.transactionDetail items | | | |
| pos_history.* items (total) | | | |

## Index service
| Index | Items before | Items after | Memory before | Memory after | Disk before | Disk after |
| --- | --- | --- | --- | --- | --- | --- |
| ix_cart_loc_date | | | | | | |
| ix_cart_name_date | | | | | | |
| ix_cart_void | | | | | | |
| ix_cart_emp | | | | | | |
| ix_cart_order | | | | | | |
| ix_orders_date | | | | | | |
| ix_orders_loc_date | | | | | | |
| ix_os_loc_date | | | | | | |
| ix_os_loc_cc_date | | | | | | |
| ix_payment_txn | | | | | | |
| ix_payment_settle | | | | | | |
| (others) | | | | | | |
| **Index service total** | | | | | | |

## Analytics
| Metric | Before | After |
| --- | --- | --- |
| Analytics disk used (GB) | | |
| pos_history.orders items (Analytics) | | |

## Query timings (Workbench elapsed)
| Query | Before | After |
| --- | --- | --- |
| Q1 cart lines of an order | | |
| Q3 order summaries page | | |
| Q4 unsettled transactions | | |
| Demo 1 platform month | | |
| Demo 4 sandwich launch | | |
| Demo 6a voids by employee | | |
| A1 full-year platform (Analytics) | n/a | |

## Production estimate (x10, first approximation)
| | Demo saving | x10 estimate | Notes |
| --- | --- | --- | --- |
| Index memory | | | Real index list and retention will refine this |
| Data disk | | | History adds disk on Magma (cheap) |
| Analytics disk | | | New cost |
