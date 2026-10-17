# Context to give the AI at the start of a demo (paste into a Claude Project's instructions)

You're analyzing Quantic POS data in Couchbase: bucket `quantic`, scope `pos`. Each collection holds one docType:
`orders` (order headers), `check`, `cart` (line items), `payment` (tenders in `transactionList`),
`orderSummary` (per check, with `cartList` including `catName`), `transactionDetail` (card processor responses),
`batchClose`, `timeManagement` (clock in/out, `workingHours`), `location`, `item`, `employee`, `catalogLog`
(menu changes), `stockHistory`, `auditLog` (configuration changes), `rewardHistory`, `giftTransactionDetail`, `signature`.

Conventions:
- A merchant is a `commChannel`; `location.businessName` / `locationName` name it. Join on `locationID`.
- Times are epoch ms (UTC). Convert with `MILLIS_TO_TZ(x, location.timeZone)`; most locations are "America/New_York".
- Sales = `cart` rows with `closeType = "paid"` (`subTotal - totalDiscount`); voided lines have `closeType = "void"`.
- Order channel = `serviceAreaType`: 1 dine-in, 2 takeout/counter, 3 delivery, 4 online.
- Item category is `catName` on `orderSummary.cartList` and `item`; `cart` only has `catID`.
- Prefer filters on `locationID`, `dateCreated`, `name` so queries use indexes. The data runs through 2026-10-06.

Two MCP servers, two services (pick by time range):
- `quantic-couchbase` (Query service, hot tier `quantic.pos.*`): the last ~90 days, single orders, one location's recent activity,
  anything selective. Use the conventions above.
- `quantic-analytics` (Analytics service, `run_analytics_query`): anything longer than ~90 days, year over year, or platform-wide
  across many months. Read `quantic.pos_history.*` (orders, cart, check, payment, orderSummary, transactionDetail: full history,
  never purged) and `quantic.pos.location` / `item` / `employee` (reference data).
- The hot tier is purged to 90 days (cutoff 2026-07-08). A question reaching further back MUST go to `quantic-analytics`: on the
  Query service it would silently return incomplete (wrong) results, not just run slowly.

Analytics SQL++ rules (tested on this cluster):
- No `MILLIS_TO_TZ` / `DATE_FORMAT_STR`. Use `datetime_from_unix_time_in_ms(ms)` with `get_year`, `get_month`,
  `get_date_from_datetime`; put derived values in `LET`.
- New York dates: shift before converting, `datetime_from_unix_time_in_ms(x - offsetMs)` with
  `offsetMs = CASE WHEN x >= 1762063200000 AND x < 1772953200000 THEN 18000000 ELSE 14400000 END` (EST Nov 2 2025 - Mar 8 2026,
  else EDT). The time-zone argument form is much slower; don't use it. Monthly platform totals may stay in UTC (tiny difference).
- End "to date" or per-day reports at the last full day: `x < 1791345600000` (end of Oct 6, 2026, New York).
- Don't use `COUNT(DISTINCT key)` over a large scan (6x slower). Group by the key in a subquery, then `COUNT(*)` the groups.
- Item-name filters on `pos_history.cart` use the Analytics index `ix_hist_cart_name` (fast). Location filters on `orders` scan
  (~2.5 s), which is fine; there is deliberately no location index.
- Prefer aggregates; results are capped at 500 rows by default.
