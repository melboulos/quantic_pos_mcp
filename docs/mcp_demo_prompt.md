# Helper prompt for the demo (paste into the Claude Project's instructions)
# A data dictionary only: what the data is and how to query it. No answers, no findings - Claude finds those.

You're analyzing Quantic POS data in Couchbase: bucket `quantic`. Each collection holds one docType:
`orders` (order headers), `check`, `cart` (line items), `payment` (tenders in `transactionList`),
`orderSummary` (per check, with `cartList` including `catName`), `transactionDetail` (card processor responses),
`batchClose`, `timeManagement` (clock in/out, `workingHours`), `location`, `item`, `employee`, `catalogLog`
(menu changes), `stockHistory`, `auditLog` (configuration changes), `rewardHistory`, `giftTransactionDetail`, `signature`.

Conventions:
- A merchant is a `commChannel`; `location.businessName` / `locationName` name it (parent locations have `parentLocationID = ""`).
  Join on `locationID`. Several merchants have locations in the same town, so always name the merchant and the location.
- Times are epoch ms (UTC). Most locations are "America/New_York".
- Sales = `cart` rows with `closeType = "paid"` (`subTotal - totalDiscount`); voided lines have `closeType = "void"`.
- Payments: `payment.transactionList[]` with `transactionType` ("pay", "return", ...), `paymentType` ("creditCard", ...),
  `paidAmount`, `gateWay` (card processor).
- Order channel = `serviceAreaType`: 1 dine-in, 2 takeout/counter, 3 delivery, 4 online.
- Item category is `catName` on `orderSummary.cartList` and `item`; `cart` only has `catID`.
- Merchants that left have `location.dateDeactivated` set.
- The data runs from 2023-10-18 through 2026-10-16 (end = 1792209600000, i.e. midnight Oct 17 2026 New York).

Two MCP servers, two services (pick by time range):
- `quantic-couchbase` (Query service, hot tier `quantic.pos.*`): the last ~90 days (rolling - every order, check, cart,
  payment, orderSummary and transactionDetail expires 90 days after it was created), single orders, one location's
  recent activity, anything selective. Prefer filters on `locationID`, `dateCreated`, `name` so queries use indexes.
  Collections without a TTL (location, item, employee, timeManagement, catalogLog, stockHistory, auditLog, ...) keep full history.
- `quantic-analytics` (Analytics service, `run_analytics_query`): anything older than ~90 days, year over year, or platform-wide
  across many months. Read `quantic.pos_history.*` (orders, cart, check, payment, orderSummary, transactionDetail: full
  3-year history, never purged) and `quantic.pos.location` / `item` / `employee` / `timeManagement` / `catalogLog` /
  `stockHistory` / `rewardHistory`.
- A question reaching further back than ~90 days MUST go to `quantic-analytics`: on the Query service it would silently
  return incomplete (wrong) results, not just run slowly.

Analytics SQL++ rules (tested on this cluster):
- No `MILLIS_TO_TZ` / `DATE_FORMAT_STR`. Use `datetime_from_unix_time_in_ms(ms)` with `get_year`, `get_month`,
  `get_day`, `get_date_from_datetime`; put derived values in `LET`.
- New York dates: shift before converting, `datetime_from_unix_time_in_ms(x - offsetMs)`, offset 18000000 (EST) inside these
  windows and 14400000 (EDT) otherwise: [1699164000000, 1710054000000), [1730613600000, 1741503600000),
  [1762063200000, 1772953200000). Monthly platform totals may stay in UTC (tiny difference).
- End "to date" or per-day reports at the last full day: `x < 1792209600000`.
- Don't use `COUNT(DISTINCT key)` over a large scan. Group by the key in a subquery, then `COUNT(*)` the groups.
- Item-name filters on `pos_history.cart` use the Analytics index `ix_hist_cart_name` (fast). Full scans take ~10-60 s
  (orders ~10 s, payments ~17 s, cart ~45 s); computing several time windows in one query can run out of memory -
  group by location and period instead.
- Prefer aggregates; results are capped at 500 rows by default.
