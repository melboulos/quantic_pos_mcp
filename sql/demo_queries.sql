/* =====================================================================
   DEMO QUERIES - hard-coded, tested against the loaded 60-merchant dataset (data through 2026-10-06)
   Demo chain: Ember & Oak Kitchen, commChannel "847"
     King of Prussia  ED56DDC7-E76B-48A3-9D1D-719007A83CF9
     Fishtown         8456EDCC-D1BF-4474-85D6-74384FA03230
   Platform-wide queries are bounded to a month: a full-year scan of ~9M orders exceeds the Query
   service / Workbench time limits on this cluster (that workload belongs in rollups or Analytics).
   Times are UTC epoch ms; "2026-09-01T04:00:00Z" = midnight New York (EDT).
   ===================================================================== */

-- 1. Platform, one month: orders, active merchants and locations, net sales, average check
--    Tested: 1,231,182 orders, 56 merchants, 152 locations, $54.4M, $44.16 avg
SELECT COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants, COUNT(DISTINCT o.locationID) AS locations,
       ROUND(SUM(o.subTotal) - SUM(o.totalDiscount), 0) AS netSales,
       ROUND((SUM(o.subTotal) - SUM(o.totalDiscount)) / COUNT(*), 2) AS avgCheck
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-10-01T04:00:00Z");

-- 2a. Platform channel mix, one month (1 dine-in, 2 takeout/counter, 3 delivery, 4 online)
SELECT o.serviceAreaType AS channel, COUNT(*) AS orders, ROUND(SUM(o.subTotal) - SUM(o.totalDiscount), 0) AS netSales
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-10-01T04:00:00Z")
GROUP BY o.serviceAreaType ORDER BY o.serviceAreaType;

-- 2b. Online + delivery share, then vs now (run for Nov 2025 and Sep 2026)
--    Tested: Nov 2025 12.1% (36 merchants) -> Sep 2026 22.7% (56 merchants)
SELECT ROUND(SUM(CASE WHEN o.serviceAreaType IN [3, 4] THEN 1 ELSE 0 END) / COUNT(*), 3) AS onlineDeliveryShare,
       COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2025-11-01T04:00:00Z") AND o.dateCreated < MILLIS("2025-12-01T05:00:00Z");

-- 3. Churn early warning: July vs September orders per merchant, worst first, with names
--    Tested: 4 merchants already at zero; next tier down ~20%
SELECT x.commChannel, (SELECT RAW l.businessName FROM quantic.pos.location l WHERE l.commChannel = x.commChannel LIMIT 1)[0] AS merchant,
       x.jul, x.sep, ROUND(x.sep / x.jul - 1, 3) AS change
FROM (SELECT o.commChannel,
        SUM(CASE WHEN o.dateCreated < MILLIS("2026-08-01T04:00:00Z") THEN 1 ELSE 0 END) AS jul,
        SUM(CASE WHEN o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") THEN 1 ELSE 0 END) AS sep
      FROM quantic.pos.orders o
      WHERE o.isClosed = 1 AND o.closeType = "paid"
        AND ((o.dateCreated >= MILLIS("2026-07-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-08-01T04:00:00Z"))
          OR (o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-10-01T04:00:00Z")))
      GROUP BY o.commChannel) x
WHERE x.jul > 300
ORDER BY x.sep / x.jul
LIMIT 8;

-- 4. New item launch at the demo chain, weekly units (covered once ix_cart_name_date includes commChannel)
--    Tested: 3,729 (launch week) -> 11,286 -> 15,771 -> 19,947
SELECT DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week") AS week, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.name = "NASHVILLE HOT CHICKEN SANDWICH" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated >= MILLIS("2026-08-01T04:00:00Z")
GROUP BY DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week")
ORDER BY week;

-- 5a. Salmon in Fishtown, by day. Tested: ~40-90/day, then nothing Sep 28-30
SELECT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11") AS day, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.locationID = "8456EDCC-D1BF-4474-85D6-74384FA03230" AND c.closeType = "paid" AND c.name = "GRILLED ATLANTIC SALMON"
  AND c.dateCreated >= MILLIS("2026-09-20T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-07T04:00:00Z")
GROUP BY DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")
ORDER BY day;

-- 5b. ...and why: the menu change log. Tested: 86'd Sep 27 12:00, restocked Oct 1 16:00
SELECT MILLIS_TO_TZ(g.dateCreated, "America/New_York") AS changedAt, g.masterName, g.modifiedFields
FROM quantic.pos.catalogLog g
WHERE g.locationID = "8456EDCC-D1BF-4474-85D6-74384FA03230" AND g.dateCreated >= MILLIS("2026-09-01T04:00:00Z")
  AND g.masterName = "GRILLED ATLANTIC SALMON"
ORDER BY g.dateCreated;

-- 6a. Void rate by employee, King of Prussia, last 90 days (covered by ix_cart_emp)
--    Tested: Tyler Brennan 11.9% (938 of 7,887 lines); everyone else 1.6-2.1%
SELECT c.saleEmpName, COUNT(*) AS lines,
       SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) AS voids,
       ROUND(SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) / COUNT(*), 3) AS voidRate
FROM quantic.pos.cart c
WHERE c.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9"
  AND c.dateCreated >= MILLIS("2026-07-08T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-07T04:00:00Z")
GROUP BY c.saleEmpName HAVING COUNT(*) > 200
ORDER BY voidRate DESC LIMIT 5;

-- 6b. ...his void reasons and value. Tested: $12.8K voided; MANAGER OVERRIDE 360, NO REASON 303
SELECT c.voidReason, COUNT(*) AS voids, ROUND(SUM(c.subTotal), 2) AS voidedValue
FROM quantic.pos.cart c
WHERE c.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND c.closeType = "void" AND c.saleEmpName = "Tyler Brennan"
  AND c.dateCreated >= MILLIS("2026-07-08T04:00:00Z")
GROUP BY c.voidReason ORDER BY voids DESC;

-- 7. Seasonal LTO, this season vs last (data starts Oct 7 2025, so last season is Oct 7 - Nov 30 only)
--    Tested: 2025 season 243 units/day over 56 days -> 2026 season-to-date 310/day over 22 days (+27%)
SELECT DATE_PART_MILLIS(c.dateCreated, "year", "America/New_York") AS season, SUM(c.quantity) AS units,
       COUNT(DISTINCT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")) AS daysOnMenu,
       ROUND(SUM(c.quantity) / COUNT(DISTINCT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")), 1) AS unitsPerDay
FROM quantic.pos.cart c
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated >= MILLIS("2025-09-01T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-07T04:00:00Z")
GROUP BY DATE_PART_MILLIS(c.dateCreated, "year", "America/New_York")
ORDER BY season;


/* =====================================================================
   QUANTIC'S PROBLEM QUERIES - rewritten, hard-coded, plans verified
   ===================================================================== */

-- Q1. Cart lines of one order. Plan: ix_cart_order, exact span on orderID+locationID+commChannel, ~9 fetches
--     (without this index the planner scanned and fetched ~575,000 King of Prussia lines to find them)
SELECT c.*
FROM quantic.pos.cart c
WHERE c.orderID = "FC13FD96-9AF8-4071-B6F9-C4095C7C5067" AND c.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9"
  AND c.commChannel = "847" AND c.isPurged = 0;

-- Q2. Inventory history for CRISPY CALAMARI at King of Prussia from vendors matching '%test%'.
--     Plan: ix_stock_loc_item, exact span on 3 keys, LIKE evaluated inside the index scan -> 14 fetches
SELECT s.empName, s.`type`, s.commChannel, s.transferFrom, s.transferTo, s.`transfer`, s.entryType, s.qtyRemaining,
       s.modifiedByName, s.unit, s.reason, s.created, s.dateCreated, s.modified, s.vendorName, s.manufactureName,
       s.margin, s.cog, s.upc, s.sku, s.alu, s.qtyReceived, s.reorderPoint, s.size, s.note, s.qtySold, s.itemID, s.stockID
FROM quantic.pos.stockHistory s
WHERE s.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND s.itemID = "1F2BC0A9-4150-47C2-8506-319DCF2BCD3C"
  AND s.commChannel = "847" AND LOWER(s.vendorName) LIKE "%test%"
ORDER BY s.dateCreated DESC;

-- Q3. September order summaries for King of Prussia, newest first, first page.
--     Plan: ix_os_loc_cc_date in index order (desc), LIMIT 100 pushed into the index scan, no sort -> 100 fetches
SELECT s.orderID, s.refNumber, s.orderTotal AS `total`, s.total AS checkTotal, s.paymentType, s.dateCreated,
       s.empName, s.cartList, s.transactionType, s.paidAmount
FROM quantic.pos.orderSummary s
WHERE s.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND s.commChannel = "847" AND s.closeType != "noSale"
  AND s.dateCreated BETWEEN 1788235200000 AND 1790827200000
ORDER BY s.dateCreated DESC
LIMIT 100 OFFSET 0;

-- Q4. Unsettled, non-return transactions at King of Prussia, Oct 5-6, by payment type and time.
--     Plan: ix_payment_settle with spans on all 5 keys (location, merchant, isForSettle, date range, type).
--     NOTE: the ANY variable must be named exactly as in the index definition (t); with any other name the
--     array keys are ignored and every payment for the location is fetched. UNNEST therefore uses tx.
--     Tested: 621 card, 67 cash, 42 third-party, 9 gift transactions
SELECT p.orderID, p.terminalID, p.totalAmount, p.gratuityAmount, p.driverID, p.extraChargeList,
       tx.transactionID, tx.paymentType, tx.transactionType, tx.paidAmount, tx.dateCreated
FROM quantic.pos.payment p UNNEST p.transactionList tx
WHERE p.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND p.commChannel = "847"
  AND ANY t IN p.transactionList SATISFIES t.isForSettle = 0 AND t.dateCreated BETWEEN 1791172800000 AND 1791345600000
      AND t.transactionType != "return" END
  AND tx.isForSettle = 0 AND tx.dateCreated BETWEEN 1791172800000 AND 1791345600000 AND tx.transactionType != "return"
ORDER BY tx.paymentType, tx.dateCreated;
