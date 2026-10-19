-- GSI DEMO QUERIES - run in the Query workbench (hot tier = last 90 days, data through 2026-10-16)
-- Ember & Oak Kitchen (847): King of Prussia 439B465C-AA84-4DA7-B067-C1905331BB83 | Fishtown 693DABE7-6ADB-4B62-BF96-A9C5F191BF43
-- Quote EXECUTION time; the ~250-500 ms round trip is laptop-to-cloud network.

-- 1. VOID STORY - Who voids the most? (90 ms)
--   Tyler Brennan voids 11.6% of his lines vs 1.6-2.0% for everyone else: ~6x the store norm
--   Say: "A manager's hunch, answered in 90 ms from the index, on live data"
SELECT c.saleEmpName, COUNT(*) AS lines,
       SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) AS voids,
       ROUND(SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) / COUNT(*), 3) AS voidRate
FROM quantic.pos.cart c
WHERE c.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83"
  AND c.dateCreated >= MILLIS("2026-07-19T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-17T04:00:00Z")
GROUP BY c.saleEmpName HAVING COUNT(*) > 200
ORDER BY voidRate DESC LIMIT 5;

-- 2. VOID STORY - Why? Tyler's void reasons
--   69% are MANAGER OVERRIDE or NO REASON ($3,617 in ~11 weeks); only 7% are honest WRONG ITEM
--   Say: "If he were careless, 'wrong item' would dominate - these voids can't be verified"
SELECT c.voidReason, COUNT(*) AS voids, ROUND(SUM(c.subTotal), 2) AS voidedValue
FROM quantic.pos.cart c
WHERE c.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83" AND c.closeType = "void" AND c.saleEmpName = "Tyler Brennan"
  AND c.dateCreated >= MILLIS("2026-07-19T04:00:00Z")
GROUP BY c.voidReason ORDER BY voids DESC;

-- 3. VOID STORY - Only him? Tyler vs the rest of the store
--   NO REASON: Tyler 75, everyone else 0; he holds 68% of the store's overrides
--   Say: "From a hunch to a case file in three questions, before the shift ends" (no approver is recorded - a product gap for Quantic)
SELECT CASE WHEN c.saleEmpName = "Tyler Brennan" THEN "Tyler" ELSE "Everyone else" END AS who,
       c.voidReason, COUNT(*) AS voids
FROM quantic.pos.cart c
WHERE c.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83" AND c.closeType = "void"
  AND c.dateCreated >= MILLIS("2026-07-19T04:00:00Z")
GROUP BY CASE WHEN c.saleEmpName = "Tyler Brennan" THEN "Tyler" ELSE "Everyone else" END, c.voidReason
ORDER BY who, voids DESC;

-- 4. QUANTIC'S QUERY, FIXED - Open a check (3 ms)
--   Restaurant: "Your server taps table 12 in the Friday rush - the check opens instantly, at every store"
--   Engineers: old version read every item ever sold at the store; ix_cart_order goes straight to the 7 lines (show Plan tab). Avoid zooming on modifiers (generator quirk)
SELECT c.*
FROM quantic.pos.cart c
WHERE c.orderID = "3F4B03A6-DD5D-44F1-8B9C-4CEC7F57AEE4" AND c.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83"
  AND c.commChannel = "847" AND c.isPurged = 0;

-- 5. SALMON STORY - Why? The menu change log
--   Out of stock Oct 7 11:00 am (count 4 -> 0), restocked Oct 11 8:00 am (60) - matches the gap to the day
--   Say: "Demand didn't drop - the kitchen ran out. A low-stock alert would have caught it at 4 left"
SELECT MILLIS_TO_TZ(g.dateCreated, "America/New_York") AS changedAt, g.masterName, g.modifiedFields
FROM quantic.pos.catalogLog g
WHERE g.locationID = "693DABE7-6ADB-4B62-BF96-A9C5F191BF43" AND g.dateCreated >= MILLIS("2026-09-01T04:00:00Z")
  AND g.masterName = "GRILLED ATLANTIC SALMON"
ORDER BY g.dateCreated;

-- 6. LAUNCH - Nashville Hot Chicken Sandwich, weekly units, all locations (213 ms)
--   115 (launch week) -> 1,630 -> 2,849 -> 3,712; last week is partial (6 days, ~4,000 pace)
--   Say: "Still growing week over week, across every store, live"
SELECT DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week") AS week, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.name = "NASHVILLE HOT CHICKEN SANDWICH" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated >= MILLIS("2026-08-01T04:00:00Z")
GROUP BY DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week")
ORDER BY week;



-- 7. QUANTIC'S QUERY, FIXED - Stock history, one item from one vendor (5.8 ms)
--   Restaurant: 3 years of Seasonal Vegetables deliveries; cost +18% ($1.59 -> $1.87), menu price kept pace, margin held 62%
--   Engineers: the vendor LIKE filter runs inside the index scan (ix_stock_loc_item) - only the 26 matches are fetched
SELECT s.empName, s.`type`, s.commChannel, s.transferFrom, s.transferTo, s.`transfer`, s.entryType, s.qtyRemaining,
       s.modifiedByName, s.unit, s.reason, s.created, s.dateCreated, s.modified, s.vendorName, s.manufactureName,
       s.margin, s.cog, s.upc, s.sku, s.alu, s.qtyReceived, s.reorderPoint, s.size, s.note, s.qtySold, s.itemID, s.stockID
FROM quantic.pos.stockHistory s
WHERE s.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83" AND s.itemID = "840ABEEE-ABD0-40BC-86F2-D44F3679DFF0"
  AND s.commChannel = "847" AND LOWER(s.vendorName) LIKE "%test%"
ORDER BY s.dateCreated DESC;

-- 8. SALMON STORY - Salmon sold per day, Fishtown
--   No salmon sold Oct 7-10, back Oct 11 (normally 1-9 a day, so go straight to the next query)
--   Covered by ix_cart_loc_date: location + date range, paid lines only
SELECT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11") AS day, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.locationID = "693DABE7-6ADB-4B62-BF96-A9C5F191BF43" AND c.closeType = "paid" AND c.name = "GRILLED ATLANTIC SALMON"
  AND c.dateCreated >= MILLIS("2026-09-28T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-17T04:00:00Z")
GROUP BY DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")
ORDER BY day;
