-- NOT YET RE-TESTED after the 2026-10-18 reload (kept for later; dates/IDs may need updating)
-- Out-of-window dates (Nov 2025, last season) belong in Analytics now; Q3 references orderTotal (not in our data).

-- Platform, one month
--   Old test (60-merchant data): 1.23M orders, $54.4M
SELECT COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants, COUNT(DISTINCT o.locationID) AS locations,
       ROUND(SUM(o.subTotal) - SUM(o.totalDiscount), 0) AS netSales,
       ROUND((SUM(o.subTotal) - SUM(o.totalDiscount)) / COUNT(*), 2) AS avgCheck
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-10-01T04:00:00Z");

-- Platform channel mix, one month
--   1 dine-in, 2 takeout, 3 delivery, 4 online
SELECT o.serviceAreaType AS channel, COUNT(*) AS orders, ROUND(SUM(o.subTotal) - SUM(o.totalDiscount), 0) AS netSales
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2026-09-01T04:00:00Z") AND o.dateCreated < MILLIS("2026-10-01T04:00:00Z")
GROUP BY o.serviceAreaType ORDER BY o.serviceAreaType;

-- Online + delivery share (Nov 2025 is outside the 90-day hot tier - use Analytics)
SELECT ROUND(SUM(CASE WHEN o.serviceAreaType IN [3, 4] THEN 1 ELSE 0 END) / COUNT(*), 3) AS onlineDeliveryShare,
       COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid"
  AND o.dateCreated >= MILLIS("2025-11-01T04:00:00Z") AND o.dateCreated < MILLIS("2025-12-01T05:00:00Z");

-- Churn early warning, July vs September (July partly outside the hot tier)
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

-- Seasonal LTO, this season vs last (last season outside the hot tier - use Analytics)
SELECT DATE_PART_MILLIS(c.dateCreated, "year", "America/New_York") AS season, SUM(c.quantity) AS units,
       COUNT(DISTINCT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")) AS daysOnMenu,
       ROUND(SUM(c.quantity) / COUNT(DISTINCT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")), 1) AS unitsPerDay
FROM quantic.pos.cart c
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated >= MILLIS("2025-09-01T04:00:00Z") AND c.dateCreated < MILLIS("2026-10-07T04:00:00Z")
GROUP BY DATE_PART_MILLIS(c.dateCreated, "year", "America/New_York")
ORDER BY season;

-- Quantic Q3 - order summaries, newest first, first page
--   Plan: ix_os_loc_cc_date, LIMIT pushed into the index scan
SELECT s.orderID, s.refNumber, s.orderTotal AS `total`, s.total AS checkTotal, s.paymentType, s.dateCreated,
       s.empName, s.cartList, s.transactionType, s.paidAmount
FROM quantic.pos.orderSummary s
WHERE s.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83" AND s.commChannel = "847" AND s.closeType != "noSale"
  AND s.dateCreated BETWEEN 1788235200000 AND 1790827200000
ORDER BY s.dateCreated DESC
LIMIT 100 OFFSET 0;

-- Quantic Q4 - unsettled transactions by payment type
--   ANY variable must be named t to match the index
SELECT p.orderID, p.terminalID, p.totalAmount, p.gratuityAmount, p.driverID, p.extraChargeList,
       tx.transactionID, tx.paymentType, tx.transactionType, tx.paidAmount, tx.dateCreated
FROM quantic.pos.payment p UNNEST p.transactionList tx
WHERE p.locationID = "439B465C-AA84-4DA7-B067-C1905331BB83" AND p.commChannel = "847"
  AND ANY t IN p.transactionList SATISFIES t.isForSettle = 0 AND t.dateCreated BETWEEN 1791172800000 AND 1791345600000
      AND t.transactionType != "return" END
  AND tx.isForSettle = 0 AND tx.dateCreated BETWEEN 1791172800000 AND 1791345600000 AND tx.transactionType != "return"
ORDER BY tx.paymentType, tx.dateCreated;
