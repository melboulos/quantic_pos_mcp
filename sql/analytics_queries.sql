/* =====================================================================
   ANALYTICS QUERIES - run in the ANALYTICS workbench (not the Query workbench)
   They read quantic.pos_history.* (never purged), so they cover the full history even after the
   hot tier (quantic.pos.*) keeps only 90 days. Analytics needs no GSI indexes for these scans.
   Times are UTC epoch ms; month/year boundaries here are UTC (a few hours off New York time).
   Paste only the statements, not the comment lines.
   ===================================================================== */

-- Setup (once): map the reference collections too. They never expire, so mirroring them is safe;
-- the rule is to never map a collection that has a TTL.
ALTER COLLECTION quantic.pos.location ENABLE ANALYTICS;
ALTER COLLECTION quantic.pos.item ENABLE ANALYTICS;
ALTER COLLECTION quantic.pos.employee ENABLE ANALYTICS;

-- A1. The full-year platform view that timed out on the operational Query service
SELECT yr, mo, COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants,
       ROUND(SUM(o.subTotal - o.totalDiscount), 0) AS netSales
FROM quantic.pos_history.orders o
LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
WHERE o.closeType = "paid"
GROUP BY get_year(dt) AS yr, get_month(dt) AS mo
ORDER BY yr, mo;

-- A2. Online + delivery share of orders, every month of history (serviceAreaType 3 delivery, 4 online)
SELECT yr, mo, COUNT(*) AS orders,
       ROUND(SUM(CASE WHEN o.serviceAreaType IN [3, 4] THEN 1 ELSE 0 END) / COUNT(*), 3) AS onlineDeliveryShare
FROM quantic.pos_history.orders o
LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
WHERE o.closeType = "paid"
GROUP BY get_year(dt) AS yr, get_month(dt) AS mo
ORDER BY yr, mo;

-- A3. Churn early warning: July vs September 2026 orders per merchant, with names (needs location mapped)
SELECT x.commChannel, l.businessName AS merchant, x.jul, x.sep, ROUND(x.sep / x.jul - 1, 3) AS change
FROM (SELECT o.commChannel,
             SUM(CASE WHEN o.dateCreated < 1785556800000 THEN 1 ELSE 0 END) AS jul,
             SUM(CASE WHEN o.dateCreated >= 1788235200000 THEN 1 ELSE 0 END) AS sep
      FROM quantic.pos_history.orders o
      WHERE o.closeType = "paid"
        AND ((o.dateCreated >= 1782878400000 AND o.dateCreated < 1785556800000)
          OR (o.dateCreated >= 1788235200000 AND o.dateCreated < 1790827200000))
      GROUP BY o.commChannel) x
JOIN (SELECT DISTINCT l0.commChannel, l0.businessName FROM quantic.pos.location l0 WHERE l0.parentLocationID = "") l
  ON l.commChannel = x.commChannel
WHERE x.jul > 300
ORDER BY x.sep / x.jul
LIMIT 8;

-- A4. Pumpkin Cheesecake, this season vs last season, units per day on the menu (demo chain 847)
SELECT season, SUM(c.quantity) AS units, COUNT(DISTINCT d) AS daysOnMenu,
       ROUND(SUM(c.quantity) / COUNT(DISTINCT d), 1) AS unitsPerDay
FROM quantic.pos_history.cart c
LET dt = datetime_from_unix_time_in_ms(c.dateCreated), d = get_date_from_datetime(dt)
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.commChannel = "847"
GROUP BY get_year(dt) AS season
ORDER BY season;

-- A5. Year of sales for one location by month (King of Prussia): merchant statement style
SELECT yr, mo, COUNT(*) AS orders, ROUND(SUM(o.subTotal - o.totalDiscount), 2) AS netSales,
       ROUND(SUM(o.subTotal - o.totalDiscount) / COUNT(*), 2) AS avgCheck
FROM quantic.pos_history.orders o
LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
WHERE o.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND o.closeType = "paid"
GROUP BY get_year(dt) AS yr, get_month(dt) AS mo
ORDER BY yr, mo;
