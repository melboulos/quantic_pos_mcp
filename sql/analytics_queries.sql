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
--     Merchants are counted in two steps (group by month + merchant, then count rows) instead of
--     COUNT(DISTINCT commChannel): each partition pre-aggregates locally, so almost nothing is shuffled.
--     Tested 2026-10-16 on 2 Analytics nodes, 9.18M orders: 5.0 s (COUNT(DISTINCT) version: 29.5 s;
--     without the merchant count at all: 4.3 s). Same results; netSales can differ by $1 (double rounding).
SELECT yr, mo, SUM(m.orders) AS orders, COUNT(*) AS merchants, ROUND(SUM(m.netSales), 0) AS netSales
FROM (SELECT yr, mo, o.commChannel, COUNT(*) AS orders, SUM(o.subTotal - o.totalDiscount) AS netSales
      FROM quantic.pos_history.orders o
      LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
      WHERE o.closeType = "paid"
      GROUP BY get_year(dt) AS yr, get_month(dt) AS mo, o.commChannel) m
GROUP BY m.yr AS yr, m.mo AS mo
ORDER BY yr, mo;

-- A1 (original, kept for the demo's before/after: 29.5 s). COUNT(DISTINCT) over a large scan is the expensive part.
-- SELECT yr, mo, COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants,
--        ROUND(SUM(o.subTotal - o.totalDiscount), 0) AS netSales
-- FROM quantic.pos_history.orders o
-- LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
-- WHERE o.closeType = "paid"
-- GROUP BY get_year(dt) AS yr, get_month(dt) AS mo
-- ORDER BY yr, mo;

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
--     Needs the Analytics index below: without it A4 scans every cart line (40.8 s); with it 85 ms warm, 1.1 s first run.
--     CREATE INDEX ix_hist_cart_name ON quantic.pos_history.cart(name: string);
--     Days are New York dates: shift by the UTC offset (EST Nov 2 2025 - Mar 8 2026, else EDT). The time-zone
--     argument of datetime_from_unix_time_in_ms was much slower (35.8 s). The upper bound ends the season at the
--     last full day (Oct 6): 4 units sold just after midnight on Oct 7 otherwise add a 23rd day.
--     Tested 2026-10-16: 2025 243.1/day over 56 days; 2026 309.8/day over 22 days (+27%), matching demo query 7.
SELECT season, SUM(c.quantity) AS units, COUNT(DISTINCT d) AS daysOnMenu,
       ROUND(SUM(c.quantity) / COUNT(DISTINCT d), 1) AS unitsPerDay
FROM quantic.pos_history.cart c
LET offsetMs = CASE WHEN c.dateCreated >= 1762063200000 AND c.dateCreated < 1772953200000
                    THEN 18000000 ELSE 14400000 END,
    dt = datetime_from_unix_time_in_ms(c.dateCreated - offsetMs),
    d = get_date_from_datetime(dt)
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated < 1791345600000
GROUP BY get_year(dt) AS season
ORDER BY season;

-- A5. Year of sales for one location by month (King of Prussia): merchant statement style
--     Do NOT index locationID for this: tested 2026-10-16, 5.4 s with ix_hist_orders_loc vs 2.6 s without.
--     One location is ~1% of orders (~110K); index lookups cost more than a parallel scan at that share.
SELECT yr, mo, COUNT(*) AS orders, ROUND(SUM(o.subTotal - o.totalDiscount), 2) AS netSales,
       ROUND(SUM(o.subTotal - o.totalDiscount) / COUNT(*), 2) AS avgCheck
FROM quantic.pos_history.orders o
LET dt = datetime_from_unix_time_in_ms(o.dateCreated)
WHERE o.locationID = "ED56DDC7-E76B-48A3-9D1D-719007A83CF9" AND o.closeType = "paid"
GROUP BY get_year(dt) AS yr, get_month(dt) AS mo
ORDER BY yr, mo;
