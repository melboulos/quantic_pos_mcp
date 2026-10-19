-- =====================================================================================================
-- Quantic value demo: Analytics queries (Analytics workbench / quantic-analytics MCP)
-- Data: 225 merchants, 2023-10-18 .. 2026-10-16 (end = 2026-10-17 04:00 UTC = 1792209600000).
-- pos_history holds every closed record (history pass + live Eventing copies), so "latest" = up to now.
-- Synthetic data: findings illustrate the method; the numbers are not Quantic's.
-- =====================================================================================================

-- -----------------------------------------------------------------------------------------------------
-- W1. WORKBENCH OPENER - Pumpkin Cheesecake, same 32 days (Sep 15 - Oct 16) each season, Ember & Oak (2.4 s)
--     Tested 2026-10-19: 2024 43.6/day, 2025 46.7/day, 2026 61.7/day (+32% vs last year, +42% vs 2024)
--     Say: "This season is beating the last two - and two of those three seasons no longer exist in the hot tier."
--     Uses the Analytics index ix_hist_cart_name, so it never scans the 112M cart lines.
-- -----------------------------------------------------------------------------------------------------
SELECT yr, SUM(c.quantity) AS units, ROUND(SUM(c.quantity) / 32.0, 1) AS units_per_day
FROM quantic.pos_history.cart c
LET dt = datetime_from_unix_time_in_ms(c.dateCreated),
    yr = get_year(dt), md = get_month(dt) * 100 + get_day(dt)
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.commChannel = "847"
  AND md BETWEEN 915 AND 1016
GROUP BY yr
ORDER BY yr;

-- -----------------------------------------------------------------------------------------------------
-- V1a. Payments: volume flowing through the platform by month, 3 years.
--      Volume, transactions, average ticket, card share, year-over-year growth (same month last year).
--      This is processed VOLUME, not Quantic revenue (take rate unknown).
--      First (Oct 2023) and last (Oct 2026) months are partial.
-- -----------------------------------------------------------------------------------------------------
SELECT m.ym, m.txns,
       ROUND(m.volume, 0) AS volume,
       ROUND(m.card / m.volume, 3) AS card_share,
       ROUND(m.volume / m.txns, 2) AS avg_ticket,
       ROUND(m.volume / LAG(m.volume, 12) OVER (ORDER BY m.ym) - 1, 3) AS yoy_growth
FROM (SELECT ym, COUNT(*) AS txns,
             SUM(t.paidAmount) AS volume,
             SUM(CASE WHEN t.paymentType = "creditCard" THEN t.paidAmount ELSE 0 END) AS card
      FROM quantic.pos_history.payment p UNNEST p.transactionList t
      LET dt = datetime_from_unix_time_in_ms(t.dateCreated),
          ym = get_year(dt) * 100 + get_month(dt)
      WHERE t.transactionType = "pay" AND t.dateCreated < 1792209600000
      GROUP BY ym) m
ORDER BY m.ym;

-- -----------------------------------------------------------------------------------------------------
-- V1b. Card volume by processor gateway, by year: how much card volume runs on each gateway.
--      Tested 2026-10-19: 17.2 s (full scan, ~29M payment docs). ~$992M card volume over 3 years;
--      cardConnect 88-89% every year, dejavoo 11-12%; 2025 $352M (+32% vs 2024), 2026 $331M through Oct 16.
--      Say: "Every card dollar over 3 years, by processor - the payment-partner conversation, from the POS cluster."
--      If asked "can it be faster?": an index won't help - it touches ~half of all transactions, so a full scan IS
--      the fastest plan (indexes win when they skip data, like W1's 2.4 s). Recurring KPIs -> a small daily
--      rollup (day x gateway, ~3K rows) answers in milliseconds; ad hoc questions -> Analytics scan in seconds.
-- -----------------------------------------------------------------------------------------------------
SELECT yr, t.gateWay AS gateway, COUNT(*) AS txns, ROUND(SUM(t.paidAmount), 0) AS card_volume
FROM quantic.pos_history.payment p UNNEST p.transactionList t
LET yr = get_year(datetime_from_unix_time_in_ms(t.dateCreated))
WHERE t.transactionType = "pay" AND t.paymentType = "creditCard" AND t.dateCreated < 1792209600000
GROUP BY yr, t.gateWay
ORDER BY yr, card_volume DESC;

-- -----------------------------------------------------------------------------------------------------
-- V2. Merchant risk: latest 13 weeks vs the same 13 weeks last year (52 weeks back, same weekdays).
--     now = 2026-07-18 .. 2026-10-16, last year = 2025-07-19 .. 2025-10-17 (New York day bounds).
--     Signal: "insufficient history" if the merchant was active < 10 of 13 weeks last year
--     (new merchants are never called at risk); "stopped" if active < 3 weeks now; "at risk" if
--     sales fell > 15%; else "stable" / "growing". Ranked by sales lost.
-- -----------------------------------------------------------------------------------------------------
SELECT l.businessName AS merchant, s.cc AS commChannel,
       ROUND(s.ly_sales, 0) AS sales_last_year, ROUND(s.now_sales, 0) AS sales_now,
       ROUND(s.now_sales / s.ly_sales - 1, 3) AS change,
       ROUND(s.ly_sales - s.now_sales, 0) AS sales_lost,
       s.ly_weeks, s.now_weeks,
       CASE WHEN s.ly_weeks < 10 THEN "insufficient history"
            WHEN s.now_weeks < 3 THEN "stopped transacting"
            WHEN s.now_sales / s.ly_sales - 1 < -0.15 THEN "at risk"
            WHEN s.now_sales / s.ly_sales - 1 > 0.15 THEN "growing"
            ELSE "stable" END AS `signal`
FROM (SELECT w.cc,
             SUM(CASE WHEN w.per = "ly" THEN w.sales ELSE 0 END) AS ly_sales,
             SUM(CASE WHEN w.per = "now" THEN w.sales ELSE 0 END) AS now_sales,
             SUM(CASE WHEN w.per = "ly" THEN 1 ELSE 0 END) AS ly_weeks,
             SUM(CASE WHEN w.per = "now" THEN 1 ELSE 0 END) AS now_weeks
      FROM (SELECT o.commChannel AS cc, per, wk, SUM(o.subTotal - o.totalDiscount) AS sales
            FROM quantic.pos_history.orders o
            LET per = CASE WHEN o.dateCreated >= 1784347200000 THEN "now" ELSE "ly" END,
                wk = FLOOR((o.dateCreated - CASE WHEN o.dateCreated >= 1784347200000 THEN 1784347200000
                                                 ELSE 1752897600000 END) / 604800000)
            WHERE o.closeType = "paid"
              AND ((o.dateCreated >= 1784347200000 AND o.dateCreated < 1792209600000)
                OR (o.dateCreated >= 1752897600000 AND o.dateCreated < 1760760000000))
            GROUP BY o.commChannel, per, wk) w
      GROUP BY w.cc) s
JOIN (SELECT DISTINCT l0.commChannel, l0.businessName FROM quantic.pos.location l0 WHERE l0.parentLocationID = "") l
  ON l.commChannel = s.cc
WHERE s.ly_sales > 0
ORDER BY s.ly_sales - s.now_sales DESC
LIMIT 15;
