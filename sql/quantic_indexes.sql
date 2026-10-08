/* =====================================================================
   Quantic POS (v2, Quantic schema) - indexes + demo queries
   Scope: quantic.pos   One collection per docType; `order` docs live in `orders`.
   Times are epoch ms in UTC -> use MILLIS_TO_TZ(x, location.timeZone) for local time.
   Sales lines: cart.closeType = "paid". Channel: serviceAreaType 1 dine-in, 2 takeout/counter,
   3 delivery (in-house or 3rd party), 4 online ordering. A merchant = commChannel.
   At ~1B docs: create these deferred + partitioned (see bottom) and BUILD together.
   ===================================================================== */

CREATE INDEX ix_location ON quantic.pos.location(commChannel, locationID, locationName, businessName, restaurantType, state, timeZone, isActive, dateCreated);
CREATE INDEX ix_item ON quantic.pos.item(locationID, name, catName, superCatName, itemPrice, cog);
CREATE INDEX ix_employee ON quantic.pos.employee(locationID, empID, empName, roleName, hireDate, terminationDate, isActive);

CREATE INDEX ix_orders_date ON quantic.pos.orders(dateCreated, commChannel, locationID, serviceAreaType, subTotal, totalDiscount, total, guest)
  WHERE isClosed = 1 AND closeType = "paid";
CREATE INDEX ix_orders_loc_date ON quantic.pos.orders(locationID, dateCreated, serviceAreaType, serviceAreaName, subTotal, totalDiscount, total, guest)
  WHERE isClosed = 1 AND closeType = "paid";

CREATE INDEX ix_cart_loc_date ON quantic.pos.cart(locationID, dateCreated, name, quantity, subTotal, totalDiscount)
  WHERE closeType = "paid";
CREATE INDEX ix_cart_name_date ON quantic.pos.cart(name, dateCreated, locationID, quantity, subTotal)
  WHERE closeType = "paid";
CREATE INDEX ix_cart_void ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, voidReason)
  WHERE closeType = "void";
CREATE INDEX ix_cart_emp ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, closeType);

CREATE INDEX ix_os_loc_date ON quantic.pos.orderSummary(locationID, dateCreated, paymentType, serviceAreaType, total, tipAmount, totalSurcharge);
CREATE INDEX ix_payment_txn ON quantic.pos.payment(DISTINCT ARRAY [t.transactionType, t.paymentType] FOR t IN transactionList END, locationID, dateCreated);
CREATE INDEX ix_tm_loc_date ON quantic.pos.timeManagement(locationID, clockIn, empID, workingHours, hourlyRate);
CREATE INDEX ix_catalog_loc_date ON quantic.pos.catalogLog(locationID, dateCreated, masterName, keyChanges);
CREATE INDEX ix_audit_date ON quantic.pos.auditLog(dateCreated, commChannel, module);


/* =====================================================================
   DEMO QUERIES
   ===================================================================== */

-- 1. Platform view (Quantic's own business): orders and active merchants by month
SELECT DATE_FORMAT_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "1111-11") AS month,
       COUNT(*) AS orders, COUNT(DISTINCT o.commChannel) AS merchants,
       ROUND(SUM(o.subTotal - o.totalDiscount), 0) AS netSales
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid" AND o.dateCreated >= 0
GROUP BY DATE_FORMAT_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "1111-11")
ORDER BY month;

-- 2. Online + delivery adoption across the platform, by quarter
SELECT DATE_PART_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "year") AS yr,
       DATE_PART_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "quarter") AS qtr,
       ROUND(SUM(CASE WHEN o.serviceAreaType IN [3, 4] THEN 1 ELSE 0 END) / COUNT(*), 3) AS onlineDeliveryShare
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid" AND o.dateCreated >= 0
GROUP BY DATE_PART_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "year"),
         DATE_PART_STR(MILLIS_TO_TZ(o.dateCreated, "America/New_York"), "quarter")
ORDER BY yr, qtr;

-- 3. Churn early-warning: merchants whose last 90 days fell vs the 90 days before
WITH cutoff AS (MILLIS("2026-10-07") - 90 * 86400000)
SELECT o.commChannel,
       SUM(CASE WHEN o.dateCreated >= cutoff THEN 1 ELSE 0 END) AS last90,
       SUM(CASE WHEN o.dateCreated < cutoff THEN 1 ELSE 0 END) AS prior90
FROM quantic.pos.orders o
WHERE o.isClosed = 1 AND o.closeType = "paid" AND o.dateCreated >= cutoff - 90 * 86400000
GROUP BY o.commChannel
HAVING SUM(CASE WHEN o.dateCreated < cutoff THEN 1 ELSE 0 END) > 200
ORDER BY SUM(CASE WHEN o.dateCreated >= cutoff THEN 1 ELSE 0 END) / SUM(CASE WHEN o.dateCreated < cutoff THEN 1 ELSE 0 END)
LIMIT 10;

-- 4. Demo chain (commChannel "847"): new sandwich launch, weekly units across all locations
SELECT DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week") AS week, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.name = "NASHVILLE HOT CHICKEN SANDWICH" AND c.closeType = "paid" AND c.commChannel = "847"
  AND c.dateCreated >= MILLIS("2026-08-01")
GROUP BY DATE_TRUNC_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "week")
ORDER BY week;

-- 5. Why did salmon disappear in Fishtown? sales by day + the catalog change log
SELECT DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11") AS day, SUM(c.quantity) AS units
FROM quantic.pos.cart c JOIN quantic.pos.location l ON c.locationID = l.locationID
WHERE l.locationName = "Ember & Oak Kitchen - Fishtown" AND c.name = "GRILLED ATLANTIC SALMON"
  AND c.closeType = "paid" AND c.dateCreated >= MILLIS("2026-09-15")
GROUP BY DATE_FORMAT_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "1111-11-11")
ORDER BY day;

SELECT MILLIS_TO_TZ(g.dateCreated, "America/New_York") AS changedAt, g.masterName, g.modifiedFields
FROM quantic.pos.catalogLog g JOIN quantic.pos.location l ON g.locationID = l.locationID
WHERE l.locationName = "Ember & Oak Kitchen - Fishtown" AND g.masterName = "GRILLED ATLANTIC SALMON"
  AND g.dateCreated >= MILLIS("2026-09-01");

-- 6. Loss prevention: void rate by employee at King of Prussia, last 90 days
SELECT c.saleEmpName, COUNT(*) AS lines,
       SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) AS voids,
       ROUND(SUM(CASE WHEN c.closeType = "void" THEN 1 ELSE 0 END) / COUNT(*), 3) AS voidRate
FROM quantic.pos.cart c JOIN quantic.pos.location l ON c.locationID = l.locationID
WHERE l.locationName = "Ember & Oak Kitchen - King of Prussia" AND c.dateCreated >= MILLIS("2026-07-09")
GROUP BY c.saleEmpName
HAVING COUNT(*) > 100
ORDER BY voidRate DESC LIMIT 5;

-- 7. Year over year (only possible because history is retained): Pumpkin Cheesecake each fall
SELECT DATE_PART_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "year") AS yr, SUM(c.quantity) AS units
FROM quantic.pos.cart c
WHERE c.name = "PUMPKIN CHEESECAKE" AND c.closeType = "paid" AND c.dateCreated >= 0
GROUP BY DATE_PART_STR(MILLIS_TO_TZ(c.dateCreated, "America/New_York"), "year")
ORDER BY yr;

-- 8. Labor: sales per labor hour by location for one merchant, last 30 days
SELECT l.locationName, s.sales, h.hours, ROUND(s.sales / h.hours, 2) AS salesPerLaborHour
FROM quantic.pos.location l
JOIN (SELECT o.locationID, ROUND(SUM(o.subTotal - o.totalDiscount), 2) AS sales
      FROM quantic.pos.orders o
      WHERE o.isClosed = 1 AND o.closeType = "paid" AND o.dateCreated >= MILLIS("2026-09-06")
      GROUP BY o.locationID) s ON s.locationID = l.locationID
JOIN (SELECT t.locationID, ROUND(SUM(t.workingHours), 1) AS hours
      FROM quantic.pos.timeManagement t
      WHERE t.clockIn >= MILLIS("2026-09-06")
      GROUP BY t.locationID) h ON h.locationID = l.locationID
WHERE l.commChannel = "847"
ORDER BY salesPerLaborHour DESC;


/* =====================================================================
   BILLION-DOC SCALE: create the large indexes deferred + partitioned, then build together
   ===================================================================== */
-- CREATE INDEX ix_cart_loc_date ON quantic.pos.cart(locationID, dateCreated, name, quantity, subTotal, totalDiscount)
--   PARTITION BY HASH(locationID) WHERE closeType = "paid" WITH {"defer_build": true, "num_partition": 8};
-- (same pattern for ix_cart_name_date, ix_orders_date, ix_orders_loc_date, ix_os_loc_date)
-- BUILD INDEX ON quantic.pos.cart(ix_cart_loc_date, ix_cart_name_date, ix_cart_void, ix_cart_emp);
-- BUILD INDEX ON quantic.pos.orders(ix_orders_date, ix_orders_loc_date);
