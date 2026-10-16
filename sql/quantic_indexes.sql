/* =====================================================================
   Quantic POS (v2, Quantic schema) - indexes + demo queries
   Scope: quantic.pos   One collection per docType; `order` docs live in `orders`.
   Times are epoch ms in UTC -> use MILLIS_TO_TZ(x, location.timeZone) for local time.
   Sales lines: cart.closeType = "paid". Channel: serviceAreaType 1 dine-in, 2 takeout/counter,
   3 delivery (in-house or 3rd party), 4 online ordering. A merchant = commChannel.
   Paste only statements (no comment lines) into the Query Workbench; it splits on semicolons.
   At ~1B docs: also partition the large indexes (see bottom).
   ===================================================================== */

-- Step 1: register every index (instant). Nothing is built yet; indexes show as "Created".
CREATE INDEX ix_location ON quantic.pos.location(commChannel, locationID, locationName, businessName, restaurantType, state, timeZone, isActive, dateCreated) WITH {"defer_build": true};
CREATE INDEX ix_item ON quantic.pos.item(locationID, name, catName, superCatName, itemPrice, cog) WITH {"defer_build": true};
CREATE INDEX ix_employee ON quantic.pos.employee(locationID, empID, empName, roleName, hireDate, terminationDate, isActive) WITH {"defer_build": true};
CREATE INDEX ix_orders_date ON quantic.pos.orders(dateCreated, commChannel, locationID, serviceAreaType, subTotal, totalDiscount, total, guest) WHERE isClosed = 1 AND closeType = "paid" WITH {"defer_build": true};
CREATE INDEX ix_orders_loc_date ON quantic.pos.orders(locationID, dateCreated, serviceAreaType, serviceAreaName, subTotal, totalDiscount, total, guest) WHERE isClosed = 1 AND closeType = "paid" WITH {"defer_build": true};
CREATE INDEX ix_os_loc_date ON quantic.pos.orderSummary(locationID, dateCreated, paymentType, serviceAreaType, total, tipAmount, totalSurcharge) WITH {"defer_build": true};
CREATE INDEX ix_payment_txn ON quantic.pos.payment(DISTINCT ARRAY [t.transactionType, t.paymentType] FOR t IN transactionList END, locationID, dateCreated) WITH {"defer_build": true};
CREATE INDEX ix_tm_loc_date ON quantic.pos.timeManagement(locationID, clockIn, empID, workingHours, hourlyRate) WITH {"defer_build": true};
CREATE INDEX ix_catalog_loc_date ON quantic.pos.catalogLog(locationID, dateCreated, masterName, keyChanges) WITH {"defer_build": true};
CREATE INDEX ix_audit_date ON quantic.pos.auditLog(dateCreated, commChannel, module) WITH {"defer_build": true};
CREATE INDEX ix_cart_loc_date ON quantic.pos.cart(locationID, dateCreated, name, quantity, subTotal, totalDiscount) WHERE closeType = "paid" WITH {"defer_build": true};
CREATE INDEX ix_cart_name_date ON quantic.pos.cart(name, dateCreated, commChannel, locationID, quantity, subTotal) WHERE closeType = "paid" WITH {"defer_build": true};
CREATE INDEX ix_cart_void ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, voidReason) WHERE closeType = "void" WITH {"defer_build": true};
CREATE INDEX ix_cart_emp ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, closeType) WITH {"defer_build": true};

-- Step 2: build. One statement per collection builds all its indexes in a single pass over the data
-- (cart: one scan of ~40M docs instead of four). Each returns immediately; watch Data Tools -> Indexes
-- go Created -> Building -> Ready.
BUILD INDEX ON quantic.pos.location(ix_location);
BUILD INDEX ON quantic.pos.item(ix_item);
BUILD INDEX ON quantic.pos.employee(ix_employee);
BUILD INDEX ON quantic.pos.orders(ix_orders_date, ix_orders_loc_date);
BUILD INDEX ON quantic.pos.orderSummary(ix_os_loc_date);
BUILD INDEX ON quantic.pos.payment(ix_payment_txn);
BUILD INDEX ON quantic.pos.timeManagement(ix_tm_loc_date);
BUILD INDEX ON quantic.pos.catalogLog(ix_catalog_loc_date);
BUILD INDEX ON quantic.pos.auditLog(ix_audit_date);
BUILD INDEX ON quantic.pos.cart(ix_cart_loc_date, ix_cart_name_date, ix_cart_void, ix_cart_emp);


/* =====================================================================
   CUSTOMER QUERY PATTERNS - rewritten for collections
   The four queries Quantic struggled with on `_default`, with the index each one needs.
   Use named parameters ($loc, $cc, ...) so plans are prepared and cached.
   ===================================================================== */

-- Step 3: indexes for the customer queries (deferred, then built per collection)
CREATE INDEX ix_cart_order ON quantic.pos.cart(orderID, locationID, commChannel) WHERE isPurged = 0 WITH {"defer_build": true};
CREATE INDEX ix_stock_loc_item ON quantic.pos.stockHistory(locationID, itemID, commChannel, LOWER(vendorName)) WITH {"defer_build": true};
CREATE INDEX ix_os_loc_cc_date ON quantic.pos.orderSummary(locationID, commChannel, dateCreated DESC) WHERE closeType != "noSale" WITH {"defer_build": true};
CREATE INDEX ix_payment_settle ON quantic.pos.payment(locationID, commChannel, DISTINCT ARRAY FLATTEN_KEYS(t.isForSettle, t.dateCreated, t.transactionType) FOR t IN transactionList END) WITH {"defer_build": true};
BUILD INDEX ON quantic.pos.cart(ix_cart_order);
BUILD INDEX ON quantic.pos.stockHistory(ix_stock_loc_item);
BUILD INDEX ON quantic.pos.orderSummary(ix_os_loc_cc_date);
BUILD INDEX ON quantic.pos.payment(ix_payment_settle);

-- Q1. Cart lines of one order.
-- Was: SELECT * FROM quantic WHERE docType='cart' AND locationID=... AND commChannel=... AND isPurged=0 AND orderID=...
-- Change: collection replaces the docType filter; index leads with orderID (a UUID, the most selective key),
-- so it also serves lookups by orderID alone. c.* returns plain documents instead of {"quantic": {...}} wrappers.
SELECT c.*
FROM quantic.pos.cart c
WHERE c.orderID = $orderID AND c.locationID = $loc AND c.commChannel = $cc AND c.isPurged = 0;

-- Q2. Inventory history for one item, filtered by vendor.
-- Was: ... FROM quantic WHERE docType='cart' AND ... AND itemID=... AND lower(vendorName) LIKE '%test%'
-- Change: every selected field (stockID, qtyRemaining, qtyReceived, reorderPoint, entryType, upc, sku, alu, margin,
-- vendorName, manufactureName ...) belongs to stockHistory documents, not cart, so the original returned mostly
-- empty rows. Run it against stockHistory. The '%test%' leading wildcard cannot narrow an index range, so the index
-- narrows on locationID + itemID and evaluates LOWER(vendorName) inside the index before fetching.
-- For free-text vendor search across all items, use a Search (FTS) index instead.
SELECT s.empName, s.`type`, s.commChannel, s.transferFrom, s.transferTo, s.`transfer`, s.entryType, s.qtyRemaining,
       s.modifiedByName, s.unit, s.reason, s.created, s.dateCreated, s.modified, s.vendorName, s.manufactureName,
       s.margin, s.cog, s.upc, s.sku, s.alu, s.qtyReceived, s.reorderPoint, s.size, s.note, s.qtySold, s.itemID, s.stockID
FROM quantic.pos.stockHistory s
WHERE s.locationID = $loc AND s.itemID = $itemID AND s.commChannel = $cc AND LOWER(s.vendorName) LIKE "%test%"
ORDER BY s.dateCreated DESC;

-- Q3. Order summaries for a location (excluding no-sale drawer opens).
-- Was: ... FROM `quantic where docType='orderSummary' AND locationID=... AND commChannel=... AND closeType != 'noSale'
-- (stray backtick in the original). Main problem: no date range, so it returns every summary the location ever had,
-- each carrying its whole cartList (~5.7 KB). With retained history that grows without bound.
-- Change: bounded by date and paginated; the partial index matches closeType != "noSale" and is sorted by date
-- descending, so ORDER BY + LIMIT stop early without sorting.
SELECT s.orderID, s.refNumber, s.orderTotal AS `total`, s.total AS checkTotal, s.paymentType, s.dateCreated,
       s.empName, s.cartList, s.transactionType, s.paidAmount
FROM quantic.pos.orderSummary s
WHERE s.locationID = $loc AND s.commChannel = $cc AND s.closeType != "noSale"
  AND s.dateCreated BETWEEN $fromMs AND $toMs
ORDER BY s.dateCreated DESC
LIMIT 100 OFFSET $offset;

-- Q4. Unsettled, non-return transactions in a time window, by payment type and time.
-- Was: SELECT results.transactionList, ... FROM quantic AS results WHERE results.docType='payment' AND ...
--      AND (ANY trans IN transactionList SATISFIES trans.isForSettle=0 AND trans.transactionType NOT IN ['return']
--      AND trans.dateCreated BETWEEN ... END) ORDER BY trans.paymentType, trans.dateCreated ASC
-- Bugs: (1) `trans` only exists inside ANY ... END, so ORDER BY trans.paymentType sorts on a missing value and the
-- ordering silently does nothing. (2) It returns the whole transactionList, including transactions that did not match.
-- Change: ANY narrows documents through the array index (equality on isForSettle, range on dateCreated);
-- UNNEST then emits only the matching transactions, so ORDER BY works on real values.
-- The ANY variable must match the index definition's variable (t), or the array keys are ignored.
SELECT p.orderID, p.terminalID, p.totalAmount, p.gratuityAmount, p.driverID, p.extraChargeList,
       tx.transactionID, tx.paymentType, tx.transactionType, tx.paidAmount, tx.dateCreated
FROM quantic.pos.payment p UNNEST p.transactionList tx
WHERE p.locationID = $loc AND p.commChannel = $cc
  AND ANY t IN p.transactionList SATISFIES t.isForSettle = 0 AND t.dateCreated BETWEEN $fromMs AND $toMs
      AND t.transactionType != "return" END
  AND tx.isForSettle = 0 AND tx.dateCreated BETWEEN $fromMs AND $toMs AND tx.transactionType != "return"
ORDER BY tx.paymentType, tx.dateCreated;


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
   BILLION-DOC SCALE: same deferred pattern, plus hash partitioning on the large indexes, e.g.
   ===================================================================== */
-- CREATE INDEX ix_cart_loc_date ON quantic.pos.cart(locationID, dateCreated, name, quantity, subTotal, totalDiscount)
--   PARTITION BY HASH(locationID) WHERE closeType = "paid" WITH {"defer_build": true, "num_partition": 8};
-- (same for ix_cart_name_date, ix_orders_date, ix_orders_loc_date, ix_os_loc_date), then the BUILD statements above.
