/* =====================================================================
   Quantic POS - indexes for quantic.pos.{orders, cart, payment, orderSummary}
   Order headers live in the "orders" collection (docType is still 'order').
   docType predicates are no longer needed - the collection is the type.
   ===================================================================== */

/* ---------- Baseline benchmark set (translated from the framework doc) ---------- */

-- A. Operational cart lookup
CREATE INDEX ix_cart_order ON quantic.pos.cart(locationID, commChannel, orderID)
  WHERE isPurged = 0;

-- B. Array index on payment transactions (as in the framework doc)
CREATE INDEX ix_payment_trans ON quantic.pos.payment(
  DISTINCT ARRAY [t.transactionType, t.dateCreated] FOR t IN transactionList END,
  locationID, commChannel);

-- B2. Alternative that actually serves pattern C (isForSettle = 0 AND transactionType != 'return')
CREATE INDEX ix_payment_settle ON quantic.pos.payment(
  DISTINCT ARRAY [t.isForSettle, t.transactionType] FOR t IN transactionList END,
  locationID, commChannel);

-- C. Reporting lookup
CREATE INDEX ix_ordersummary_lookup ON quantic.pos.orderSummary(locationID, commChannel, dateCreated);

/* ---------- Demo / MCP indexes (covering for the common questions) ---------- */

-- Top sellers by date / location / item
CREATE INDEX ix_cart_sales ON quantic.pos.cart(businessDate, locationID, itemName, category, qtySold, netSales, daypart)
  WHERE isVoided = false;

-- Item trend over time
CREATE INDEX ix_cart_item_date ON quantic.pos.cart(itemName, businessDate, locationID, qtySold, netSales)
  WHERE isVoided = false;

-- Voids / loss prevention
CREATE INDEX ix_cart_voids ON quantic.pos.cart(locationID, employeeID, empName, voidReason, businessDate)
  WHERE isVoided = true;

-- Order-level revenue, channel mix, check size
CREATE INDEX ix_order_daily ON quantic.pos.orders(businessDate, locationID, channelName, daypart, total, guestCount)
  WHERE status = 'closed';


/* =====================================================================
   Demo queries - each one surfaces a planted story
   ===================================================================== */

-- 1. Top 10 sellers in the last 7 days
SELECT c.itemName, SUM(c.qtySold) AS qty, ROUND(SUM(c.netSales), 2) AS sales
FROM quantic.pos.cart c
WHERE c.isVoided = false
  AND c.businessDate >= DATE_FORMAT_STR(DATE_ADD_STR(NOW_STR(), -7, 'day'), '1111-11-11')
GROUP BY c.itemName ORDER BY qty DESC LIMIT 10;

-- 2. New item launch: Nashville Hot Chicken Sandwich, weekly units
SELECT DATE_TRUNC_STR(c.businessDate, 'week') AS week, SUM(c.qtySold) AS qty
FROM quantic.pos.cart c
WHERE c.itemName = 'Nashville Hot Chicken Sandwich' AND c.isVoided = false
GROUP BY DATE_TRUNC_STR(c.businessDate, 'week') ORDER BY week;

-- 3. 86'd: salmon sales by day at Nashville (LOC_002) - look for the gap
SELECT c.businessDate, SUM(c.qtySold) AS qty
FROM quantic.pos.cart c
WHERE c.itemName = 'Grilled Atlantic Salmon' AND c.locationID = 'LOC_002' AND c.isVoided = false
GROUP BY c.businessDate ORDER BY c.businessDate DESC LIMIT 21;

-- 4. Loss prevention: void rate by employee at Charlotte (LOC_001)
SELECT c.empName, c.employeeID, COUNT(*) AS lines,
       SUM(CASE WHEN c.isVoided THEN 1 ELSE 0 END) AS voids,
       ROUND(SUM(CASE WHEN c.isVoided THEN 1 ELSE 0 END) / COUNT(*), 3) AS voidRate
FROM quantic.pos.cart c
WHERE c.locationID = 'LOC_001'
GROUP BY c.empName, c.employeeID ORDER BY voidRate DESC LIMIT 5;

-- 5. Delivery share trend at Austin (LOC_009)
SELECT DATE_TRUNC_STR(o.businessDate, 'week') AS week,
       ROUND(SUM(CASE WHEN o.channelName = 'delivery' THEN 1 ELSE 0 END) / COUNT(*), 2) AS deliveryShare
FROM quantic.pos.orders o
WHERE o.locationID = 'LOC_009'
GROUP BY DATE_TRUNC_STR(o.businessDate, 'week') ORDER BY week;

-- 6. Dessert by daypart (Molten Chocolate Cake sells at dinner, barely at lunch)
SELECT c.itemName, c.daypart, SUM(c.qtySold) AS qty
FROM quantic.pos.cart c
WHERE c.category = 'Desserts' AND c.isVoided = false
GROUP BY c.itemName, c.daypart ORDER BY c.itemName, qty DESC;
