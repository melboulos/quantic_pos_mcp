/* =====================================================================
   One-time fixes to the loaded v2 dataset so Quantic's real query patterns behave as in production.
   (New loads from the updated generator already produce this data.)
   Run as a user with write access (not the read-only mcp user). Paste only statements, not comments.
   Run AFTER Step 3 in quantic_indexes.sql has built (fix 3 uses ix_stock_loc_item).
   ===================================================================== */

-- Fix 1: refunds are transactionType "return" in Quantic's data (we generated "refund"). ~40-60K payment docs.
-- A temporary index finds the affected documents, then is dropped.
CREATE INDEX ix_tmp_txn_type ON quantic.pos.payment(DISTINCT ARRAY t.transactionType FOR t IN transactionList END);

UPDATE quantic.pos.payment p
SET t.transactionType = "return" FOR t IN p.transactionList WHEN t.transactionType = "refund" END,
    t.currentAction = "return" FOR t IN p.transactionList WHEN t.currentAction = "refund" END
WHERE ANY t IN p.transactionList SATISFIES t.transactionType = "refund" END;

DROP INDEX ix_tmp_txn_type ON quantic.pos.payment;

-- Fix 2: add "noSale" order summaries (cash drawer opened without a sale), ~0.3% of in-store summaries.
-- Each is cloned from an existing in-store summary at the same location, 10 minutes later, with no items or money.
-- Filtering on closeType = "paid" keeps the newly inserted docs from being picked up again.
INSERT INTO quantic.pos.orderSummary (KEY k, VALUE v)
SELECT "orderSummary:" || u AS k,
       OBJECT_CONCAT(s, {
         "_id": "orderSummary:" || u, "orderSummaryID": u, "transactionID": u,
         "closeType": "noSale", "transactionType": "noSale", "paymentType": "cash",
         "orderID": "", "checkID": "", "checkName": "", "paymentID": "", "orderNumber": "", "refNumber": "",
         "cartList": [], "guest": 0, "subTotal": 0, "tax": 0, "total": 0, "paidAmount": 0, "totalPaidAmount": 0,
         "paidTax": 0, "tipAmount": 0, "checkTotalDiscount": 0, "checkTotalSurcharge": 0, "totalSurcharge": 0,
         "paidSurcharge": 0, "tipSurcharge": 0, "surchargeLabel": "", "cardType": "", "cardNumber": "",
         "cardHolderName": "", "authCode": "", "token": "", "transactionRef": "", "gateWay": "cash",
         "dateCreated": s.dateCreated + 600000, "dateClosed": s.dateClosed + 600000,
         "orderDateCreated": s.dateCreated + 600000, "osDateCreated": s.dateCreated + 603000,
         "tipedAt": s.dateCreated + 600000 }) AS v
FROM quantic.pos.orderSummary s
LET u = UPPER(UUID())
WHERE s.locationID IS NOT MISSING AND s.closeType = "paid" AND s.serviceAreaType IN [1, 2]
  AND META(s).id LIKE "orderSummary:00%";

-- Fix 3: some suppliers whose names contain "test", so the vendor filter in Q2 finds rows.
UPDATE quantic.pos.stockHistory s
SET s.vendorName = "TEST KITCHEN SUPPLY"
WHERE s.locationID IS NOT MISSING AND s.vendorName = "LOCAL PRODUCE CO";
