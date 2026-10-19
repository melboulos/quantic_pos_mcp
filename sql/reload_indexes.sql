-- Reload (2026-10-17): create the 18 hot-tier GSI indexes on EMPTY pos collections, before the hot pass.
-- Built immediately (collections are empty), with 1 replica so each index survives an Index node loss.
-- Definitions are identical to sql/quantic_indexes.sql; only the WITH clause differs.

CREATE INDEX ix_location ON quantic.pos.location(commChannel, locationID, locationName, businessName, restaurantType, state, timeZone, isActive, dateCreated) WITH {"num_replica": 1};
CREATE INDEX ix_location_name ON quantic.pos.location(businessName, city, locationName, locationID) WITH {"num_replica": 1};
CREATE INDEX ix_item ON quantic.pos.item(locationID, name, catName, superCatName, itemPrice, cog) WITH {"num_replica": 1};
CREATE INDEX ix_employee ON quantic.pos.employee(locationID, empID, empName, roleName, hireDate, terminationDate, isActive) WITH {"num_replica": 1};
CREATE INDEX ix_orders_date ON quantic.pos.orders(dateCreated, commChannel, locationID, serviceAreaType, subTotal, totalDiscount, total, guest) WHERE isClosed = 1 AND closeType = "paid" WITH {"num_replica": 1};
CREATE INDEX ix_orders_loc_date ON quantic.pos.orders(locationID, dateCreated, serviceAreaType, serviceAreaName, subTotal, totalDiscount, total, guest) WHERE isClosed = 1 AND closeType = "paid" WITH {"num_replica": 1};
CREATE INDEX ix_os_loc_date ON quantic.pos.orderSummary(locationID, dateCreated, paymentType, serviceAreaType, total, tipAmount, totalSurcharge) WITH {"num_replica": 1};
CREATE INDEX ix_payment_txn ON quantic.pos.payment(DISTINCT ARRAY [t.transactionType, t.paymentType] FOR t IN transactionList END, locationID, dateCreated) WITH {"num_replica": 1};
CREATE INDEX ix_tm_loc_date ON quantic.pos.timeManagement(locationID, clockIn, empID, workingHours, hourlyRate) WITH {"num_replica": 1};
CREATE INDEX ix_catalog_loc_date ON quantic.pos.catalogLog(locationID, dateCreated, masterName, keyChanges) WITH {"num_replica": 1};
CREATE INDEX ix_audit_date ON quantic.pos.auditLog(dateCreated, commChannel, module) WITH {"num_replica": 1};
CREATE INDEX ix_cart_loc_date ON quantic.pos.cart(locationID, dateCreated, name, quantity, subTotal, totalDiscount) WHERE closeType = "paid" WITH {"num_replica": 1};
CREATE INDEX ix_cart_name_date ON quantic.pos.cart(name, dateCreated, commChannel, locationID, quantity, subTotal) WHERE closeType = "paid" WITH {"num_replica": 1};
CREATE INDEX ix_cart_void ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, voidReason) WHERE closeType = "void" WITH {"num_replica": 1};
CREATE INDEX ix_cart_emp ON quantic.pos.cart(locationID, dateCreated, createdBy, saleEmpName, closeType) WITH {"num_replica": 1};
CREATE INDEX ix_cart_order ON quantic.pos.cart(orderID, locationID, commChannel) WHERE isPurged = 0 WITH {"num_replica": 1};
CREATE INDEX ix_stock_loc_item ON quantic.pos.stockHistory(locationID, itemID, commChannel, LOWER(vendorName)) WITH {"num_replica": 1};
CREATE INDEX ix_os_loc_cc_date ON quantic.pos.orderSummary(locationID, commChannel, dateCreated DESC) WHERE closeType != "noSale" WITH {"num_replica": 1};
CREATE INDEX ix_payment_settle ON quantic.pos.payment(locationID, commChannel, DISTINCT ARRAY FLATTEN_KEYS(t.isForSettle, t.dateCreated, t.transactionType) FOR t IN transactionList END) WITH {"num_replica": 1};
