-- Reload (2026-10-17): Analytics collections, created on empty KV collections BEFORE the load so ingestion
-- can be watched as data arrives. Run in the ANALYTICS workbench.
-- History (shadow) only for the transactional data: a link on a TTL'd collection would mirror expirations.
CREATE ANALYTICS COLLECTION quantic.pos_history.cart ON quantic.pos_history.cart;
CREATE ANALYTICS COLLECTION quantic.pos_history.`check` ON quantic.pos_history.`check`;
CREATE ANALYTICS COLLECTION quantic.pos_history.orderSummary ON quantic.pos_history.orderSummary;
CREATE ANALYTICS COLLECTION quantic.pos_history.orders ON quantic.pos_history.orders;
CREATE ANALYTICS COLLECTION quantic.pos_history.payment ON quantic.pos_history.payment;
CREATE ANALYTICS COLLECTION quantic.pos_history.transactionDetail ON quantic.pos_history.transactionDetail;
-- Reference data (no TTL, never expires) for joins: merchant names, menus, staff.
CREATE ANALYTICS COLLECTION quantic.pos.employee ON quantic.pos.employee;
CREATE ANALYTICS COLLECTION quantic.pos.item ON quantic.pos.item;
CREATE ANALYTICS COLLECTION quantic.pos.location ON quantic.pos.location;
-- Item-name lookups on history cart (A4: 40.8 s -> 0.1 s)
CREATE INDEX ix_hist_cart_name ON quantic.pos_history.cart(name: string);
CONNECT LINK Local;

-- Added 2026-10-17: operational collections with NO TTL, linked directly (safe: nothing expires).
-- If any of these later gets a TTL, it must move to the Eventing -> pos_history pattern instead.
CREATE ANALYTICS COLLECTION quantic.pos.timeManagement ON quantic.pos.timeManagement;
CREATE ANALYTICS COLLECTION quantic.pos.catalogLog ON quantic.pos.catalogLog;
CREATE ANALYTICS COLLECTION quantic.pos.stockHistory ON quantic.pos.stockHistory;
CREATE ANALYTICS COLLECTION quantic.pos.rewardHistory ON quantic.pos.rewardHistory;
CONNECT LINK Local;
