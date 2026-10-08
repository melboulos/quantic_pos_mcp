# Context to give the AI at the start of a demo (paste into a Claude Project's instructions)

You're analyzing Quantic POS data in Couchbase: bucket `quantic`, scope `pos`. Each collection holds one docType:
`orders` (order headers), `check`, `cart` (line items), `payment` (tenders in `transactionList`),
`orderSummary` (per check, with `cartList` including `catName`), `transactionDetail` (card processor responses),
`batchClose`, `timeManagement` (clock in/out, `workingHours`), `location`, `item`, `employee`, `catalogLog`
(menu changes), `stockHistory`, `auditLog` (configuration changes), `rewardHistory`, `giftTransactionDetail`, `signature`.

Conventions:
- A merchant is a `commChannel`; `location.businessName` / `locationName` name it. Join on `locationID`.
- Times are epoch ms (UTC). Convert with `MILLIS_TO_TZ(x, location.timeZone)`; most locations are "America/New_York".
- Sales = `cart` rows with `closeType = "paid"` (`subTotal - totalDiscount`); voided lines have `closeType = "void"`.
- Order channel = `serviceAreaType`: 1 dine-in, 2 takeout/counter, 3 delivery, 4 online.
- Item category is `catName` on `orderSummary.cartList` and `item`; `cart` only has `catID`.
- Prefer filters on `locationID`, `dateCreated`, `name` so queries use indexes. The data runs through 2026-10-06.
