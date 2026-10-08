# Modeling assumptions

The generator follows the sample documents Quantic provided. Where the samples were silent,
these assumptions were made. Each one is easy to change in `loader/quantic_pos_gen.py`.

## Taken directly from Quantic's samples
- Document keys `docType:UUID`; uppercase v4 UUIDs for every ID; `isPurged: 0`; empty strings for unused fields.
- Field names and nesting for order, check, cart, payment (+ `transactionList`), orderSummary (+ embedded `cartList`),
  transactionDetail, batchClose, timeManagement, giftTransactionDetail, rewardHistory, signature (Sync Gateway blob
  metadata), stockHistory, catalogLog (`key:..##oldValue:..##updatedValue:..`), auditLog.
- `commChannel` identifies the merchant (sync channel); portal-written audit logs prefix it with `A` (`847` -> `A847`).
- Epoch-millisecond timestamps; iOS and web write whole seconds, Android writes milliseconds.
- Money stored as raw doubles (line tax unrounded, e.g. `3.3600000000000003`); tendered amounts rounded to cents.
- `serviceAreaType` 1 = dine-in (`Main Dining`), 4 = online (`Online TakeOut`); online orders use `empName: "ONLINE "`
  and order numbers like `O1415`; payment `refNumber` like `60610T6`.
- Gateways `cardConnect`, `cash`, `metisGift`; credit-card surcharge programs (e.g. "Rewards Pay", 3.5%).

## Assumed (no sample available)
- **location, item, employee docs**: shapes invented in Quantic's style (see one of each in a dry run's sample file).
  `location.timeZone` is what local-time questions rely on.
- **serviceAreaType 2** = takeout / counter, **3** = delivery (in-house `Delivery`, or third party `DoorDash`,
  `Uber Eats`, `Grubhub`). Third-party orders are paid with `paymentType: "other"` + `paymentTypeName`.
- **Voids**: `cart.closeType = "void"` plus `voidReason` / `voidedBy`; check `voidedBalance`. Whole-order void:
  `order.closeType = "void"`, no payment docs.
- **Discounts / modifiers**: `discountList` and `modifierList` entry shapes.
- **Refunds**: `transactionList` entry with `transactionType: "refund"`, `isForReturn: 1`.
- **Second gateway** `dejavoo` for ~15% of merchants (Quantic is processor-agnostic).
- `batchClose` totals approximate the day's card volume per terminal; they do not reconcile exactly to orders.
- `isArchived` = 1 on signatures / gift transactions older than 180 days.

## Customer base (synthetic, modeled on Quantic's public profile)
- Mostly independent SMB restaurants sold through resellers, mid-Atlantic heavy (PA/NJ/NY/DE/MD), plus a national tail.
- Locations per merchant: 75% single, 20% 2-5, 5% 6-25. Merchant 0 is the demo chain **Ember & Oak Kitchen**
  (20 locations, commChannel `847`).
- Concepts: casual full-service, Mexican, pizzeria, cafe, quick service, bar, food truck, Asian - each with its own menu,
  hours, day-of-week pattern, channel mix, staffing and average check.
- Merchants onboard over time, ~10% churn (volume fades ~120 days before they leave), and adopt online ordering,
  third-party delivery and surcharging at different dates. Staff turnover is modeled per role.
- Yearly menu price increases (logged in catalogLog each Jan 2), seasonality, and holidays
  (Valentine's, Super Bowl, St. Patrick's, Cinco de Mayo, Mother's Day, Thanksgiving, Christmas closure, NYE).
- Customer PII is fake: names from lists, `@example.com` emails, `555-01xx` phone numbers, masked cards.

## Planted demo stories (relative to `--end_date`)
| Story | Where |
|---|---|
| New item launch ramping up | NASHVILLE HOT CHICKEN SANDWICH, Ember & Oak, last 28 days |
| Seasonal LTO beating last year | PUMPKIN CHEESECAKE, every Sep 15 - Nov 30 |
| Declining item | QUINOA POWER BOWL, Ember & Oak |
| 86'd item (catalogLog + stockHistory trail) | GRILLED ATLANTIC SALMON, Ember & Oak - Fishtown, 10-7 days ago |
| New hire with ~13% void rate | Ember & Oak - King of Prussia, server hired 75 days ago |
| Delivery surge | Ember & Oak - Strip District, last 6 months |
| New store ramp | Ember & Oak - Brooklyn, opened 150 days ago |
| Platform: online adoption, surcharge adoption, merchant churn signals | all merchants |
