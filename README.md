# Quantic POS + MCP + AIDP Demo

Synthetic restaurant POS data for a Quantic-style workload on Couchbase Capella,
plus config for the [Couchbase MCP server](https://github.com/couchbase/mcp-server-couchbase)
so an AI assistant can answer questions about sales, menu performance, voids and channels.

```
loader/quantic_pos_gen.py   data generator / loader
sql/quantic_indexes.sql     indexes + demo queries
mcp/                        MCP client config example
.env.example                connection settings template (copy to .env)
```

## Data model

Bucket `quantic`, scope `pos`. Four collections, one per docType:

| Collection     | docType        | Contents |
|----------------|----------------|----------|
| `orders`       | `order`        | Order header: location, channel, server, guests, totals, timing |
| `cart`         | `cart`         | Line items: menu item, category, qty, price, modifiers, voids/comps, cost |
| `payment`      | `payment`      | Tenders in `transactionList` (sale, void, return), tips, settlement flag |
| `orderSummary` | `orderSummary` | Reporting rollup with embedded `cartList` and margin |

20 restaurants (2 hot locations carry ~70% of traffic), a 62-item casual-dining menu,
dine-in / takeout / delivery channels, realistic dayparts and weekly patterns.
Every doc carries `businessDate`, `dayOfWeek`, `hourOfDay` and `daypart` for easy filtering.

### Planted demo stories

| Story | Where to look |
|---|---|
| New item taking off | Nashville Hot Chicken Sandwich, launched 28 days before end date |
| Seasonal LTO | Pumpkin Cheesecake, last 22 days |
| Declining item | Quinoa Power Bowl, steady decline across the window |
| 86'd item | Grilled Atlantic Salmon missing at LOC_002 (Nashville) for 4 days |
| Void outlier | One server at LOC_001 (Charlotte) voids ~12% of lines |
| Delivery surge | LOC_009 (Austin) delivery share climbs from ~18% to 40%+ |
| Daypart contrast | Molten Chocolate Cake sells at dinner, almost never at lunch |

## Setup

```bash
git clone https://github.com/melboulos/quantic_pos_mcp.git && cd quantic_pos_mcp
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then edit .env with your cluster details
```

## Load data

```bash
# preview: no cluster needed, prints realism stats + writes sample docs to output/
python3 loader/quantic_pos_gen.py --dry_run --num_orders 5000

# load 100K orders (~500K cart lines); creates scope/collections if missing
python3 loader/quantic_pos_gen.py --num_orders 100000 --workers 8 --end_date 2026-10-07

# grow later - keep the same --end_date so stories line up
python3 loader/quantic_pos_gen.py --start_offset 100000 --num_orders 50000 --end_date 2026-10-07
```

Output is deterministic: same arguments, same data, regardless of worker count.
If your user can't manage collections, create them in the UI and add `--no_create`.

Then run `sql/quantic_indexes.sql` in the Capella Query Workbench.

## MCP server

1. Create a read-only database user for MCP (e.g. `mcp_reader` with Data Reader +
   Query Select on `quantic`). Don't reuse the loader account.
2. Add the block from `mcp/claude_desktop_config.example.json` to your MCP client config
   (Claude Desktop: Settings → Developer → Edit Config) with real credentials.
   The client config lives outside this repo, so secrets stay out of git.
3. For Capella, allow the MCP host's IP in the cluster's allowed IP list.

Questions to try: *What were the top 10 items last week? How is the Nashville Hot Chicken
Sandwich trending since launch? Which servers have unusual void rates? Why did salmon sales
drop in Nashville? How is delivery share changing in Austin?*

> Query tip for the assistant: order headers are in `quantic.pos.orders`; use `isVoided = false`
> on `cart` for sales figures; time filters work best on `businessDate` (YYYY-MM-DD).
