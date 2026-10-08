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

The dataset follows **Quantic's production document shapes** (from sample docs they provided): uppercase UUIDs,
`docType:UUID` keys, epoch-ms timestamps, raw-double money, `commChannel` per merchant. Bucket `quantic`,
scope `pos`, one collection per docType (`order` docs live in `orders`, since ORDER is reserved in SQL++):

`orders`, `check`, `cart`, `payment`, `orderSummary`, `transactionDetail`, `batchClose`, `timeManagement`,
`giftTransactionDetail`, `rewardHistory`, `signature`, `stockHistory`, `catalogLog`, `auditLog`,
plus `location`, `item`, `employee` (assumed shapes).

The merchant base mirrors Quantic's market: mostly independent restaurants (8 concepts), mid-Atlantic heavy,
onboarding, churn, feature adoption and staff turnover over time, plus a 20-location demo chain,
**Ember & Oak Kitchen** (commChannel `847`), carrying the planted stories.
See **ASSUMPTIONS.md** for every assumption and the story list, and **docs/mcp_demo_prompt.md** for the AI context.

## Setup

```bash
git clone https://github.com/melboulos/quantic_pos_mcp.git && cd quantic_pos_mcp
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then edit .env with your cluster details
```

## Load data

```bash
# preview: projected doc counts per collection, mix by concept/channel/year, one sample doc per type
python loader/quantic_pos_gen.py --dry_run --merchants 300 --days 1095 --end_date 2026-10-07

# demo size: 60 merchants, 1 year (~90M docs)
python loader/quantic_pos_gen.py --merchants 60 --days 365 --end_date 2026-10-07 --processes 4

# ~1.1B docs: 300 merchants (~700 locations), 3 years - run on a VM in the cluster's region
python loader/quantic_pos_gen.py --merchants 300 --days 1095 --end_date 2026-10-07 --processes 16 --workers 8
```

Phases run in order: `reference` (location/item/employee), `daily` (batch closes, time clock, inventory,
catalog and audit logs), `orders` (everything per order). `--merchants`, `--days`, `--end_date` and `--seed` define
the dataset; keep them identical across runs. Resume orders with `--phases orders --start_offset N`.
Start from an empty bucket (ideally Magma), then run `sql/quantic_indexes.sql`.

## MCP server

1. Create a read-only database user for MCP (e.g. `mcp_reader` with Data Reader +
   Query Select on `quantic`). Don't reuse the loader account.
2. Add the block from `mcp/claude_desktop_config.example.json` to your MCP client config
   (Claude Desktop: Settings → Developer → Edit Config) with real credentials.
   The client config lives outside this repo, so secrets stay out of git.
3. For Capella, allow the MCP host's IP in the cluster's allowed IP list.

Questions to try: *Which merchants look at risk of churning? How has online ordering adoption grown across the
platform? How is Ember & Oak's new Nashville Hot Chicken Sandwich doing? Why did salmon disappear in Fishtown?
Which servers have unusual void rates? How did Pumpkin Cheesecake do this fall vs last fall?*

> Give the assistant the context in `docs/mcp_demo_prompt.md` (field conventions, time zones, sales filters).
