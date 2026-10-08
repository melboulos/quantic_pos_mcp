#!/usr/bin/env python3
"""
quantic_pos_gen.py - Quantic restaurant POS synthetic data loader

Loads four doc types into four collections (docType -> collection):

    quantic.<scope>.orders          order header (docType "order", one per order)
    quantic.<scope>.cart            line items (avg ~5 per order)
    quantic.<scope>.payment         payment with embedded transactionList
    quantic.<scope>.orderSummary    reporting rollup

The order collection is "orders" (plural) because ORDER is a reserved word
in SQL++. The docType field inside the documents is still "order".

HOW TO RUN
----------
  Connection settings come from the repo's .env file (copy .env.example to .env).
  The variable names match the Couchbase MCP server, so one .env serves both.

  # Preview without touching the cluster (writes samples + realism stats)
  python3 quantic_pos_gen.py --dry_run --num_orders 5000

  # Full load
  python3 quantic_pos_gen.py --num_orders 100000 --workers 8

  # Grow the dataset later - pass the SAME --end_date so stories line up
  python3 quantic_pos_gen.py --start_offset 100000 --num_orders 50000 --end_date 2026-10-07

Every order is generated from its own seeded RNG (seed + order number), so
the same arguments always produce identical data regardless of thread count.
"""

import argparse
import json
import logging
import os
import random
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:  # load ../.env (repo root) if python-dotenv is installed; real env vars still win
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

# =========================================================
# ARGS / CONFIG
# =========================================================

parser = argparse.ArgumentParser(description="Quantic restaurant POS data generator")
parser.add_argument("--start_offset", type=int, default=0, help="first order number (deterministic IDs)")
parser.add_argument("--num_orders", type=int, default=50000)
parser.add_argument("--workers", type=int, default=8)
parser.add_argument("--bucket", default=os.environ.get("CB_BUCKET", "quantic"))
parser.add_argument("--scope", default=os.environ.get("CB_SCOPE", "pos"))
parser.add_argument("--days", type=int, default=90, help="days of history ending the day before --end_date")
parser.add_argument("--end_date", default=None, help="YYYY-MM-DD (default: today). Fix this for reproducible/incremental loads")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--flush_size", type=int, default=800, help="docs buffered per collection before a bulk upsert")
parser.add_argument("--no_create", action="store_true", help="skip creating scope/collections")
parser.add_argument("--dry_run", action="store_true", help="generate only; write samples + stats, no cluster")
parser.add_argument("--sample_out", default="output/sample_docs.json")
args = parser.parse_args()

SEED = args.seed
DAYS = max(2, args.days)
END_DATE = date.fromisoformat(args.end_date) if args.end_date else date.today()

# docType -> collection name
DOC_TO_COLLECTION = {
    "order": "orders",          # ORDER is reserved in SQL++, so the collection is plural
    "cart": "cart",
    "payment": "payment",
    "orderSummary": "orderSummary",
}
COLLECTIONS = list(DOC_TO_COLLECTION.values())

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("quantic")

# =========================================================
# REFERENCE DATA (built once, deterministically)
# =========================================================

REF = random.Random(f"{SEED}:reference")

# commChannel codes kept from the original model
CHANNELS = {
    "dine_in":  "123",
    "takeout":  "847",
    "delivery": "848",
}

# Locations: hot locations first. Tax rates are approximate restaurant rates.
_LOCS = [
    ("Charlotte Uptown",            "Charlotte",    "NC", "America/New_York",    0.0825),
    ("Nashville The Gulch",         "Nashville",    "TN", "America/Chicago",     0.0925),
    ("Atlanta Midtown",             "Atlanta",      "GA", "America/New_York",    0.0890),
    ("Raleigh Glenwood South",      "Raleigh",      "NC", "America/New_York",    0.0825),
    ("Charleston King Street",      "Charleston",   "SC", "America/New_York",    0.1100),
    ("Orlando International Drive", "Orlando",      "FL", "America/New_York",    0.0750),
    ("Tampa Channelside",           "Tampa",        "FL", "America/New_York",    0.0750),
    ("Dallas Uptown",               "Dallas",       "TX", "America/Chicago",     0.0825),
    ("Austin South Congress",       "Austin",       "TX", "America/Chicago",     0.0825),
    ("Houston Heights",             "Houston",      "TX", "America/Chicago",     0.0825),
    ("Denver LoDo",                 "Denver",       "CO", "America/Denver",      0.1015),
    ("Scottsdale Old Town",         "Scottsdale",   "AZ", "America/Phoenix",     0.0805),
    ("Chicago River North",         "Chicago",      "IL", "America/Chicago",     0.1175),
    ("Columbus Short North",        "Columbus",     "OH", "America/New_York",    0.0750),
    ("Indianapolis Mass Ave",       "Indianapolis", "IN", "America/Indiana/Indianapolis", 0.0900),
    ("Louisville NuLu",             "Louisville",   "KY", "America/Kentucky/Louisville",  0.0600),
    ("Richmond Scott's Addition",   "Richmond",     "VA", "America/New_York",    0.1130),
    ("Pittsburgh Strip District",   "Pittsburgh",   "PA", "America/New_York",    0.0700),
    ("Kansas City Power & Light",   "Kansas City",  "MO", "America/Chicago",     0.0985),
    ("San Diego Gaslamp",           "San Diego",    "CA", "America/Los_Angeles", 0.0775),
]

HOT_LOCATION_FRACTION = 0.10   # 10% of locations ...
HOT_TRAFFIC_SHARE = 0.70       # ... generate ~70% of traffic (per framework doc)

LOCATIONS = []
_n_hot = max(1, round(len(_LOCS) * HOT_LOCATION_FRACTION))
for i, (name, city, state, tz, tax) in enumerate(_LOCS):
    hot = i < _n_hot
    share = HOT_TRAFFIC_SHARE / _n_hot if hot else (1 - HOT_TRAFFIC_SHARE) / (len(_LOCS) - _n_hot)
    LOCATIONS.append({
        "locationID": f"LOC_{i + 1:03d}",
        "num": i + 1,
        "locationName": name,
        "city": city,
        "state": state,
        "tz": ZoneInfo(tz),
        "taxRate": tax,
        "isHot": hot,
        "weight": share * REF.uniform(0.85, 1.15),
    })
LOC_BY_ID = {l["locationID"]: l for l in LOCATIONS}

# ---- Employees (per location) ----
FIRST = ["James", "Maria", "Tyler", "Ashley", "Marcus", "Jasmine", "Kevin", "Brittany", "Andre", "Lauren",
         "Carlos", "Megan", "Derek", "Alyssa", "Jordan", "Kayla", "Brandon", "Destiny", "Ryan", "Taylor",
         "Luis", "Hannah", "Chris", "Morgan", "Darius", "Emily", "Nick", "Sofia", "Trevor", "Gabriela",
         "Justin", "Aaliyah", "Cody", "Olivia", "Malik", "Rachel", "Sean", "Vanessa", "Eric", "Chloe"]
LAST = ["Johnson", "Garcia", "Smith", "Williams", "Brown", "Martinez", "Davis", "Rodriguez", "Wilson", "Moore",
        "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Lopez", "Lee",
        "Walker", "Hall", "Allen", "Young", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
        "Green", "Adams", "Baker", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips"]
ROLE_COUNTS = {"General Manager": 1, "Manager": 2, "Server": 10, "Bartender": 3, "Cashier": 4}

EMPLOYEES = defaultdict(lambda: defaultdict(list))  # locationID -> role -> [emp]
_emp_n = 0
for loc in LOCATIONS:
    for role, n in ROLE_COUNTS.items():
        for _ in range(n):
            _emp_n += 1
            EMPLOYEES[loc["locationID"]][role].append({
                "employeeID": f"EMP_{_emp_n:04d}",
                "empName": f"{REF.choice(FIRST)} {REF.choice(LAST)}",
                "role": role,
                "shiftWeight": REF.uniform(0.4, 1.6),  # some people work a lot more shifts
            })

# ---- Menu ----
# Dayparts: breakfast (weekday 7-10), brunch (weekend 8-13), lunch, afternoon, dinner, late_night
ALL_DP = {"breakfast", "brunch", "lunch", "afternoon", "dinner", "late_night"}
MAIN_DP = {"brunch", "lunch", "afternoon", "dinner", "late_night"}
NO_LATE = {"brunch", "lunch", "afternoon", "dinner"}

# category: (food cost % range, vendor pool, availability, daypart affinity)
CATEGORY_META = {
    "Breakfast":            ((0.24, 0.30), ["Sysco Foods", "US Foods"], {"breakfast", "brunch"}, {}),
    "Appetizers":           ((0.26, 0.32), ["Sysco Foods", "US Foods", "Performance Food Group"], MAIN_DP, {"afternoon": 1.4, "late_night": 1.8}),
    "Salads":               ((0.27, 0.33), ["Sysco Foods", "Performance Food Group"], MAIN_DP, {"lunch": 1.6, "dinner": 0.7}),
    "Burgers & Sandwiches": ((0.30, 0.35), ["Sysco Foods", "US Foods", "Performance Food Group"], MAIN_DP, {"lunch": 1.5, "dinner": 0.8, "late_night": 1.4}),
    "Entrees":              ((0.30, 0.38), ["Sysco Foods", "US Foods", "Performance Food Group"], NO_LATE, {"lunch": 0.6, "dinner": 1.5}),
    "Sides":                ((0.18, 0.25), ["Sysco Foods", "US Foods"], MAIN_DP, {}),
    "Kids":                 ((0.25, 0.30), ["Sysco Foods"], NO_LATE, {}),
    "Desserts":             ((0.22, 0.28), ["US Foods", "Performance Food Group"], MAIN_DP, {"lunch": 0.5, "dinner": 1.4}),
    "Beverages":            ((0.10, 0.18), ["Global Beverage Supply"], ALL_DP, {}),
    "Beer":                 ((0.22, 0.28), ["Global Beverage Supply"], MAIN_DP, {}),
    "Wine":                 ((0.28, 0.33), ["Global Beverage Supply"], MAIN_DP, {"dinner": 1.3}),
    "Cocktails":            ((0.18, 0.24), ["Global Beverage Supply"], MAIN_DP, {"late_night": 1.5}),
}

# (name, category, price, popularity, availability override, item daypart affinity)
_MENU = [
    ("Classic Eggs Benedict",           "Breakfast", 15.95, 9, None, {}),
    ("Buttermilk Pancake Stack",        "Breakfast", 11.95, 8, None, {}),
    ("Avocado Toast",                   "Breakfast", 12.50, 6, None, {}),
    ("Chicken & Waffles",               "Breakfast", 16.95, 7, None, {"brunch": 1.6}),
    ("Breakfast Burrito",               "Breakfast", 12.95, 7, None, {"breakfast": 1.5}),
    ("Steel-Cut Oatmeal",               "Breakfast",  7.95, 3, None, {"breakfast": 1.5, "brunch": 0.5}),
    ("Crispy Calamari",                 "Appetizers", 14.95, 6, None, {}),
    ("Spinach Artichoke Dip",           "Appetizers", 12.95, 8, None, {}),
    ("Buffalo Wings",                   "Appetizers", 13.95, 10, None, {"late_night": 1.4}),
    ("Loaded Potato Skins",             "Appetizers", 11.95, 5, None, {}),
    ("Truffle Fries",                   "Appetizers",  9.95, 7, None, {}),
    ("Pretzel Bites & Beer Cheese",     "Appetizers", 10.95, 6, None, {"late_night": 1.5}),
    ("Ahi Tuna Nachos",                 "Appetizers", 15.95, 4, None, {}),
    ("Classic Caesar Salad",            "Salads", 11.95, 8, None, {}),
    ("Cobb Salad",                      "Salads", 15.95, 7, None, {}),
    ("Quinoa Power Bowl",               "Salads", 14.95, 6, None, {}),
    ("Strawberry Spinach Salad",        "Salads", 13.95, 4, None, {}),
    ("Classic Cheeseburger",            "Burgers & Sandwiches", 15.95, 12, None, {}),
    ("Bacon BBQ Burger",                "Burgers & Sandwiches", 17.45, 9, None, {}),
    ("Mushroom Swiss Burger",           "Burgers & Sandwiches", 16.95, 5, None, {}),
    ("Impossible Burger",               "Burgers & Sandwiches", 17.95, 4, None, {}),
    ("Nashville Hot Chicken Sandwich",  "Burgers & Sandwiches", 15.95, 6, None, {}),
    ("Turkey Club",                     "Burgers & Sandwiches", 14.45, 6, None, {"lunch": 1.4}),
    ("French Dip",                      "Burgers & Sandwiches", 16.95, 5, None, {}),
    ("Grilled Chicken Wrap",            "Burgers & Sandwiches", 13.95, 6, None, {"lunch": 1.4}),
    ("Grilled Atlantic Salmon",         "Entrees", 26.95, 9, None, {}),
    ("12oz Ribeye",                     "Entrees", 38.95, 7, None, {"dinner": 1.4}),
    ("Chicken Parmesan",                "Entrees", 22.95, 9, None, {}),
    ("Shrimp & Grits",                  "Entrees", 24.95, 7, None, {"brunch": 1.5}),
    ("Fish & Chips",                    "Entrees", 19.95, 6, None, {}),
    ("Baby Back Ribs",                  "Entrees", 27.95, 6, None, {}),
    ("Blackened Chicken Alfredo",       "Entrees", 21.95, 8, None, {}),
    ("Short Rib Mac & Cheese",          "Entrees", 23.95, 5, None, {}),
    ("French Fries",                    "Sides", 4.95, 10, None, {}),
    ("Sweet Potato Fries",              "Sides", 5.45, 6, None, {}),
    ("Side Mac & Cheese",               "Sides", 5.95, 7, None, {}),
    ("Seasonal Vegetables",             "Sides", 4.95, 4, None, {}),
    ("Side House Salad",                "Sides", 5.45, 5, None, {}),
    ("Kids Chicken Tenders",            "Kids", 8.95, 10, None, {}),
    ("Kids Cheeseburger",               "Kids", 8.95, 6, None, {}),
    ("Kids Mac & Cheese",               "Kids", 7.95, 7, None, {}),
    ("Molten Chocolate Cake",           "Desserts", 9.95, 9, None, {"lunch": 0.15, "afternoon": 0.4, "dinner": 2.0}),
    ("New York Cheesecake",             "Desserts", 8.95, 8, None, {}),
    ("Pumpkin Cheesecake",              "Desserts", 9.45, 5, None, {}),
    ("Warm Apple Cobbler",              "Desserts", 8.45, 5, None, {}),
    ("Brownie Sundae",                  "Desserts", 8.95, 6, None, {}),
    ("Fountain Soda",                   "Beverages", 3.25, 14, None, {"breakfast": 0.3}),
    ("Fresh Brewed Iced Tea",           "Beverages", 3.25, 11, None, {"breakfast": 0.4}),
    ("Fresh Lemonade",                  "Beverages", 3.95, 6, None, {}),
    ("Coffee",                          "Beverages", 2.95, 6, None, {"breakfast": 6.0, "brunch": 2.5, "dinner": 0.5}),
    ("Bottled Water",                   "Beverages", 2.75, 3, None, {}),
    ("Draft IPA",                       "Beer", 7.50, 9, None, {}),
    ("Draft Light Lager",               "Beer", 6.00, 10, None, {}),
    ("Seasonal Craft Draft",            "Beer", 7.50, 6, None, {}),
    ("Cabernet Sauvignon",              "Wine", 12.00, 8, None, {}),
    ("Chardonnay",                      "Wine", 11.00, 7, None, {}),
    ("Prosecco",                        "Wine", 10.00, 4, None, {"brunch": 2.0}),
    ("Classic Margarita",               "Cocktails", 11.00, 10, None, {}),
    ("Old Fashioned",                   "Cocktails", 13.00, 7, None, {"dinner": 1.3}),
    ("Espresso Martini",                "Cocktails", 13.50, 6, None, {"late_night": 1.6}),
    ("Mimosa",                          "Cocktails", 9.00, 9, {"brunch"}, {}),
    ("Bloody Mary",                     "Cocktails", 11.00, 7, {"brunch"}, {}),
]

# Vendor whose name matches LOWER(vendorName) LIKE '%test%' (keeps the GSI vs FTS benchmark working)
TEST_VENDOR = "Test Kitchen Provisions"
TEST_VENDOR_FRACTION = 0.15

MENU = []
_food_items = [m for m in _MENU if m[1] not in ("Beverages", "Beer", "Wine", "Cocktails")]
_test_names = {m[0] for m in REF.sample(_food_items, round(len(_food_items) * TEST_VENDOR_FRACTION))}
for i, (name, cat, price, pop, avail, aff) in enumerate(_MENU):
    cost_range, vendors, cat_avail, cat_aff = CATEGORY_META[cat]
    affinity = dict(cat_aff)
    affinity.update(aff)
    MENU.append({
        "itemID": f"ITEM_{1001 + i}",
        "itemName": name,
        "category": cat,
        "price": price,
        "unitCost": round(price * REF.uniform(*cost_range), 2),
        "vendorName": TEST_VENDOR if name in _test_names else REF.choice(vendors),
        "popularity": pop,
        "avail": avail or cat_avail,
        "affinity": affinity,
    })
MENU_BY_CAT = defaultdict(list)
for m in MENU:
    MENU_BY_CAT[m["category"]].append(m)

MODIFIERS = {
    "Burgers & Sandwiches": [("Add Bacon", 2.00), ("Add Avocado", 1.50), ("Extra Cheese", 1.00), ("No Onions", 0.0),
                             ("Sub Sweet Potato Fries", 1.50), ("Gluten-Free Bun", 2.00), ("Make It Spicy", 0.0)],
    "Entrees":              [("Medium Rare", 0.0), ("Medium", 0.0), ("Sauce on Side", 0.0), ("Add Shrimp", 6.00),
                             ("Sub Side Caesar", 2.00), ("Gluten-Free Pasta", 2.50)],
    "Salads":               [("Add Grilled Chicken", 5.00), ("Add Salmon", 8.00), ("Add Shrimp", 6.00),
                             ("Dressing on Side", 0.0), ("No Croutons", 0.0)],
    "Appetizers":           [("Extra Sauce", 0.75), ("Make It Spicy", 0.0), ("Ranch on Side", 0.50)],
    "Breakfast":            [("Add Bacon", 2.50), ("Sub Egg Whites", 1.00), ("Extra Syrup", 0.0), ("Add Fruit", 2.00)],
    "Beverages":            [("Oat Milk", 0.75), ("Extra Shot", 1.00), ("No Ice", 0.0)],
    "Cocktails":            [("Top Shelf", 3.00), ("Make It a Double", 5.00), ("Salt Rim", 0.0)],
}
MODIFIER_RATE = 0.35

# =========================================================
# PLANTED DEMO STORIES (all relative to --end_date)
# =========================================================

STORIES = {
    # New menu launch that is taking off, strongest at hot locations
    "new_item":     {"item": "Nashville Hot Chicken Sandwich", "launch_days_ago": 28, "ramp_days": 21,
                     "peak_mult": 4.0, "hot_loc_boost": 1.3},
    # Seasonal limited-time offer
    "lto":          {"item": "Pumpkin Cheesecake", "launch_days_ago": 22, "mult": 2.5},
    # Item losing popularity across the window
    "declining":    {"item": "Quinoa Power Bowl", "start_mult": 1.8, "end_mult": 0.35},
    # 86'd: item unavailable at one location for a few days
    "outage":       {"item": "Grilled Atlantic Salmon", "locationID": "LOC_002", "from_days_ago": 10, "to_days_ago": 7},
    # Loss-prevention outlier: one location, one employee with abnormal voids
    "void_hotspot": {"locationID": "LOC_001", "employee_rate": 0.12, "location_rate": 0.03},
    # Delivery share climbing at one location
    "delivery_surge": {"locationID": "LOC_009", "end_mult": 3.2},
}
BASE_VOID_RATE = 0.012
BASE_COMP_RATE = 0.010
ORDER_VOID_RATE = 0.004

_ITEM_BY_NAME = {m["itemName"]: m for m in MENU}
for _key in ("new_item", "lto", "declining", "outage"):
    assert STORIES[_key]["item"] in _ITEM_BY_NAME, f"story item missing from menu: {STORIES[_key]['item']}"

VOID_EMPLOYEE_ID = EMPLOYEES[STORIES["void_hotspot"]["locationID"]]["Server"][0]["employeeID"]


def story_mult(item, loc, days_ago, progress):
    """Multiplier on an item's popularity for a given location/day. 0 = unavailable."""
    name = item["itemName"]
    s = STORIES["new_item"]
    if name == s["item"]:
        since = s["launch_days_ago"] - days_ago
        if since < 0:
            return 0.0
        m = 0.6 + (s["peak_mult"] - 0.6) * min(1.0, since / s["ramp_days"])
        return m * (s["hot_loc_boost"] if loc["isHot"] else 1.0)
    s = STORIES["lto"]
    if name == s["item"]:
        return s["mult"] if days_ago <= s["launch_days_ago"] else 0.0
    s = STORIES["declining"]
    if name == s["item"]:
        return s["start_mult"] + (s["end_mult"] - s["start_mult"]) * progress
    s = STORIES["outage"]
    if name == s["item"] and loc["locationID"] == s["locationID"] and s["to_days_ago"] <= days_ago <= s["from_days_ago"]:
        return 0.0
    return 1.0

# =========================================================
# TIME MODEL
# =========================================================

DOW_WEIGHT = [0.75, 0.80, 0.90, 1.00, 1.35, 1.40, 1.15]  # Mon..Sun
DAYS_AGO = list(range(DAYS, 0, -1))                       # oldest -> newest
_day_w = []
for idx, d_ago in enumerate(DAYS_AGO):
    d = END_DATE - timedelta(days=d_ago)
    growth = 1 + 0.12 * idx / (DAYS - 1)                  # gentle business growth over the window
    _day_w.append(DOW_WEIGHT[d.weekday()] * growth)
DAY_CUM = [sum(_day_w[: i + 1]) for i in range(len(_day_w))]

HOURS = list(range(7, 24))
_WD = [2, 3, 3, 2, 6, 10, 8, 3, 2, 3, 7, 10, 10, 7, 4, 2, 1]
_WE = [1, 3, 6, 8, 9, 10, 8, 4, 3, 4, 8, 11, 11, 8, 5, 3, 2]
HOUR_CUM_WD = [sum(_WD[: i + 1]) for i in range(len(_WD))]
HOUR_CUM_WE = [sum(_WE[: i + 1]) for i in range(len(_WE))]


def daypart(hour, weekend):
    if weekend and 8 <= hour <= 13:
        return "brunch"
    if hour <= 10:
        return "breakfast"
    if hour <= 13:
        return "lunch"
    if hour <= 16:
        return "afternoon"
    if hour <= 20:
        return "dinner"
    return "late_night"


CHANNEL_MIX = {  # dine_in, takeout, delivery
    "breakfast":  [70, 25, 5],
    "brunch":     [85, 10, 5],
    "lunch":      [55, 30, 15],
    "afternoon":  [45, 30, 25],
    "dinner":     [65, 15, 20],
    "late_night": [50, 10, 40],
}
CHANNEL_NAMES = ["dine_in", "takeout", "delivery"]
DELIVERY_PARTNERS = (["DoorDash", "Uber Eats", "Grubhub", "In-House"], [45, 35, 10, 10])

MAIN_CAT_W = {
    "breakfast":  {"Breakfast": 1.0},
    "brunch":     {"Breakfast": 0.6, "Burgers & Sandwiches": 0.2, "Salads": 0.1, "Entrees": 0.1},
    "lunch":      {"Burgers & Sandwiches": 0.45, "Salads": 0.30, "Entrees": 0.25},
    "afternoon":  {"Burgers & Sandwiches": 0.45, "Salads": 0.25, "Entrees": 0.30},
    "dinner":     {"Entrees": 0.55, "Burgers & Sandwiches": 0.30, "Salads": 0.15},
    "late_night": {"Burgers & Sandwiches": 0.75, "Appetizers": 0.15, "Salads": 0.10},
}
ALCOHOL_SHARE = {"breakfast": 0.0, "brunch": 0.30, "lunch": 0.12, "afternoon": 0.20, "dinner": 0.38, "late_night": 0.65}
DESSERT_P = {"breakfast": 0.0, "brunch": 0.05, "lunch": 0.06, "afternoon": 0.10, "dinner": 0.30, "late_night": 0.15}
DURATION_MIN = {"breakfast": (30, 50), "brunch": (50, 85), "lunch": (35, 60), "afternoon": (30, 60),
                "dinner": (55, 105), "late_night": (40, 90)}

PAY_TYPES = (["visa", "mastercard", "amex", "discover", "apple_pay", "google_pay", "cash", "gift_card"],
             [38, 22, 12, 4, 10, 3, 9, 2])
CARD_TYPES = {"visa", "mastercard", "amex", "discover", "apple_pay", "google_pay"}

VOID_REASONS = (["Customer Changed Mind", "Wrong Item Entered", "Kitchen Error", "Item 86'd", "Manager Override"],
                [35, 30, 15, 10, 10])
VOID_REASONS_HOTSPOT = (["Manager Override", "No Reason Given", "Customer Changed Mind", "Wrong Item Entered"],
                        [40, 30, 20, 10])
COMP_REASONS = ["Guest Complaint", "Manager Comp", "Birthday", "Long Ticket Time", "Employee Meal"]
RETURN_REASONS = ["Wrong Item Delivered", "Missing Item", "Food Quality", "Order Arrived Cold", "Duplicate Charge"]


def money(x):
    return round(x + 1e-9, 2)

# =========================================================
# GENERATOR - one order bundle per order number
# =========================================================

def pick_item(rng, category, loc, dp, days_ago, progress):
    cands, weights = [], []
    for m in MENU_BY_CAT[category]:
        if dp not in m["avail"]:
            continue
        w = m["popularity"] * m["affinity"].get(dp, 1.0) * story_mult(m, loc, days_ago, progress)
        if w > 0:
            cands.append(m)
            weights.append(w)
    return rng.choices(cands, weights=weights)[0] if cands else None


def pick_employee(rng, loc, channel):
    staff = EMPLOYEES[loc["locationID"]]
    if channel == "dine_in":
        pool = staff["Bartender"] if rng.random() < 0.12 else staff["Server"]
    else:
        pool = staff["Cashier"]
    return rng.choices(pool, weights=[e["shiftWeight"] for e in pool])[0]


def make_bundle(g):
    rng = random.Random(f"{SEED}:{g}")

    # ---- where / when / how ----
    loc = rng.choices(LOCATIONS, weights=[l["weight"] for l in LOCATIONS])[0]
    days_ago = rng.choices(DAYS_AGO, cum_weights=DAY_CUM)[0]
    progress = 1 - (days_ago - 1) / (DAYS - 1)  # 0 = oldest day, 1 = newest
    bdate = END_DATE - timedelta(days=days_ago)
    weekend = bdate.weekday() >= 5
    hour = rng.choices(HOURS, cum_weights=HOUR_CUM_WE if weekend else HOUR_CUM_WD)[0]
    dp = daypart(hour, weekend)
    opened = datetime(bdate.year, bdate.month, bdate.day, hour, rng.randint(0, 59), rng.randint(0, 59), tzinfo=loc["tz"])
    open_ms = int(opened.timestamp() * 1000)

    mix = list(CHANNEL_MIX[dp])
    surge = STORIES["delivery_surge"]
    if loc["locationID"] == surge["locationID"]:
        mix[2] *= 1 + (surge["end_mult"] - 1) * progress
    channel = rng.choices(CHANNEL_NAMES, weights=mix)[0]
    partner = rng.choices(*DELIVERY_PARTNERS)[0] if channel == "delivery" else None

    emp = pick_employee(rng, loc, channel)
    if channel == "dine_in":
        guests = rng.choices([1, 2, 3, 4, 5, 6], weights=[12, 40, 18, 18, 7, 5])[0]
    else:
        guests = rng.choices([1, 2, 3, 4], weights=[42, 33, 15, 10])[0]

    # ---- build the ticket like a real table would order ----
    lines = {}  # itemID -> line
    seq = [0]

    def add(item, course_offset_min):
        if item is None:
            return
        if item["itemID"] in lines:
            lines[item["itemID"]]["qty"] += 1
            return
        mods = []
        opts = MODIFIERS.get(item["category"])
        if opts and rng.random() < MODIFIER_RATE:
            mods = rng.sample(opts, k=min(len(opts), rng.choice([1, 1, 2])))
        seq[0] += 1
        lines[item["itemID"]] = {"item": item, "qty": 1, "mods": mods, "seq": seq[0], "offset": course_offset_min}

    pick = lambda cat: pick_item(rng, cat, loc, dp, days_ago, progress)

    kids = 0
    if channel == "dine_in" and guests >= 3 and dp not in ("late_night",) and rng.random() < 0.30:
        kids = rng.randint(1, min(2, guests - 2))

    # appetizers (table level)
    p_app = 0.55 if channel == "dine_in" and guests > 1 else (0.2 if channel == "dine_in" else 0.3)
    if dp == "late_night":
        p_app *= 1.3
    if dp != "breakfast" and rng.random() < p_app:
        add(pick("Appetizers"), rng.uniform(1, 6))
        if guests >= 4 and rng.random() < 0.35:
            add(pick("Appetizers"), rng.uniform(1, 6))

    # mains + beverages + sides per guest
    cat_w = MAIN_CAT_W[dp]
    for gi in range(guests):
        if gi < kids:
            add(pick("Kids"), rng.uniform(8, 15))
        else:
            cat = rng.choices(list(cat_w), weights=list(cat_w.values()))[0]
            add(pick(cat) or pick("Burgers & Sandwiches"), rng.uniform(8, 15))
            if dp != "breakfast" and rng.random() < 0.15:
                add(pick("Sides"), rng.uniform(8, 15))
        p_bev = {"dine_in": 0.85, "takeout": 0.35, "delivery": 0.30}[channel]
        if rng.random() < p_bev:
            if channel == "dine_in" and gi >= kids and rng.random() < ALCOHOL_SHARE[dp]:
                if dp == "brunch":
                    alc = rng.choices(["Cocktails", "Beer", "Wine"], weights=[60, 25, 15])[0]
                else:
                    alc = rng.choices(["Beer", "Cocktails", "Wine"], weights=[40, 35, 25])[0]
                add(pick(alc), rng.uniform(0, 4))
            else:
                add(pick("Beverages"), rng.uniform(0, 4))

    # dessert (table level)
    p_des = DESSERT_P[dp] * (1.3 if channel == "dine_in" else 0.6)
    if rng.random() < p_des:
        add(pick("Desserts"), None)
        if guests >= 4 and rng.random() < 0.3:
            add(pick("Desserts"), None)

    if not lines:
        add(_ITEM_BY_NAME["Fountain Soda"], 1)

    # ---- timing ----
    if channel == "dine_in":
        duration = rng.randint(*DURATION_MIN[dp])
    elif channel == "takeout":
        duration = rng.randint(8, 25)
    else:
        duration = rng.randint(15, 35)
    close_ms = open_ms + duration * 60_000

    # ---- voids / comps ----
    vh = STORIES["void_hotspot"]
    if loc["locationID"] == vh["locationID"]:
        void_rate = vh["employee_rate"] if emp["employeeID"] == VOID_EMPLOYEE_ID else vh["location_rate"]
        reasons = VOID_REASONS_HOTSPOT if emp["employeeID"] == VOID_EMPLOYEE_ID else VOID_REASONS
    else:
        void_rate, reasons = BASE_VOID_RATE, VOID_REASONS
    order_voided = rng.random() < ORDER_VOID_RATE

    g_id = f"ORDER_{g}"
    common = {
        "orderID": g_id,
        "locationID": loc["locationID"],
        "locationName": loc["locationName"],
        "city": loc["city"],
        "state": loc["state"],
        "commChannel": CHANNELS[channel],
        "channelName": channel,
        "employeeID": emp["employeeID"],
        "empName": emp["empName"],
        "businessDate": bdate.isoformat(),
        "dayOfWeek": bdate.strftime("%A"),
        "hourOfDay": hour,
        "daypart": dp,
    }

    carts = []
    subtotal = discount = food_cost = 0.0
    item_count = 0
    has_void = has_comp = False
    for ln in sorted(lines.values(), key=lambda x: x["seq"]):
        item, qty = ln["item"], ln["qty"]
        mod_total = sum(p for _, p in ln["mods"])
        gross = money(qty * (item["price"] + mod_total))
        voided = order_voided or rng.random() < void_rate
        comped = (not voided) and rng.random() < BASE_COMP_RATE
        net = 0.0 if voided or comped else gross
        offset = ln["offset"] if ln["offset"] is not None else duration * rng.uniform(0.65, 0.8)
        line_cost = money(qty * item["unitCost"]) if not voided else 0.0
        has_void |= voided
        has_comp |= comped
        if not voided:
            subtotal += gross
            food_cost += line_cost
            item_count += qty
        if comped:
            discount += gross
        cart = {
            "_id": f"cart:{g_id}:{ln['seq']}",
            "docType": "cart",
            "cartID": f"{g_id}:{ln['seq']}",
            "lineNumber": ln["seq"],
            **common,
            "itemID": item["itemID"],
            "itemName": item["itemName"],
            "category": item["category"],
            "vendorName": item["vendorName"],
            "qtySold": qty,
            "unitPrice": item["price"],
            "modifiers": [{"name": n, "price": p} for n, p in ln["mods"]],
            "modifierTotal": money(mod_total),
            "lineTotal": gross,
            "netSales": net,
            "unitCost": item["unitCost"],
            "lineCost": line_cost,
            "isVoided": voided,
            "voidReason": rng.choices(*reasons)[0] if voided and not order_voided else ("Order Voided" if voided else None),
            "isComped": comped,
            "compReason": rng.choice(COMP_REASONS) if comped else None,
            "dateCreated": open_ms + int(offset * 60_000),
            "isPurged": 0,
        }
        carts.append(cart)

    subtotal, discount, food_cost = money(subtotal), money(discount), money(food_cost)
    taxable = money(subtotal - discount)
    tax = money(taxable * loc["taxRate"])

    # ---- payment ----
    status = "voided" if order_voided else "closed"
    txns = []
    tip_total = 0.0
    if order_voided:
        pay_type = rng.choices(*PAY_TYPES)[0]
        txns.append({"transactionID": f"TXN_{g}_1", "transactionType": "void", "paymentType": pay_type,
                     "amount": money(sum(c["lineTotal"] for c in carts)), "tipAmount": 0.0,
                     "isForSettle": 0, "dateCreated": close_ms})
    else:
        if channel == "delivery" and partner != "In-House":
            tenders = ["third_party"]
        elif channel == "dine_in" and guests >= 2 and rng.random() < 0.12:
            tenders = [rng.choices(*PAY_TYPES)[0] for _ in range(min(guests, rng.choice([2, 2, 3])))]
        else:
            tenders = [rng.choices(*PAY_TYPES)[0]]
        share = money((taxable + tax) / len(tenders))
        for k, pt in enumerate(tenders):
            amount = share if k < len(tenders) - 1 else money(taxable + tax - share * (len(tenders) - 1))
            if pt in ("third_party", "cash"):
                tip = 0.0
            elif channel == "dine_in":
                tip = money(amount * rng.uniform(0.15, 0.25))
            elif channel == "takeout":
                tip = money(amount * rng.uniform(0.05, 0.12)) if rng.random() < 0.4 else 0.0
            else:
                tip = money(amount * rng.uniform(0.10, 0.15))
            tip_total += tip
            txns.append({
                "transactionID": f"TXN_{g}_{k + 1}",
                "transactionType": "sale",
                "paymentType": pt,
                "tenderProvider": partner if pt == "third_party" else None,
                "amount": amount,
                "tipAmount": tip,
                # 1 = card txn already in a settlement batch; 0 = cash/third-party or not yet settled (last business day)
                "isForSettle": 1 if pt in CARD_TYPES and days_ago > 1 else 0,
                "dateCreated": close_ms,
            })
    tip_total = money(tip_total)

    refund = 0.0
    p_return = 0.015 if channel == "delivery" else 0.004
    sold = [c for c in carts if c["netSales"] > 0]
    if not order_voided and sold and rng.random() < p_return:
        refund = rng.choice(sold)["netSales"]
        txns.append({"transactionID": f"TXN_{g}_{len(txns) + 1}", "transactionType": "return",
                     "paymentType": txns[0]["paymentType"], "tenderProvider": txns[0].get("tenderProvider"),
                     "amount": refund, "tipAmount": 0.0, "isForSettle": 0,
                     "returnReason": rng.choice(RETURN_REASONS),
                     "dateCreated": close_ms + rng.randint(1, 48) * 3_600_000})

    total = 0.0 if order_voided else money(taxable + tax + tip_total)
    ref_number = f"R{loc['num']:03d}-{bdate:%Y%m%d}-{g % 100000:05d}"
    primary_pay = txns[0]["paymentType"]

    order = {
        "_id": f"order:{g_id}",
        "docType": "order",
        **common,
        "refNumber": ref_number,
        "role": emp["role"],
        "deliveryPartner": partner,
        "tableNumber": rng.randint(1, 40) if channel == "dine_in" else None,
        "guestCount": guests,
        "lineCount": len(carts),
        "itemCount": item_count,
        "subtotal": subtotal,
        "discountTotal": discount,
        "tax": tax,
        "tip": tip_total,
        "total": total,
        "status": status,
        "closeType": "void" if order_voided else "paid",
        "dateCreated": open_ms,
        "dateClosed": close_ms,
        "durationMinutes": duration,
        "isPurged": 0,
    }

    payment = {
        "_id": f"payment:{g_id}",
        "docType": "payment",
        "paymentID": f"PAY_{g}",
        **common,
        "refNumber": ref_number,
        "paymentType": primary_pay,
        "tenderCount": sum(1 for t in txns if t["transactionType"] == "sale"),
        "totalPaid": 0.0 if order_voided else money(taxable + tax + tip_total),
        "transactionList": txns,
        "dateCreated": close_ms,
        "isPurged": 0,
    }

    summary = {
        "_id": f"summary:{g_id}",
        "docType": "orderSummary",
        **common,
        "refNumber": ref_number,
        "deliveryPartner": partner,
        "paymentType": primary_pay,
        "guestCount": guests,
        "cartList": [{"itemID": c["itemID"], "itemName": c["itemName"], "category": c["category"],
                      "qtySold": c["qtySold"], "netSales": c["netSales"], "isVoided": c["isVoided"]} for c in carts],
        "lineCount": len(carts),
        "itemCount": item_count,
        "subtotal": subtotal,
        "discountTotal": discount,
        "tax": tax,
        "tip": tip_total,
        "total": total,
        "refundTotal": refund,
        "foodCost": food_cost,
        "grossMargin": money(taxable - food_cost) if not order_voided else 0.0,
        "hasVoid": has_void,
        "hasComp": has_comp,
        "hasReturn": refund > 0,
        "status": status,
        "dateCreated": open_ms,
        "dateClosed": close_ms,
        "durationMinutes": duration,
        "isPurged": 0,
    }

    return {"order": [order], "cart": carts, "payment": [payment], "orderSummary": [summary]}

# =========================================================
# COUCHBASE LOADER
# =========================================================

class Loader:
    def __init__(self):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions

        conn = os.environ.get("CB_CONNECTION_STRING")
        user = os.environ.get("CB_USERNAME")
        pwd = os.environ.get("CB_PASSWORD")
        if not (conn and user and pwd):
            raise SystemExit("Set CB_CONNECTION_STRING, CB_USERNAME and CB_PASSWORD in .env (or use --dry_run).")
        if os.environ.get("CB_CA_CERT_PATH"):
            os.environ["SSL_CERT_FILE"] = os.environ["CB_CA_CERT_PATH"]

        opts = ClusterOptions(
            PasswordAuthenticator(user, pwd),
            timeout_options=ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=10),
                query_timeout=timedelta(seconds=30),
                connect_timeout=timedelta(seconds=30),
            ),
        )
        self.cluster = Cluster(conn, opts)
        self.cluster.wait_until_ready(timedelta(seconds=60))
        self.bucket = self.cluster.bucket(args.bucket)
        if not args.no_create:
            self._ensure_collections()
        scope = self.bucket.scope(args.scope)
        self.cols = {name: scope.collection(name) for name in COLLECTIONS}

    def _ensure_collections(self):
        from couchbase.exceptions import CollectionAlreadyExistsException, ScopeAlreadyExistsException
        mgr = self.bucket.collections()
        try:
            mgr.create_scope(args.scope)
            log.info(f"created scope {args.scope}")
        except ScopeAlreadyExistsException:
            pass
        for name in COLLECTIONS:
            try:
                try:
                    mgr.create_collection(args.scope, name)          # SDK 4.1.9+
                except TypeError:
                    from couchbase.management.collections import CollectionSpec
                    mgr.create_collection(CollectionSpec(name, scope_name=args.scope))  # older 4.x
                log.info(f"created collection {args.scope}.{name}")
            except CollectionAlreadyExistsException:
                pass
        # wait until the manifest shows all collections
        for _ in range(30):
            scopes = {s.name: {c.name for c in s.collections} for s in mgr.get_all_scopes()}
            if set(COLLECTIONS) <= scopes.get(args.scope, set()):
                return
            time.sleep(1)
        log.warning("collections not visible in manifest yet; upserts will retry")

    def upsert_many(self, name, docs, retries=4):
        """Bulk upsert; individually retry any keys that failed. Returns docs written."""
        col = self.cols[name]
        res = col.upsert_multi(docs)
        if res.all_ok:
            return len(docs)
        failed = list(res.exceptions.keys())
        written = len(docs) - len(failed)
        for key in failed:
            for i in range(retries):
                try:
                    col.upsert(key, docs[key])
                    written += 1
                    break
                except Exception as e:  # noqa: BLE001
                    if i == retries - 1:
                        log.error(f"upsert failed {name}/{key}: {e}")
                    time.sleep(0.05 * (2 ** i))
        return written

# =========================================================
# WORKER / MAIN
# =========================================================

def worker(loader, start_g, end_g):
    bufs = {name: {} for name in COLLECTIONS}  # keyed by collection name
    written = Counter()

    def flush(name):
        if bufs[name]:
            written[name] += loader.upsert_many(name, bufs[name])
            bufs[name] = {}

    for g in range(start_g, end_g):
        for doc_type, docs in make_bundle(g).items():
            name = DOC_TO_COLLECTION[doc_type]
            for d in docs:
                bufs[name][d["_id"]] = d
            if len(bufs[name]) >= args.flush_size:
                flush(name)
    for name in COLLECTIONS:
        flush(name)
    return written


def dry_run():
    n = args.num_orders
    stats = defaultdict(Counter)
    samples = []
    t0 = time.time()
    for g in range(args.start_offset, args.start_offset + n):
        b = make_bundle(g)
        if len(samples) < 3:
            samples.append(b)
        o, s = b["order"][0], b["orderSummary"][0]
        for name, docs in b.items():
            stats["docs"][DOC_TO_COLLECTION[name]] += len(docs)
        stats["location"][o["locationName"]] += 1
        stats["channel"][o["channelName"]] += 1
        stats["daypart"][o["daypart"]] += 1
        for c in b["cart"]:
            if not c["isVoided"]:
                stats["items"][c["itemName"]] += c["qtySold"]
            if c["isVoided"]:
                stats["voids_by_emp"][f'{c["locationID"]} {c["empName"]} ({c["employeeID"]})'] += 1
        stats["sales"]["net"] += s["subtotal"] - s["discountTotal"]
    elapsed = time.time() - t0

    os.makedirs(os.path.dirname(args.sample_out) or ".", exist_ok=True)
    with open(args.sample_out, "w") as f:
        json.dump(samples, f, indent=2)

    def top(counter, k=8):
        tot = sum(counter.values())
        return "\n".join(f"    {v / tot:6.1%}  {key}" for key, v in counter.most_common(k))

    print(f"\nDRY RUN: {n} orders in {elapsed:.1f}s ({n / elapsed:,.0f} orders/s), window {END_DATE - timedelta(days=DAYS)} .. {END_DATE - timedelta(days=1)}")
    print(f"docs per collection: {dict(stats['docs'])}  (carts/order = {stats['docs']['cart'] / n:.2f})")
    print(f"avg check: ${stats['sales']['net'] / n:,.2f}")
    print("location share (top):\n" + top(stats["location"], 5))
    print("channel mix:\n" + top(stats["channel"]))
    print("daypart mix:\n" + top(stats["daypart"]))
    print("top items by qty:\n" + top(stats["items"], 10))
    print("most voids by employee:\n" + "\n".join(f"    {v:5d}  {k}" for k, v in stats["voids_by_emp"].most_common(3)))
    print(f"\nsample bundles written to {args.sample_out}")


def main():
    if args.dry_run:
        dry_run()
        return

    log.info(f"POS LOAD START bucket={args.bucket} scope={args.scope} offset={args.start_offset} "
             f"orders={args.num_orders} workers={args.workers} window={DAYS}d ending {END_DATE}")
    loader = Loader()
    t0 = time.time()

    chunk = args.num_orders // args.workers
    totals = Counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = []
        for t in range(args.workers):
            s = args.start_offset + t * chunk
            e = s + chunk if t < args.workers - 1 else args.start_offset + args.num_orders
            futures.append(ex.submit(worker, loader, s, e))
        for f in as_completed(futures):
            totals.update(f.result())

    elapsed = time.time() - t0
    total = sum(totals.values())
    log.info(f"DONE {dict(totals)} TOTAL={total} DURATION={elapsed:.1f}s OPS/SEC={total / elapsed:,.0f}")


if __name__ == "__main__":
    main()
