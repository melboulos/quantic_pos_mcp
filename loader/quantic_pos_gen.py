#!/usr/bin/env python3
"""
quantic_pos_gen.py (v2) - synthetic data modeled on Quantic's production schema

Document shapes follow the sample documents Quantic provided (order, check, cart,
payment, orderSummary, transactionDetail, batchClose, timeManagement,
giftTransactionDetail, rewardHistory, signature, stockHistory, catalogLog, auditLog).
`location`, `item` and `employee` docs are ASSUMED shapes (no samples yet) - see ASSUMPTIONS.md.

Every docType goes to a collection of the same name, except `order` -> `orders`
(ORDER is reserved in SQL++). All IDs are uppercase UUIDs, keys are docType:UUID,
times are epoch milliseconds, money is stored as raw doubles like the source system.

PHASES
  reference  location / item / employee docs + onboarding audit logs   (per merchant)
  daily      batchClose, timeManagement, stockHistory, catalogLog, auditLog (per location-day)
  orders     order, check, cart, payment, orderSummary, transactionDetail,
             giftTransactionDetail, rewardHistory, signature             (per order)

HOW TO RUN
  # shape check - samples across the whole range, prints projected doc counts
  python loader/quantic_pos_gen.py --dry_run --merchants 450 --days 1095 --end_date 2026-10-07

  # demo-size load (~60 merchants, 1 year)
  python loader/quantic_pos_gen.py --merchants 60 --days 365 --end_date 2026-10-07

  # ~1B docs (run on a VM near the cluster)
  python loader/quantic_pos_gen.py --merchants 450 --days 1095 --end_date 2026-10-07 \\
      --processes 16 --workers 8

  --merchants, --days, --end_date and --seed define the dataset: keep them identical
  across runs. Resume the orders phase with --phases orders --start_offset N.
"""

import argparse
import base64
import bisect
import json
import logging
import math
import multiprocessing
import os
import random
import threading
import time
from array import array
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

# =========================================================
# ARGS
# =========================================================

parser = argparse.ArgumentParser(description="Quantic POS synthetic data generator (v2, Quantic schema)")
parser.add_argument("--phases", default="reference,daily,orders")
parser.add_argument("--merchants", type=int, default=60, help="number of merchants (tenants); merchant 0 is the demo chain")
parser.add_argument("--days", type=int, default=365)
parser.add_argument("--end_date", default=None, help="YYYY-MM-DD; data runs through the day before")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--num_orders", type=int, default=0, help="0 = natural volume implied by the merchant base")
parser.add_argument("--start_offset", type=int, default=0, help="orders phase: first order number to generate")
parser.add_argument("--workers", type=int, default=8)
parser.add_argument("--processes", type=int, default=1)
parser.add_argument("--flush_size", type=int, default=800)
parser.add_argument("--bucket", default=os.environ.get("CB_BUCKET", "quantic"))
parser.add_argument("--scope", default=os.environ.get("CB_SCOPE", "pos"))
parser.add_argument("--no_create", action="store_true")
parser.add_argument("--dry_run", action="store_true")
parser.add_argument("--dry_run_sample", type=int, default=8000)
parser.add_argument("--sample_out", default="output/sample_docs.json")
parser.add_argument("--progress_every", type=int, default=250000)
args = parser.parse_args()

SEED = args.seed
DAYS = max(30, args.days)
END_DATE = date.fromisoformat(args.end_date) if args.end_date else date.today()
START_DATE = END_DATE - timedelta(days=DAYS)          # first day of data (index 0)
LAST_DATE = END_DATE - timedelta(days=1)              # last day of data (index DAYS-1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("quantic")

DOC_TYPES = ["order", "check", "cart", "payment", "orderSummary", "transactionDetail", "batchClose",
             "timeManagement", "giftTransactionDetail", "rewardHistory", "signature", "stockHistory",
             "catalogLog", "auditLog", "location", "item", "employee"]
DOC_TO_COLLECTION = {d: d for d in DOC_TYPES}
DOC_TO_COLLECTION["order"] = "orders"
COLLECTIONS = list(DOC_TO_COLLECTION.values())

# =========================================================
# SCHEMA-STYLE HELPERS
# =========================================================

def uid(rng):
    """Uppercase v4-style UUID, like every Quantic ID."""
    b = rng.getrandbits(128)
    h = f"{b:032X}"
    return f"{h[0:8]}-{h[8:12]}-4{h[13:16]}-{'89AB'[b & 3]}{h[17:20]}-{h[20:32]}"


def day_of(idx):
    return START_DATE + timedelta(days=idx)


_MIDNIGHT = {}


def local_midnight_ms(tz, idx):
    key = (tz.key, idx)
    v = _MIDNIGHT.get(key)
    if v is None:
        d = day_of(idx)
        v = int(datetime(d.year, d.month, d.day, tzinfo=tz).timestamp() * 1000)
        _MIDNIGHT[key] = v
    return v


def stamp(ms, app_type, rng):
    """iOS / web clients write whole seconds; Android writes milliseconds (as in the samples)."""
    ms = int(ms)
    if app_type == "posLiteAndroid":
        return ms - ms % 1000 + rng.randint(0, 999)
    return ms - ms % 1000


def fake_digits(rng, n):
    return "".join(str(rng.randint(0, 9)) for _ in range(n))


def fake_token(rng, n=16):
    return "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(n))

# =========================================================
# GEOGRAPHY  (weighted to Quantic's mid-Atlantic home market; tax = approx. restaurant rate %)
# =========================================================

CITIES = [
    # city, state, tz, taxPercent, weight, cold_winter
    ("Philadelphia", "PA", "America/New_York", 8, 9, True), ("Pittsburgh", "PA", "America/New_York", 7, 5, True),
    ("King of Prussia", "PA", "America/New_York", 6, 3, True), ("Allentown", "PA", "America/New_York", 6, 3, True),
    ("Lancaster", "PA", "America/New_York", 6, 3, True), ("Harrisburg", "PA", "America/New_York", 6, 2, True),
    ("West Chester", "PA", "America/New_York", 6, 2, True), ("Reading", "PA", "America/New_York", 6, 2, True),
    ("Scranton", "PA", "America/New_York", 6, 2, True), ("State College", "PA", "America/New_York", 6, 2, True),
    ("Bethlehem", "PA", "America/New_York", 6, 2, True), ("Erie", "PA", "America/New_York", 6, 1, True),
    ("Cherry Hill", "NJ", "America/New_York", 6.625, 3, True), ("Hoboken", "NJ", "America/New_York", 6.625, 2, True),
    ("Jersey City", "NJ", "America/New_York", 6.625, 2, True), ("Princeton", "NJ", "America/New_York", 6.625, 1, True),
    ("Atlantic City", "NJ", "America/New_York", 6.625, 1, True), ("New York", "NY", "America/New_York", 8.875, 4, True),
    ("Brooklyn", "NY", "America/New_York", 8.875, 3, True), ("Buffalo", "NY", "America/New_York", 8.75, 1, True),
    ("Albany", "NY", "America/New_York", 8, 1, True), ("Wilmington", "DE", "America/New_York", 0, 2, True),
    ("Rehoboth Beach", "DE", "America/New_York", 0, 1, True), ("Baltimore", "MD", "America/New_York", 6, 2, True),
    ("Annapolis", "MD", "America/New_York", 6, 1, True), ("Richmond", "VA", "America/New_York", 6, 1, False),
    ("Charlotte", "NC", "America/New_York", 7.25, 2, False), ("Raleigh", "NC", "America/New_York", 7.25, 1, False),
    ("Atlanta", "GA", "America/New_York", 8.9, 2, False), ("Orlando", "FL", "America/New_York", 6.5, 2, False),
    ("Tampa", "FL", "America/New_York", 7.5, 2, False), ("Miami", "FL", "America/New_York", 7, 2, False),
    ("Nashville", "TN", "America/Chicago", 9.25, 2, False), ("Dallas", "TX", "America/Chicago", 8.25, 2, False),
    ("Houston", "TX", "America/Chicago", 8.25, 2, False), ("Austin", "TX", "America/Chicago", 8.25, 1, False),
    ("Chicago", "IL", "America/Chicago", 10.25, 2, True), ("Columbus", "OH", "America/New_York", 7.5, 1, True),
    ("Cleveland", "OH", "America/New_York", 8, 1, True), ("Boston", "MA", "America/New_York", 7, 1, True),
    ("Hartford", "CT", "America/New_York", 7.35, 1, True), ("Denver", "CO", "America/Denver", 8.81, 1, True),
    ("Phoenix", "AZ", "America/Phoenix", 8.6, 1, False), ("Los Angeles", "CA", "America/Los_Angeles", 9.5, 1, False),
    ("San Diego", "CA", "America/Los_Angeles", 7.75, 1, False),
]
CITY_W = [c[4] for c in CITIES]
NEIGHBORHOODS = ["Downtown", "Midtown", "Old City", "Riverside", "University", "Main St", "Station", "Market Square",
                 "North", "South", "East", "West", "Airport", "Harbor", "Village", "Crossing", "Commons", "Plaza"]

FIRST = ["James", "Maria", "Tyler", "Ashley", "Marcus", "Jasmine", "Kevin", "Brittany", "Andre", "Lauren", "Carlos",
         "Megan", "Derek", "Alyssa", "Jordan", "Kayla", "Brandon", "Destiny", "Ryan", "Taylor", "Luis", "Hannah",
         "Chris", "Morgan", "Darius", "Emily", "Nick", "Sofia", "Trevor", "Gabriela", "Justin", "Aaliyah", "Cody",
         "Olivia", "Malik", "Rachel", "Sean", "Vanessa", "Eric", "Chloe", "Priya", "Wei", "Mohammed", "Ana", "Tom",
         "Kim", "Joe", "Rosa", "Danny", "Mike", "Jen", "Steph", "Vinny", "Tony", "Gina", "Paul", "Hector", "Linh"]
LAST = ["Johnson", "Garcia", "Smith", "Williams", "Brown", "Martinez", "Davis", "Rodriguez", "Wilson", "Moore",
        "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Lopez", "Lee", "Walker",
        "Hall", "Allen", "Young", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Green", "Adams", "Baker",
        "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Russo", "DiMarco", "Kowalski",
        "O'Brien", "Patel", "Chen", "Kim", "Murphy", "Sullivan", "Romano", "Esposito", "Nowak", "Shah"]

# =========================================================
# CONCEPTS & MENUS   (uppercase menus, like the samples)
#   item tuple: (name, price, popularity)
# =========================================================

FOOD, BEV, ALC = "FOOD", "BEVERAGES", "ALCOHOL"

CONCEPTS = {
    "casual": {
        "weight": 22, "rate": (140, 320), "price_style": 0.05,
        "menu": {
            "APPETIZERS": (FOOD, [("CRISPY CALAMARI", 14.95, 6), ("SPINACH ARTICHOKE DIP", 12.95, 8), ("BUFFALO WINGS", 13.95, 10),
                                  ("LOADED POTATO SKINS", 11.95, 5), ("TRUFFLE FRIES", 9.95, 7), ("PRETZEL BITES", 10.95, 6)]),
            "SALADS": (FOOD, [("CLASSIC CAESAR", 11.95, 8), ("COBB SALAD", 15.95, 7), ("QUINOA POWER BOWL", 14.95, 6),
                              ("HOUSE SALAD", 8.95, 5)]),
            "BURGERS & SANDWICHES": (FOOD, [("CLASSIC CHEESEBURGER", 15.95, 12), ("BACON BBQ BURGER", 17.45, 9),
                                            ("MUSHROOM SWISS BURGER", 16.95, 5), ("IMPOSSIBLE BURGER", 17.95, 4),
                                            ("NASHVILLE HOT CHICKEN SANDWICH", 15.95, 6), ("TURKEY CLUB", 14.45, 6),
                                            ("FRENCH DIP", 16.95, 5)]),
            "ENTREES": (FOOD, [("GRILLED ATLANTIC SALMON", 26.95, 9), ("12OZ RIBEYE", 38.95, 7), ("CHICKEN PARMESAN", 22.95, 9),
                               ("SHRIMP & GRITS", 24.95, 7), ("FISH & CHIPS", 19.95, 6), ("BABY BACK RIBS", 27.95, 6)]),
            "BRUNCH": (FOOD, [("EGGS BENEDICT", 15.95, 9), ("PANCAKE STACK", 11.95, 8), ("CHICKEN & WAFFLES", 16.95, 7)]),
            "SIDES": (FOOD, [("FRENCH FRIES", 4.95, 10), ("SWEET POTATO FRIES", 5.45, 6), ("SIDE MAC & CHEESE", 5.95, 7),
                             ("SEASONAL VEGETABLES", 4.95, 4)]),
            "KIDS MENU": (FOOD, [("KIDS CHICKEN TENDERS", 8.95, 10), ("KIDS CHEESEBURGER", 8.95, 6), ("KIDS PASTA", 7.95, 7)]),
            "DESSERTS": (FOOD, [("MOLTEN CHOCOLATE CAKE", 9.95, 9), ("NY CHEESECAKE", 8.95, 8), ("PUMPKIN CHEESECAKE", 9.45, 5),
                                ("APPLE COBBLER", 8.45, 5)]),
            "NA BEVERAGES": (BEV, [("FOUNTAIN SODA", 3.25, 14), ("ICED TEA", 3.25, 11), ("LEMONADE", 3.95, 6), ("COFFEE", 2.95, 6)]),
            "DRAFT BEER": (ALC, [("DRAFT IPA", 7.50, 9), ("DRAFT LIGHT LAGER", 6.00, 10), ("SEASONAL DRAFT", 7.50, 6)]),
            "WINE": (ALC, [("HOUSE CABERNET GLS", 12.00, 8), ("HOUSE CHARDONNAY GLS", 11.00, 7), ("PROSECCO GLS", 10.00, 4)]),
            "COCKTAILS": (ALC, [("HOUSE MARGARITA", 11.00, 10), ("OLD FASHIONED", 13.00, 7), ("ESPRESSO MARTINI", 13.50, 6)]),
        },
        # (category, weight, hour_from, hour_to, weekend_only)
        "main": [("BURGERS & SANDWICHES", 40, 11, 23, False), ("ENTREES", 38, 16, 23, False), ("ENTREES", 12, 11, 15, False),
                 ("SALADS", 22, 11, 23, False), ("BRUNCH", 70, 9, 13, True)],
        # (category, probability, hour_from, hour_to, area types allowed, per 'guest' or 'ticket')
        "extras": [("NA BEVERAGES", .55, 0, 30, (1, 2, 3, 4), "guest"), ("DRAFT BEER", .14, 11, 30, (1,), "guest"),
                   ("COCKTAILS", .11, 11, 30, (1,), "guest"), ("WINE", .09, 16, 30, (1,), "guest"),
                   ("SIDES", .12, 0, 30, (1, 2, 3, 4), "guest"), ("APPETIZERS", .45, 11, 30, (1,), "ticket"),
                   ("APPETIZERS", .25, 11, 30, (2, 3, 4), "ticket"), ("KIDS MENU", .12, 11, 20, (1, 2, 4), "ticket"),
                   ("DESSERTS", .24, 17, 30, (1,), "ticket"), ("DESSERTS", .05, 11, 16, (1, 2, 3, 4), "ticket")],
        "hours": {11: 5, 12: 9, 13: 7, 14: 3, 15: 2, 16: 3, 17: 7, 18: 10, 19: 10, 20: 7, 21: 4, 22: 2, 23: 1},
        "weekend_hours": {9: 2, 10: 5, 11: 8, 12: 9, 13: 7},
        "dow": [0.80, 0.82, 0.90, 1.00, 1.35, 1.40, 1.10],
        "areas": [("Main Dining", 1, 55), ("Bar", 1, 12), ("Patio", 1, 6), ("Take Out", 2, 12), ("Online TakeOut", 4, 9),
                  ("DoorDash", 3, 4), ("Uber Eats", 3, 2)],
        "guests": [1, 2, 3, 4, 5, 6, 8], "guest_w": [12, 40, 17, 18, 7, 4, 2],
        "courses": True, "cash_share": .14,
        "roles": {"Manager": 2, "Server": 10, "Bartender": 3, "Host": 2},
        "takers": {1: ["Server", "Server", "Server", "Bartender"], 2: ["Host", "Bartender"]},
    },
    "mexican": {
        "weight": 12, "rate": (110, 260), "price_style": 0.05,
        "menu": {
            "APPETIZERS (BOTANAS)": (FOOD, [("NACHOS SUPREMOS", 13.50, 9), ("QUESO DIP", 8.95, 8), ("GUACAMOLE FRESCO", 11.95, 9),
                                            ("CHICKEN TAQUITOS", 9.95, 5), ("ELOTE", 6.95, 5)]),
            "TACOS": (FOOD, [("TACOS AL PASTOR (3)", 13.95, 10), ("CARNE ASADA TACOS (3)", 14.95, 9), ("BIRRIA TACOS", 15.95, 8),
                             ("FISH TACOS", 14.50, 5)]),
            "BURRITOS": (FOOD, [("BURRITO CALIFORNIA", 14.25, 7), ("CHICKEN BURRITO", 12.95, 8), ("VEGGIE BURRITO", 11.95, 4)]),
            "PLATOS FUERTES": (FOOD, [("FAJITAS DE POLLO", 19.95, 8), ("CARNE ASADA PLATTER", 23.95, 6),
                                      ("ENCHILADAS VERDES", 16.95, 7), ("CHILE RELLENO", 15.95, 4)]),
            "SIDES": (FOOD, [("SIDE RICE", 3.50, 7), ("SIDE BEANS", 3.50, 6), ("SIDE PICO", 1.50, 5), ("CHIPS & SALSA", 4.50, 10),
                             ("SIDE SOUR CREAM", 1.00, 5)]),
            "POSTRES": (FOOD, [("CHURROS", 6.95, 7), ("FLAN", 5.95, 5), ("TRES LECHES", 7.50, 5)]),
            "BEBIDAS": (BEV, [("HORCHATA", 3.95, 7), ("JARRITOS", 3.50, 8), ("FOUNTAIN SODA", 2.95, 9)]),
            "MARGARITAS": (ALC, [("HOUSE MARGARITA", 9.50, 10), ("SPICY MARGARITA", 11.50, 6), ("MANGO MARGARITA", 10.50, 5)]),
            "CERVEZA": (ALC, [("CORONA", 6.00, 9), ("MODELO ESPECIAL", 6.00, 9), ("PACIFICO", 6.00, 5)]),
        },
        "main": [("TACOS", 38, 11, 23, False), ("BURRITOS", 25, 11, 23, False), ("PLATOS FUERTES", 37, 11, 23, False)],
        "extras": [("BEBIDAS", .45, 0, 30, (1, 2, 3, 4), "guest"), ("MARGARITAS", .18, 11, 30, (1,), "guest"),
                   ("CERVEZA", .14, 11, 30, (1,), "guest"), ("SIDES", .3, 0, 30, (1, 2, 3, 4), "guest"),
                   ("APPETIZERS (BOTANAS)", .5, 0, 30, (1,), "ticket"), ("APPETIZERS (BOTANAS)", .25, 0, 30, (2, 3, 4), "ticket"),
                   ("POSTRES", .12, 0, 30, (1,), "ticket")],
        "hours": {11: 5, 12: 8, 13: 6, 14: 3, 15: 2, 16: 3, 17: 7, 18: 10, 19: 9, 20: 6, 21: 3, 22: 1},
        "dow": [0.85, 0.95, 0.90, 1.00, 1.35, 1.35, 1.05],
        "areas": [("Main Dining", 1, 50), ("Bar", 1, 10), ("Take Out", 2, 18), ("Online TakeOut", 4, 12),
                  ("DoorDash", 3, 6), ("Uber Eats", 3, 4)],
        "guests": [1, 2, 3, 4, 5, 6], "guest_w": [14, 40, 18, 18, 6, 4],
        "courses": False, "cash_share": .2,
        "roles": {"Manager": 1, "Server": 6, "Bartender": 2, "Cashier": 2},
        "takers": {1: ["Server", "Server", "Server", "Bartender"], 2: ["Cashier"]},
    },
    "pizzeria": {
        "weight": 18, "rate": (90, 230), "price_style": 0.01,
        "menu": {
            "PIZZA": (FOOD, [("LG CHEESE PIZZA", 17.99, 12), ("LG PEPPERONI PIZZA", 19.99, 12), ("SM CHEESE PIZZA", 12.99, 6),
                             ("LG SUPREME PIZZA", 23.99, 6), ("LG BUFFALO CHICKEN PIZZA", 22.99, 5), ("LG WHITE PIZZA", 19.99, 4),
                             ("SICILIAN SQUARE", 21.99, 4)]),
            "STROMBOLI & CALZONES": (FOOD, [("PEPPERONI STROMBOLI", 14.99, 5), ("CHEESE CALZONE", 12.99, 5)]),
            "HOAGIES & STEAKS": (FOOD, [("ITALIAN HOAGIE", 12.49, 8), ("CHEESESTEAK", 12.99, 10), ("CHICKEN CHEESESTEAK", 12.99, 6),
                                        ("MEATBALL PARM HOAGIE", 11.99, 5)]),
            "WINGS": (FOOD, [("10 PC WINGS", 15.99, 9), ("20 PC WINGS", 28.99, 4), ("BONELESS WINGS", 13.99, 6)]),
            "SIDES": (FOOD, [("GARLIC KNOTS", 5.99, 8), ("MOZZ STICKS", 8.99, 7), ("FRENCH FRIES", 4.99, 8), ("CHEESE FRIES", 6.99, 5)]),
            "SALADS": (FOOD, [("GARDEN SALAD", 8.99, 4), ("CHEF SALAD", 11.99, 3)]),
            "DESSERTS": (FOOD, [("CANNOLI", 4.99, 5), ("ZEPPOLE", 5.99, 3)]),
            "DRINKS": (BEV, [("2 LITER SODA", 4.29, 9), ("CAN SODA", 1.79, 8), ("BOTTLED WATER", 1.99, 4)]),
        },
        "main": [("PIZZA", 55, 0, 30, False), ("HOAGIES & STEAKS", 30, 0, 30, False), ("STROMBOLI & CALZONES", 10, 0, 30, False),
                 ("SALADS", 5, 0, 30, False)],
        "extras": [("WINGS", .35, 0, 30, (1, 2, 3, 4), "ticket"), ("SIDES", .45, 0, 30, (1, 2, 3, 4), "ticket"),
                   ("DRINKS", .5, 0, 30, (1, 2, 3, 4), "ticket"), ("DESSERTS", .08, 0, 30, (1, 2, 3, 4), "ticket")],
        "hours": {11: 4, 12: 7, 13: 5, 14: 2, 15: 2, 16: 4, 17: 8, 18: 10, 19: 9, 20: 7, 21: 5, 22: 3, 23: 2},
        "dow": [0.85, 0.85, 0.90, 1.00, 1.45, 1.35, 1.15],
        "areas": [("Dine In", 1, 18), ("Pick Up", 2, 40), ("Delivery", 3, 14), ("Online TakeOut", 4, 16),
                  ("DoorDash", 3, 7), ("Grubhub", 3, 5)],
        "guests": [1, 2, 3, 4, 5, 6], "guest_w": [30, 30, 14, 16, 6, 4],
        "courses": False, "cash_share": .2, "shared_mains": 2.5,
        "roles": {"Manager": 1, "Cashier": 4, "Driver": 3},
        "takers": {1: ["Cashier"], 2: ["Cashier"]},
    },
    "cafe": {
        "weight": 14, "rate": (180, 420), "price_style": 0.0,
        "menu": {
            "COFFEE": (BEV, [("DRIP COFFEE 12OZ", 2.95, 10), ("DRIP COFFEE 16OZ", 3.45, 8), ("LATTE", 5.25, 12), ("CAPPUCCINO", 4.95, 6),
                             ("COLD BREW", 4.75, 9), ("AMERICANO", 3.75, 6), ("CHAI LATTE", 5.25, 6), ("MOCHA", 5.50, 6)]),
            "TEA & MORE": (BEV, [("HOT TEA", 2.95, 3), ("MATCHA LATTE", 5.75, 5), ("HOT CHOCOLATE", 3.95, 3), ("FRESH OJ", 4.50, 3)]),
            "BAKERY": (FOOD, [("BUTTER CROISSANT", 3.75, 9), ("BLUEBERRY MUFFIN", 3.50, 7), ("EVERYTHING BAGEL W/ CC", 3.95, 8),
                              ("CINNAMON ROLL", 4.25, 5), ("CHOC CHIP COOKIE", 2.95, 6)]),
            "BREAKFAST": (FOOD, [("BACON EGG & CHEESE", 6.95, 9), ("AVOCADO TOAST", 9.50, 5), ("BREAKFAST BURRITO", 8.95, 5),
                                 ("YOGURT PARFAIT", 6.50, 4)]),
            "LUNCH": (FOOD, [("TURKEY PESTO PANINI", 10.95, 6), ("CHICKEN SALAD SANDWICH", 9.95, 5), ("SOUP OF THE DAY", 5.95, 4),
                             ("CAPRESE PANINI", 9.95, 4)]),
        },
        "main": [("COFFEE", 70, 0, 30, False), ("TEA & MORE", 30, 0, 30, False)],
        "extras": [("BAKERY", .35, 0, 30, (2, 4), "guest"), ("BREAKFAST", .28, 0, 11, (2, 4), "guest"),
                   ("LUNCH", .32, 11, 30, (2, 4), "guest")],
        "hours": {6: 6, 7: 10, 8: 10, 9: 8, 10: 6, 11: 5, 12: 6, 13: 4, 14: 3, 15: 2},
        "dow": [1.05, 1.05, 1.05, 1.05, 1.05, 1.00, 0.80],
        "areas": [("Counter", 2, 82), ("Online TakeOut", 4, 18)],
        "guests": [1, 2, 3], "guest_w": [70, 24, 6],
        "courses": False, "cash_share": .1,
        "roles": {"Manager": 1, "Barista": 6},
        "takers": {1: ["Barista"], 2: ["Barista"]},
    },
    "qsr": {
        "weight": 12, "rate": (220, 520), "price_style": 0.01,
        "menu": {
            "BURGERS": (FOOD, [("SMASH BURGER", 8.99, 10), ("DOUBLE SMASH", 11.49, 9), ("BACON CHEESEBURGER", 10.49, 7),
                               ("VEGGIE BURGER", 9.49, 3)]),
            "CHICKEN": (FOOD, [("CHICKEN SANDWICH", 8.99, 9), ("SPICY CHICKEN SANDWICH", 9.29, 8), ("CHICKEN TENDERS 3PC", 8.49, 7),
                               ("NUGGETS 10PC", 7.99, 6)]),
            "SIDES": (FOOD, [("FRIES", 3.49, 12), ("ONION RINGS", 4.29, 5), ("CHEESE FRIES", 4.99, 4)]),
            "SHAKES": (BEV, [("VANILLA SHAKE", 5.49, 5), ("CHOCOLATE SHAKE", 5.49, 6), ("OREO SHAKE", 5.99, 4)]),
            "DRINKS": (BEV, [("FOUNTAIN DRINK MD", 2.49, 10), ("FOUNTAIN DRINK LG", 2.89, 6), ("BOTTLED WATER", 1.99, 3)]),
        },
        "main": [("BURGERS", 55, 0, 30, False), ("CHICKEN", 45, 0, 30, False)],
        "extras": [("SIDES", .7, 0, 30, (1, 2, 3, 4), "guest"), ("DRINKS", .65, 0, 30, (1, 2, 3, 4), "guest"),
                   ("SHAKES", .12, 0, 30, (1, 2, 3, 4), "guest")],
        "hours": {10: 2, 11: 7, 12: 10, 13: 8, 14: 4, 15: 3, 16: 3, 17: 6, 18: 7, 19: 6, 20: 4, 21: 2, 22: 1},
        "dow": [0.95, 0.95, 1.00, 1.00, 1.15, 1.10, 0.90],
        "areas": [("Counter", 2, 55), ("Drive Thru", 2, 15), ("Online TakeOut", 4, 15), ("DoorDash", 3, 9), ("Uber Eats", 3, 6)],
        "guests": [1, 2, 3, 4], "guest_w": [50, 30, 12, 8],
        "courses": False, "cash_share": .12,
        "roles": {"Manager": 2, "Cashier": 6},
        "takers": {1: ["Cashier"], 2: ["Cashier"]},
    },
    "bar": {
        "weight": 12, "rate": (80, 220), "price_style": 0.0,
        "menu": {
            "BAR BITES": (FOOD, [("WINGS 12 PC", 14.99, 9), ("NACHOS", 12.99, 7), ("PRETZEL & BEER CHEESE", 10.99, 6),
                                 ("CHICKEN TENDERS", 11.99, 6), ("LOADED FRIES", 10.49, 6)]),
            "BURGERS": (FOOD, [("PUB BURGER", 14.99, 8), ("BLACK & BLUE BURGER", 15.99, 4)]),
            "DRAFT BEER": (ALC, [("LAGER DRAFT", 5.00, 10), ("LITE DRAFT", 4.50, 9), ("LOCAL IPA DRAFT", 7.00, 8),
                                 ("STOUT DRAFT", 7.50, 5), ("SEASONAL DRAFT", 7.00, 5)]),
            "BOTTLES & CANS": (ALC, [("DOMESTIC BTL", 5.00, 7), ("HARD SELTZER", 6.00, 6), ("IMPORT BTL", 6.00, 5)]),
            "COCKTAILS": (ALC, [("VODKA SODA", 8.00, 8), ("MARGARITA", 10.00, 6), ("OLD FASHIONED", 12.00, 5), ("LONG ISLAND", 11.00, 4)]),
            "SHOTS": (ALC, [("WHISKEY SHOT", 7.00, 6), ("CINNAMON WHISKY SHOT", 5.00, 5), ("TEQUILA SHOT", 7.00, 4)]),
            "NA": (BEV, [("SODA", 2.50, 5), ("ENERGY DRINK", 4.50, 4)]),
        },
        "main": [("DRAFT BEER", 45, 0, 30, False), ("BOTTLES & CANS", 20, 0, 30, False), ("COCKTAILS", 30, 0, 30, False),
                 ("NA", 5, 0, 30, False)],
        "extras": [("DRAFT BEER", .45, 0, 30, (1,), "guest"), ("COCKTAILS", .2, 0, 30, (1,), "guest"),
                   ("SHOTS", .2, 19, 30, (1,), "guest"), ("BAR BITES", .45, 0, 30, (1, 2), "ticket"),
                   ("BURGERS", .25, 0, 30, (1, 2), "ticket")],
        "hours": {15: 2, 16: 4, 17: 7, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 8, 24: 6, 25: 4, 26: 2},
        "dow": [0.60, 0.65, 0.80, 1.00, 1.50, 1.60, 0.90],
        "areas": [("Bar", 1, 70), ("Main Dining", 1, 22), ("Take Out", 2, 8)],
        "guests": [1, 2, 3, 4, 5], "guest_w": [30, 35, 15, 14, 6],
        "courses": False, "cash_share": .25, "happy_hour": (16, 18),
        "roles": {"Manager": 1, "Bartender": 5, "Server": 3},
        "takers": {1: ["Bartender", "Bartender", "Server"], 2: ["Bartender"]},
    },
    "truck": {
        "weight": 4, "rate": (60, 160), "price_style": 0.0,
        "menu": {
            "MAINS": (FOOD, [("CHEESESTEAK", 12.00, 10), ("CHICKEN CHEESESTEAK", 12.00, 7), ("PORK ROLL SANDWICH", 9.00, 4),
                             ("BIRRIA TACOS (3)", 13.00, 6), ("LOADED FRIES", 9.00, 5)]),
            "SIDES": (FOOD, [("FRIES", 4.00, 9), ("OLD BAY FRIES", 5.00, 5)]),
            "DRINKS": (BEV, [("CAN SODA", 2.00, 8), ("BOTTLED WATER", 2.00, 6), ("LEMONADE", 4.00, 4)]),
        },
        "main": [("MAINS", 100, 0, 30, False)],
        "extras": [("SIDES", .5, 0, 30, (2, 4), "guest"), ("DRINKS", .5, 0, 30, (2, 4), "guest")],
        "hours": {11: 6, 12: 10, 13: 8, 14: 4, 17: 3, 18: 4, 19: 3},
        "dow": [1.10, 1.10, 1.10, 1.10, 1.10, 0.60, 0.30],
        "areas": [("Window", 2, 90), ("Online TakeOut", 4, 10)],
        "guests": [1, 2, 3], "guest_w": [70, 24, 6],
        "courses": False, "cash_share": .3,
        "roles": {"Owner": 1, "Cashier": 2},
        "takers": {1: ["Cashier"], 2: ["Cashier", "Owner"]},
    },
    "asian": {
        "weight": 6, "rate": (90, 220), "price_style": 0.05,
        "menu": {
            "APPETIZERS": (FOOD, [("PORK DUMPLINGS (6)", 8.95, 8), ("EGG ROLL", 2.95, 8), ("CRAB RANGOON", 7.95, 7), ("EDAMAME", 5.95, 5)]),
            "SOUPS": (FOOD, [("WONTON SOUP", 4.95, 6), ("HOT & SOUR SOUP", 4.95, 5), ("MISO SOUP", 3.50, 4)]),
            "SUSHI ROLLS": (FOOD, [("CALIFORNIA ROLL", 7.95, 8), ("SPICY TUNA ROLL", 8.95, 8), ("SALMON AVOCADO ROLL", 8.95, 6),
                                   ("DRAGON ROLL", 14.95, 4), ("PHILLY ROLL", 8.95, 5)]),
            "ENTREES": (FOOD, [("GENERAL TSO CHICKEN", 14.95, 12), ("SESAME CHICKEN", 14.95, 8), ("BEEF & BROCCOLI", 15.95, 7),
                               ("ORANGE CHICKEN", 14.95, 7), ("SHRIMP LO MEIN", 14.95, 5)]),
            "FRIED RICE & NOODLES": (FOOD, [("CHICKEN FRIED RICE", 11.95, 8), ("VEGETABLE LO MEIN", 10.95, 6), ("PAD THAI", 14.95, 6)]),
            "LUNCH SPECIALS": (FOOD, [("LUNCH GENERAL TSO", 10.95, 9), ("LUNCH SESAME CHICKEN", 10.95, 6),
                                      ("LUNCH BEEF & BROCCOLI", 11.45, 5)]),
            "BEVERAGES": (BEV, [("CAN SODA", 1.75, 7), ("BUBBLE TEA", 5.95, 5), ("THAI ICED TEA", 4.50, 4)]),
        },
        "main": [("LUNCH SPECIALS", 60, 11, 15, False), ("ENTREES", 45, 11, 30, False), ("SUSHI ROLLS", 25, 11, 30, False),
                 ("FRIED RICE & NOODLES", 20, 11, 30, False)],
        "extras": [("APPETIZERS", .45, 0, 30, (1, 2, 3, 4), "ticket"), ("SOUPS", .25, 0, 30, (1, 2, 3, 4), "guest"),
                   ("BEVERAGES", .3, 0, 30, (1, 2, 3, 4), "guest")],
        "hours": {11: 5, 12: 8, 13: 6, 14: 3, 15: 2, 16: 3, 17: 7, 18: 9, 19: 8, 20: 6, 21: 3},
        "dow": [0.90, 0.90, 0.95, 1.00, 1.30, 1.30, 1.10],
        "areas": [("Main Dining", 1, 30), ("Take Out", 2, 35), ("Online TakeOut", 4, 18), ("DoorDash", 3, 10), ("Grubhub", 3, 7)],
        "guests": [1, 2, 3, 4, 5], "guest_w": [30, 35, 15, 14, 6],
        "courses": False, "cash_share": .18,
        "roles": {"Manager": 1, "Server": 4, "Cashier": 2},
        "takers": {1: ["Server"], 2: ["Cashier"]},
    },
}
CONCEPT_NAMES = list(CONCEPTS)
CONCEPT_W = [CONCEPTS[c]["weight"] for c in CONCEPT_NAMES]

MODIFIERS = {
    "BURGERS & SANDWICHES": [("ADD BACON", 2.00), ("ADD AVOCADO", 1.50), ("NO ONION", 0.0), ("GLUTEN FREE BUN", 2.00), ("SUB SWEET FRIES", 1.50)],
    "BURGERS": [("ADD BACON", 1.50), ("EXTRA CHEESE", 0.75), ("NO PICKLE", 0.0), ("ADD JALAPENOS", 0.50)],
    "ENTREES": [("MEDIUM RARE", 0.0), ("MEDIUM", 0.0), ("SAUCE ON SIDE", 0.0), ("ADD SHRIMP", 6.00)],
    "SALADS": [("ADD CHICKEN", 5.00), ("ADD SALMON", 8.00), ("DRESSING ON SIDE", 0.0)],
    "PIZZA": [("EXTRA CHEESE", 2.50), ("ADD MUSHROOMS", 2.00), ("ADD SAUSAGE", 2.50), ("WELL DONE", 0.0)],
    "HOAGIES & STEAKS": [("FRIED ONIONS", 0.0), ("ADD PEPPERS", 0.75), ("WIZ", 0.0), ("PROVOLONE", 0.0), ("AMERICAN", 0.0)],
    "COFFEE": [("OAT MILK", 0.75), ("EXTRA SHOT", 1.00), ("VANILLA SYRUP", 0.75), ("ICED", 0.0)],
    "TACOS": [("NO CILANTRO", 0.0), ("ADD CHEESE", 1.00), ("CORN TORTILLA", 0.0)],
    "COCKTAILS": [("TOP SHELF", 3.00), ("MAKE IT A DOUBLE", 5.00)],
    "MAINS": [("WIZ", 0.0), ("FRIED ONIONS", 0.0), ("HOT PEPPERS", 0.0)],
}

NAME_PARTS = {
    "adj": ["Golden", "Rustic", "Little", "Blue", "Copper", "Iron", "Lucky", "Red", "Black", "Wild", "Happy", "Urban"],
    "noun": ["Oak", "Anchor", "Fox", "Lantern", "Barrel", "Kettle", "Spoon", "Crown", "Owl", "Harbor", "Pine", "Stone"],
    "sp": ["El Sol", "La Esquina", "Los Amigos", "El Toro", "Mi Pueblo", "La Fiesta", "Casa Verde", "El Jefe"],
    "it": ["Napoli", "Roma", "Sorrento", "Bella", "Mamma Mia", "Vesuvio", "Capri", "Tuscan"],
    "cn": ["Lucky Dragon", "Jade", "Golden Wok", "Panda", "Bamboo", "Lotus", "Sakura", "Ginza"],
}
NAME_PATTERNS = {
    "casual": ["{last}'s Grill", "The {noun} Tavern & Grill", "{city} Kitchen & Bar", "{adj} {noun} Bistro"],
    "mexican": ["Taqueria {sp}", "{sp} Cantina", "{sp} Mexican Grill"],
    "pizzeria": ["{last}'s Pizzeria", "{city} Pizza & Subs", "{it} Pizza", "Pizza {it}"],
    "cafe": ["{adj} Bean Coffee", "{city} Coffee Co.", "The {noun} Cafe", "{last} & Co. Coffee"],
    "qsr": ["{city} Burger Co.", "{adj} Bird Chicken", "{last}'s Burgers", "Smash {noun}"],
    "bar": ["The {noun} Pub", "{last}'s Tap House", "{city} Ale House", "The {adj} {noun} Saloon"],
    "truck": ["{adj} {noun} Street Eats", "{last}'s Cheesesteak Truck", "{sp} Taco Truck"],
    "asian": ["{cn} Garden", "{cn} Sushi & Grill", "Golden {noun} Chinese", "{cn} Noodle House"],
}

# =========================================================
# CALENDAR: seasonality and holidays (multipliers by concept)
# =========================================================

MONTH_W = [0.86, 0.93, 1.00, 1.00, 1.04, 1.07, 1.08, 1.03, 0.97, 1.00, 0.98, 1.06]


def _nth_weekday(year, month, weekday, n):
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def holiday_for(d):
    md = (d.month, d.day)
    if md == (12, 25): return "Christmas Day"
    if md == (12, 24): return "Christmas Eve"
    if md == (12, 31): return "New Year's Eve"
    if md == (1, 1): return "New Year's Day"
    if md == (2, 14): return "Valentine's Day"
    if md == (3, 17): return "St. Patrick's Day"
    if md == (5, 5): return "Cinco de Mayo"
    if md == (7, 4): return "Independence Day"
    if d == _nth_weekday(d.year, 2, 6, 2): return "Super Bowl Sunday"
    if d == _nth_weekday(d.year, 5, 6, 2): return "Mother's Day"
    if d == _nth_weekday(d.year, 11, 3, 4): return "Thanksgiving"
    return None


HOLIDAY_TRAFFIC = {   # concept -> multiplier ("*" = default)
    "Christmas Day": {"*": 0.0, "asian": 0.6},
    "Christmas Eve": {"*": 0.6},
    "New Year's Eve": {"*": 0.9, "bar": 1.6, "casual": 1.3},
    "New Year's Day": {"*": 0.9, "cafe": 0.6},
    "Valentine's Day": {"*": 1.0, "casual": 1.45, "mexican": 1.35, "asian": 1.3},
    "St. Patrick's Day": {"*": 1.0, "bar": 1.9, "casual": 1.15},
    "Cinco de Mayo": {"*": 1.0, "mexican": 2.3, "bar": 1.2},
    "Independence Day": {"*": 0.75, "truck": 1.2},
    "Super Bowl Sunday": {"*": 1.0, "pizzeria": 1.9, "bar": 1.5, "qsr": 1.2},
    "Mother's Day": {"*": 1.0, "casual": 1.7, "cafe": 1.25, "mexican": 1.3},
    "Thanksgiving": {"*": 0.3, "pizzeria": 0.5},
}
HOLIDAY_ITEMS = {    # holiday -> category multipliers
    "Valentine's Day": {"DESSERTS": 1.7, "POSTRES": 1.7, "WINE": 1.8},
    "Super Bowl Sunday": {"WINGS": 2.5, "BAR BITES": 1.8, "APPETIZERS": 1.4},
    "St. Patrick's Day": {"DRAFT BEER": 1.8, "SHOTS": 1.5},
    "Cinco de Mayo": {"MARGARITAS": 2.2, "CERVEZA": 1.6},
    "New Year's Eve": {"COCKTAILS": 1.6, "WINE": 1.4},
}
HOLIDAY_BY_IDX = {i: holiday_for(day_of(i)) for i in range(DAYS)}

# =========================================================
# PLANTED STORIES  (all relative to --end_date; idx = DAYS - days_ago)
# =========================================================

SHOWCASE = {
    "name": "Ember & Oak Kitchen",
    "concept": "casual",
    "locations": [("King of Prussia", None), ("Philadelphia", "Fishtown"), ("Philadelphia", "Rittenhouse"),
                  ("Cherry Hill", None), ("Wilmington", None), ("West Chester", None), ("Lancaster", None),
                  ("Allentown", None), ("Pittsburgh", "Strip District"), ("Harrisburg", None), ("Princeton", None),
                  ("Hoboken", None), ("Baltimore", None), ("Annapolis", None), ("State College", None),
                  ("Reading", None), ("Bethlehem", None), ("Scranton", None), ("Brooklyn", None), ("Rehoboth Beach", None)],
}
STORIES = {
    "new_item":       {"item": "NASHVILLE HOT CHICKEN SANDWICH", "launch_days_ago": 28, "ramp_days": 21, "peak": 4.0},
    "lto":            {"item": "PUMPKIN CHEESECAKE", "start": (9, 15), "end": (11, 30), "mult": 2.2, "current_mult": 3.0},
    "declining":      {"item": "QUINOA POWER BOWL", "start": 1.8, "end": 0.35},
    "outage":         {"item": "GRILLED ATLANTIC SALMON", "loc": 2, "from_days_ago": 10, "to_days_ago": 7},
    "void_hotspot":   {"loc": 1, "hired_days_ago": 75, "rate": 0.12},
    "delivery_surge": {"loc": 9, "end_mult": 3.2},
    "new_store":      {"loc": 19, "opened_days_ago": 150},
}
BASE_VOID_RATE, BASE_COMP_RATE, ORDER_VOID_RATE, STALE_OPEN_RATE = 0.010, 0.012, 0.003, 0.0008


def idx_of_days_ago(n):
    return DAYS - n

# =========================================================
# BUILD THE MERCHANT BASE (deterministic)
# =========================================================

W = random.Random(f"{SEED}:world")
CURRENT_YEAR = LAST_DATE.year


def _round_price(x, style):
    p = round(x * 4) / 4 - style
    return round(max(0.5, p), 2)


def _messy(name, style, rng):
    if style == "mixed" and rng.random() < 0.35:
        name = " ".join(w.capitalize() if len(w) > 3 and rng.random() < 0.6 else w for w in name.split())
    return name


def _make_comm_channel(rng, used):
    while True:
        style = rng.choices(["num", "AC", "C"], weights=[55, 25, 20])[0]
        code = str(rng.randint(100, 9999)) if style == "num" else f"{style}{rng.randint(1000, 9999)}"
        if code not in used:
            used.add(code)
            return code


def _feature_date(rng, p, onboard_idx, pre_window_share=0.25):
    """Index when a merchant adopted a feature; None = never; <=0 = before the window."""
    if rng.random() >= p:
        return None
    if onboard_idx < 0 and rng.random() < pre_window_share:
        return -1
    lo = max(0, onboard_idx)
    return rng.randint(lo, DAYS - 1) if lo < DAYS - 1 else None


def _employee_timeline(rng, open_idx, mean_tenure):
    starts, t = [], open_idx - rng.randint(0, 500)
    while t < DAYS:
        starts.append(t)
        t += max(14, int(rng.expovariate(1.0 / mean_tenure)))
    return starts


TENURE = {"Owner": 100000, "General Manager": 900, "Manager": 600, "Server": 260, "Bartender": 320, "Host": 150,
          "Cashier": 190, "Barista": 210, "Driver": 200}
HOURLY = {"Owner": 0, "Manager": 24, "Server": 3.5, "Bartender": 5.5, "Host": 14, "Cashier": 15, "Barista": 15.5, "Driver": 13}

used_channels, used_names = set(), set()
MERCHANTS, LOCATIONS, EMPLOYEES = [], [], []
_city_by_name = {c[0]: c for c in CITIES}


def _merchant_name(concept, city, rng):
    for _ in range(50):
        pat = rng.choice(NAME_PATTERNS[concept])
        name = pat.format(last=rng.choice(LAST), city=city, **{k: rng.choice(v) for k, v in NAME_PARTS.items()})
        if name not in used_names:
            used_names.add(name)
            return name
    return f"{name} {rng.randint(2, 99)}"


for m in range(max(1, args.merchants)):
    show = m == 0
    concept = SHOWCASE["concept"] if show else W.choices(CONCEPT_NAMES, weights=CONCEPT_W)[0]
    spec = CONCEPTS[concept]
    home = _city_by_name["King of Prussia"] if show else W.choices(CITIES, weights=CITY_W)[0]
    n_locs = len(SHOWCASE["locations"]) if show else W.choices([1, W.randint(2, 5), W.randint(6, 25)], weights=[75, 20, 5])[0]
    onboard = -W.randint(900, 1800) if show else (-W.randint(30, 1800) if W.random() < 0.55 else W.randint(0, DAYS - 45))
    churn = None
    if not show and W.random() < 0.10 and max(onboard, 0) + 200 < DAYS:
        churn = W.randint(max(onboard, 0) + 200, DAYS - 1)
    app = "posIOS" if (show or W.random() < (0.72 if onboard < 0 else 0.55)) else "posLiteAndroid"
    merchant = {
        "idx": m, "show": show, "concept": concept, "spec": spec,
        "name": SHOWCASE["name"] if show else _merchant_name(concept, home[0], W),
        "commChannel": "847" if show else _make_comm_channel(W, used_channels),
        "onboard": onboard, "churn": churn, "appType": app,
        "gateway": "cardConnect" if (show or W.random() < 0.85) else "dejavoo",
        "surcharge": _feature_date(W, 0.45, onboard) if not show else None,
        "surchargeRate": W.choice([3.0, 3.5, 3.5, 4.0]),
        "surchargeLabel": W.choice(["Rewards Pay", "Non-Cash Adjustment", "Credit Card Surcharge"]),
        "online": -1 if show else _feature_date(W, 0.2 if concept == "truck" else 0.62, onboard),
        "thirdParty": -1 if show else (_feature_date(W, 0.5, onboard) if concept in ("casual", "mexican", "pizzeria", "qsr", "asian") else None),
        "gift": show or W.random() < 0.3, "loyalty": show or W.random() < 0.35, "sigCapture": W.random() < 0.5,
        "inventory": show or W.random() < 0.25, "laborRates": show or W.random() < 0.55, "tracksCost": show or W.random() < 0.4,
        "priceFactor": (1.0 if show else W.uniform(0.88, 1.15)) * (1.08 if home[1] in ("NY", "NJ") else 1.0),
        "annualRaise": 0.045 if show else W.choices([0, 0.03, 0.04, 0.05, 0.06], weights=[15, 25, 30, 20, 10])[0],
        "nameStyle": "upper" if (show or W.random() < 0.75) else "mixed",
        "taxClassID": uid(W), "roleIDs": {r: uid(W) for r in list(spec["roles"]) + ["Owner", "Server"]},
        "courseIDs": {c: uid(W) for c in ("Appetizers", "Entrees", "Desserts", "Drinks")},
        "customerPool": [],
    }
    if churn is not None:
        merchant["surcharge"] = merchant["surcharge"] if (merchant["surcharge"] or 0) < churn else None

    # ---- menu / items (shared by all locations of the merchant) ----
    items, cats = [], {}
    super_ids = {s: uid(W) for s in (FOOD, BEV, ALC)}
    for cat, (supercat, entries) in spec["menu"].items():
        cats[cat] = {"catID": uid(W), "catName": cat, "superCatName": supercat, "superCatID": super_ids[supercat], "items": []}
        for (name, price, pop) in entries:
            story_item = name in (STORIES["new_item"]["item"], STORIES["outage"]["item"], STORIES["declining"]["item"],
                                  STORIES["lto"]["item"])
            if not show and not story_item and W.random() < 0.08:
                continue                                    # merchants don't carry every template item
            p = _round_price(price * merchant["priceFactor"], spec["price_style"])
            it = {"itemID": uid(W), "name": _messy(name, merchant["nameStyle"], W), "base": name, "price": p,
                  "pop": pop * (W.uniform(0.7, 1.3) if not show else 1.0), "cat": cats[cat],
                  "cogRatio": W.uniform(0.24, 0.38) if merchant["tracksCost"] else 0.0,
                  "trail": "  " if W.random() < 0.3 else "", "trackInventory": 1 if (merchant["inventory"] and supercat != BEV and W.random() < 0.3) else 0,
                  "created": -W.randint(30, 1500)}
            if show and name == STORIES["new_item"]["item"]:
                it["created"] = idx_of_days_ago(STORIES["new_item"]["launch_days_ago"])
            cats[cat]["items"].append(it)
            items.append(it)
    merchant["items"], merchant["cats"] = items, cats
    merchant["itemByBase"] = {it["base"]: it for it in items}

    # ---- locations ----
    merchant["locations"] = []
    for k in range(n_locs):
        if show:
            cname, hood = SHOWCASE["locations"][k]
            city = _city_by_name[cname]
            lname = f"{merchant['name']} - {hood or cname}"
        else:
            city = home if (k == 0 or W.random() < 0.8) else W.choices(CITIES, weights=CITY_W)[0]
            lname = merchant["name"] if n_locs == 1 else f"{merchant['name']} - {W.choice(NEIGHBORHOODS) if city == home else city[0]}"
        open_idx = onboard if k == 0 else (onboard - W.randint(0, 600) if onboard < 0 and W.random() < 0.6 else
                                           W.randint(max(onboard, 0), max(max(onboard, 0), DAYS - 30)))
        if show:
            open_idx = onboard - k * 20
            if k + 1 == STORIES["new_store"]["loc"]:
                open_idx = idx_of_days_ago(STORIES["new_store"]["opened_days_ago"])
        rate = W.uniform(*spec["rate"]) * math.exp(W.gauss(0, 0.35)) * (1.35 if show else 1.0)
        n_term = max(1, min(6, 1 + int(rate // 90)))
        tname = {"casual": "POS Station", "mexican": "POS Station", "bar": "Bar Terminal", "cafe": "Register",
                 "qsr": "Register", "pizzeria": "Counter", "truck": "iPad", "asian": "POS Station"}[concept]
        loc = {
            "idx": len(LOCATIONS), "merchant": merchant, "num": k + 1, "locationID": uid(W), "locationName": lname,
            "city": city[0], "state": city[1], "tz": ZoneInfo(city[2]), "taxPercent": city[3], "cold": city[5],
            "open": open_idx, "churn": churn, "rate": rate, "growth": W.gauss(0.04, 0.05),
            "terminals": [{"terminalID": uid(W), "terminalName": f"{tname} {t + 1}", "deviceID": uid(W),
                           "serialNumber": f"{W.randint(10000, 99999)}PP{W.randint(10000000, 99999999)}", "num": t + 1}
                          for t in range(n_term)],
            "areas": [{"serviceAreaID": uid(W), "serviceAreaName": a, "serviceAreaType": t, "w": w} for (a, t, w) in spec["areas"]],
            "mid": fake_digits(W, 12), "gatewayID": uid(W), "cashGatewayID": uid(W), "giftGatewayID": uid(W),
            "onlineEmpID": uid(W), "address1": f"{W.randint(10, 9999)} {W.choice(['Main', 'Market', 'High', 'Church', 'Walnut', 'Chestnut', 'Lancaster', 'Broad', 'Penn', 'Union'])} {W.choice(['St', 'Ave', 'Pike', 'Rd', 'Blvd'])}",
            "zip": f"{W.randint(10000, 99999)}",
            "orderBase": W.randint(1000, 60000), "onlineBase": W.randint(100, 3000), "batchBase": W.randint(100, 3000),
            "slots": {},
        }
        # employees: role slots with turnover over time
        scale = max(0.6, rate / sum(spec["rate"]) * 2)
        for role, n in spec["roles"].items():
            loc["slots"][role] = []
            for s in range(max(1, round(n * scale))):
                starts = _employee_timeline(W, open_idx, TENURE.get(role, 300))
                emps = []
                for st in starts:
                    e = {"empID": uid(W), "first": W.choice(FIRST), "last": W.choice(LAST), "role": role,
                         "roleID": merchant["roleIDs"][role], "hire": st, "loc": loc,
                         "rate": round(HOURLY.get(role, 15) * W.uniform(0.9, 1.25), 2) if merchant["laborRates"] else 0}
                    emps.append(e)
                    EMPLOYEES.append(e)
                for i, e in enumerate(emps):
                    e["term"] = starts[i + 1] if i + 1 < len(starts) else None
                loc["slots"][role].append({"starts": starts, "emps": emps, "w": W.uniform(0.5, 1.5)})
        if "Owner" not in loc["slots"]:
            owner = {"empID": uid(W), "first": W.choice(FIRST), "last": W.choice(LAST), "role": "Owner",
                     "roleID": merchant["roleIDs"]["Owner"], "hire": open_idx - 30, "loc": loc, "rate": 0, "term": None}
            EMPLOYEES.append(owner)
            loc["owner"] = owner
        else:
            loc["owner"] = loc["slots"]["Owner"][0]["emps"][0]
        merchant["locations"].append(loc)
        LOCATIONS.append(loc)
    MERCHANTS.append(merchant)

SHOW = MERCHANTS[0]
SHOW_LOC = {loc["num"]: loc for loc in SHOW["locations"]}

# void-hotspot story: a server hired ~75 days ago at showcase location 1
_vh = STORIES["void_hotspot"]
_slot = SHOW_LOC[_vh["loc"]]["slots"]["Server"][0]
_hire = idx_of_days_ago(_vh["hired_days_ago"])
_keep = [i for i, s in enumerate(_slot["starts"]) if s < _hire]
_slot["starts"] = [_slot["starts"][i] for i in _keep] + [_hire]
_slot["emps"] = [_slot["emps"][i] for i in _keep]
if _slot["emps"]:
    _slot["emps"][-1]["term"] = _hire
_void_emp = {"empID": uid(W), "first": "Tyler", "last": "Brennan", "role": "Server", "roleID": SHOW["roleIDs"]["Server"],
             "hire": _hire, "loc": SHOW_LOC[_vh["loc"]], "rate": 3.75, "term": None}
_slot["emps"].append(_void_emp)
_slot["w"] = 1.4
EMPLOYEES.append(_void_emp)
VOID_EMP_ID = _void_emp["empID"]


def employee_at(loc, roles, idx, rng):
    role = rng.choice(roles)
    slots = loc["slots"].get(role) or next(iter(loc["slots"].values()))
    slot = rng.choices(slots, weights=[s["w"] for s in slots])[0]
    i = max(0, bisect.bisect_right(slot["starts"], idx) - 1)
    return slot["emps"][i]

# =========================================================
# PER-LOCATION DAILY DEMAND CURVES  ->  order sampling
# =========================================================

for loc in LOCATIONS:
    spec = loc["merchant"]["spec"]
    concept = loc["merchant"]["concept"]
    cum = array("d")
    acc = 0.0
    churn = loc["churn"]
    for i in range(DAYS):
        w = 0.0
        if i >= loc["open"] and (churn is None or i < churn):
            d = day_of(i)
            mw = MONTH_W[d.month - 1]
            if concept == "truck" and loc["cold"] and d.month in (12, 1, 2):
                mw *= 0.45
            hol = HOLIDAY_BY_IDX[i]
            hm = 1.0
            if hol:
                ht = HOLIDAY_TRAFFIC[hol]
                hm = ht.get(concept, ht["*"])
            age = i - loc["open"]
            ramp = 1.0
            if 0 <= age < 120:                                   # new store: opening buzz, then settles
                ramp = 1.25 - 0.25 * age / 120 if age > 21 else 0.7 + 0.55 * age / 21
            if churn is not None and churn - i < 120:            # churning merchants fade before they leave
                ramp *= 0.4 + 0.6 * (churn - i) / 120
            growth = (1 + loc["growth"]) ** (i / 365.0)
            w = loc["rate"] * spec["dow"][d.weekday()] * mw * hm * ramp * growth
        acc += w
        cum.append(acc)
    loc["cum"] = cum
    loc["total"] = acc

NATURAL_ORDERS = sum(l["total"] for l in LOCATIONS)
NUM_ORDERS = args.num_orders or int(NATURAL_ORDERS)
SCALE = NUM_ORDERS / NATURAL_ORDERS if NATURAL_ORDERS else 1.0
LOC_CUM = []
_acc = 0.0
for _l in LOCATIONS:
    _acc += _l["total"]
    LOC_CUM.append(_acc)


def pick_location(rng):
    return LOCATIONS[min(bisect.bisect_right(LOC_CUM, rng.random() * LOC_CUM[-1]), len(LOCATIONS) - 1)]


def pick_day(rng, loc):
    r = rng.random() * loc["total"]
    return min(bisect.bisect_right(loc["cum"], r), DAYS - 1)


HOUR_TABLES = {}
for _c, _s in CONCEPTS.items():
    hw = dict(_s["hours"])
    we = dict(hw)
    for h, v in _s.get("weekend_hours", {}).items():
        we[h] = we.get(h, 0) + v
    HOUR_TABLES[_c] = ((list(hw), list(hw.values())), (list(we), list(we.values())))

# app release history (buildNumber = release date, clients lag behind the newest build)
_R = random.Random(f"{SEED}:releases")
RELEASES = {}
for _app in ("posIOS", "posLiteAndroid"):
    d, rel = START_DATE - timedelta(days=400), []
    while d <= LAST_DATE:
        rel.append(d)
        d += timedelta(days=_R.randint(21, 45))
    RELEASES[_app] = rel


def build_number(app, d, rng):
    rel = RELEASES[app]
    i = bisect.bisect_right(rel, d) - 1 - rng.choices([0, 1, 2, 3], weights=[55, 25, 12, 8])[0]
    return rel[max(0, i)].strftime("%Y%m%d")


_PRICE = {}


def price_on(item, merchant, d):
    yb = CURRENT_YEAR - d.year
    key = (item["itemID"], yb)
    v = _PRICE.get(key)
    if v is None:
        v = item["price"] if yb <= 0 else _round_price(item["price"] / (1 + merchant["annualRaise"]) ** yb,
                                                         merchant["spec"]["price_style"])
        _PRICE[key] = v
    return v


def story_mult(merchant, item, loc, idx):
    base = item["base"]
    d = day_of(idx)
    if base == STORIES["lto"]["item"]:
        s = STORIES["lto"]
        if not (s["start"] <= (d.month, d.day) <= s["end"]):
            return 0.0
        return s["current_mult"] if d.year == CURRENT_YEAR else s["mult"]
    if not merchant["show"]:
        return 1.0
    days_ago = DAYS - idx
    s = STORIES["new_item"]
    if base == s["item"]:
        since = s["launch_days_ago"] - days_ago
        if since < 0:
            return 0.0
        return 0.6 + (s["peak"] - 0.6) * min(1.0, since / s["ramp_days"])
    s = STORIES["declining"]
    if base == s["item"]:
        return s["start"] + (s["end"] - s["start"]) * idx / (DAYS - 1)
    s = STORIES["outage"]
    if base == s["item"] and loc["num"] == s["loc"] and s["to_days_ago"] <= days_ago <= s["from_days_ago"]:
        return 0.0
    return 1.0

# =========================================================
# ORDER BUNDLE  (order, check, cart, payment, orderSummary, transactionDetail,
#                giftTransactionDetail, rewardHistory, signature)
# =========================================================

CARD_TYPES = (["VISA", "MASTERCARD", "AMEX", "DISCOVER"], [52, 26, 15, 7])
ENTRY = (["Chip Read", "Contactless", "Swipe", "Manual Entry"], [52, 38, 6, 4])
AREA_CODES = {"PA": ["215", "267", "610", "484", "412", "717", "570", "814"], "NJ": ["856", "201", "609", "551"],
              "NY": ["212", "718", "917", "716", "518"], "DE": ["302"], "MD": ["410", "443"]}
COMP_REASONS = ["MANAGER COMP", "GUEST COMPLAINT", "EMPLOYEE MEAL 50%", "BIRTHDAY DESSERT", "LONG TICKET TIME"]
VOID_REASONS = (["CUSTOMER CHANGED MIND", "WRONG ITEM", "KITCHEN ERROR", "86'D", "MANAGER OVERRIDE"], [35, 30, 15, 10, 10])
VOID_REASONS_HOT = (["MANAGER OVERRIDE", "NO REASON", "CUSTOMER CHANGED MIND", "WRONG ITEM"], [40, 30, 20, 10])
REFUND_REASONS = ["WRONG ITEM", "MISSING ITEM", "FOOD QUALITY", "ORDER ARRIVED COLD", "DUPLICATE CHARGE"]

_CUSTOMERS, _UDP = {}, {}


def customer(loc, rng):
    k = rng.randint(0, max(50, int(loc["rate"] * 25)))
    key = (loc["idx"], k)
    c = _CUSTOMERS.get(key)
    if c is None:
        r = random.Random(f"{SEED}:cust:{loc['idx']}:{k}")
        first, last = r.choice(FIRST), r.choice(LAST)
        ac = r.choice(AREA_CODES.get(loc["state"], ["555"]))
        c = {"customerID": uid(r), "name": f"{first} {last}", "billingAddressID": uid(r),
             "email": f"{first.lower()}.{last.lower().replace(chr(39), '')}{r.randint(1, 99)}@example.com",
             "phone": f"({ac}) 555-01{r.randint(0, 99):02d}"}
        _CUSTOMERS[key] = c
    return c


def user_defined_pay_id(merchant, name):
    key = (merchant["idx"], name)
    if key not in _UDP:
        _UDP[key] = uid(random.Random(f"{SEED}:udp:{merchant['idx']}:{name}"))
    return _UDP[key]


def _pick_item(rng, merchant, loc, cat, idx, hol):
    c = merchant["cats"].get(cat)
    if not c:
        return None
    hmult = HOLIDAY_ITEMS.get(hol, {}).get(cat, 1.0) if hol else 1.0
    cands, ws = [], []
    for it in c["items"]:
        if it["created"] > idx:
            continue
        w = it["pop"] * story_mult(merchant, it, loc, idx)
        if w > 0:
            cands.append(it)
            ws.append(w * hmult)
    return rng.choices(cands, weights=ws)[0] if cands else None


def make_order(g):
    rng = random.Random(f"{SEED}:o:{g}")
    loc = pick_location(rng)
    m, spec, concept = loc["merchant"], loc["merchant"]["spec"], loc["merchant"]["concept"]
    idx = pick_day(rng, loc)
    d = day_of(idx)
    weekend = d.weekday() >= 5
    hol = HOLIDAY_BY_IDX[idx]
    hours, hw = HOUR_TABLES[concept][1 if weekend else 0]
    hour = rng.choices(hours, weights=hw)[0]
    open_ms = local_midnight_ms(loc["tz"], idx) + hour * 3_600_000 + rng.randint(0, 3599) * 1000
    progress = idx / (DAYS - 1)
    days_ago = DAYS - idx

    # ---- service area (dine-in / takeout / delivery / online) ----
    aw = []
    for a in loc["areas"]:
        w, t = a["w"], a["serviceAreaType"]
        if t == 4:
            w = 0 if (m["online"] is None or idx < m["online"]) else w * (1 + 0.8 * progress)
        elif t == 3 and a["serviceAreaName"] != "Delivery":
            w = 0 if (m["thirdParty"] is None or idx < m["thirdParty"]) else w * (1 + 0.5 * progress)
            if m["show"] and loc["num"] == STORIES["delivery_surge"]["loc"]:
                w *= 1 + (STORIES["delivery_surge"]["end_mult"] - 1) * max(0.0, (idx - (DAYS - 180)) / 180)
            if hol == "Super Bowl Sunday":
                w *= 1.8
        elif a["serviceAreaName"] == "Patio" and loc["cold"] and d.month not in (5, 6, 7, 8, 9):
            w = 0
        aw.append(w)
    area = rng.choices(loc["areas"], weights=aw)[0]
    at = area["serviceAreaType"]
    partner = at == 3 and area["serviceAreaName"] != "Delivery"
    online = at == 4 or partner
    app = "web" if online else m["appType"]

    if online:
        emp = {"empID": loc["onlineEmpID"], "first": "ONLINE", "last": "", "role": "Server", "roleID": m["roleIDs"]["Server"]}
        emp_name = "ONLINE "
        term = None
    else:
        emp = employee_at(loc, spec["takers"].get(at, spec["takers"][2]), idx, rng)
        emp_name = f"{emp['first']} {emp['last']}"
        term = rng.choice(loc["terminals"])
    guests = rng.choices(spec["guests"], weights=spec["guest_w"])[0]
    if at != 1:
        guests = min(guests, 4)

    # ---- build the ticket ----
    lines, seq = {}, [0]

    def add(item, offset, course=None):
        if item is None:
            return
        ln = lines.get(item["itemID"])
        if ln:
            ln["qty"] += 1
            return
        mods = []
        opts = MODIFIERS.get(item["cat"]["catName"])
        if opts and rng.random() < 0.3:
            mods = rng.sample(opts, k=min(len(opts), rng.choice([1, 1, 2])))
        seq[0] += 1
        lines[item["itemID"]] = {"item": item, "qty": 1, "mods": mods, "seq": seq[0], "offset": offset, "course": course}

    mains = [(c, w) for (c, w, h0, h1, wk) in spec["main"] if h0 <= hour <= h1 and (not wk or weekend)]
    n_mains = guests if "shared_mains" not in spec else max(1, round(guests / spec["shared_mains"] + rng.random() - 0.4))
    for _ in range(n_mains):
        cat = rng.choices([c for c, _ in mains], weights=[w for _, w in mains])[0] if mains else spec["main"][0][0]
        add(_pick_item(rng, m, loc, cat, idx, hol) or _pick_item(rng, m, loc, spec["main"][0][0], idx, hol),
            rng.uniform(1, 6), "Entrees")
    for (cat, p, h0, h1, areas, per) in spec["extras"]:
        if at not in areas or not (h0 <= hour <= h1):
            continue
        if hol and cat in HOLIDAY_ITEMS.get(hol, {}):
            p = min(0.95, p * HOLIDAY_ITEMS[hol][cat])
        course = "Desserts" if cat in ("DESSERTS", "POSTRES") else ("Appetizers" if "APPET" in cat or cat == "BAR BITES" else "Drinks")
        offset = None if course == "Desserts" else (rng.uniform(0, 3) if course != "Entrees" else rng.uniform(1, 6))
        for _ in range(guests if per == "guest" else 1):
            if rng.random() < p:
                add(_pick_item(rng, m, loc, cat, idx, hol), offset, course)
        if per == "ticket" and guests >= 4 and rng.random() < p * 0.3:
            add(_pick_item(rng, m, loc, cat, idx, hol), offset, course)
    if not lines:
        add(m["items"][0], 1, "Entrees")

    # ---- timing ----
    if at == 1:
        dur = rng.randint(40, 100) if concept != "bar" else rng.randint(25, 160)
    elif at == 2:
        dur = rng.randint(2, 9) if concept in ("cafe", "qsr", "truck") else rng.randint(10, 25)
    else:
        dur = rng.randint(15, 40)
    close_ms = open_ms + dur * 60_000

    stale = rng.random() < STALE_OPEN_RATE
    order_void = not stale and rng.random() < ORDER_VOID_RATE
    void_rate = STORIES["void_hotspot"]["rate"] if emp["empID"] == VOID_EMP_ID else BASE_VOID_RATE
    void_reasons = VOID_REASONS_HOT if emp["empID"] == VOID_EMP_ID else VOID_REASONS
    surcharge_on = m["surcharge"] is not None and idx >= m["surcharge"]
    happy = spec.get("happy_hour")

    order_id = uid(rng)
    est = (loc["cum"][idx - 1] if idx > 0 else 0.0) * SCALE + (hour - 6) / 18 * (loc["cum"][idx] - (loc["cum"][idx - 1] if idx else 0)) * SCALE
    order_number = loc["orderBase"] + int(est)
    if online:
        order_number_s = f"O{loc['onlineBase'] + int(est * 0.15)}"
        ref_number = order_number_s
    else:
        order_number_s = str(order_number)
        ref_number = str(order_number - loc["orderBase"] // 3)
    pay_ref = ref_number if online else f"{ref_number}T{term['num']}"
    terminal_id = term["terminalID"] if term else ""
    device_id = term["deviceID"] if term else ""
    build = build_number(app, d, rng) if app != "web" else None
    created_by = emp["empID"]
    cust = customer(loc, rng) if (online or (m["loyalty"] and rng.random() < 0.22)) else None

    # ---- split into checks ----
    n_checks = 1
    if at == 1 and guests >= 2 and rng.random() < 0.12:
        n_checks = min(guests, rng.choice([2, 2, 3]))
    checks = [{"checkID": uid(rng), "name": str(i + 1), "lines": []} for i in range(n_checks)]
    for i, ln in enumerate(sorted(lines.values(), key=lambda x: x["seq"])):
        checks[i % n_checks]["lines"].append(ln)
    checks = [c for c in checks if c["lines"]]

    docs = defaultdict(list)
    o_sub = o_tax = o_disc = o_total = o_paid = o_grat = 0.0
    paid_types_all = []
    tax_pct = loc["taxPercent"]

    for ck in checks:
        ck_sub = ck_tax = ck_disc = ck_void = 0.0
        cart_docs, summary_lines = [], []
        for ln in ck["lines"]:
            it = ln["item"]
            price = price_on(it, m, d)
            mod_total = sum(p for _, p in ln["mods"])
            qty = ln["qty"]
            sub = (price + mod_total) * qty
            disc_list, disc = [], 0.0
            if happy and it["cat"]["superCatName"] == ALC and happy[0] <= hour < happy[1]:
                disc = 1.0 * qty
                disc_list.append({"discountID": uid(rng), "discountName": "HAPPY HOUR", "discountType": "amount",
                                  "discountValue": 1, "discountAmount": disc})
            elif rng.random() < BASE_COMP_RATE:
                disc = sub
                disc_list.append({"discountID": uid(rng), "discountName": rng.choice(COMP_REASONS), "discountType": "percent",
                                  "discountValue": 100, "discountAmount": disc})
            voided = order_void or rng.random() < void_rate
            tax = (sub - disc) * tax_pct / 100
            off = ln["offset"] if ln["offset"] is not None else dur * rng.uniform(0.65, 0.8)
            if at != 1:
                off = rng.uniform(0, max(0.3, dur * 0.3))
            c_ms = min(open_ms + int(off * 60_000), close_ms - 150_000 if dur > 5 else open_ms + 20_000)
            cdate = stamp(c_ms, app, rng)
            sent = stamp(c_ms + rng.randint(20, 120) * 1000, app, rng)
            closed = 0 if stale else 1
            close_type = "void" if voided else ("" if stale else "paid")
            course = ln["course"] if spec["courses"] else None
            cart_id = uid(rng)
            cart = {
                "_id": f"cart:{cart_id}", "buildNumber": build or "", "cartID": cart_id, "catID": it["cat"]["catID"],
                "checkID": ck["checkID"], "closeType": close_type, "cog": round(price * it["cogRatio"], 2),
                "commChannel": m["commChannel"], "courseID": m["courseIDs"][course] if course else "",
                "courseName": course or "", "createdBy": created_by,
                "dateClosed": 0 if stale else stamp(close_ms, app, rng), "dateCreated": cdate,
                "dateModified": stamp(close_ms, app, rng), "deviceID": device_id, "discountAllowed": 1,
                "discountList": disc_list, "docType": "cart", "enableCourse": 1 if spec["courses"] else 0,
                "enableNegativeQty": 1, "gratuityAllowed": 1, "inventDeductCount": it["trackInventory"] * qty,
                "isClosed": closed, "isModified": 1, "isPaid": 0 if (voided or stale) else 1, "isPrinted": 1,
                "isPurged": 0, "isSent": 1, "itemID": it["itemID"], "itemPrice": price, "itemType": 0, "kdsList": [],
                "kitchenFireTime": sent, "kitchenName": it["name"] + it["trail"], "kitchenSentTime": sent,
                "kitchenStatus": 1, "locationID": loc["locationID"],
                "modifierList": [{"modifierID": uid(rng), "modifierName": n, "modifierPrice": p, "quantity": 1} for n, p in ln["mods"]],
                "name": it["name"], "oCartID": cart_id, "oItemPrice": price, "oTaxClassID": m["taxClassID"],
                "orderID": order_id, "originalQuantity": qty, "paidType": [], "positionList": [], "quantity": qty,
                "receiptName": it["name"] + it["trail"], "saleEmpName": emp_name, "saleReceivingEmp": created_by,
                "sentToFire": 1, "sentToKitchen": 1, "subTotal": sub, "superCatID": it["cat"]["superCatID"], "tax": tax,
                "taxClassID": m["taxClassID"], "taxClassName": "Default Tax", "taxExemptAllowed": 1, "taxPercent": tax_pct,
                "terminalID": terminal_id, "totalDiscount": disc, "trackInventory": it["trackInventory"], "unitID": "",
                "versionCode": 1,
            }
            if build is None:
                del cart["buildNumber"]
            if voided:
                cart["voidReason"] = "ORDER VOIDED" if order_void else rng.choices(*void_reasons)[0]
                cart["voidedBy"] = created_by
                ck_void += sub
            else:
                ck_sub += sub
                ck_tax += tax
                ck_disc += disc
                summary_lines.append((cart, it))
            cart_docs.append(cart)

        ck_total = ck_sub - ck_disc + ck_tax
        grat = 0.0
        if at == 1 and guests >= 6 and concept in ("casual", "mexican") and not order_void:
            grat = round((ck_sub - ck_disc) * 0.18, 2)
        ck_closed = not stale
        ck_close_type = "" if stale else ("void" if order_void else "paid")

        # ---- payment / tenders ----
        txns, tdetails, paid_types = [], [], []
        tip_total = 0.0
        surcharge_total = 0.0
        primary = None
        if ck_closed and not order_void and ck_total > 0:
            due = round(ck_total + grat, 2)
            if partner:
                tenders = [("other", due)]
            elif at == 4:
                tenders = [("creditCard", due)]
            else:
                cash_p = spec["cash_share"] + (0.06 if surcharge_on else 0)
                r = rng.random()
                if m["gift"] and r < 0.015:
                    tenders = [("metisGift", due)]
                elif r < cash_p:
                    tenders = [("cash", due)]
                elif r < cash_p + 0.03 and due > 20:
                    part = round(due * rng.uniform(0.3, 0.6), 2)
                    tenders = [("cash", part), ("creditCard", round(due - part, 2))]
                else:
                    tenders = [("creditCard", due)]
            pay_ms = open_ms + int(dur * rng.uniform(0.75, 0.95) * 60_000)
            for (ptype, amt) in tenders:
                txn_id = uid(rng)
                card = ptype == "creditCard"
                tip = 0.0
                if card:
                    if at == 1:
                        tip = 0.0 if rng.random() < 0.05 else round(amt * rng.uniform(0.15, 0.25), 2)
                    elif at == 4:
                        tip = round(amt * rng.uniform(0.1, 0.18), 2) if rng.random() < 0.45 else 0.0
                    elif at == 3:
                        tip = round(amt * rng.uniform(0.12, 0.2), 2) if rng.random() < 0.6 else 0.0
                    else:
                        tip = round(amt * rng.uniform(0.1, 0.2), 2) if rng.random() < (0.3 if surcharge_on else 0.36) else 0.0
                surcharge = (amt + tip) * m["surchargeRate"] / 100 if (card and surcharge_on) else 0.0
                surcharge_total += surcharge
                tip_total += tip
                gw = m["gateway"] if card else {"cash": "cash", "metisGift": "metisGift", "other": "other"}[ptype]
                gw_id = loc["gatewayID"] if card else (loc["giftGatewayID"] if ptype == "metisGift" else loc["cashGatewayID"])
                ctype = rng.choices(*CARD_TYPES)[0] if card else ""
                auth = fake_digits(rng, 6) if card else ""
                tref = fake_digits(rng, 12) if card else ""
                t_ms = stamp(pay_ms, app, rng)
                txn = {
                    "actualPaidAmount": amt, "aid": "", "appType": app, "authCode": auth, "batchClosed": 1 if days_ago > 1 else 0,
                    "cardHolderName": "", "cardNumber": "", "cardType": ctype, "cashDrawerID": "", "cashDrawerName": "",
                    "cashierID": created_by, "cashierRoleID": emp["roleID"], "changeAmount": round(rng.choice([0, 0, 0.5, 1.25, 3.75]), 2) if ptype == "cash" else 0,
                    "checkID": ck["checkID"], "commChannel": m["commChannel"], "createdBy": created_by, "currentAction": "payment",
                    "customerID": cust["customerID"] if cust else "", "dateCreated": t_ms, "dateModified": 0,
                    "entryMethod": ("Card Not Present" if online else rng.choices(*ENTRY)[0]) if card else "",
                    "gateWay": gw, "gatewayID": gw_id, "hostCode": "", "isForReturn": 0, "isForSettle": 0, "isPurged": 0,
                    "merchantID": loc["mid"] if card or ptype == "cash" else "", "modifiedBy": "", "paidAmount": amt,
                    "paymentType": ptype, "paymentTypeName": area["serviceAreaName"].upper() if partner else "",
                    "referenceNumber": "", "responseCode": "00" if card else "", "roleID": emp["roleID"],
                    "taxAmount": ck_tax if len(tenders) == 1 else 0, "terminalID": terminal_id,
                    "token": fake_token(rng) if card else "", "transactionID": txn_id, "transactionNumber": "",
                    "transactionRef": tref, "transactionType": "pay",
                    "userDefinedPayID": user_defined_pay_id(m, area["serviceAreaName"]) if partner else "", "versionCode": 1,
                }
                txns.append(txn)
                paid_types.append(ptype)
                primary = primary or (txn, ptype, ctype, auth, tref, tip, surcharge)
                if card and tip > 0 and at == 1:
                    tip_txn = dict(txn, actualPaidAmount=0, currentAction="tipAdd", transactionType="tipAdd", paidAmount=tip,
                                   transactionID=uid(rng), parentTransactionID=txn_id, authCode="", responseCode="",
                                   dateCreated=stamp(pay_ms + rng.randint(5, 180) * 60_000, app, rng), taxAmount=0)
                    txns.append(tip_txn)
                elif ptype == "cash" and at == 1 and rng.random() < 0.3:
                    txns.append(dict(txn, actualPaidAmount=0, currentAction="tipAdd", transactionType="tipAdd", paidAmount=0,
                                     transactionID=uid(rng), parentTransactionID=txn_id, merchantID="", taxAmount=0,
                                     dateCreated=stamp(pay_ms + rng.randint(5, 180) * 60_000, app, rng)))
                if card:
                    if rng.random() < 0.025:      # declined first attempt
                        tdetails.append(("decline", amt, txn, ctype))
                    tdetails.append(("approve", amt + tip + surcharge, txn, ctype, surcharge))
                if ptype == "metisGift":
                    docs["giftTransactionDetail"].append(_gift_doc(rng, loc, m, order_id, amt, "redeem", t_ms, created_by, emp["roleID"], terminal_id, txn_id))
            # refunds (more common on delivery / online)
            if rng.random() < (0.012 if online else 0.003):
                rf = summary_lines[0][0]["subTotal"] if summary_lines else 0
                if rf:
                    base_txn = txns[0]
                    txns.append(dict(base_txn, actualPaidAmount=rf, paidAmount=rf, currentAction="refund", transactionType="refund",
                                     isForReturn=1, transactionID=uid(rng), parentTransactionID=base_txn["transactionID"],
                                     dateCreated=stamp(close_ms + rng.randint(1, 48) * 3_600_000, app, rng),
                                     refundReason=rng.choice(REFUND_REASONS)))

        # ---- check doc ----
        paid_amt = round(ck_total + grat, 2) if ck_closed and not order_void else 0
        check = {
            "_id": f"check:{ck['checkID']}", "adjustedAmount": 0, "balanceAmount": 0 if ck_closed else round(ck_total, 2),
            "checkID": ck["checkID"], "checkName": ck["name"], "closeType": ck_close_type, "commChannel": m["commChannel"],
            "createdBy": created_by, "dateCreated": stamp(open_ms, app, rng), "dateModified": stamp(close_ms, app, rng),
            "deviceID": device_id, "docType": "check", "extraChargeTax": 0, "gratuityAmount": grat,
            "isClosed": 1 if ck_closed else 0, "isModified": 1, "isPrinted": 1 if ck_closed else 0, "isPurged": 0,
            "locationID": loc["locationID"], "orderID": order_id, "paidAmount": paid_amt, "paidType": sorted(set(paid_types)),
            "roundedVal": 0, "subTotal": ck_sub, "tax": ck_tax, "total": ck_total, "totalDiscount": ck_disc,
            "totalExtraCharge": 0, "versionCode": 1, "voidedBalance": ck_void,
        }
        docs["check"].append(check)
        for c in cart_docs:
            c["paidType"] = sorted(set(paid_types))
            docs["cart"].append(c)

        if txns:
            pay_id = uid(rng)
            docs["payment"].append({
                "_id": f"payment:{pay_id}", "actualPaidAmount": paid_amt, "appType": app, "batchClosed": 1 if days_ago > 1 else 0,
                **({"buildNumber": build} if build else {}), "checkID": ck["checkID"], "checkName": ck["name"],
                "commChannel": m["commChannel"], "createdBy": created_by, "dateClosed": stamp(close_ms, app, rng),
                "dateCreated": stamp(close_ms, app, rng), "dateModified": max([stamp(close_ms, app, rng)] + [t["dateCreated"] for t in txns]),
                "discountAmount": ck_disc, "docType": "payment", "empName": emp_name, "isClosed": 1, "isModified": 1,
                "isPurged": 0, "locationID": loc["locationID"], "modifiedBy": created_by, "orderID": order_id,
                "orderNumber": order_number_s, "paymentID": pay_id, "refNumber": pay_ref, "roleID": emp["roleID"],
                "roundedVal": 0, "saleReceivingEmp": created_by, "splitCount": n_checks, "taxAmount": ck_tax,
                "terminalID": terminal_id, "tipAmount": tip_total, "tipReceivingEmp": created_by,
                "totalAmount": paid_amt, "totalPaidAmount": paid_amt, "transactionList": txns, "versionCode": 1,
            })
            # card processor detail
            for td in tdetails:
                docs["transactionDetail"].append(_txn_detail(rng, td, loc, m, order_id, pay_ref, ck["checkID"], app,
                                                             created_by, emp["roleID"], terminal_id))
            # summary (per check)
            ptxn, ptype, ctype, auth, tref, ptip, psur = primary
            os_id = uid(rng)
            docs["orderSummary"].append({
                "_id": f"orderSummary:{os_id}", "appType": app, "authCode": auth,
                "billingAddressID": cust["billingAddressID"] if (cust and online) else "",
                "cardHolderName": cust["name"] if (cust and ptype == "creditCard" and online) else "",
                "cardNumber": f"XXXXXXXXXXXX{fake_digits(rng, 4)}" if ptype == "creditCard" else "", "cardType": ctype,
                "cartList": [{
                    "cartID": c["cartID"], "cartTerminalID": "", "catID": it["cat"]["catID"], "catName": it["cat"]["catName"],
                    "checkID": ck["checkID"], "closeType": "paid", "courseID": c["courseID"], "courseName": c["courseName"],
                    "createdBy": created_by, "createdByName": "", "dateCreated": c["dateCreated"], "discountList": c["discountList"],
                    "enableNegativeQty": 1, "inventDeductCount": c["inventDeductCount"], "isClosed": 1, "itemID": c["itemID"],
                    "itemPrice": c["itemPrice"], "itemType": 0, "kdsList": [], "modifierList": c["modifierList"], "name": c["name"],
                    "parentCartID": "", "quantity": c["quantity"], "subTotal": c["subTotal"], "superCatID": it["cat"]["superCatID"],
                    "superCatName": it["cat"]["superCatName"], "tax": c["tax"], "taxClassID": m["taxClassID"], "taxClassList": [],
                    "taxClassName": "Default Tax", "taxExemptAllowed": "", "taxInclusive": 0, "totalDiscount": c["totalDiscount"],
                    "trackInventory": c["trackInventory"], "unitID": ""} for (c, it) in summary_lines],
                "checkID": ck["checkID"], "checkName": ck["name"], "checkTotalDiscount": ck_disc, "checkTotalSurcharge": surcharge_total,
                "closeType": "paid", "commChannel": m["commChannel"], "createdBy": created_by,
                "custEmail": cust["email"] if (cust and online) else "", "custPhoneNum": cust["phone"] if (cust and online) else "",
                "customerName": cust["name"] if (cust and online) else "", "dateClosed": stamp(close_ms, app, rng),
                "dateCreated": stamp(close_ms, app, rng), "deliveryDate": stamp(close_ms, app, rng) if online else 0,
                "docType": "orderSummary", "empName": emp_name, "extraChargeList": [], "gateWay": ptxn["gateWay"],
                "gatewayID": ptxn["gatewayID"], "guest": guests, "isClosed": 1, "isForSettle": 0, "isOrderClosed": 1,
                "isPurged": 0, "locationID": loc["locationID"], "orderDateCreated": stamp(open_ms, app, rng), "orderID": order_id,
                "orderNumber": order_number_s, "orderSummaryID": os_id, "osDateCreated": stamp(close_ms + 3000, app, rng),
                "osVersion": 1.1, "paidAmount": paid_amt, "paidSurcharge": round(paid_amt * m["surchargeRate"] / 100, 2) if psur else 0,
                "paidTax": round(ck_tax, 2), "paymentID": pay_id,
                "paymentType": {"other": "other", "cash": "cash", "metisGift": "metisGift"}.get(ptype, "creditCard"),
                "refNumber": pay_ref, "roleID": emp["roleID"], "roleName": emp["role"], "saleEmpName": emp_name,
                "saleReceivingEmp": created_by, "saleReceivingRoleID": emp["roleID"], "saleReceivingRoleName": emp["role"],
                "serviceAreaID": area["serviceAreaID"], "serviceAreaName": area["serviceAreaName"],
                "serviceAreaType": at, "subTotal": ck_sub, "surchargeLabel": m["surchargeLabel"] if psur else "",
                "tax": ck_tax, "tipAmount": tip_total, "tipEmpName": emp_name, "tipReceivingRoleID": emp["roleID"],
                "tipReceivingRoleName": emp["role"], "tipSurcharge": round(tip_total * m["surchargeRate"] / 100, 2) if psur else 0,
                "tipedAt": stamp(close_ms, app, rng), "token": ptxn["token"], "total": paid_amt, "totalExtraCharge": 0,
                "totalPaidAmount": paid_amt, "totalSurcharge": surcharge_total, "transactionID": os_id,
                "transactionRef": tref, "transactionType": "pay",
            })
            if m["sigCapture"] and ptype == "creditCard" and not online and paid_amt >= 25 and rng.random() < 0.35:
                sid = uid(rng)
                docs["signature"].append({
                    "_id": f"signature:{sid}", "checkID": ck["checkID"], "commChannel": m["commChannel"],
                    "dateCreated": stamp(close_ms, "posLiteAndroid", rng), "docType": "signature",
                    "isArchived": 1 if days_ago > 180 else 0, "isPurged": 0, "locationID": loc["locationID"], "orderID": order_id,
                    "signature": {"@type": "blob", "content_type": "image/jpeg",
                                  "digest": "sha1-" + base64.b64encode(rng.getrandbits(160).to_bytes(20, "big")).decode(),
                                  "length": rng.randint(6000, 22000)},
                    "transactionID": ptxn["transactionID"]})

        o_sub += ck_sub
        o_tax += ck_tax
        o_disc += ck_disc
        o_total += ck_total
        o_paid += paid_amt
        o_grat += grat
        paid_types_all += paid_types

    # ---- order doc ----
    order = {
        "_id": f"order:{order_id}", "adjustedAmount": 0, "appType": app, "balanceAmount": o_total if stale else 0,
        "batchClosed": 1 if (days_ago > 1 and not stale) else 0, **({"buildNumber": build} if build else {}),
        "closeType": "" if stale else ("void" if order_void else "paid"), "commChannel": m["commChannel"],
        "createdBy": created_by, "dateCreated": stamp(open_ms, app, rng), "dateModified": stamp(close_ms, app, rng),
        "deviceID": device_id, "docType": "order", "empName": emp_name, "gratuityAmount": o_grat, "guest": guests,
        "isAllSentToKitchen": 1, "isClosed": 0 if stale else 1, "isConfirmed": 1, "isModified": 1, "isPurged": 0,
        "kitchenStatus": 1, "locationID": loc["locationID"], "modifiedBy": "", "orderID": order_id,
        "orderNumber": order_number_s, "paidAmount": o_paid, "paidType": sorted(set(paid_types_all)), "refNumber": ref_number,
        "roleID": emp["roleID"], "serviceAreaID": area["serviceAreaID"], "serviceAreaName": area["serviceAreaName"],
        "serviceAreaType": at, "splitCount": len(checks), "status": 1, "subTotal": o_sub, "tax": o_tax,
        "terminalID": terminal_id, "total": o_total, "totalDiscount": o_disc, "versionCode": 1,
    }
    if not stale:
        order["dateClosed"] = stamp(close_ms, app, rng)
    docs["order"].append(order)

    # ---- loyalty / gift side docs ----
    if cust and m["loyalty"] and not stale and not order_void:
        pts = int(o_sub - o_disc)
        rh_id = uid(rng)
        docs["rewardHistory"].append({
            "_id": f"rewardHistory:{rh_id}", "appType": app, "availablePoints": 0, "checkID": checks[0]["checkID"] if checks else "",
            "commChannel": m["commChannel"], "createdBy": created_by, "customerID": cust["customerID"],
            "dateCreated": stamp(close_ms, app, rng), "dateModified": stamp(close_ms, app, rng), "discountAmount": 0,
            "docType": "rewardHistory", "historyType": "added", "isModified": 1, "isPurged": 0, "locationID": loc["locationID"],
            "points": pts, "rewardHistoryID": rh_id})
        if rng.random() < 0.08:
            rh2 = uid(rng)
            amt = rng.choice([5, 10])
            docs["rewardHistory"].append({
                "_id": f"rewardHistory:{rh2}", "appType": app, "availablePoints": 0, "checkID": checks[0]["checkID"],
                "commChannel": m["commChannel"], "createdBy": created_by, "customerID": cust["customerID"],
                "dateCreated": stamp(close_ms, app, rng), "dateModified": stamp(close_ms, app, rng), "discountAmount": amt,
                "docType": "rewardHistory", "historyType": "redeemed", "isModified": 1, "isPurged": 0,
                "locationID": loc["locationID"], "points": -amt * 100, "rewardHistoryID": rh2})
    if m["gift"] and not online and not stale and rng.random() < 0.004:
        docs["giftTransactionDetail"].append(_gift_doc(rng, loc, m, order_id, rng.choice([25, 50, 50, 75, 100]), "issue",
                                                       stamp(close_ms, app, rng), created_by, emp["roleID"], terminal_id, None))
    return docs


def _gift_doc(rng, loc, m, order_id, amount, trans_type, ms, created_by, role_id, terminal_id, txn_id):
    tid = txn_id or uid(rng)
    return {
        "_id": f"giftTransactionDetail:{tid}", "account": "", "aid": "", "amount": amount, "applicationCrytptogram": "",
        "applicationId": "", "applicationName": "", "approvalCode": "", "authCode": "", "balanceAmount": 0,
        "cardHolderName": "", "cardType": "metisGift", "commChannel": m["commChannel"], "command": "", "createdBy": created_by,
        "customerID": "", "dateCreated": ms, "dateModified": ms + rng.randint(0, 3), "docType": "giftTransactionDetail",
        "dueAmount": 0, "entryMethod": "swipe", "gateWay": "metisGift", "gatewayID": loc["giftGatewayID"],
        "giftBalance": round(rng.uniform(0, 150), 2) if trans_type == "redeem" else amount,
        "giftCardID": uid(random.Random(f"{SEED}:gift:{loc['idx']}:{rng.randint(0, 500)}")),
        "isArchived": 1 if ms < local_midnight_ms(loc["tz"], max(0, DAYS - 180)) else 0, "isBarTabRefunded": 0,
        "isForBarTab": 0, "isModified": 1, "isPurged": 0, "locationID": loc["locationID"],
        "maskedCardNumber": f"XXXXXXXXXXXX{fake_digits(rng, 4)}", "merchantID": "", "orderID": order_id, "orderRefNum": "",
        "paymentType": "metisGift", "referenceNumber": "", "responseCode": "", "responseText": "", "roleID": role_id,
        "terminalID": terminal_id, "token": fake_token(rng), "totalAmount": 0, "transType": trans_type,
        "transactionID": tid, "transactionNumber": "", "transactionRef": "", "version": ""}


def _txn_detail(rng, td, loc, m, order_id, ref, check_id, app, created_by, role_id, terminal_id):
    kind, amt, txn, ctype = td[0], td[1], td[2], td[3]
    surcharge = td[4] if len(td) > 4 else 0.0
    tid = txn["transactionID"] if kind == "approve" else uid(rng)
    ok = kind == "approve"
    return {
        "_id": f"transactionDetail:{tid}", "account": f"XXXXXXXXXXXX{fake_digits(rng, 4)}", "aid": f"A00000000{rng.randint(3, 4)}1010",
        "amount": round(amt, 2), "appType": app, "applicationCrytptogram": "", "applicationId": "", "applicationName": "",
        "approvalCode": "00" if ok else "05", "authCode": txn["authCode"] if ok else "", "batchID": fake_digits(rng, 6),
        "cardHolderName": "", "cardType": ctype, "cashDrawerID": "", "ccTerminalModel": "", "ccTerminalType": "",
        "checkID": check_id, "commChannel": m["commChannel"], "command": "", "createdBy": created_by,
        "dateCreated": txn["dateCreated"] - (0 if ok else 40_000), "dateModified": txn["dateCreated"],
        "docType": "transactionDetail", "dueAmount": 0, "entryMethod": txn["entryMethod"], "gateWay": m["gateway"],
        "gatewayID": loc["gatewayID"], "giftBalance": 0, "hostOrderID": "", "isBarTabRefunded": 0, "isForBarTab": 0,
        "isModified": 1, "isPurged": 0, "locationID": loc["locationID"], "maskedCardNumber": f"XXXXXXXXXXXX{fake_digits(rng, 4)}",
        "merchantID": loc["mid"], "orderID": order_id, "orderRefNum": ref, "paymentType": "creditCard",
        "referenceNumber": "", "responseCode": "00" if ok else "05", "responseText": "Approval" if ok else "Do not honor",
        "responseTransRef": fake_digits(rng, 12), "roleID": role_id, "surChargeFee": round(surcharge, 2),
        "terminalID": terminal_id, "token": fake_token(rng), "tokenExpiry": f"{rng.randint(1, 12):02d}{rng.randint(26, 31)}",
        "totalAmount": amt, "transType": "pay", "transactionID": tid, "transactionNumber": "",
        "transactionRef": txn["transactionRef"] if ok else "", "version": "", "versionCode": 1}

# =========================================================
# DAILY DOCS  (one unit = one location-day)
# =========================================================

AVG_TICKET = {"casual": 62, "mexican": 45, "pizzeria": 34, "cafe": 11, "qsr": 15, "bar": 38, "truck": 16, "asian": 32}
ROLE_SHIFT = {"Manager": (9, 9), "General Manager": (9, 9), "Owner": (9, 10), "Server": (15, 6), "Bartender": (15, 7),
              "Host": (16, 5), "Cashier": (10, 7), "Barista": (6, 7), "Driver": (16, 5)}
VENDORS = ["", "", "SYSCO", "US FOODS", "PERFORMANCE FOOD GROUP", "RESTAURANT DEPOT", "LOCAL PRODUCE CO"]

_ranges, _acc = [], 0
for _l in LOCATIONS:
    s_, e_ = max(0, _l["open"]), (_l["churn"] if _l["churn"] is not None else DAYS)
    n_ = max(0, e_ - s_)
    _ranges.append((_acc, s_))
    _acc += n_
DAILY_STARTS = [r[0] for r in _ranges]
DAILY_TOTAL = _acc


def _audit(rng, loc, m, ms, new_val, old_val, created_by, module="configuration"):
    aid = uid(rng)
    cfg = uid(rng)
    return {"_id": f"auditLog:{aid}", "auditLogID": aid,
            "change": [{"configID": cfg, "newVal": new_val, "oldVal": f"{cfg}:{old_val}" if old_val else ""}],
            "commChannel": f"A{m['commChannel']}" if m["commChannel"][0].isdigit() else m["commChannel"],
            "createdBy": created_by, "dateCreated": ms, "docType": "auditLog", "isPurged": 0,
            "locationID": loc["locationID"], "module": module, "source": "portal", "status": 1}


def _catalog_log(rng, loc, m, ms, item, key_changes, fields, app="web", push=None):
    lid = uid(rng)
    return {"_id": f"catalogLog:{lid}", "appType": app, "commChannel": m["commChannel"], "createdBy": loc["owner"]["empID"],
            "dateCreated": ms, "docType": "catalogLog", "isPurged": 0, "keyChanges": key_changes,
            "locationID": loc["locationID"], "logID": lid, "masterDocType": "item", "masterItemID": item["itemID"],
            "masterName": item["name"], "masterType": "item", "modifiedFields": fields, "pushToAll": 0,
            "pushToChildLocationList": push or []}


def make_daily(u):
    rng = random.Random(f"{SEED}:d:{u}")
    li = bisect.bisect_right(DAILY_STARTS, u) - 1
    loc = LOCATIONS[li]
    idx = _ranges[li][1] + (u - DAILY_STARTS[li])
    m, concept = loc["merchant"], loc["merchant"]["concept"]
    d = day_of(idx)
    days_ago = DAYS - idx
    midnight = local_midnight_ms(loc["tz"], idx)
    app = m["appType"]
    dayw = (loc["cum"][idx] - (loc["cum"][idx - 1] if idx else 0)) * SCALE
    docs = defaultdict(list)

    # ---- batch close: one per terminal per business day ----
    if dayw > 0 and days_ago > 1:
        card_vol = dayw * AVG_TICKET[concept] * (1 - m["spec"]["cash_share"]) * 0.85
        for t in loc["terminals"]:
            target = card_vol / len(loc["terminals"]) * rng.uniform(0.7, 1.3)
            cnt = max(0, int(target / (AVG_TICKET[concept] * 1.15)))
            share = 0.0
            for _ in range(cnt):                      # sum of cent-rounded tickets -> float artifacts like the source
                share += round(AVG_TICKET[concept] * 1.15 * rng.uniform(0.3, 1.9), 2)
            bid = uid(rng)
            ms = stamp(midnight + rng.randint(22 * 60, 27 * 60) * 60_000, app, rng)
            refunds = rng.choice([0, 0, 0, 0, 1])
            docs["batchClose"].append({
                "_id": f"batchClose:{bid}", "appType": app, "batchCloseID": bid, "batchCloseTime": ms,
                "batchNum": str(loc["batchBase"] + idx * len(loc["terminals"]) + t["num"]),
                "batchTotal": share, "commChannel": m["commChannel"],
                "createdBy": loc["owner"]["empID"], "creditAmount": share, "creditCount": cnt,
                "dateCreated": ms, "debitCount": 0, "docType": "batchClose", "ebtCount": 0, "gateway": m["gateway"],
                "gatewayID": loc["gatewayID"], "giftCount": 0, "isPurged": 0, "isSource": 0,
                "isSuccess": 0 if rng.random() < 0.004 else 1, "locationID": loc["locationID"], "mid": loc["mid"],
                "modifiedBy": "", "refundCount": refunds, "serialNumber": t["serialNumber"], "terminalID": t["terminalID"],
                "terminalName": t["terminalName"], "tipCount": int(cnt * 0.6) if concept in ("casual", "mexican", "bar") else int(cnt * 0.25),
                "totalRefund": round(rng.uniform(8, 60), 2) if refunds else 0, "totalTransactions": 0, "voidCount": rng.choice([0, 0, 0, 1])})

    # ---- time clock ----
    if dayw > 0:
        load = min(1.2, dayw / max(1.0, loc["rate"] * SCALE))
        for role, slots in loc["slots"].items():
            n_on = min(len(slots), max(1, round(len(slots) * (0.35 + 0.35 * load))))
            for slot in rng.sample(slots, n_on):
                i = max(0, bisect.bisect_right(slot["starts"], idx) - 1)
                e = slot["emps"][i]
                start_h, length = ROLE_SHIFT.get(role, (10, 7))
                cin = midnight + int((start_h + rng.uniform(-1.5, 1.5)) * 3_600_000)
                hours = max(2.5, length + rng.uniform(-2, 2))
                cout = cin + int(hours * 3_600_000)
                open_now = days_ago == 1 and rng.random() < 0.25
                tid = uid(rng)
                docs["timeManagement"].append({
                    "_id": f"timeManagement:{tid}", "appType": app, "clockIn": stamp(cin, app, rng),
                    "clockOut": 0 if open_now else stamp(cout, app, rng), "commChannel": m["commChannel"],
                    "docType": "timeManagement", "empID": e["empID"], "fileNumber": "0", "hourlyRate": e["rate"],
                    "isPurged": 0, "locationID": loc["locationID"], "otHourlyRate": round(e["rate"] * 1.5, 2) if e["rate"] else 0,
                    "payRollId": "", "roleID": e["roleID"], "status": "clockIn" if open_now else "clockOut",
                    "terminalID": rng.choice(loc["terminals"])["terminalID"], "timeID": tid,
                    "workingHours": 0 if open_now else (cout - cin) / 3_600_000})

    # ---- inventory receipts (Tuesday deliveries) + the salmon 86 story ----
    out = STORIES["outage"]
    is_out_loc = m["show"] and loc["num"] == out["loc"]
    if m["inventory"] and (d.weekday() == 1 or (is_out_loc and days_ago in (out["from_days_ago"], out["to_days_ago"] - 1))):
        for it in m["items"]:
            if not it["trackInventory"] and not (is_out_loc and it["base"] == out["item"]):
                continue
            entry, qty, remaining = "received", rng.randint(12, 90), rng.randint(5, 40)
            if is_out_loc and it["base"] == out["item"]:
                if days_ago == out["from_days_ago"]:
                    entry, qty, remaining = "adjustment", 0, 0
                elif days_ago == out["to_days_ago"] - 1:
                    entry, qty, remaining = "received", 60, 60
                elif d.weekday() != 1:
                    continue
            sh = uid(rng)
            cog = round(price_on(it, m, d) * max(it["cogRatio"], 0.3), 2)
            docs["stockHistory"].append({
                "_id": f"stockHistory:{sh}", "alu": "", "cog": cog, "commChannel": m["commChannel"],
                "createdBy": loc["owner"]["empID"], "dateCreated": stamp(midnight + rng.randint(7, 11) * 3_600_000, "web", rng),
                "docType": "stockHistory", "entryType": entry, "isPurged": 0, "itemID": it["itemID"], "locationID": loc["locationID"],
                "manufactureID": "", "manufactureName": "", "margin": round(1 - cog / price_on(it, m, d), 4) if cog else 0,
                "modified": "", "modifiedBy": "", "note": "OUT OF STOCK" if entry == "adjustment" else "", "qtyReceived": qty,
                "qtyRemaining": remaining, "reorderPoint": 10, "size": "", "sku": "", "stockHistoryID": sh,
                "stockID": uid(random.Random(f"{SEED}:stock:{loc['idx']}:{it['itemID']}")), "stockType": "web", "unit": "",
                "unitQty": 0, "upc": "", "vendorCode": "", "vendorID": "", "vendorName": rng.choice(VENDORS)})

    # ---- catalog changes ----
    first_loc = loc["num"] == 1
    if first_loc and d.month == 1 and d.day == 2 and m["annualRaise"] > 0:      # yearly price increase
        children = [l["locationID"] for l in m["locations"][1:]]
        for it in m["items"]:
            old, new = price_on(it, m, d - timedelta(days=3)), price_on(it, m, d)
            if old != new:
                docs["catalogLog"].append(_catalog_log(rng, loc, m, stamp(midnight + 9 * 3_600_000, "web", rng), it, "itemPrice",
                                                       [f"key:itemPrice##oldValue:{old}##updatedValue:{new}"], push=children))
    if m["show"] and first_loc and idx == idx_of_days_ago(STORIES["new_item"]["launch_days_ago"]):
        it = m["itemByBase"][STORIES["new_item"]["item"]]
        docs["catalogLog"].append(_catalog_log(rng, loc, m, stamp(midnight + 8 * 3_600_000, "web", rng), it, "name,itemPrice,catID",
                                               [f"key:name##oldValue:##updatedValue:{it['name']}",
                                                f"key:itemPrice##oldValue:0.0##updatedValue:{it['price']}"],
                                               push=[l["locationID"] for l in m["locations"][1:]]))
    if is_out_loc and days_ago in (out["from_days_ago"], out["to_days_ago"] - 1):
        it = m["itemByBase"][out["item"]]
        going_out = days_ago == out["from_days_ago"]
        docs["catalogLog"].append(_catalog_log(rng, loc, m, stamp(midnight + rng.randint(11, 18) * 3_600_000, app, rng), it,
                                               "itemCount,enableItemCount",
                                               [f"key:itemCount##oldValue:{'4.0' if going_out else '0.0'}##updatedValue:{'0.0' if going_out else '60.0'}",
                                                f"key:enableItemCount##oldValue:{'0.0' if going_out else '1.0'}##updatedValue:{'1.0' if going_out else '0.0'}"],
                                               app=app))
    if rng.random() < 0.004 and m["items"]:
        it = rng.choice(m["items"])
        a, b = rng.randint(1, 20), rng.randint(0, 20)
        docs["catalogLog"].append(_catalog_log(rng, loc, m, stamp(midnight + rng.randint(10, 20) * 3_600_000, app, rng), it,
                                               "itemCount,enableItemCount",
                                               [f"key:itemCount##oldValue:{a}.0##updatedValue:{b}.0",
                                                "key:enableItemCount##oldValue:0.0##updatedValue:1.0"], app=app))

    # ---- configuration audit trail (feature adoption, churn) ----
    if first_loc:
        ms = stamp(midnight + rng.randint(9, 17) * 3_600_000, "web", rng)
        owner = loc["owner"]["empID"]
        if m["surcharge"] == idx:
            docs["auditLog"].append(_audit(rng, loc, m, ms, f"{m['surchargeRate']}% PROCESSING SURCHARGE IS ADDED FOR CREDIT CARD PAYMENTS", "", owner))
        if m["online"] == idx:
            docs["auditLog"].append(_audit(rng, loc, m, ms, "ONLINE ORDERING ENABLED", "ONLINE ORDERING DISABLED", owner))
        if m["thirdParty"] == idx:
            docs["auditLog"].append(_audit(rng, loc, m, ms, "THIRD PARTY DELIVERY INTEGRATION ENABLED", "", owner, module="integration"))
        if m["churn"] is not None and idx == m["churn"] - 1:
            docs["auditLog"].append(_audit(rng, loc, m, ms, "ACCOUNT DEACTIVATION REQUESTED", "ACTIVE", owner, module="account"))
        if rng.random() < 0.003:
            a, b = rng.choice([(15, 18), (18, 20), (20, 22), (18, 15)])
            docs["auditLog"].append(_audit(rng, loc, m, ms, f"TIP SUGGESTIONS {a}%,{a + 2}%,{b + 5}%", f"TIP SUGGESTIONS {b}%,{b + 2}%,{b + 5}%", owner))
    return docs

# =========================================================
# REFERENCE DOCS (assumed shapes) - one unit per merchant
# =========================================================

CONCEPT_LABEL = {"casual": "Full Service", "mexican": "Full Service", "asian": "Full Service", "pizzeria": "Pizzeria",
                 "cafe": "Cafe / Coffee", "qsr": "Quick Service", "bar": "Bar / Nightclub", "truck": "Food Truck"}


def make_reference(u):
    m = MERCHANTS[u]
    rng = random.Random(f"{SEED}:r:{u}")
    docs = defaultdict(list)
    parent = m["locations"][0]
    for loc in m["locations"]:
        docs["location"].append({
            "_id": f"location:{loc['locationID']}", "docType": "location", "locationID": loc["locationID"],
            "locationName": loc["locationName"], "businessName": m["name"], "commChannel": m["commChannel"],
            "parentLocationID": "" if loc is parent else parent["locationID"], "businessType": "restaurant",
            "restaurantType": CONCEPT_LABEL[m["concept"]], "address1": loc["address1"], "city": loc["city"],
            "state": loc["state"], "zip": loc["zip"], "country": "US", "timeZone": loc["tz"].key,
            "phone": f"({rng.choice(AREA_CODES.get(loc['state'], ['555']))}) 555-01{rng.randint(0, 99):02d}",
            "taxPercent": loc["taxPercent"], "currency": "USD", "gateway": m["gateway"], "mid": loc["mid"],
            "appType": m["appType"], "terminalCount": len(loc["terminals"]),
            "onlineOrderingEnabled": 1 if m["online"] is not None else 0,
            "surchargeEnabled": 1 if m["surcharge"] is not None else 0,
            "surchargePercent": m["surchargeRate"] if m["surcharge"] is not None else 0,
            "giftCardEnabled": 1 if m["gift"] else 0, "loyaltyEnabled": 1 if m["loyalty"] else 0,
            "isActive": 0 if loc["churn"] is not None else 1,
            "dateCreated": local_midnight_ms(loc["tz"], loc["open"]) + 10 * 3_600_000,
            "dateDeactivated": local_midnight_ms(loc["tz"], loc["churn"]) if loc["churn"] is not None else 0,
            "isPurged": 0})
        docs["employee"].append({
            "_id": f"employee:{loc['onlineEmpID']}", "docType": "employee", "empID": loc["onlineEmpID"], "firstName": "ONLINE",
            "lastName": "", "empName": "ONLINE ", "roleID": m["roleIDs"]["Server"], "roleName": "Server",
            "locationID": loc["locationID"], "commChannel": m["commChannel"], "hourlyRate": 0,
            "hireDate": local_midnight_ms(loc["tz"], loc["open"]), "terminationDate": 0, "isActive": 1,
            "dateCreated": local_midnight_ms(loc["tz"], loc["open"]), "isPurged": 0})
    for e in EMPLOYEES:
        if e["loc"]["merchant"] is not m:
            continue
        tz = e["loc"]["tz"]
        docs["employee"].append({
            "_id": f"employee:{e['empID']}", "docType": "employee", "empID": e["empID"], "firstName": e["first"],
            "lastName": e["last"], "empName": f"{e['first']} {e['last']}", "roleID": e["roleID"], "roleName": e["role"],
            "locationID": e["loc"]["locationID"], "commChannel": m["commChannel"], "hourlyRate": e["rate"],
            "hireDate": local_midnight_ms(tz, e["hire"]),
            "terminationDate": local_midnight_ms(tz, e["term"]) if e["term"] is not None and e["term"] < DAYS else 0,
            "isActive": 0 if (e["term"] is not None and e["term"] < DAYS) else 1,
            "dateCreated": local_midnight_ms(tz, e["hire"]), "isPurged": 0})
    children = [l["locationID"] for l in m["locations"][1:]]
    for it in m["items"]:
        created = local_midnight_ms(parent["tz"], it["created"]) + 9 * 3_600_000
        docs["item"].append({
            "_id": f"item:{it['itemID']}", "docType": "item", "itemID": it["itemID"], "name": it["name"],
            "kitchenName": it["name"] + it["trail"], "receiptName": it["name"] + it["trail"], "catID": it["cat"]["catID"],
            "catName": it["cat"]["catName"], "superCatID": it["cat"]["superCatID"], "superCatName": it["cat"]["superCatName"],
            "itemPrice": it["price"], "cog": round(it["price"] * it["cogRatio"], 2), "taxClassID": m["taxClassID"],
            "taxClassName": "Default Tax", "trackInventory": it["trackInventory"], "itemType": 0, "isActive": 1,
            "commChannel": m["commChannel"], "locationID": parent["locationID"], "pushToChildLocationList": children,
            "dateCreated": created, "dateModified": created, "isPurged": 0})
    return docs

# =========================================================
# COUCHBASE LOADER
# =========================================================

class Loader:
    def __init__(self, create=True):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions

        conn, user, pwd = os.environ.get("CB_CONNECTION_STRING"), os.environ.get("CB_USERNAME"), os.environ.get("CB_PASSWORD")
        if not (conn and user and pwd):
            raise SystemExit("Set CB_CONNECTION_STRING, CB_USERNAME and CB_PASSWORD in .env (or use --dry_run).")
        if os.environ.get("CB_CA_CERT_PATH"):
            os.environ["SSL_CERT_FILE"] = os.environ["CB_CA_CERT_PATH"]
        opts = ClusterOptions(PasswordAuthenticator(user, pwd), timeout_options=ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=10), query_timeout=timedelta(seconds=30), connect_timeout=timedelta(seconds=30)))
        self.cluster = Cluster(conn, opts)
        self.cluster.wait_until_ready(timedelta(seconds=60))
        self.bucket = self.cluster.bucket(args.bucket)
        if create and not args.no_create:
            self._ensure_collections()
        scope = self.bucket.scope(args.scope)
        self.cols = {name: scope.collection(name) for name in COLLECTIONS}

    def _ensure_collections(self):
        from couchbase.exceptions import CollectionAlreadyExistsException, ScopeAlreadyExistsException
        mgr = self.bucket.collections()
        try:
            mgr.create_scope(args.scope)
        except ScopeAlreadyExistsException:
            pass
        for name in COLLECTIONS:
            try:
                try:
                    mgr.create_collection(args.scope, name)
                except TypeError:
                    from couchbase.management.collections import CollectionSpec
                    mgr.create_collection(CollectionSpec(name, scope_name=args.scope))
                log.info(f"created collection {args.scope}.{name}")
            except CollectionAlreadyExistsException:
                pass
        for _ in range(30):
            scopes = {s.name: {c.name for c in s.collections} for s in mgr.get_all_scopes()}
            if set(COLLECTIONS) <= scopes.get(args.scope, set()):
                return
            time.sleep(1)
        log.warning("collections not visible in manifest yet; upserts will retry")

    def upsert_many(self, name, docs, retries=4):
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
# PHASE RUNNER
# =========================================================

PHASES = {
    "reference": (make_reference, lambda: (0, len(MERCHANTS))),
    "daily": (make_daily, lambda: (0, DAILY_TOTAL)),
    "orders": (make_order, lambda: (args.start_offset, NUM_ORDERS)),
}


class Progress:
    def __init__(self, label, total):
        self.label, self.total, self.n, self.t0 = label, max(1, total), 0, time.time()
        self.lock = threading.Lock()
        self.every = args.progress_every

    def tick(self):
        with self.lock:
            self.n += 1
            if self.n % self.every == 0 or self.n == self.total:
                el = time.time() - self.t0
                rate = self.n / el if el else 0
                eta = (self.total - self.n) / rate / 60 if rate else 0
                log.info(f"{self.label}: {self.n:,}/{self.total:,} ({self.n / self.total:.0%}) {rate:,.0f} units/s ETA {eta:,.0f} min")


def split_range(start, end, parts):
    count = end - start
    chunk = max(1, count // parts)
    out = []
    for t in range(parts):
        s_ = start + t * chunk
        e_ = s_ + chunk if t < parts - 1 else end
        if e_ > s_:
            out.append((s_, e_))
    return out


def worker(loader, maker, start, end, progress):
    bufs = {name: {} for name in COLLECTIONS}
    written = Counter()

    def flush(name):
        if bufs[name]:
            written[name] += loader.upsert_many(name, bufs[name])
            bufs[name] = {}

    for u in range(start, end):
        for doc_type, docs in maker(u).items():
            name = DOC_TO_COLLECTION[doc_type]
            for d in docs:
                bufs[name][d["_id"]] = dict(sorted(d.items()))   # source docs are key-sorted
            if len(bufs[name]) >= args.flush_size:
                flush(name)
        progress.tick()
    for name in COLLECTIONS:
        flush(name)
    return written


def run_range(phase, start, end, create=False, label="load"):
    maker = PHASES[phase][0]
    loader = Loader(create=create)
    progress = Progress(f"{phase} {label} [{start:,}-{end:,})", end - start)
    totals = Counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(worker, loader, maker, s_, e_, progress) for s_, e_ in split_range(start, end, args.workers)]
        for f in as_completed(futs):
            totals.update(f.result())
    return dict(totals)


def _proc_entry(job):
    phase, i, s_, e_ = job
    return run_range(phase, s_, e_, create=False, label=f"p{i:02d}")

# =========================================================
# DRY RUN REPORT
# =========================================================

def dry_run():
    t0 = time.time()
    n = min(args.dry_run_sample, NUM_ORDERS)
    step = max(1, NUM_ORDERS // n)
    st = defaultdict(Counter)
    samples = {}
    for g in range(0, n * step, step):
        b = make_order(g)
        for dt, docs in b.items():
            st["docs"][dt] += len(docs)
            if docs and dt not in samples:
                samples[dt] = docs[0]
        o = b["order"][0]
        loc = next(l for l in LOCATIONS if l["locationID"] == o["locationID"])
        y = datetime.fromtimestamp(o["dateCreated"] / 1000).year
        st["year"][y] += 1
        st["concept"][loc["merchant"]["concept"]] += 1
        st["concept_sales"][loc["merchant"]["concept"]] += o["subTotal"] - o["totalDiscount"]
        st["area"][o["serviceAreaName"]] += 1
        st["online_by_year"][(y, o["serviceAreaType"] in (3, 4))] += 1
        st["app"][o["appType"]] += 1
        st["close"][o["closeType"] or "open"] += 1
        for p in b["payment"]:
            for t in p["transactionList"]:
                st["tender"][t["paymentType"]] += 1 if t["transactionType"] == "pay" else 0
    gen_rate = n / (time.time() - t0)
    dn = min(2000, DAILY_TOTAL)
    dstep = max(1, DAILY_TOTAL // max(1, dn))
    for u in range(0, dn * dstep, dstep):
        for dt, docs in make_daily(u).items():
            st["daily_docs"][dt] += len(docs)
            if docs and dt not in samples:
                samples[dt] = docs[0]
    ref = Counter()
    for u in range(len(MERCHANTS)):
        for dt, docs in make_reference(u).items():
            ref[dt] += len(docs)
            if docs and dt not in samples:
                samples[dt] = docs[0]

    per_order = sum(st["docs"].values()) / n
    per_day = sum(st["daily_docs"].values()) / max(1, dn)
    proj = {dt: int(c / n * NUM_ORDERS) for dt, c in st["docs"].items()}
    for dt, c in st["daily_docs"].items():
        proj[dt] = proj.get(dt, 0) + int(c / max(1, dn) * DAILY_TOTAL)
    for dt, c in ref.items():
        proj[dt] = proj.get(dt, 0) + c
    total_docs = sum(proj.values())

    os.makedirs(os.path.dirname(args.sample_out) or ".", exist_ok=True)
    with open(args.sample_out, "w") as f:
        json.dump(samples, f, indent=2)

    def pct(counter, k=10):
        tot = sum(counter.values()) or 1
        return "\n".join(f"    {v / tot:6.1%}  {key}" for key, v in counter.most_common(k))

    active_locs = sum(1 for l in LOCATIONS if l["churn"] is None)
    print(f"\nDRY RUN  window {START_DATE} .. {LAST_DATE} ({DAYS} days)")
    print(f"merchants {len(MERCHANTS):,}   locations {len(LOCATIONS):,} ({active_locs:,} active at end)   "
          f"employees {len(EMPLOYEES):,}   menu items {sum(len(m['items']) for m in MERCHANTS):,}")
    print(f"orders {NUM_ORDERS:,} (natural volume {int(NATURAL_ORDERS):,}, scale {SCALE:.2f})   "
          f"location-days {DAILY_TOTAL:,}   generator ~{gen_rate:,.0f} orders/s per process")
    print(f"docs/order {per_order:.2f}   daily docs/location-day {per_day:.2f}")
    print(f"PROJECTED DOCUMENTS: {total_docs:,}")
    for dt, c in sorted(proj.items(), key=lambda x: -x[1]):
        print(f"    {c:>15,}  {DOC_TO_COLLECTION[dt]}")
    print("orders by concept (share, avg check):")
    for c, v in st["concept"].most_common():
        print(f"    {v / n:6.1%}  {c:9s} ${st['concept_sales'][c] / v:,.2f}")
    print("orders by year:\n" + "\n".join(f"    {y}: {c / n:6.1%}" for y, c in sorted(st["year"].items())))
    print("online + delivery share by year:\n" + "\n".join(
        f"    {y}: {st['online_by_year'][(y, True)] / max(1, st['online_by_year'][(y, True)] + st['online_by_year'][(y, False)]):.1%}"
        for y in sorted(st["year"])))
    print("service areas:\n" + pct(st["area"], 8))
    print("app types:\n" + pct(st["app"]))
    print("tenders:\n" + pct(st["tender"]))
    print("order closeType:\n" + pct(st["close"]))
    print(f"one sample doc per docType written to {args.sample_out}")


def main():
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    if args.dry_run:
        dry_run()
        return
    log.info(f"QUANTIC LOAD bucket={args.bucket} scope={args.scope} phases={phases} merchants={len(MERCHANTS)} "
             f"locations={len(LOCATIONS)} orders={NUM_ORDERS:,} window={START_DATE}..{LAST_DATE} "
             f"processes={args.processes} workers={args.workers}")
    if not args.no_create:
        Loader(create=True).cluster.close()
    grand = Counter()
    for phase in phases:
        start, end = PHASES[phase][1]()
        if end <= start:
            continue
        t0 = time.time()
        if args.processes <= 1 or end - start < 1000:
            res = run_range(phase, start, end, label="p00")
            grand.update(res)
        else:
            jobs = [(phase, i, s_, e_) for i, (s_, e_) in enumerate(split_range(start, end, args.processes))]
            with multiprocessing.get_context("spawn").Pool(processes=len(jobs)) as pool:
                for res in pool.imap_unordered(_proc_entry, jobs):
                    grand.update(res)
        log.info(f"phase {phase} done in {(time.time() - t0) / 60:,.1f} min")
    total = sum(grand.values())
    log.info(f"DONE {dict(grand)} TOTAL={total:,}")


if __name__ == "__main__":
    main()
