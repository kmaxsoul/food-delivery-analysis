"""
seed_data_baghdad_vendors.py
Populates the demo schema with Baghdad-only data:
- Drivers, Customers (Baghdad), Vendors per area, and synthetic Orders.
The goal is to have realistic analysis-friendly data for a portfolio/demo.
"""

import random
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector

# ---- DB connection settings ----
DB_CONFIG = {
    "user": "root",
    "password": "TRooNT007",   # <- set your MySQL root password
    "host": "127.0.0.1",
    "port": 3307,
    "database": "food_delivery",
}

fake = Faker()
# Use fixed seeds for reproducibility across runs
Faker.seed(101)
random.seed(101)

# ---- Fixed Baghdad areas (no airport) ----
BAGHDAD_AREAS = [
    "Mansour", "Karada", "Adhamiyah", "Kadhimiya", "Jadriyah",
    "Zayouna", "Palestine Street", "Dora", "Sadr City",
    "Yarmouk", "Bab Al Sharqi", "Karradat Mariam"
]

# ---- Vendors (restaurants) pinned to areas, with cuisine labels ----
AREA_VENDORS = {
    "Mansour": [
        ("Route 99", "Burger"),
        ("Pizza Roma", "Pizza"),
        ("Samad Baghdad", "Iraqi"),
        ("Cinnabon", "Dessert"),
        ("Texas Chicken", "Fried Chicken"),
        ("DipnDip", "Dessert"),
        ("Kabab Al Baghdadi", "Kebab"),
    ],
    "Karada": [
        ("Shawarma Al Reem", "Shawarma"),
        ("Pizza Hut", "Pizza"),
        ("Saj Al Reef", "Iraqi"),
        ("MADO Café", "Dessert"),
        ("Johnny Rockets", "Burger"),
        ("KFC", "Fried Chicken"),
        ("Masgouf House", "BBQ"),
    ],
    "Adhamiyah": [
        ("Shawarma Time", "Shawarma"),
        ("Little Italy Pizza", "Pizza"),
        ("Al Samadi Sweets", "Dessert"),
        ("Al Baghdadi Restaurant", "Iraqi"),
        ("Crunchy Bite", "Fried Chicken"),
        ("Wok Station", "Sushi"),
    ],
    "Kadhimiya": [
        ("Kabab Abu Ali", "Kebab"),
        ("Dough House", "Pizza"),
        ("Blue Wave", "Seafood"),
        ("Sushi House", "Sushi"),
        ("Al Reef Grill", "BBQ"),
        ("Café La Roche", "Cafe"),
    ],
    "Jadriyah": [
        ("Route 99", "Burger"),
        ("Cinnabon", "Dessert"),
        ("Pizza Roma", "Pizza"),
        ("Saj Al Reef", "Iraqi"),
        ("Shawarma Al Reem", "Shawarma"),
        ("Khan Murjan", "Iraqi"),
    ],
    "Zayouna": [
        ("Shawarma Time", "Shawarma"),
        ("Domino's Pizza", "Pizza"),
        ("Texas Chicken", "Fried Chicken"),
        ("DipnDip", "Dessert"),
        ("Istanbul Shawarma", "Turkish"),
        ("Ocean Fish", "Seafood"),
    ],
    "Palestine Street": [
        ("Shawarma Palace", "Shawarma"),
        ("Little Italy Pizza", "Pizza"),
        ("Big Bite", "Burger"),
        ("Kabab Al Baghdadi", "Kebab"),
        ("Sushi House", "Sushi"),
        ("Cinnabon", "Dessert"),
    ],
    "Dora": [
        ("Chicken Time", "Fried Chicken"),
        ("Pizza Hut", "Pizza"),
        ("Al Baghdadi Restaurant", "Iraqi"),
        ("BBQ Nation", "BBQ"),
        ("Blue Wave", "Seafood"),
    ],
    "Sadr City": [
        ("Shawarma Time", "Shawarma"),
        ("Kabab Abu Ali", "Kebab"),
        ("Route 99", "Burger"),
        ("Dough House", "Pizza"),
        ("KFC", "Fried Chicken"),
    ],
    "Yarmouk": [
        ("Johnny Rockets", "Burger"),
        ("Pizza Roma", "Pizza"),
        ("Saj Al Reef", "Iraqi"),
        ("Al Samadi Sweets", "Dessert"),
        ("Wok Station", "Sushi"),
    ],
    "Bab Al Sharqi": [
        ("Shawarma Palace", "Shawarma"),
        ("Slice & Bake", "Pizza"),
        ("Khan Murjan", "Iraqi"),
        ("DipnDip", "Dessert"),
        ("Ocean Fish", "Seafood"),
    ],
    "Karradat Mariam": [
        ("MADO Café", "Dessert"),
        ("Istanbul Shawarma", "Turkish"),
        ("Al Reef Grill", "BBQ"),
        ("Little Italy Pizza", "Pizza"),
        ("Big Bite", "Burger"),
    ],
}

def connect():
    return mysql.connector.connect(**DB_CONFIG)

def insert_many(cur, query, rows):
    cur.executemany(query, rows)

# ---- Seed routine ----
conn = connect()
cur = conn.cursor()

# Drivers (moderate size)
drivers = []
for _ in range(50):
    drivers.append((
        fake.name(),
        round(random.uniform(3.5, 4.9), 2),
        fake.date_between(start_date="-2y", end_date="today")
    ))
insert_many(cur, "INSERT INTO drivers (driver_name, rating, start_date) VALUES (%s,%s,%s)", drivers)
conn.commit()

# Customers (all Baghdad)
customers = []
for _ in range(500):
    customers.append((
        fake.name(),
        "Baghdad",
        fake.date_between(start_date="-3y", end_date="today")
    ))
insert_many(cur, "INSERT INTO customers (customer_name, city, signup_date) VALUES (%s,%s,%s)", customers)
conn.commit()

# Cache IDs
cur.execute("SELECT driver_id FROM drivers")
driver_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT customer_id FROM customers")
customer_ids = [r[0] for r in cur.fetchall()]

# Vendors: ensure unique naming if a brand spans multiple areas
vendors_rows = []
name_counts = {}
for area, items in AREA_VENDORS.items():
    for name, cuisine in items:
        name_counts[name] = name_counts.get(name, 0) + 1

for area, items in AREA_VENDORS.items():
    for name, cuisine in items:
        unique_name = f"{name} - {area}" if name_counts[name] > 1 else name
        vendors_rows.append((
            unique_name,
            cuisine,
            area,
            round(random.uniform(3.6, 4.9), 2),
            fake.date_between(start_date="-2y", end_date="today")
        ))

insert_many(cur, "INSERT INTO vendors (vendor_name, cuisine, area, rating, join_date) VALUES (%s,%s,%s,%s,%s)", vendors_rows)
conn.commit()

# Vendor meta for order generation
cur.execute("SELECT vendor_id, cuisine, area FROM vendors")
vendor_meta = cur.fetchall()  # (vendor_id, cuisine, area)

# Orders: generate a few months of activity with peak-hour weighting
orders = []
base_dt = datetime.now() - timedelta(days=120)

for _ in range(1200):  # adjust volume if needed
    cust_id = random.choice(customer_ids)
    drv_id  = random.choice(driver_ids)

    vendor_id, vendor_cuisine, vendor_area = random.choice(vendor_meta)
    food_category = vendor_cuisine

    # Slight peak around lunch/dinner
    hour = random.choices(
        population=list(range(24)),
        weights=[2,2,2,2,3,4,6,7,8,9,10,10,9,8,7,7,7,9,10,10,9,6,4,3],
        k=1
    )[0]
    day_offset = random.randint(0, 119)
    minute = random.randint(0, 59)
    order_dt = (base_dt + timedelta(days=day_offset)).replace(hour=hour, minute=minute, second=0, microsecond=0)

    pickup_area = vendor_area
    dropoff_area = random.choice(BAGHDAD_AREAS)
    while dropoff_area == pickup_area:
        dropoff_area = random.choice(BAGHDAD_AREAS)

    distance = round(random.uniform(0.8, 10.0), 2)
    delivery_min = max(10, int(distance * random.uniform(4.2, 7.2) + random.randint(-3, 7)))

    subtotal = round(random.uniform(4.0, 30.0), 2)
    delivery_fee = round(max(1.0, distance * random.uniform(0.2, 0.8)), 2)
    tip = round(max(0.0, random.gauss(1.0, 1.0)), 2)

    status = random.choices(["delivered","canceled","returned"], weights=[92,6,2], k=1)[0]
    driver_rating = round(min(5.0, max(1.0, random.gauss(4.4, 0.5))), 2) if status == "delivered" else None

    orders.append((
        cust_id, drv_id, vendor_id, food_category, order_dt, pickup_area, dropoff_area,
        distance, delivery_min, subtotal, delivery_fee, tip, driver_rating, status
    ))

insert_many(cur, """
INSERT INTO orders
(customer_id, driver_id, vendor_id, food_category, order_datetime, pickup_area, dropoff_area,
 distance_km, delivery_minutes, subtotal, delivery_fee, tip, driver_rating, status)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", orders)
conn.commit()

cur.close()
conn.close()

print("Seeded Baghdad vendors by area and orders successfully.")
