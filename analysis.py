"""
analysis.py
Exploratory analysis for the Baghdad food delivery demo.
Outputs:
- CSVs in ./outputs
- Figures in ./figures
Notes are concise and practical for a portfolio or take-home task.
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# ---- DB connection ----
USER = "root"
PASSWORD = "TRooNT007"  # <- set your MySQL root password
HOST = "127.0.0.1"
PORT = 3307
DB   = "food_delivery"

engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

# ---- output folders ----
os.makedirs("outputs", exist_ok=True)
os.makedirs("figures", exist_ok=True)

# ---- load tables ----
orders = pd.read_sql("SELECT * FROM orders", engine)
vendors = pd.read_sql("SELECT * FROM vendors", engine)
drivers = pd.read_sql("SELECT * FROM drivers", engine)
customers = pd.read_sql("SELECT * FROM customers", engine)

# ---- feature engineering ----
orders["order_datetime"] = pd.to_datetime(orders["order_datetime"])
orders["date"]   = orders["order_datetime"].dt.date
orders["hour"]   = orders["order_datetime"].dt.hour
orders["dow"]    = orders["order_datetime"].dt.day_name()
orders["week"]   = orders["order_datetime"].dt.isocalendar().week.astype(int)
orders["month"]  = orders["order_datetime"].dt.to_period("M").astype(str)
orders["is_weekend"] = orders["dow"].isin(["Friday","Saturday","Sunday"])  # adjust if needed

deliv = orders[orders["status"]=="delivered"].copy()
deliv["revenue"] = deliv["subtotal"] + deliv["delivery_fee"] + deliv["tip"]
deliv_v = deliv.merge(vendors, on="vendor_id", how="left", suffixes=("", "_vendor"))

def pct(x: float) -> str:
    return f"{100*x:.2f}%"

def section(title: str):
    print("\n" + title)
    print("-" * len(title))

# --------------------------
# Global KPIs
# --------------------------
section("Global KPIs")
total_orders        = len(orders)
delivered_orders    = len(deliv)
canceled_orders     = (orders["status"]=="canceled").sum()
returned_orders     = (orders["status"]=="returned").sum()
cancel_rate         = canceled_orders / total_orders if total_orders else 0
return_rate         = returned_orders / total_orders if total_orders else 0
avg_delivery_min    = round(deliv["delivery_minutes"].mean(), 2) if delivered_orders else 0
p95_delivery_min    = round(deliv["delivery_minutes"].quantile(0.95), 2) if delivered_orders else 0
total_revenue       = round(deliv["revenue"].sum(), 2)
aov                 = round(deliv["revenue"].mean(), 2) if delivered_orders else 0
avg_distance        = round(deliv["distance_km"].mean(), 2) if delivered_orders else 0

print(f"Total Orders: {total_orders}")
print(f"Delivered: {delivered_orders}  |  Canceled: {canceled_orders} ({pct(cancel_rate)})  |  Returned: {returned_orders} ({pct(return_rate)})")
print(f"Avg Delivery Time: {avg_delivery_min} min  |  P95: {p95_delivery_min} min")
print(f"Total Revenue: ${total_revenue}  |  AOV: ${aov}")
print(f"Avg Distance: {avg_distance} km")

for sla in [25, 30, 35, 40]:
    sla_ok = (deliv["delivery_minutes"] <= sla).mean() if delivered_orders else 0
    print(f"SLA ≤ {sla} min: {pct(sla_ok)}")

# --------------------------
# Time series
# --------------------------
section("Time Series")
daily = deliv.groupby("date").agg(
    orders=("order_id","count"),
    revenue=("revenue","sum"),
    avg_minutes=("delivery_minutes","mean")
).reset_index()
daily.to_csv("outputs/daily_kpis.csv", index=False)
print("saved: outputs/daily_kpis.csv")

plt.figure(figsize=(10,4))
plt.plot(pd.to_datetime(daily["date"]), daily["orders"])
plt.title("Daily Orders"); plt.xlabel("Date"); plt.ylabel("Orders")
plt.tight_layout(); plt.savefig("figures/daily_orders.png"); plt.close()

plt.figure(figsize=(10,4))
plt.plot(pd.to_datetime(daily["date"]), daily["revenue"])
plt.title("Daily Revenue"); plt.xlabel("Date"); plt.ylabel("Revenue")
plt.tight_layout(); plt.savefig("figures/daily_revenue.png"); plt.close()

hourly = deliv.groupby("hour").size()
plt.figure(figsize=(8,4))
hourly.plot(kind="bar")
plt.title("Orders by Hour"); plt.xlabel("Hour"); plt.ylabel("Orders")
plt.tight_layout(); plt.savefig("figures/orders_by_hour.png"); plt.close()

dow_counts = deliv["dow"].value_counts()
plt.figure(figsize=(7,4))
dow_counts.loc[["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]].plot(kind="bar")
plt.title("Orders by Day of Week"); plt.xlabel("Day"); plt.ylabel("Orders")
plt.tight_layout(); plt.savefig("figures/orders_by_dow.png"); plt.close()

# --------------------------
# Area (dropoff) analysis
# --------------------------
section("Area Analysis (Dropoff)")
area_kpis = (deliv.groupby("dropoff_area")
             .agg(orders=("order_id","count"),
                  avg_minutes=("delivery_minutes","mean"),
                  p95_minutes=("delivery_minutes", lambda s: s.quantile(0.95)),
                  sla30=("delivery_minutes", lambda s: (s<=30).mean()),
                  revenue=("revenue","sum"))
             .sort_values("orders", ascending=False))
area_kpis["sla30"] = (area_kpis["sla30"]*100).round(2).astype(str) + "%"
area_kpis["avg_minutes"] = area_kpis["avg_minutes"].round(2)
area_kpis["p95_minutes"] = area_kpis["p95_minutes"].round(2)
area_kpis["revenue"]     = area_kpis["revenue"].round(2)
print(area_kpis.head(12).to_string())
area_kpis.to_csv("outputs/area_kpis.csv")
print("saved: outputs/area_kpis.csv")

plt.figure(figsize=(10,4))
area_kpis.sort_values("avg_minutes").head(12)["avg_minutes"].plot(kind="bar")
plt.title("Avg Delivery Minutes by Dropoff Area (Top 12 fastest)")
plt.xlabel("Area"); plt.ylabel("Avg Minutes")
plt.tight_layout(); plt.savefig("figures/avg_minutes_by_area.png"); plt.close()

# --------------------------
# Vendor analysis
# --------------------------
section("Vendor Analysis")
vend = deliv_v.groupby(["vendor_id","vendor_name","area","cuisine"]).agg(
    orders=("order_id","count"),
    revenue=("revenue","sum"),
    avg_minutes=("delivery_minutes","mean"),
    sla30=("delivery_minutes", lambda s: (s<=30).mean())
).reset_index()
vend["revenue"] = vend["revenue"].round(2)
vend["avg_minutes"] = vend["avg_minutes"].round(2)
vend["sla30"] = (vend["sla30"]*100).round(2).astype(str) + "%"

top_vendors_orders = vend.sort_values("orders", ascending=False).head(15)
print("\nTop Vendors by Orders:")
print(top_vendors_orders[["vendor_name","area","cuisine","orders","avg_minutes","sla30","revenue"]].to_string(index=False))
top_vendors_orders.to_csv("outputs/top_vendors_by_orders.csv", index=False)

plt.figure(figsize=(10,4))
(vend.sort_values("revenue", ascending=False).head(10)
     .set_index("vendor_name")["revenue"]).plot(kind="bar")
plt.title("Top 10 Vendors by Revenue"); plt.xlabel("Vendor"); plt.ylabel("Revenue")
plt.tight_layout(); plt.savefig("figures/top10_vendors_revenue.png"); plt.close()

# --------------------------
# Cuisine analysis
# --------------------------
section("Cuisine Analysis")
cui = deliv_v.groupby("cuisine").agg(
    orders=("order_id","count"),
    revenue=("revenue","sum"),
    avg_minutes=("delivery_minutes","mean"),
    sla30=("delivery_minutes", lambda s: (s<=30).mean())
).sort_values("orders", ascending=False)
cui["revenue"] = cui["revenue"].round(2)
cui["avg_minutes"] = cui["avg_minutes"].round(2)
cui["sla30"] = (cui["sla30"]*100).round(2).astype(str) + "%"
print(cui.to_string())
cui.to_csv("outputs/cuisine_kpis.csv")

plt.figure(figsize=(8,4))
cui["orders"].head(10).plot(kind="bar")
plt.title("Top Cuisines by Orders"); plt.xlabel("Cuisine"); plt.ylabel("Orders")
plt.tight_layout(); plt.savefig("figures/top_cuisines_orders.png"); plt.close()

# --------------------------
# Driver performance
# --------------------------
section("Driver Performance")
drv = (deliv.groupby("driver_id")
       .agg(orders=("order_id","count"),
            avg_rating=("driver_rating","mean"),
            avg_minutes=("delivery_minutes","mean"),
            avg_distance=("distance_km","mean"))
       .reset_index())
drv = drv.merge(drivers, on="driver_id", how="left")
drv["efficiency_min_per_km"] = (drv["avg_minutes"] / drv["avg_distance"]).round(2)
drv["avg_minutes"]  = drv["avg_minutes"].round(2)
drv["avg_distance"] = drv["avg_distance"].round(2)
drv["avg_rating"]   = drv["avg_rating"].round(2)

best_drivers = drv[drv["orders"]>=20].sort_values(["avg_rating","orders"], ascending=[False,False]).head(10)
print("\nTop Drivers (orders>=20):")
print(best_drivers[["driver_name","orders","avg_rating","avg_minutes","avg_distance","efficiency_min_per_km"]].to_string(index=False))
best_drivers.to_csv("outputs/top_drivers.csv", index=False)

# --------------------------
# Customer behavior
# --------------------------
section("Customer Behavior")
cust_orders = deliv.groupby("customer_id").agg(
    orders=("order_id","count"),
    revenue=("revenue","sum"),
    first_order=("order_datetime","min"),
    last_order=("order_datetime","max")
).reset_index()
cust_orders = cust_orders.merge(customers, on="customer_id", how="left")
cust_orders["revenue"] = cust_orders["revenue"].round(2)
repeat_rate = (cust_orders["orders"]>=2).mean() if len(cust_orders) else 0
print(f"Unique Customers (delivered): {len(cust_orders)} | Repeat Customers (>=2 orders): {pct(repeat_rate)}")
cust_orders.sort_values("orders", ascending=False).head(15).to_csv("outputs/top_customers.csv", index=False)

plt.figure(figsize=(8,4))
cust_orders["orders"].value_counts().sort_index().plot(kind="bar")
plt.title("Orders per Customer (Frequency)"); plt.xlabel("# Orders per Customer"); plt.ylabel("# Customers")
plt.tight_layout(); plt.savefig("figures/orders_per_customer_dist.png"); plt.close()

# --------------------------
# Distance vs time relationship + simple linear fit
# --------------------------
section("Distance vs Delivery Minutes (Fit)")
x = deliv["distance_km"].values
y = deliv["delivery_minutes"].values
if len(x) > 5:
    slope, intercept = np.polyfit(x, y, 1)
    print(f"minutes ≈ {slope:.2f} * distance_km + {intercept:.2f}")
    plt.figure(figsize=(6,4))
    plt.scatter(x, y, s=8, alpha=0.4)
    xs = np.linspace(x.min(), x.max(), 100)
    plt.plot(xs, slope*xs + intercept)
    plt.title("Distance vs Delivery Minutes (linear fit)")
    plt.xlabel("Distance (km)"); plt.ylabel("Delivery Minutes")
    plt.tight_layout(); plt.savefig("figures/distance_vs_minutes_fit.png"); plt.close()

    # Simple outlier scan based on residual z-score
    residuals = y - (slope*x + intercept)
    std = residuals.std() if residuals.std() != 0 else 1.0
    z = (residuals - residuals.mean()) / std
    tmp = deliv_v.copy()
    tmp["residual_z"] = z
    worst = tmp.sort_values("residual_z", ascending=False).head(15)
    cols = ["order_id","vendor_name","area","distance_km","delivery_minutes","residual_z"]
    worst[cols].to_csv("outputs/outliers_slow_deliveries.csv", index=False)
    print("saved: outputs/outliers_slow_deliveries.csv")
    print(worst[cols].to_string(index=False))

# --------------------------
# Peak hour per area
# --------------------------
section("Peak Hour per Area")
peak_area = (deliv_v.groupby(["area","hour"])["order_id"].count()
             .reset_index(name="orders")
             .sort_values(["area","orders"], ascending=[True,False])
             .groupby("area").head(1))
peak_area.to_csv("outputs/peak_hour_per_area.csv", index=False)
print(peak_area.to_string(index=False))

# --------------------------
# Top vendors per area
# --------------------------
section("Top Vendors per Area (Top 3)")
va = (deliv_v.groupby(["area","vendor_name"])["order_id"].count()
      .reset_index(name="orders")
      .sort_values(["area","orders"], ascending=[True,False]))
top3 = va.groupby("area").head(3)
top3.to_csv("outputs/top3_vendors_per_area.csv", index=False)
print(top3.to_string(index=False))

print("\nDone. CSVs in outputs/, figures in figures/.")
