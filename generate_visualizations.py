import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import json
from datetime import datetime

sns.set_style("whitegrid")

# ----------------------------------------------------
# Load Data
# ----------------------------------------------------

users = pd.read_json("data/users.json")
products = pd.read_json("data/products.json")
transactions = pd.read_json("data/transactions.json")

# Load all session chunks
session_files = glob.glob("data/sessions_*.json")
sessions = pd.concat([pd.read_json(f) for f in session_files], ignore_index=True)

# ----------------------------------------------------
# Helper: Date Parsing
# ----------------------------------------------------

transactions["timestamp"] = pd.to_datetime(transactions["timestamp"])
sessions["start_time"] = pd.to_datetime(sessions["start_time"])

# ----------------------------------------------------
# 1️ Sales Performance Over Time (Figure: sales_over_time.png)
# ----------------------------------------------------

sales_over_time = (
    transactions
    .set_index("timestamp")
    .resample("W")["total"]
    .sum()
    .reset_index()
)

plt.figure(figsize=(10, 6))
plt.plot(sales_over_time["timestamp"], sales_over_time["total"], marker="o")
plt.title("Weekly Sales Performance Over Time")
plt.xlabel("Week")
plt.ylabel("Total Revenue ($)")
plt.tight_layout()
plt.savefig("figures/sales_over_time.png")
plt.close()

# ----------------------------------------------------
# 2️ Customer Segmentation by Spending (user_segmentation.png)
# ----------------------------------------------------

user_spending = (
    transactions.groupby("user_id")["total"]
    .sum()
    .reset_index()
)

user_spending["segment"] = pd.cut(
    user_spending["total"],
    bins=[0, 100, 500, 1000, float("inf")],
    labels=["Low", "Medium", "High", "Very High"]
)

segment_summary = user_spending.groupby("segment")["total"].mean().reset_index()

plt.figure(figsize=(8, 6))
sns.barplot(data=segment_summary, x="segment", y="total")
plt.title("Customer Segmentation by Average Spending")
plt.xlabel("Customer Segment")
plt.ylabel("Average Spend ($)")
plt.tight_layout()
plt.savefig("figures/user_segmentation.png")
plt.close()

# ----------------------------------------------------
# 3️ Top-Selling vs Most-Viewed Products (product_performance.png)
# ----------------------------------------------------

# Count product sales
sales_counts = (
    transactions.explode("items")
    .assign(product_id=lambda x: x["items"].apply(lambda i: i["product_id"]))
    .groupby("product_id")
    .size()
    .reset_index(name="purchases")
)

# Count product views
view_counts = (
    sessions.explode("viewed_products")
    .groupby("viewed_products")
    .size()
    .reset_index(name="views")
    .rename(columns={"viewed_products": "product_id"})
)

product_perf = pd.merge(sales_counts, view_counts, on="product_id", how="outer").fillna(0)
top_products = product_perf.sort_values("purchases", ascending=False).head(10)

plt.figure(figsize=(10, 6))
plt.scatter(top_products["views"], top_products["purchases"])
plt.title("Top-Selling vs Most-Viewed Products")
plt.xlabel("Number of Views")
plt.ylabel("Number of Purchases")
plt.tight_layout()
plt.savefig("figures/product_performance.png")
plt.close()

# ----------------------------------------------------
# 4️ Conversion Funnel Analysis (conversion_funnel.png)
# ----------------------------------------------------

views = sessions["viewed_products"].apply(len).sum()
cart_adds = sessions["cart_contents"].apply(lambda x: len(x) if isinstance(x, dict) else 0).sum()
purchases = len(transactions)

funnel = pd.DataFrame({
    "Stage": ["Product Views", "Cart Additions", "Purchases"],
    "Count": [views, cart_adds, purchases]
})

plt.figure(figsize=(8, 6))
sns.barplot(data=funnel, x="Stage", y="Count")
plt.title("E-Commerce Conversion Funnel")
plt.ylabel("Event Count")
plt.tight_layout()
plt.savefig("figures/conversion_funnel.png")
plt.close()

print(" All visualizations generated successfully in /figures/")
