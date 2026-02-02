# dataset_generator.py
import json
import random
import datetime
import uuid
import threading
import numpy as np
from faker import Faker

fake = Faker()

# --- Configuration ---
NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 500000
NUM_SESSIONS = 2000000
TIMESPAN_DAYS = 90
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe

# --- Initialization ---
np.random.seed(42)
random.seed(42)
Faker.seed(42)

print("Initializing dataset generation...")

# --- ID Generators ---
def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"

def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"

# --- Inventory Management ---
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()  # For thread safety

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)

# --- Helper Functions ---
def determine_page_type(position, previous_pages):
    """Determine page type based on position in user journey and previous pages viewed."""
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"
    prev_page = previous_pages[-1]["page_type"]
    if prev_page == "home":
        return random.choices(
            ["category_listing", "search", "product_detail"],
            weights=[0.5, 0.3, 0.2]
        )[0]
    elif prev_page == "category_listing":
        return random.choices(
            ["product_detail", "category_listing", "search", "home"],
            weights=[0.7, 0.1, 0.1, 0.1]
        )[0]
    elif prev_page == "search":
        return random.choices(
            ["product_detail", "search", "category_listing", "home"],
            weights=[0.6, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "product_detail":
        return random.choices(
            ["product_detail", "cart", "category_listing", "search", "home"],
            weights=[0.3, 0.3, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "cart":
        return random.choices(
            ["checkout", "product_detail", "category_listing", "home"],
            weights=[0.6, 0.2, 0.1, 0.1]
        )[0]
    elif prev_page == "checkout":
        return random.choices(
            ["confirmation", "cart", "home"],
            weights=[0.8, 0.1, 0.1]
        )[0]
    elif prev_page == "confirmation":
        return random.choices(
            ["home", "product_detail", "category_listing"],
            weights=[0.6, 0.2, 0.2]
        )[0]
    else:
        return "home"

def get_page_content(page_type, products_list, categories_list, inventory):
    """Get appropriate product and category based on page type."""
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category_id = product["category_id"]
                category = next((c for c in categories_list if c["category_id"] == category_id), None)
                return product, category
            attempts += 1
        # fallback
        product = random.choice(products_list)
        category_id = product["category_id"]
        category = next((c for c in categories_list if c["category_id"] == category_id), None)
        return product, category
    elif page_type == "category_listing":
        category = random.choice(categories_list)
        return None, category
    else:
        return None, None

# --- Category Generation ---
categories = []
for cat_id in range(NUM_CATEGORIES):
    category = {
        "category_id": f"cat_{cat_id:03d}",
        "name": fake.company(),
        "subcategories": []
    }
    for sub_id in range(random.randint(3, 5)):
        subcategory = {
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)
        }
        category["subcategories"].append(subcategory)
    categories.append(category)
print(f"Generated {len(categories)} categories")

# --- Product Generation ---
products = []
product_creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS*2)
for prod_id in range(NUM_PRODUCTS):
    category = random.choice(categories)
    base_price = round(random.uniform(5, 500), 2)
    price_history = [{"price": base_price, "date": product_creation_start.isoformat()}]
    products.append({
        "product_id": f"prod_{prod_id:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "base_price": base_price,
        "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": price_history,
        "creation_date": price_history[0]["date"]
    })
print(f"Generated {len(products)} products")

# --- User Generation ---
users = []
for user_id in range(NUM_USERS):
    reg_date = fake.date_time_between(
        start_date=f"-{TIMESPAN_DAYS*3}d",
        end_date=f"-{TIMESPAN_DAYS}d"
    )
    users.append({
        "user_id": f"user_{user_id:06d}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": fake.country_code()
        },
        "registration_date": reg_date.isoformat(),
        "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat()
    })
print(f"Generated {len(users)} users")

# --- Session & Transaction Generation ---
inventory = InventoryManager(products)
sessions = []
transactions = []
transaction_counter = 0
session_counter = 0
iteration = 0

print("Generating sessions and transactions...")
while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
    iteration += 1
    # --- Session generation skipped for brevity ---
    # --- Transaction generation ---
    if transaction_counter < NUM_TRANSACTIONS:
        user = random.choice(users)
        products_in_txn = random.sample(products, k=min(3, len(products)))
        transaction_items = []
        for product in products_in_txn:
            if product["is_active"]:
                quantity = random.randint(1, 3)
                if inventory.update_stock(product["product_id"], quantity):
                    transaction_items.append({
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": product["base_price"],
                        "subtotal": round(quantity * product["base_price"], 2)
                    })
        if transaction_items:
            subtotal = sum(item["subtotal"] for item in transaction_items)
            discount = 0
            if random.random() < 0.2:
                discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                discount = round(subtotal * discount_rate, 2)
            total = round(subtotal - discount, 2)
            transactions.append({
                "transaction_id": generate_transaction_id(),
                "session_id": None,
                "user_id": user["user_id"],
                "timestamp": fake.date_time_between(
                    start_date=f"-{TIMESPAN_DAYS}d",
                    end_date="now"
                ).isoformat(),
                "items": transaction_items,
                "subtotal": subtotal,
                "discount": discount,
                "total": total,
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                "status": random.choice(["completed", "processing", "shipped", "delivered"])
            })
            transaction_counter += 1
    # Progress
    if iteration % 10000 == 0:
        print(f"Progress: {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")

# --- Save datasets ---
def json_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

with open("users.json", "w") as f:
    json.dump(users, f, default=json_serializer)
with open("products.json", "w") as f:
    json.dump(list(inventory.products.values()), f, default=json_serializer)
with open("categories.json", "w") as f:
    json.dump(categories, f, default=json_serializer)
with open("transactions.json", "w") as f:
    json.dump(transactions, f, default=json_serializer)

print(f"""
Dataset generation complete!
- Transactions: {len(transactions):,} (target: {NUM_TRANSACTIONS:,})
- Remaining products: {sum(p['current_stock'] for p in inventory.products.values()):,}
""")
