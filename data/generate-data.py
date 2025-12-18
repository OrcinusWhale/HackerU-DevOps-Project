import json
import random
import re
from faker import Faker

fake = Faker()

# Configuration
NUM_SUPPLIERS = 10
NUM_PRODUCTS = 2000
NUM_ORDERS = 10000

# Data Definitions for Logic
PRODUCT_CATALOG = {
    "Electronics": [
        "Smartphone",
        "Laptop",
        "Wireless Earbuds",
        "Smart Watch",
        "4K Monitor",
        "Mechanical Keyboard",
        "Gaming Mouse",
        "Tablet",
        "Bluetooth Speaker",
        "External Hard Drive",
        "Power Bank",
    ],
    "Home & Kitchen": [
        "Blender",
        "Coffee Maker",
        "Air Fryer",
        "Standing Desk",
        "Ergonomic Chair",
        "Bookshelf",
        "Table Lamp",
        "Throw Pillow",
        "Cookware Set",
        "Vacuum Cleaner",
    ],
    "Fashion": [
        "Denim Jeans",
        "Cotton T-Shirt",
        "Running Shoes",
        "Leather Jacket",
        "Wristwatch",
        "Sunglasses",
        "Backpack",
        "Winter Scarf",
        "Baseball Cap",
        "Hoodie",
    ],
    "Beauty & Personal Care": [
        "Face Moisturizer",
        "Vitamin C Serum",
        "Shampoo",
        "Electric Toothbrush",
        "Sunscreen",
        "Hair Dryer",
        "Perfume",
        "Beard Oil",
    ],
    "Sports & Outdoors": [
        "Yoga Mat",
        "Dumbbell Set",
        "Camping Tent",
        "Sleeping Bag",
        "Water Bottle",
        "Tennis Racket",
        "Bicycle",
        "Hiking Boots",
    ],
}

ADJECTIVES = [
    "Premium",
    "Budget",
    "Pro",
    "Ultra",
    "Compact",
    "Portable",
    "Deluxe",
    "Vintage",
    "Modern",
    "Heavy-Duty",
    "Lightweight",
    "Wireless",
    "Smart",
    "Eco-Friendly",
]


def clean_string(text):
    """Helper to remove spaces and special chars for emails"""
    return re.sub(r"[^a-zA-Z0-9]", "", text).lower()


def generate_data():
    suppliers = []
    products = []
    orders = []

    # ---------------------------------------------------------
    # 1. Generate Suppliers (Custom Email Format)
    # ---------------------------------------------------------
    print("Generating Suppliers...")
    for i in range(1, NUM_SUPPLIERS + 1):
        company_name = fake.company()
        contact_name = fake.name()

        # Create email: contact.name@companyname.com
        user_slug = contact_name.replace(" ", ".").lower()
        domain_slug = clean_string(company_name)
        custom_email = f"{user_slug}@{domain_slug}.com"

        suppliers.append(
            {
                "id": i,
                "name": company_name,
                "contact_name": contact_name,
                "email": custom_email,
                "phone": fake.phone_number(),
                "country": fake.country(),
            }
        )

    # ---------------------------------------------------------
    # 2. Generate Products (Smart Matching)
    # ---------------------------------------------------------
    print("Generating Products...")
    categories_list = list(PRODUCT_CATALOG.keys())

    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(categories_list)
        base_product = random.choice(PRODUCT_CATALOG[category])
        adjective = random.choice(ADJECTIVES)
        full_name = f"{adjective} {base_product}"

        products.append(
            {
                "id": i,
                "name": full_name,
                "category": category,
                "price": round(random.uniform(15.0, 500.0), 2),
                "stock_quantity": random.randint(0, 200),
                "supplier_id": random.randint(1, NUM_SUPPLIERS),
            }
        )

    # ---------------------------------------------------------
    # 3. Generate Orders (With Addresses)
    # ---------------------------------------------------------
    print("Generating Orders...")
    for i in range(1, NUM_ORDERS + 1):
        items = []
        total_amount = 0

        # Random items per order
        for _ in range(random.randint(1, 4)):
            product = random.choice(products)
            qty = random.randint(1, 5)
            line_total = round(product["price"] * qty, 2)
            total_amount += line_total

            items.append(
                {
                    "product_id": product["id"],
                    "quantity": qty,
                    "unit_price": product["price"],
                    "line_total": line_total,
                }
            )

        # Address Object
        shipping_address = {
            "street": fake.street_address(),
            "city": fake.city(),
            "zip_code": fake.zipcode(),
            "country": fake.country(),
        }

        orders.append(
            {
                "id": i,
                "customer_name": fake.name(),
                "customer_email": fake.email(),
                "shipping_address": shipping_address,
                "date": fake.date_between(
                    start_date="-1y", end_date="today"
                ).isoformat(),
                "status": random.choice(
                    ["Pending", "Shipped", "Delivered", "Returned"]
                ),
                "total_amount": round(total_amount, 2),
                "items": items,
            }
        )

    return suppliers, products, orders


if __name__ == "__main__":
    suppliers_data, products_data, orders_data = generate_data()

    # Save Files
    with open("suppliers.json", "w") as f:
        json.dump(suppliers_data, f, indent=2)
    print("Saved suppliers.json")

    with open("products.json", "w") as f:
        json.dump(products_data, f, indent=2)
    print("Saved products.json")

    with open("orders.json", "w") as f:
        json.dump(orders_data, f, indent=2)
    print("Saved orders.json")
