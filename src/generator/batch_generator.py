# src/generator/batch_generator.py
from faker import Faker
import random
from datetime import datetime, timezone

fake = Faker("id_ID")


def generate_users(n: int):
    """
    Generate n user dicts WITHOUT user_id (Postgres will generate it).
    Fields: name, email, phone_number, created_date
    """
    email_domains = ["gmail.com", "yahoo.com", "outlook.com", "yahoo.co.id", "hotmail.com"]
    ts = datetime.now(timezone.utc)
    users = []

    for _ in range(n):
        full_name = fake.name()
        username = full_name.lower().replace(" ", ".")
        domain = random.choice(email_domains)
        email = f"{username}@{domain}"

        users.append({
            "name": full_name,
            "email": email,
            "phone_number": fake.phone_number(),
            "created_date": ts
        })

    return users


CATEGORY_DEFINITION = {
    "beauty & health": {
        "brands": ["Wardah", "Somethinc", "Emina", "Skintific", "Scarlett"],
        "subcategories": ["Serum", "Moisturizer", "Sunscreen", "Lip Product"],
        "price_range": (10000, 1500000)
    },
    "toys & baby products": {
        "brands": ["Fisher-Price", "Pampers", "Johnson & Johnson", "Mothercare"],
        "subcategories": ["Baby Diaper", "Educational Toy", "Baby Bottle"],
        "price_range": (20000, 4000000)
    },
    "bags & luggage": {
        "brands": ["Eiger", "Consina", "American Tourister", "Bodypack", "Osprey", "Thule"],
        "subcategories": ["Backpack", "Travel Bag", "Laptop Bag"],
        "price_range": (30000, 7000000)
    },
    "sports & fitness": {
        "brands": ["Nike", "Adidas", "Yonex", "Puma", "Asics", "Under Armour"],
        "subcategories": ["Shoes", "Equipment", "Jersey"],
        "price_range": (50000, 12000000)
    },
    "tv, audio & cameras": {
        "brands": ["Sony", "Samsung", "Canon", "Panasonic"],
        "subcategories": ["Smart TV", "Headphone", "Camera"],
        "price_range": (100000, 35000000)
    },
    "men's shoes": {
        "brands": ["Compass", "Ventela", "Nike", "Adidas", "Puma", "Asics"],
        "subcategories": ["Sneakers", "Running Shoes", "Boots"],
        "price_range": (100000, 6000000)
    },
    "appliances": {
        "brands": ["Miyako", "Philips", "Cosmos", "LG"],
        "subcategories": ["Rice Cooker", "Air Purifier", "Iron"],
        "price_range": (100000, 50000000)
    },
}

DESCRIPTORS = [
    "Ultra", "Pro", "Plus", "Max", "Lite", "Fresh",
    "Soft", "Prime", "Essential", "Advance", "Boost",
    "Smart", "Clean", "Pure", "Active", "Compact"
]


def generate_product_name(brand: str, subcat: str) -> str:
    r = random.random()
    if r < 0.6:
        n_words = 1
    elif r < 0.9:
        n_words = 2
    else:
        n_words = 3

    desc = random.sample(DESCRIPTORS, n_words)
    descriptor_part = " ".join(desc)
    return f"{brand} {subcat} {descriptor_part}"


def generate_products(n: int):
    """
    Generate n product dicts WITHOUT product_id (Postgres will generate it).
    Fields: product_name, brand, category, sub_category, currency, price, cost, created_date
    """
    rows = []
    ts = datetime.now(timezone.utc)
    used_names = set()

    for _ in range(n):
        category = random.choice(list(CATEGORY_DEFINITION.keys()))
        config = CATEGORY_DEFINITION[category]

        brand = random.choice(config["brands"])
        subcat = random.choice(config["subcategories"])
        price = random.randint(*config["price_range"])
        cost = int(price * random.uniform(0.6, 0.8))

        while True:
            name = generate_product_name(brand, subcat)
            if name not in used_names:
                used_names.add(name)
                break

        rows.append({
            "product_name": name,
            "brand": brand,
            "category": category,
            "sub_category": subcat,
            "currency": "IDR",
            "price": price,
            "cost": cost,
            "created_date": ts
        })

    return rows