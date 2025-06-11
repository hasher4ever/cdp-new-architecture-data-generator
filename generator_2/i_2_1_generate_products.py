import uuid
import random
import json
from utils import logger, PRODUCT_BRANDS, PRODUCT_CATEGORIES, PRODUCT_COLORS, PRODUCT_SIZES, PRODUCT_TYPES, write_csv_with_types

NUM_PRODUCTS = 500

# Generate products
products = []
product_ids = []
product_field_types = {
    "product_id": "VARCHAR_1000",
    "price": "DOUBLE",
    "brand": "VARCHAR_1000",
    "category": "VARCHAR_1000",
    "color": "VARCHAR_1000",
    "size": "VARCHAR_1000",
    "type": "VARCHAR_1000"
}

logger.info(f"Generating {NUM_PRODUCTS} products")
for _ in range(NUM_PRODUCTS):
    category = random.choice(PRODUCT_CATEGORIES)
    product = {
        "product_id": str(uuid.uuid4()),
        "price": round(random.uniform(10, 500), 2),
        "brand": random.choice(PRODUCT_BRANDS[category]),
        "category": category,
        "color": random.choice(PRODUCT_COLORS),
        "size": random.choice(PRODUCT_SIZES[category]),
        "type": random.choice(PRODUCT_TYPES[category])
    }
    products.append(product)
    product_ids.append(product["product_id"])
logger.info(f"Generated {len(products)} products")

# Write products to CSV
with open("products.csv", "w", newline='') as f:
    fieldnames = list(product_field_types.keys())
    write_csv_with_types(products, "products.csv", fieldnames)

# Save product IDs and field types for other scripts
with open("product_data.json", "w", encoding="utf-8") as f:
    json.dump({"product_ids": product_ids, "product_field_types": product_field_types}, f, indent=2)
logger.info("Completed writing to product_data.json")