import csv
import json
import requests
import config
from faker import Faker

logger = config.logger
fake = Faker()

# Constants
EVENT_TYPES = ['add_to_cart', 'purchase', 'login', 'logout', 'page_view', 'search']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PLATFORMS = ['web', 'iOS', 'Android']
CURRENCIES = ['USD', 'EUR', 'RUB']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
PRODUCT_BRANDS = {
    'Clothing': ['Nike', 'Adidas', 'Puma', 'Zara', 'H&M'],
    'Electronics': ['Apple', 'Samsung', 'Sony'],
    'Books': ['Penguin', 'Random House'],
    'Home': ['IKEA', 'West Elm'],
    'Sports': ['Under Armour', 'Reebok']
}
PRODUCT_CATEGORIES = ['Clothing', 'Electronics', 'Books', 'Home', 'Sports']
PRODUCT_TYPES = {
    'Clothing': ['Shirt', 'Pants', 'Jacket', 'Dress', 'Shoes'],
    'Electronics': ['Phone', 'Laptop', 'Headphones', 'Camera'],
    'Books': ['Fiction', 'Non-Fiction', 'Textbook'],
    'Home': ['Furniture', 'Decor', 'Appliance'],
    'Sports': ['Equipment', 'Apparel', 'Accessories']
}
PRODUCT_COLORS = ['Red', 'Blue', 'Green', 'Black', 'White', 'Yellow']
PRODUCT_SIZES = {
    'Clothing': ['XS', 'S', 'M', 'L', 'XL'],
    'Electronics': ['N/A'],
    'Books': ['N/A'],
    'Home': ['Small', 'Medium', 'Large'],
    'Sports': ['S', 'M', 'L']
}
EVENT_FIELD_RULES = {
    "add_to_cart": {"primary_id", "quantity", "product_id", "user_id", "session_id", "device_type", "platform", "price", "brand", "category", "color", "size", "type"},
    "purchase": {"primary_id", "amount", "quantity", "product_id", "items", "user_id", "session_id", "device_type", "platform", "currency", "payment_method", "price", "brand", "category", "color", "size", "type"},
    "login": {"primary_id", "user_id", "session_id", "device_type", "platform"},
    "logout": {"primary_id", "user_id", "session_id", "device_type", "platform"},
    "page_view": {"primary_id", "page_url", "user_id", "session_id", "device_type", "platform"},
    "search": {"primary_id", "user_id", "session_id", "device_type", "platform"}
}

def infer_dtype(value):
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, str):
        if "T" in value and "Z" in value:
            return "DATETIME"
    return "VARCHAR_1000"

def write_csv_with_types(data, filename, fieldnames):
    logger.info(f"Writing data to {filename}")
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            string_row = {k: "" if v is None else str(v).lower() if isinstance(v, bool) else str(v) for k, v in row.items()}
            writer.writerow(string_row)
    logger.info(f"Completed writing to {filename}")

def get_tenant_schema(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/info"
    logger.info(f"Fetching tenant schema from {url}")
    response = requests.get(url)
    config.handle_curl_debug("GET", url, headers=None, data=None, response=response)
    if not response.ok:
        logger.error(f"Failed to fetch tenant schema: {response.status_code} {response.text}")
        raise Exception(f"Failed to fetch tenant schema: {response.status_code} {response.text}")
    data = response.json()
    return data.get("customerFields", []), data.get("eventFields", []), data.get("productFields", [])