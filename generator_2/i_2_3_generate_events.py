import uuid
import random
import json
import csv
from datetime import datetime, timezone
from collections import defaultdict
from utils import (logger, fake, config, get_tenant_schema, write_csv_with_types, infer_dtype,
                  EVENT_TYPES, DEVICE_TYPES, PLATFORMS, CURRENCIES, PAYMENT_METHODS,
                  PRODUCT_BRANDS, PRODUCT_CATEGORIES, PRODUCT_COLORS, PRODUCT_SIZES, PRODUCT_TYPES,
                  EVENT_FIELD_RULES)

NUM_EVENTS = 70000

with open("tenant.json", "r", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

with open("product_data.json", "r", encoding="utf-8") as f:
    product_data = json.load(f)

# Read all rows from the products.csv file
with open("products.csv", "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    products = [row for row in reader]  # Create a list of all rows

# Ensure that each product has the correct product_id from product_data
products = [{**product, "product_id": pid} for product, pid in zip(products, product_data["product_ids"])]

product_ids = product_data["product_ids"]


with open("customer_data.json", "r", encoding="utf-8") as f:
    customer_data = json.load(f)
    customer_ids = customer_data["customer_ids"]

_, event_fields, _ = get_tenant_schema(config.BASE_URL_1, tenant_id)
logger.info("Fetched tenant schema")

def generate_field_value(field, event_type=None):
    field_type = field["type"]
    nullable = field["nullable"]
    size = field["size"]

    if nullable and random.random() < 0.2:
        return None

    if field_type == "bigint":
        if field["name"] == "primary_id":
            return random.randint(100000, 999999)
        elif field["name"] == "offset" or field["name"] == "partition_id":
            return random.randint(0, 1000)
        elif field["name"] == "quantity":
            return random.randint(1, 10)
        return random.randint(0, 10000)

    elif field_type == "varchar":
        if field["name"] == "event_type":
            return event_type
        elif field["name"] in ["user_id", "session_id", "product_id"]:
            return str(uuid.uuid4())
        elif field["name"] == "items":
            return ""  # Will be populated in purchase event with semicolon-separated product IDs
        elif field["name"] == "page_url":
            return fake.url()
        elif field["name"] == "brand":
            return random.choice(PRODUCT_BRANDS[random.choice(PRODUCT_CATEGORIES)])
        elif field["name"] == "category":
            return random.choice(PRODUCT_CATEGORIES)
        elif field["name"] == "color":
            return random.choice(PRODUCT_COLORS)
        elif field["name"] == "size":
            return random.choice(PRODUCT_SIZES[random.choice(PRODUCT_CATEGORIES)])
        elif field["name"] == "type":
            return random.choice(PRODUCT_TYPES[random.choice(PRODUCT_CATEGORIES)])
        elif field["name"] in ["device_type", "platform", "currency", "payment_method"]:
            return random.choice(DEVICE_TYPES if field["name"] == "device_type" else
                              PLATFORMS if field["name"] == "platform" else
                              CURRENCIES if field["name"] == "currency" else
                              PAYMENT_METHODS)
        return fake.word()[:size] if size else fake.word()

    elif field_type in ["date", "datetime"]:
        return fake.date_time_this_year(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    elif field_type == "double":
        if field["name"] in ["price", "amount"]:  # Explicitly handle price and amount as double
            return round(random.uniform(10, 500), 2)
        return round(random.uniform(10, 500), 2)

    elif field_type == "boolean":
        return random.choice([True, False])

    raise ValueError(f"Unknown field type: {field_type}")

def generate_event_data(event_name, user_id):
    event = {"event_type": event_name, "primary_id": user_id}
    allowed_fields = EVENT_FIELD_RULES.get(event_name, set())

    for field in event_fields:
        if field["name"] in ["created_at", "offset", "partition_id"] and field["flags"]["tableBuildIn"]:
            continue
        if field["name"] in ["event_type", "primary_id"]:
            continue
        if field["name"] not in allowed_fields:
            continue
        if field["name"] == "user_id" and user_id:
            event["user_id"] = user_id
        else:
            value = generate_field_value(field, event_name)
            if value is not None or not field["nullable"]:
                event[field["name"]] = value

    if event_name == "search":
        search_query = random.choice(PRODUCT_BRANDS[random.choice(PRODUCT_CATEGORIES)]) + " " + fake.word()
        event["search_query"] = search_query

        match_status = random.choices(["match", "no_match"], weights=[0.95, 0.05])[0]
        event["match_status"] = match_status

        if match_status == "match":
            matching_products = random.sample(product_ids, min(3, len(product_ids)))
            event["matching_product_ids"] = ";".join(matching_products)

    elif event_name in ["add_to_cart", "purchase"]:
        product = random.choice(products)
        price = round(float(product["price"]), 2)
        event.update({
            "product_id": product["product_id"],
            "price": price,
            "brand": product["brand"],
            "category": product["category"],
            "color": product["color"],
            "size": product["size"],
            "type": product["type"]
        })
        if event_name == "add_to_cart":
            event["quantity"] = random.randint(1, 5)
        elif event_name == "purchase":
            quantity = random.randint(1, 5)
            items = [random.choice(products) for _ in range(quantity)]
            event.update({
                "quantity": quantity,
                "amount": round(sum(float(p["price"]) for p in items), 2),
                "items": ";".join(p["product_id"] for p in items)
            })

    elif event_name == "page_view":
        page_type = random.choices(
            ["product", "category", "cart", "about", "home"],
            weights=[0.5, 0.2, 0.1, 0.1, 0.1]
        )[0]

        if page_type == "product":
            product = random.choice(products)
            event.update({"page_url": f"/products/{product['product_id']}"})
        elif page_type == "category":
            category = random.choice(PRODUCT_CATEGORIES)
            event.update({"page_url": f"/categories/{category}"})
        elif page_type == "cart":
            event.update({"page_url": "/cart"})
        elif page_type == "about":
            event.update({"page_url": "/about"})
        elif page_type == "home":
            event.update({"page_url": "/home"})

    return event

# Initialize events list and event_field_types dictionary
events = []
event_field_types = {}

logger.info(f"Generating {NUM_EVENTS} events")
for _ in range(NUM_EVENTS):
    user_id = random.choice(customer_ids) if customer_ids else random.randint(100000, 999999)
    event_type = random.choice(EVENT_TYPES)
    event = generate_event_data(event_type, user_id)
    events.append(event)
    if event_type not in event_field_types:
        event_field_types[event_type] = {}
    for k, v in event.items():
        if k not in event_field_types[event_type]:
            event_field_types[event_type][k] = infer_dtype(v)
logger.info(f"Generated {len(events)} events")

fieldnames = set()
for event in events:
    fieldnames.update(event.keys())
fieldnames = sorted(
    [f["name"] for f in event_fields if f["name"] not in ["created_at", "offset", "partition_id"]] + list(
        fieldnames - set(f["name"] for f in event_fields)))

write_csv_with_types(events, "events.csv", fieldnames)

event_mappings = defaultdict(set)
for event in events:
    event_name = event.get("event_type")
    if not event_name:
        continue
    for key in event.keys():
        if key != "event_type":
            event_mappings[event_name].add(key)

# Force correct data types for critical fields
event_field_types["purchase"]["price"] = "DOUBLE"
event_field_types["purchase"]["amount"] = "DOUBLE"
event_field_types["purchase"]["items"] = "VARCHAR_1000"
event_field_types["add_to_cart"]["price"] = "DOUBLE"

field_definitions = []
for event_type, fields in event_field_types.items():
    for field, dtype in fields.items():
        field_definitions.append({"name": field, "dtype": dtype})

mappings_to_save = {
    "fields": field_definitions,
    "mappings": {event: list(fields) for event, fields in event_mappings.items()}
}

logger.info("Writing event mappings to event_mappings.json")
with open("event_mappings.json", "w", encoding="utf-8") as f:
    json.dump(mappings_to_save, f, indent=2)
logger.info("Completed writing to event_mappings.json")

variables = {
    "customer_fields": customer_data["customer_field_types"],
    "product_fields": product_data["product_field_types"],
    "event_fields": event_field_types,
    "event_field_rules": {event: list(fields) for event, fields in EVENT_FIELD_RULES.items()}
}
logger.info("Writing variables to variables.json")
with open("variables.json", "w", encoding="utf-8") as f:
    json.dump(variables, f, indent=2)
logger.info("Completed writing to variables.json")