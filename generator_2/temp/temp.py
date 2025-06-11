import csv
import uuid
import random
import json
import requests
from faker import Faker
from datetime import datetime, timezone
from collections import defaultdict
import config

logger = config.logger
fake = Faker()

NUM_CUSTOMERS = 1000
NUM_EVENTS = 5000
NUM_PRODUCTS = 200

EVENT_TYPES = ['add_to_cart', 'purchase', 'login', 'logout', 'page_view', 'search']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PLATFORMS = ['web', 'iOS', 'Android']
CURRENCIES = ['USD', 'EUR', 'RUB']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']

EVENT_FIELD_RULES = {
    "add_to_cart": {"primary_id", "quantity", "product_id", "user_id", "session_id", "device_type", "platform"},
    "purchase": {"primary_id", "amount", "quantity", "product_id", "items", "user_id", "session_id", "device_type", "platform", "currency", "payment_method"},
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

with open("tenant.json", "r", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

def get_tenant_schema(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/info"
    logger.info(f"Fetching tenant schema from {url}")
    response = requests.get(url)
    config.handle_curl_debug("GET", url, headers=None, data=None, response=response)
    if not response.ok:
        logger.error(f"Failed to fetch tenant schema: {response.status_code} {response.text}")
        raise Exception(f"Failed to fetch tenant schema: {response.status_code} {response.text}")
    data = response.json()
    return data.get("customerFields", []), data.get("eventFields", [])

customer_fields, event_fields = get_tenant_schema(config.BASE_URL_1, tenant_id)
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
        if field["name"] == "first_name":
            return fake.first_name()
        elif field["name"] == "last_name":
            return fake.last_name()
        elif field["name"] == "gender":
            return random.choice(["Male", "Female", "Other"])
        elif field["name"] == "event_type":
            return event_type
        elif field["name"] in ["user_id", "session_id", "product_id", "items"]:
            return str(uuid.uuid4())
        elif field["name"] == "page_url":
            return fake.url()
        elif field["name"] in ["device_type", "platform", "currency", "payment_method"]:
            return random.choice(DEVICE_TYPES if field["name"] == "device_type" else
                                 PLATFORMS if field["name"] == "platform" else
                                 CURRENCIES if field["name"] == "currency" else
                                 PAYMENT_METHODS)
        return fake.word()[:size] if size else fake.word()

    elif field_type in ["date", "datetime"]:
        return fake.date_time_this_year(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    elif field_type == "double":
        return round(random.uniform(10, 500), 2)

    elif field_type == "boolean":
        return random.choice([True, False])

    raise ValueError(f"Unknown field type: {field_type}")

customers = []
customer_ids = []
customer_field_types = {}

logger.info(f"Generating {NUM_CUSTOMERS} customers")
for _ in range(NUM_CUSTOMERS):
    customer = {}
    for field in customer_fields:
        if field["name"] == "created_at" and field["flags"]["tableBuildIn"]:
            continue
        customer[field["name"]] = generate_field_value(field)
    customers.append(customer)
    if "primary_id" in customer:
        customer_ids.append(customer["primary_id"])
    if not customer_field_types:
        for field in customer_fields:
            customer_field_types[field["name"]] = field["type"].replace("boolean", "BOOL").replace("bigint", "BIGINT").replace(
                "double", "DOUBLE").replace("varchar", "VARCHAR_1000").replace("date", "DATETIME").replace("datetime", "DATETIME")
logger.info(f"Generated {len(customers)} customers")

def write_csv_with_types(data, filename, fieldnames):
    logger.info(f"Writing data to {filename}")
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            string_row = {k: "" if v is None else str(v).lower() if isinstance(v, bool) else str(v) for k, v in row.items()}
            writer.writerow(string_row)
    logger.info(f"Completed writing to {filename}")

with open("customers.csv", "w", newline='') as f:
    fieldnames = [f["name"] for f in customer_fields if f["name"] != "created_at"]
    write_csv_with_types(customers, "customers.csv", fieldnames)

events = []
event_field_types = {}

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

    if event_name == "add_to_cart":
        event.update({"quantity": random.randint(1, 5), "product_id": str(uuid.uuid4())})
    elif event_name == "purchase":
        items = [str(uuid.uuid4()) for _ in range(random.randint(1, 3))]
        event.update({"amount": round(random.uniform(50, 1000), 2), "quantity": len(items), "product_id": items[0], "items": ";".join(items)})
    elif event_name == "page_view":
        event.update({"page_url": fake.url()})

    return event

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

event_field_types["purchase"]["items"] = "VARCHAR_1000"
if "primary_id" not in event_field_types.get("purchase", {}):
    for event_type in EVENT_TYPES:
        event_field_types[event_type]["primary_id"] = "BIGINT"

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
    "customer_fields": customer_field_types,
    "event_fields": event_field_types,
    "event_field_rules": {event: list(fields) for event, fields in EVENT_FIELD_RULES.items()}
}
logger.info("Writing variables to variables.json")
with open("variables.json", "w", encoding="utf-8") as f:
    json.dump(variables, f, indent=2)
logger.info("Completed writing to variables.json")