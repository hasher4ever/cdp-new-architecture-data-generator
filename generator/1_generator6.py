import csv
import uuid
import random
import json
import requests
from faker import Faker
from datetime import datetime
from collections import defaultdict
import config

fake = Faker()

NUM_CUSTOMERS = 20
NUM_EVENTS = 40
NUM_PRODUCTS = 10

EVENT_TYPES = ['add_to_cart', 'purchase', 'login', 'logout', 'page_view', 'search']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PLATFORMS = ['web', 'iOS', 'Android']
CURRENCIES = ['USD', 'EUR', 'RUB']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']


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
        if "-" in value:
            try:
                datetime.fromisoformat(value)
                return "DATE"
            except:
                pass
    return "VARCHAR_1000"


# Load tenant_id from tenant.json
with open("tenant.json", "r", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]


# Fetch tenant schema
def get_tenant_schema(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/info"
    response = requests.get(url)
    if not response.ok:
        raise Exception(f"Failed to fetch tenant schema: {response.status_code} {response.text}")
    data = response.json()
    return data.get("customerFields", []), data.get("eventFields", [])


# Get schema fields
customer_fields, event_fields = get_tenant_schema(config.BASE_URL_1, tenant_id)


# Map field types to generators
def generate_field_value(field):
    field_type = field["type"]
    nullable = field["nullable"]
    size = field["size"]

    if nullable and random.random() < 0.2:  # 20% chance of null for nullable fields
        return None

    if field_type == "bigint":
        if field["name"] == "primary_id":
            return random.randint(100000, 999999)  # Unique ID
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
            return random.choice(EVENT_TYPES)
        elif field["name"] == "user_id" or field["name"] == "session_id" or field["name"] == "product_id":
            return str(uuid.uuid4())
        elif field["name"] == "page_url":
            return fake.url()
        elif field["name"] in ["device_type", "platform", "currency", "payment_method"]:
            return random.choice(DEVICE_TYPES if field["name"] == "device_type" else
                                 PLATFORMS if field["name"] == "platform" else
                                 CURRENCIES if field["name"] == "currency" else
                                 PAYMENT_METHODS)
        return fake.word()[:size] if size else fake.word()

    elif field_type == "date":
        return fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat()

    elif field_type == "datetime":
        return fake.date_time_this_year().isoformat() + "Z"

    elif field_type == "double":
        return round(random.uniform(10, 500), 2)

    elif field_type == "boolean":
        return random.choice([True, False])

    raise ValueError(f"Unknown field type: {field_type}")


# Generate customers
customers = []
customer_ids = []
customer_field_types = {}

for _ in range(NUM_CUSTOMERS):
    customer = {}
    for field in customer_fields:
        if field["name"] == "created_at" and field["flags"]["tableBuildIn"]:
            continue  # Skip system-managed created_at
        customer[field["name"]] = generate_field_value(field)

    customers.append(customer)
    if "primary_id" in customer:
        customer_ids.append(customer["primary_id"])

    if not customer_field_types:
        for field in customer_fields:
            customer_field_types[field["name"]] = field["type"].replace("boolean", "BOOL").replace("bigint",
                                                                                                   "BIGINT").replace(
                "double", "DOUBLE").replace("varchar", "VARCHAR_1000").replace("date", "DATE").replace("datetime",
                                                                                                       "DATETIME")

with open("customers.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=[f["name"] for f in customer_fields if f["name"] != "created_at"])
    writer.writeheader()
    writer.writerows(customers)

# Generate events
events = []
event_field_types = {}  # key: event_type -> value: {field: type}


def generate_event_data(event_name, user_id):
    event = {"event_type": event_name}
    for field in event_fields:
        if field["name"] in ["created_at", "offset", "partition_id"] and field["flags"]["tableBuildIn"]:
            continue  # Skip system-managed fields
        if field["name"] == "user_id" and user_id:
            event["user_id"] = user_id
        elif field["name"] == "event_type":
            continue  # Already set
        else:
            value = generate_field_value(field)
            if value is not None or not field["nullable"]:
                event[field["name"]] = value

    # Add event-specific fields
    if event_name == "add_to_cart":
        event.update({
            "quantity": random.randint(1, 5),
            "product_id": str(uuid.uuid4())
        })
    elif event_name == "purchase":
        items = [str(uuid.uuid4()) for _ in range(random.randint(1, 3))]
        event.update({
            "amount": round(random.uniform(50, 1000), 2),
            "quantity": len(items),
            "product_id": items[0],
            "items": ";".join(items)
        })
    elif event_name == "page_view":
        event.update({
            "page_url": fake.url()
        })

    return event


for _ in range(NUM_EVENTS):
    user_id = random.choice(customer_ids) if customer_ids else str(uuid.uuid4())
    event_type = random.choice(EVENT_TYPES)
    event = generate_event_data(event_type, user_id)
    events.append(event)

    if event_type not in event_field_types:
        event_field_types[event_type] = {}
    for k, v in event.items():
        if k not in event_field_types[event_type]:
            event_field_types[event_type][k] = infer_dtype(v)

# Write events to CSV
fieldnames = set()
for event in events:
    fieldnames.update(event.keys())
fieldnames = [f["name"] for f in event_fields if f["name"] not in ["created_at", "offset", "partition_id"]] + list(
    set(fieldnames) - set(f["name"] for f in event_fields))

with open("events.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(events)

# Prepare event mappings
event_mappings = defaultdict(set)
for event in events:
    event_name = event.get("event_type")
    if not event_name:
        continue
    for key in event.keys():
        if key != "event_type":
            event_mappings[event_name].add(key)

# Create field definitions
field_definitions = []
for event_type, fields in event_field_types.items():
    for field, dtype in fields.items():
        field_definitions.append({
            "name": field,
            "dtype": dtype
        })

# Create event-to-field mappings
mappings_to_save = {
    "fields": field_definitions,
    "mappings": {event: list(fields) for event, fields in event_mappings.items()}
}

with open("event_mappings.json", "w", encoding="utf-8") as f:
    json.dump(mappings_to_save, f, indent=2)

# Save variables
variables = {
    "customer_fields": customer_field_types,
    "event_fields": event_field_types
}
with open("variables.json", "w", encoding="utf-8") as f:
    json.dump(variables, f, indent=2)