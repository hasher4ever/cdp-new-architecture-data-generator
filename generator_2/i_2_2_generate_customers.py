import json
import random
from datetime import timezone
from faker import Faker
from utils import logger, get_tenant_schema, write_csv_with_types, config

fake = Faker()
NUM_CUSTOMERS = 30000

with open("tenant.json", "r", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

customer_fields, _, _ = get_tenant_schema(config.BASE_URL_1, tenant_id)
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

with open("customers.csv", "w", newline='') as f:
    fieldnames = [f["name"] for f in customer_fields if f["name"] != "created_at"]
    write_csv_with_types(customers, "customers.csv", fieldnames)

# Save customer IDs and field types for other scripts
with open("customer_data.json", "w", encoding="utf-8") as f:
    json.dump({"customer_ids": customer_ids, "customer_field_types": customer_field_types}, f, indent=2)
logger.info("Completed writing to customer_data.json")