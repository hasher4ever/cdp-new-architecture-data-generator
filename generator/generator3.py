import csv
import uuid
import random
import json
from faker import Faker
from datetime import datetime

fake = Faker()

NUM_CUSTOMERS = 20
NUM_EVENTS = 40
NUM_PRODUCTS = 10

EVENT_TYPES = ['add_to_cart', 'purchase', 'login', 'logout', 'page_view', 'search']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PLATFORMS = ['web', 'iOS', 'Android']
LOYALTY_STATUSES = ['bronze', 'silver', 'gold']
CURRENCIES = ['USD', 'EUR', 'RUB']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']

def infer_dtype(value):
    if isinstance(value, bool) or str(value).lower() in {"true", "false"}:
        return "BOOL"
    try:
        int(value)
        return "BIGINT"
    except:
        pass
    try:
        float(value)
        return "DOUBLE"
    except:
        pass
    if isinstance(value, str):
        if "T" in value and ":" in value:
            return "DATETIME"
        if "-" in value:
            try:
                datetime.fromisoformat(value)
                return "DATE"
            except:
                pass
    return "VARCHAR_1000"

# Shared product catalog
products = [{
    "product_id": str(uuid.uuid4()),
    "product_name": fake.word().capitalize(),
    "category": fake.word(),
    "price": round(random.uniform(10, 500), 2),
    "currency": random.choice(CURRENCIES)
} for _ in range(NUM_PRODUCTS)]

# Customers
customers = []
customer_ids = []
customer_field_types = {}

for _ in range(NUM_CUSTOMERS):
    customer = {
        "customer_id": str(uuid.uuid4()),
        "external_id": str(uuid.uuid4()),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(["Male", "Female"]),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "location": f"{fake.city()}, {fake.country()}",
        "postal_code": fake.postcode(),
        "language": fake.language_name(),
        "preferred_currency": random.choice(CURRENCIES),
        "registration_date": fake.date_this_decade().isoformat(),
        "last_login": fake.date_time_this_year().isoformat(),
        "loyalty_status": random.choice(LOYALTY_STATUSES),
        "consent_status": random.choice([True, False]),
        "marketing_opt_in": random.choice([True, False])
    }
    customers.append(customer)
    customer_ids.append(customer["customer_id"])

    if not customer_field_types:
        for k, v in customer.items():
            customer_field_types[k] = infer_dtype(v)

with open("customers.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)

# Events
events = []
event_field_types = {}  # key: event_type -> value: {field: type}

def generate_event_data(event_name, user_id):
    base_event = {
        "event_name": event_name,
        "event_timestamp": fake.date_time_this_year().isoformat(),
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "device_type": random.choice(DEVICE_TYPES),
        "platform": random.choice(PLATFORMS),
        "location": f"{fake.city()}, {fake.country()}"
    }

    if event_name == "add_to_cart":
        product = random.choice(products)
        base_event.update({
            **product,
            "quantity": random.randint(1, 5),
            "cart_id": str(uuid.uuid4())
        })
    elif event_name == "purchase":
        items = random.sample(products, k=random.randint(1, 3))
        total = sum(p["price"] for p in items)
        base_event.update({
            "order_id": str(uuid.uuid4()),
            "total_amount": round(total, 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "items": ";".join(p["product_id"] for p in items),
            "shipping_address": fake.address().replace("\n", ", ")
        })
    elif event_name == "page_view":
        base_event.update({
            "url": fake.url(),
            "referrer_url": fake.url(),
            "page_title": fake.sentence(nb_words=3),
            "duration": random.randint(5, 300)
        })
    elif event_name == "search":
        base_event.update({
            "search_term": fake.word(),
            "search_category": fake.word(),
            "results_count": random.randint(0, 100)
        })
    elif event_name in ["login", "logout"]:
        base_event.update({
            "login_method": random.choice(["email", "google", "facebook"]),
            "success": random.choice([True, False]),
            "ip_address": fake.ipv4()
        })

    return base_event

for _ in range(NUM_EVENTS):
    user_id = random.choice(customer_ids)
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
fieldnames = list(fieldnames)

with open("events.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(events)

# Prepare event mappings
event_mappings = []
for event_type, fields in event_field_types.items():
    for field, dtype in fields.items():
        event_mappings.append({
            "event_name": event_type,
            "field_name": field,
            "field_type": dtype
        })

# Write variables.json
variables = {
    "customer_fields": customer_field_types,
    "event_fields": event_field_types,
    "event_mappings": event_mappings
}
with open("variables.json", "r+", encoding="utf-8") as f:
    try:
        data = json.load(f)
    except json.JSONDecodeError:
        data = {}
    data.update(variables)
    f.seek(0)
    json.dump(data, f, indent=2)
    f.truncate()
