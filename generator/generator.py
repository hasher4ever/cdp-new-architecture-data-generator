import csv
import uuid
import random
from faker import Faker

fake = Faker()

# Constants
NUM_CUSTOMERS = 50
NUM_EVENTS = 100
EVENT_TYPES = ['add_to_cart', 'purchase', 'login', 'logout', 'page_view', 'search']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PLATFORMS = ['web', 'iOS', 'Android']
LOYALTY_STATUSES = ['bronze', 'silver', 'gold']
CURRENCIES = ['USD', 'EUR', 'GBP']
PAYMENT_METHODS = ['credit_card', 'paypal', 'bank_transfer']

# Generate Customers
customers = []
customer_ids = []

for _ in range(NUM_CUSTOMERS):
    customer_id = str(uuid.uuid4())
    customer_ids.append(customer_id)
    customers.append({
        "customer_id": customer_id,
        "external_id": str(uuid.uuid4()),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(["Male", "Female", "Other"]),
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
    })

# Write Customers to CSV
with open("customers.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)

# Helper to create event-specific data
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
        base_event.update({
            "product_id": str(uuid.uuid4()),
            "product_name": fake.word().capitalize(),
            "category": fake.word(),
            "price": round(random.uniform(10, 500), 2),
            "quantity": random.randint(1, 5),
            "currency": random.choice(CURRENCIES),
            "cart_id": str(uuid.uuid4())
        })
    elif event_name == "purchase":
        base_event.update({
            "order_id": str(uuid.uuid4()),
            "total_amount": round(random.uniform(50, 1000), 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "items": fake.words(nb=random.randint(1, 3)),
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

# Generate Events
events = [generate_event_data(random.choice(EVENT_TYPES), random.choice(customer_ids)) for _ in range(NUM_EVENTS)]

# Flatten keys
fieldnames = set()
for event in events:
    fieldnames.update(event.keys())
fieldnames = list(fieldnames)

# Write Events to CSV
with open("events.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(events)
