import os
import json
import requests
import config
from datetime import datetime, timezone
from collections import defaultdict

LOG_FILE = "_2_register_schema.log"
VARIABLES_FILE = "variables.json"
MAPPINGS_FILE = "event_mappings.json"
TENANT_FILE = "tenant.json"

def log_request_response(url, payload, response):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        timestamp = datetime.now(timezone.utc).isoformat()
        f.write(f"\n[{timestamp}] REQUEST to {url}\n")
        f.write(f"Payload: {json.dumps(payload, indent=2)}\n")
        f.write(f"Response Code: {response.status_code}\n")
        f.write(f"Response Body: {response.text}\n")

def load_tenant_id():
    if not os.path.exists(TENANT_FILE):
        raise FileNotFoundError(f"{TENANT_FILE} not found. Run tenant creation first.")
    with open(TENANT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "tenant_id" not in data:
        raise KeyError(f"tenant_id not found in {TENANT_FILE}.")
    return data["tenant_id"]

def load_variable(key):
    if not os.path.exists(VARIABLES_FILE):
        raise FileNotFoundError(f"{VARIABLES_FILE} not found. Run generator first.")
    with open(VARIABLES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if key not in data:
        raise KeyError(f"{key} not found in {VARIABLES_FILE}. Run generator first.")
    return data[key]

def load_mappings():
    if not os.path.exists(MAPPINGS_FILE):
        raise FileNotFoundError(f"{MAPPINGS_FILE} not found. Run generator first.")
    with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def get_existing_fields(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/info"
    response = requests.get(url)
    if not response.ok:
        raise Exception(f"Failed to fetch tenant info: {response.status_code} {response.text}")
    data = response.json()
    customer_fields = {field["name"] for field in data.get("customerFields", [])}
    event_fields = {field["name"] for field in data.get("eventFields", [])}
    return customer_fields, event_fields

def get_existing_event_mappings(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/schema/events/field-mappings"
    response = requests.get(url)
    if not response.ok:
        return {}
    data = response.json()
    return data.get("mappings", {})

def post_new_customer_fields(fields, base_url, tenant_id):
    existing_customer_fields, _ = get_existing_fields(base_url, tenant_id)
    for field_name, field_type in fields.items():
        if field_name in existing_customer_fields:
            continue
        payload = {"name": field_name, "dtype": field_type}
        url = f"{base_url}/api/tenants/{tenant_id}/schema/customers/fields/draft"
        response = requests.post(url, json=payload)
        log_request_response(url, payload, response)
        print(f"Customer field: {field_name} -> {response.status_code}")
        assert response.ok, f"Failed to register customer field: {response.text}"

def post_new_event_fields(fields, base_url, tenant_id):
    _, existing_fields = get_existing_fields(base_url, tenant_id)
    new_fields = []
    for field in fields:
        if field["name"] in existing_fields:
            continue
        payload = {"name": field["name"], "dtype": field["dtype"]}
        url = f"{base_url}/api/tenants/{tenant_id}/schema/events/fields/draft"
        response = requests.post(url, json=payload)
        log_request_response(url, payload, response)
        print(f"Event field: {field['name']} -> {response.status_code}")
        assert response.ok, f"Failed to register event field: {response.text}"
        new_fields.append(field["name"])
    return new_fields

def post_new_event_mappings(mappings, base_url, tenant_id, new_fields):
    existing_mappings = get_existing_event_mappings(base_url, tenant_id)
    _, existing_event_fields = get_existing_fields(base_url, tenant_id)
    all_event_fields = existing_event_fields.union(new_fields)
    event_field_rules = load_variable("event_field_rules")

    new_mappings = defaultdict(list)
    for event_name, fields in mappings.items():
        if event_name not in event_field_rules:
            continue
        allowed_fields = event_field_rules[event_name]
        existing_fields = set(existing_mappings.get(event_name, []))
        valid_fields = [f for f in fields if f in allowed_fields and f in all_event_fields and f not in existing_fields]
        if valid_fields:
            new_mappings[event_name].extend(valid_fields)

    if not new_mappings:
        print("No new mappings to register.")
        return

    payload = {"mappings": dict(new_mappings)}
    url = f"{base_url}/api/tenants/{tenant_id}/schema/events/field-mappings"
    response = requests.post(url, json=payload)
    log_request_response(url, payload, response)
    print(f"Mappings POST -> {response.status_code}")
    assert response.ok, f"Failed to post mappings: {response.text}"

if __name__ == "__main__":
    base_url = config.BASE_URL_1
    tenant_id = load_tenant_id()
    customer_fields = load_variable("customer_fields")
    mappings_data = load_mappings()
    post_new_customer_fields(customer_fields, base_url, tenant_id)
    new_event_fields = post_new_event_fields(mappings_data["fields"], base_url, tenant_id)
    post_new_event_mappings(mappings_data["mappings"], base_url, tenant_id, new_event_fields)