import os
import json
import requests
import config
from collections import defaultdict

logger = config.logger

VARIABLES_FILE = "variables.json"
MAPPINGS_FILE = "event_mappings.json"
TENANT_FILE = "tenant.json"

def load_tenant_id():
    if not os.path.exists(TENANT_FILE):
        logger.error(f"{TENANT_FILE} not found. Run tenant creation first.")
        raise FileNotFoundError(f"{TENANT_FILE} not found. Run tenant creation first.")
    with open(TENANT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "tenant_id" not in data:
        logger.error(f"tenant_id not found in {TENANT_FILE}.")
        raise KeyError(f"tenant_id not found in {TENANT_FILE}.")
    logger.info(f"Loaded tenant_id from {TENANT_FILE}")
    return data["tenant_id"]

def load_variable(key):
    if not os.path.exists(VARIABLES_FILE):
        logger.error(f"{VARIABLES_FILE} not found. Run generator first.")
        raise FileNotFoundError(f"{VARIABLES_FILE} not found. Run generator first.")
    with open(VARIABLES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if key not in data:
        logger.error(f"{key} not found in {VARIABLES_FILE}. Run generator first.")
        raise KeyError(f"{key} not found in {VARIABLES_FILE}. Run generator first.")
    logger.info(f"Loaded {key} from {VARIABLES_FILE}")
    return data[key]

def load_mappings():
    if not os.path.exists(MAPPINGS_FILE):
        logger.error(f"{MAPPINGS_FILE} not found. Run generator first.")
        raise FileNotFoundError(f"{MAPPINGS_FILE} not found. Run generator first.")
    with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
        logger.info(f"Loaded mappings from {MAPPINGS_FILE}")
        return json.load(f)

def get_existing_fields(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/info"
    logger.info(f"Fetching existing fields from {url}")
    response = requests.get(url)
    config.handle_curl_debug("GET", url, headers=None, data=None, response=response)
    if not response.ok:
        logger.error(f"Failed to fetch tenant info: {response.status_code} {response.text}")
        raise Exception(f"Failed to fetch tenant info: {response.status_code} {response.text}")
    data = response.json()
    return {field["name"] for field in data.get("customerFields", [])}, {field["name"] for field in data.get("eventFields", [])}

def get_existing_event_mappings(base_url, tenant_id):
    url = f"{base_url}/api/tenants/{tenant_id}/schema/events/field-mappings"
    logger.info(f"Fetching existing event mappings from {url}")
    response = requests.get(url)
    config.handle_curl_debug("GET", url, headers=None, data=None, response=response)
    if not response.ok:
        return {}
    data = response.json()
    logger.info("Fetched existing event mappings")
    return data.get("mappings", {})

def post_new_customer_fields(fields, base_url, tenant_id):
    existing_customer_fields, _ = get_existing_fields(base_url, tenant_id)
    for field_name, field_type in fields.items():
        if field_name in existing_customer_fields:
            logger.info(f"Customer field {field_name} already exists, skipping")
            continue
        payload = {"name": field_name, "dtype": field_type}
        url = f"{base_url}/api/tenants/{tenant_id}/schema/customers/fields/draft"
        logger.info(f"Registering customer field: {field_name}")
        response = requests.post(url, json=payload)
        config.handle_curl_debug("POST", url, headers=None, data=payload, response=response)
        logger.info(f"Customer field: {field_name} -> {response.status_code}")
        assert response.ok, f"Failed to register customer field: {response.text}"

def post_new_event_fields(fields, base_url, tenant_id):
    _, existing_fields = get_existing_fields(base_url, tenant_id)
    new_fields = []
    for field in fields:
        if field["name"] in existing_fields:
            logger.info(f"Event field {field['name']} already exists, skipping")
            continue
        payload = {"name": field["name"], "dtype": field["dtype"]}
        url = f"{base_url}/api/tenants/{tenant_id}/schema/events/fields/draft"
        logger.info(f"Registering event field: {field['name']}")
        response = requests.post(url, json=payload)
        config.handle_curl_debug("POST", url, headers=None, data=payload, response=response)
        logger.info(f"Event field: {field['name']} -> {response.status_code}")
        if not response.ok:
            logger.error(f"Failed to register event field: {field['name']} with payload {json.dumps(payload)} to {url}: {response.status_code} {response.text}")
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
            logger.warning(f"Event {event_name} not in event_field_rules, skipping")
            continue
        allowed_fields = set(event_field_rules[event_name]) | {"primary_id"}
        existing_fields = set(existing_mappings.get(event_name, []))
        valid_fields = [f for f in fields if f in allowed_fields and f in all_event_fields and f not in existing_fields]
        if valid_fields:
            new_mappings[event_name].extend(valid_fields)

    if not new_mappings:
        logger.info("No new mappings to register.")
        return

    payload = {"mappings": dict(new_mappings)}
    url = f"{base_url}/api/tenants/{tenant_id}/schema/events/field-mappings"
    logger.info(f"Registering new event mappings")
    response = requests.post(url, json=payload)
    config.handle_curl_debug("POST", url, headers=None, data=payload, response=response)
    logger.info(f"Mappings POST -> {response.status_code}")
    assert response.ok, f"Failed to post mappings: {response.text}"

if __name__ == "__main__":
    base_url = config.BASE_URL_1
    tenant_id = load_tenant_id()
    customer_fields = load_variable("customer_fields")
    mappings_data = load_mappings()
    post_new_customer_fields(customer_fields, base_url, tenant_id)
    new_event_fields = post_new_event_fields(mappings_data["fields"], base_url, tenant_id)
    post_new_event_mappings(mappings_data["mappings"], base_url, tenant_id, new_event_fields)