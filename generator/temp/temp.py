import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import requests
import config
from config import handle_curl_debug

LOG_FILE = "2_register_schema.log"
VARIABLES_FILE = "../variables.json"


def log_request_response(url, payload, response):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        timestamp = datetime.now(timezone.utc).isoformat()
        f.write(f"\n[{timestamp}] REQUEST to {url}\n")
        f.write(f"Payload: {payload}\n")
        f.write(f"Response Code: {response.status_code}\n")
        f.write(f"Response Body: {response.text}\n")


def load_variable(key):
    if not os.path.exists(VARIABLES_FILE):
        raise FileNotFoundError(f"{VARIABLES_FILE} not found. Run tenant creation first.")
    with open(VARIABLES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if key not in data:
        raise KeyError(f"{key} not found in {VARIABLES_FILE}. Run tenant creation first.")
    return data[key]


def read_csv_fields(filepath: str) -> list:
    fields = set()
    with open(filepath, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            for k, v in row.items():
                if v:
                    fields.add(k)
    return list(fields)

def read_event_mappings(filepath: str) -> dict:
    mapping = defaultdict(set)
    with open(filepath, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            event = row.get("event_name")
            if event:
                for k in row.keys():
                    if k != "event_name":
                        mapping[event].add(k)
    return {k: sorted(v) for k, v in mapping.items()}


def post_field_schema(fields: list, schema: dict, endpoint: str):
    headers = {"Content-Type": "application/json"}
    for field in fields:
        if field in config.SKIP_FIELDS:
            continue
        dtype = schema.get(field)
        print(f"Trying to register: {field}")
        if not dtype:
            print(f"❌ Skipped: No dtype for field: {field}")
            continue
        payload = {"name": field, "dtype": dtype}
        response = requests.post(endpoint, json=payload, headers=headers)
        handle_curl_debug("POST", endpoint, headers, payload, response)
        log_request_response(endpoint, payload, response)

        if not response.ok:
            print(f"❌ Failed to register field: {field}, status: {response.status_code}, response: {response.text}")


def post_event_mappings(mapping: dict, endpoint: str):
    headers = {"Content-Type": "application/json"}
    payload = {"mappings": mapping}
    response = requests.post(endpoint, json=payload, headers=headers)
    handle_curl_debug("POST", endpoint, headers, payload, response)
    log_request_response(endpoint, payload, response)

    if not response.ok:
        print(f"❌ Failed to post mappings, status: {response.status_code}, response: {response.text}")



if __name__ == "__main__":
    tenant_id = load_variable("tenant_id")
    base_url = config.BASE_URL_1


    customer_fields = read_csv_fields(config.CUSTOMERS_CSV)
    event_fields = read_csv_fields(config.EVENTS_CSV)
    event_mappings = read_event_mappings(config.EVENTS_CSV)

    with open("../variables.json", encoding="utf-8") as f:
        full_schema = json.load(f)

    customer_schema = full_schema["customer_fields"]
    event_schema = full_schema["event_fields"]

    post_field_schema(
        fields=list(customer_schema.keys()),
        schema=customer_schema,
        endpoint=f"{base_url}/api/tenants/{tenant_id}/schema/customers/fields/draft"
    )

    post_field_schema(
        fields=list(event_schema.keys()),
        schema=event_schema,
        endpoint=f"{base_url}/api/tenants/{tenant_id}/schema/events/fields/draft"
    )

    post_event_mappings(event_mappings, f"{base_url}/api/tenants/{tenant_id}/schema/events/field-mappings")
