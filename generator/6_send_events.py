import csv
import time
import json
import requests
from config import BASE_URL_2, AUTH_TOKEN, DELAY_BETWEEN_REQUESTS, handle_curl_debug

with open("tenant.json", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]

with open("variables.json", encoding="utf-8") as f:
    variables = json.load(f)
    EVENT_FIELD_RULES = {event: set(fields) for event, fields in variables.get("event_field_rules", {}).items()}

CSV_PATH = "events.csv"
API_ENDPOINT = f"{BASE_URL_2}/cdp-ignest/ingest/tenant/{tenant_id}/event"
headers = {"Content-Type": "application/json"}
if AUTH_TOKEN:
    headers["Authorization"] = AUTH_TOKEN

NUMERIC_FIELDS = ["primary_id", "quantity", "offset", "partition_id"]
FLOAT_FIELDS = ["amount"]

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        parsed_row = {}
        event_type = row.get("event_type")
        allowed_fields = EVENT_FIELD_RULES.get(event_type, set()) | {"event_type"}
        for k, v in row.items():
            if k not in allowed_fields:
                continue
            if v == "":
                parsed_row[k] = None
            elif k in NUMERIC_FIELDS:
                parsed_row[k] = int(v) if v.replace("-", "").isdigit() else v
            elif k in FLOAT_FIELDS:
                parsed_row[k] = float(v) if v.replace(".", "").replace("-", "").isdigit() else v
            elif v.lower() in ("true", "false"):
                parsed_row[k] = v.lower() == "true"
            else:
                parsed_row[k] = v
        try:
            response = requests.post(API_ENDPOINT, json=parsed_row, headers=headers)
            print(f"[{response.status_code}] {response.text}")
            handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response)
        except Exception as e:
            print("[ERROR]", e)
            handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response=None)
        time.sleep(DELAY_BETWEEN_REQUESTS)