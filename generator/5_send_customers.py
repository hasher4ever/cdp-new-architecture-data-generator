import csv
import time
import json
import requests
from config import BASE_URL_2, AUTH_TOKEN, DELAY_BETWEEN_REQUESTS, handle_curl_debug

# Load tenant_id from tenant.json
with open("tenant.json", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]

CSV_PATH = "customers.csv"

API_ENDPOINT = f"{BASE_URL_2}/cdp-ignest/ingest/tenant/{tenant_id}/customer"

# Prepare headers
headers = {"Content-Type": "application/json"}
if AUTH_TOKEN:
    headers["Authorization"] = AUTH_TOKEN

# Numeric fields to parse as integers
NUMERIC_FIELDS = ["primary_id"]

# Send rows
with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Parse row to preserve types
        parsed_row = {}
        for k, v in row.items():
            if v == "":
                parsed_row[k] = None
            elif k in NUMERIC_FIELDS:
                parsed_row[k] = int(v)
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