import csv
import time
import json
import requests
from config import BASE_URL_2, AUTH_TOKEN, DELAY_BETWEEN_REQUESTS, handle_curl_debug

# Load tenant_id from tenant.json
with open("tenant.json", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]

CSV_PATH = "events.csv"

API_ENDPOINT = f"{BASE_URL_2}cdp-ignest/ingest/tenant/{tenant_id}/event"  # update this if endpoint differs

# Prepare headers
headers = {"Content-Type": "application/json"}
if AUTH_TOKEN:
    headers["Authorization"] = AUTH_TOKEN

# Send rows
with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            response = requests.post(API_ENDPOINT, json=row, headers=headers)
            print(f"[{response.status_code}] {response.text}")
            handle_curl_debug("POST", API_ENDPOINT, headers, row, response)
        except Exception as e:
            print("[ERROR]", e)
            handle_curl_debug("POST", API_ENDPOINT, headers, row, response=None)

        time.sleep(DELAY_BETWEEN_REQUESTS)
