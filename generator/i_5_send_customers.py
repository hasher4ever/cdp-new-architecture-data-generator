import csv
import time
import json
import requests
import config

logger = config.logger

with open("tenant.json", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

CSV_PATH = "customers.csv"
API_ENDPOINT = f"{config.BASE_URL_2}/cdp-ignest/ingest/tenant/{tenant_id}/customer"

headers = {"Content-Type": "application/json"}
if config.AUTH_TOKEN:
    headers["Authorization"] = config.AUTH_TOKEN

NUMERIC_FIELDS = ["primary_id"]

logger.info(f"Reading customers from {CSV_PATH}")
with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
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
            logger.info(f"Sending customer: {parsed_row.get('primary_id')}")
            response = requests.post(API_ENDPOINT, json=parsed_row, headers=headers)
            logger.info(f"Response [{response.status_code}]: {response.text}")
            config.handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response)
        except Exception as e:
            logger.error(f"Error sending customer: {e}")
            config.handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response=None)

        time.sleep(config.DELAY_BETWEEN_REQUESTS)
logger.info("Completed sending customers")