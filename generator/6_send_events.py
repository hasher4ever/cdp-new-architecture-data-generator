import csv
import time
import json
import requests
import config

logger = config.logger

with open("tenant.json", encoding="utf-8") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

with open("variables.json", encoding="utf-8") as f:
    variables = json.load(f)
    EVENT_FIELD_RULES = {event: set(fields) for event, fields in variables.get("event_field_rules", {}).items()}
logger.info("Loaded event field rules")

CSV_PATH = "events.csv"
API_ENDPOINT = f"{config.BASE_URL_2}/cdp-ignest/ingest/tenant/{tenant_id}/event"
headers = {"Content-Type": "application/json"}
if config.AUTH_TOKEN:
    headers["Authorization"] = config.AUTH_TOKEN

NUMERIC_FIELDS = ["primary_id", "quantity", "offset", "partition_id"]
FLOAT_FIELDS = ["amount"]

logger.info(f"Reading events from {CSV_PATH}")
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
            logger.info(f"Sending event: {event_type} (primary_id: {parsed_row.get('primary_id')})")
            response = requests.post(API_ENDPOINT, json=parsed_row, headers=headers)
            logger.info(f"Response [{response.status_code}]: {response.text}")
            config.handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response)
        except Exception as e:
            logger.error(f"Error sending event: {e}")
            config.handle_curl_debug("POST", API_ENDPOINT, headers, parsed_row, response=None)
        time.sleep(config.DELAY_BETWEEN_REQUESTS)
logger.info("Completed sending events")