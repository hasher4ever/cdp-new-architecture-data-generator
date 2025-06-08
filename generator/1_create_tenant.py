import requests
import uuid
from datetime import datetime, timezone
import config
import json
import os

LOG_FILE = "1_create_tenant.log"
VARIABLES_FILE = "variables.json"

def log_request_response(url, payload, response):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        timestamp = datetime.now(timezone.utc).isoformat()
        f.write(f"\n[{timestamp}] REQUEST to {url}\n")
        f.write(f"Payload: {payload}\n")
        f.write(f"Response Code: {response.status_code}\n")
        f.write(f"Response Body: {response.text}\n")

def save_variable(key, value):
    data = {}
    if os.path.exists(VARIABLES_FILE):
        with open(VARIABLES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    data[key] = value
    with open(VARIABLES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def create_tenant():
    tenant_name = f"tenant-{uuid.uuid4().hex[:8]}"
    payload = {"name": tenant_name}
    url = f"{config.BASE_URL_1}/api/tenants"
    response = requests.post(url, json=payload)
    log_request_response(url, payload, response)

    if response.status_code == 200:
        tenant_id = response.json()["tenant"]["tenantId"]
        save_variable("tenant_id", tenant_id)
        print(f"Tenant created: {tenant_name} with ID {tenant_id}")
        return tenant_id
    else:
        raise Exception(f"Failed to create tenant: {response.status_code} {response.text}")

if __name__ == "__main__":
    create_tenant()
