import requests
import uuid
import json
import os
from datetime import datetime, timezone
import config

logger = config.logger
TENANT_FILE = "tenant.json"

def save_variable(key, value):
    data = {}
    if os.path.exists(TENANT_FILE):
        with open(TENANT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    data[key] = value
    with open(TENANT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {key} to {TENANT_FILE}")

def create_tenant():
    tenant_name = f"tenant-{uuid.uuid4().hex[:8]}"
    payload = {"name": tenant_name}
    url = f"{config.BASE_URL_1}/api/tenants"
    logger.info(f"Creating tenant: {tenant_name}")
    response = requests.post(url, json=payload)
    config.handle_curl_debug("POST", url, headers=None, data=payload, response=response)

    if response.status_code == 200:
        tenant_id = response.json()["tenant"]["tenantId"]
        save_variable("tenant_id", tenant_id)
        logger.info(f"Tenant created: {tenant_name} with ID {tenant_id}")
        return tenant_id
    else:
        logger.error(f"Failed to create tenant: {response.status_code} {response.text}")
        raise Exception(f"Failed to create tenant: {response.status_code} {response.text}")

if __name__ == "__main__":
    create_tenant()