# run_draft_schema.py

import requests
import json
import config

base_url = config.BASE_URL_1

# Load tenant_id from tenant.json
with open("tenant.json") as f:
    tenant_id = json.load(f)["tenant_id"]

# Construct URL
url = f"{base_url}/api/tenants/{tenant_id}/plan/apply/draft-schema"

# Send POST request
response = requests.post(url)

# Output response
print("Status Code:", response.status_code)
print("Response Body:", response.text)
