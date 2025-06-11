import requests
import json
import config

logger = config.logger
base_url = config.BASE_URL_1

with open("tenant.json") as f:
    tenant_id = json.load(f)["tenant_id"]
logger.info(f"Loaded tenant_id: {tenant_id}")

url = f"{base_url}/api/tenants/{tenant_id}/plan/apply/draft-schema"
logger.info(f"Validating schema at {url}")
response = requests.post(url)
config.handle_curl_debug("POST", url, headers=None, data=None, response=response)

logger.info(f"Status Code: {response.status_code}")
logger.info(f"Response Body: {response.text}")