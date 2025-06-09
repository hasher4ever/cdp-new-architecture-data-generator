import os
import json

BASE_URL_1 = os.getenv("CDP_BASE_URL", "http://10.0.10.140:30100")
BASE_URL_2 = os.getenv("CDP_BASE_URL", "http://10.0.10.140:30101")
DELAY_BETWEEN_REQUESTS = 0.1#0.5
DEBUG1 = False
DEBUG2 = True
AUTH_TOKEN = None  # or os.getenv("CDP_AUTH_TOKEN")
CURL_LOG_FILE = "_3_curl_requests.log"
CUSTOMERS_CSV = "customers.csv"
EVENTS_CSV = "events.csv"

#SKIP_FIELDS = {
#    "primary_id",
#   "created_at",
#     "first_name",
#     "last_name",
#     "birth_date"
#     "email",
#     "phone",
#     "gender"
#     "offset",
#     "event_type",
#     "partition_id",
#     "product_id",
#     "amount",
#     "quantity",
#     "user_id",
#     "session_id",
#     "page_url",
# }


def curl_from_request(method: str, url: str, headers: dict = None, data=None):
    parts = [f"curl -X {method.upper()} '{url}'"]
    if headers:
        for k, v in headers.items():
            parts.append(f"-H '{k}: {v}'")
    if data:
        body = json.dumps(data, ensure_ascii=False).replace("'", r"'\''")
        parts.append(f"--data '{body}'")
    return " ".join(parts)

def handle_curl_debug(method, url, headers, data, response):
    curl = curl_from_request(method, url, headers, data)
    if DEBUG1 and not response.ok:
        print(f"[DEBUG1] Failed request:\n{curl}")
    if DEBUG2:
        with open(CURL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(curl + "\n")


# TENANT_FILE = "variables.json"

# def load_tenant_id():
#     if os.path.exists(TENANT_FILE):
#         with open(TENANT_FILE, "r", encoding="utf-8") as f:
#             data = json.load(f)
#             return data.get("tenant_id")
#     return None
