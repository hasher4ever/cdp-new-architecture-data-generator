#older version of config.py

import os

BASE_URL = os.getenv("CDP_BASE_URL", "http://localhost:8000")
TENANT_ID = None  # will be set dynamically

CUSTOMER_CSV = "customers.csv"
EVENT_CSV = "events.csv"

DEBUG1 = True   # print failed requests as cURL
DEBUG2 = True   # write all requests as cURL to file

CURL_LOG_FILE = "curl_debug_log.txt"

def curl_from_request(method: str, url: str, headers: dict = None, data=None):
    parts = [f"curl -X {method.upper()} '{url}'"]
    if headers:
        for k, v in headers.items():
            parts.append(f"-H '{k}: {v}'")
    if data:
        import json
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
