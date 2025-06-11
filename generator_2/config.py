import logging
import json
import os

# Placeholder for existing config (e.g., BASE_URL_1, BASE_URL_2, AUTH_TOKEN, DELAY_BETWEEN_REQUESTS)
BASE_URL_1 = os.getenv("CDP_BASE_URL", "http://10.0.10.140:30100")
BASE_URL_2 = os.getenv("CDP_BASE_URL", "http://10.0.10.140:30101")
AUTH_TOKEN = None  # or os.getenv("CDP_AUTH_TOKEN")
DELAY_BETWEEN_REQUESTS = 0.1  # Update with actual value

# Logging configuration
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

APP_LOG_FILE = os.path.join(LOG_DIR, "app.log")
CURL_LOG_FILE = os.path.join(LOG_DIR, "curl.log")

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(APP_LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure cURL logger
curl_handler = logging.FileHandler(CURL_LOG_FILE)
curl_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
curl_logger = logging.getLogger("curl")
curl_logger.setLevel(logging.DEBUG)
curl_logger.addHandler(curl_handler)
curl_logger.propagate = False

def curl_from_request(method: str, url: str, headers: dict = None, data=None):
    parts = [f"curl -X {method.upper()} '{url}'"]
    if headers:
        for k, v in headers.items():
            parts.append(f"-H '{k}: {v}'")
    if data:
        body = json.dumps(data, ensure_ascii=False).replace("'", r"'\''")
        parts.append(f"--data '{body}'")
    return " ".join(parts)

def handle_curl_debug(method, url, headers, data, response=None):
    curl = curl_from_request(method, url, headers, data)
    curl_logger.debug(curl)
    if response and not response.ok:
        logger.error(f"Failed request [{response.status_code}]: {response.text}\n{curl}")