import logging
import coloredlogs
import json
import os

# Placeholder for existing config
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
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler (plain text)
file_handler = logging.FileHandler(APP_LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)

# Colored console output
coloredlogs.install(
    level='INFO',
    logger=logger,
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    level_styles={
        'info': {'color': 'green'},  # INFO in green
        'debug': {'color': 'blue'},
        'warning': {'color': 'yellow'},
        'error': {'color': 'red'},
        'critical': {'color': 'red', 'bold': True},
    }
)

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


# Example usage
if __name__ == "__main__":
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Example cURL logging
    method = "POST"
    url = f"{BASE_URL_1}/test"
    headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
    data = {"key": "value"}
    handle_curl_debug(method, url, headers, data)