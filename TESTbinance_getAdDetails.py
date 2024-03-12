
import hmac
import time
import hashlib
import requests
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
import logging
from logging_config import setup_logging
setup_logging(log_filename='TESTs_logger.log')
logger = logging.getLogger(__name__)

load_dotenv()
BASE_URL = "https://api.binance.com"
credentials_dict = {
    'account_1': {
        'KEY': os.environ.get('API_KEY_MFMP'),
        'SECRET': os.environ.get('API_SECRET_MFMP')
    },
    'account_2': {
        'KEY': os.environ.get('API_KEY_MGL'),
        'SECRET': os.environ.get('API_SECRET_MGL')
    }
}
def hashing(query_string, SECRET):
    return hmac.new(
        SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
def get_server_time():
    url = BASE_URL + "/api/v3/time"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()['serverTime']
    else:
        return None
server_time = get_server_time()
if server_time:
    time_offset = server_time - int(time.time() * 1000)
else:
    print("Could not fetch server time. Exiting.")
    exit(1)
def get_corrected_timestamp():
    return int(time.time() * 1000) + time_offset
def dispatch_request(http_method, KEY):
    session = requests.Session()
    session.headers.update({
        "X-MBX-APIKEY": KEY,
        "clientType": "WEB"
    })
    return {
        "GET": session.get,
        "DELETE": session.delete,
        "PUT": session.put,
        "POST": session.post,
    }.get(http_method, "GET")
def send_signed_request(http_method, url_path, KEY, SECRET, payload={}, dataLoad={}):
    query_string = urlencode(payload)
    if query_string:
        query_string = f"{query_string}&timestamp={get_corrected_timestamp()}"
    else:
        query_string = f"timestamp={get_corrected_timestamp()}"
    url = f"{BASE_URL}{url_path}?{query_string}&signature={hashing(query_string, SECRET)}"
    params = {"url": url, "params": {}, "data": dataLoad}
    response = dispatch_request(http_method, KEY)(**params)
    return response

if __name__ == "__main__":
    uri_path = "/sapi/v1/c2c/ads/getAdDetails"
    response = send_signed_request("GET", uri_path, credentials_dict['account_1']['KEY'], credentials_dict['account_1']['SECRET'])
    if response.status_code == 200:
        # Print the content of the response
        print(f"API Response Content:\n{response.text}")
    else:
        print(f"API Request failed with status code {response.status_code}")
