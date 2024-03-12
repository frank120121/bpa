import json
import requests
import time
import hashlib
import hmac
from credentials import bitso_credentials

API_KEY = bitso_credentials['bitso_account_MGL']['KEY']
API_SECRET = bitso_credentials['bitso_account_MGL']['SECRET']
BITSO_BASE_URL = "https://api.bitso.com"

def generate_bitso_authorization(HTTP_METHOD, REQUEST_PATH, JSON_PAYLOAD=""):
    SECS = int(time.time())
    DNONCE = str(SECS * 1000)
    signature = hmac.new(
        API_SECRET.encode('utf-8'),
        (DNONCE + HTTP_METHOD + REQUEST_PATH + JSON_PAYLOAD).encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    AUTH_HEADER = f"Bitso {API_KEY}:{DNONCE}:{signature}"
    return AUTH_HEADER

def send_request(HTTP_METHOD, REQUEST_PATH, JSON_PAYLOAD=None):
    headers = {
        'Authorization': generate_bitso_authorization(HTTP_METHOD, REQUEST_PATH, json.dumps(JSON_PAYLOAD) if JSON_PAYLOAD else ""),
        'Content-Type': 'application/json'
    }
    
    full_url = BITSO_BASE_URL + REQUEST_PATH
    response = None
    print(f"Full URL: {full_url}")
    print(f"Headers: {headers}")
    if HTTP_METHOD == "GET":
        response = requests.get(full_url, headers=headers)
    elif HTTP_METHOD == "POST":
        response = requests.post(full_url, headers=headers, json=JSON_PAYLOAD)
    else:
        print(f"Unsupported HTTP method: {HTTP_METHOD}")
        return

    if response.status_code == 200:
        print("Request successful.")
        return response.json()
    else:
        print(f"An error occurred: {response.status_code} {response.reason}")
        return None

def place_order(book, side, order_type, major=None, minor=None, price=None):
    order_payload = {
        "book": book,
        "side": side,
        "type": order_type,
    }

    if major:
        order_payload["major"] = str(major)
    if minor:
        order_payload["minor"] = str(minor)
    if price:
        order_payload["price"] = str(price)

    response = send_request("POST", "/v3/orders/", order_payload)
    return response

balance_response = send_request("GET", "/v3/balance/")
print("balance was success")
print(balance_response)

# buy_order_response = place_order("btc_mxn", "buy", "limit", major=0.00011, price=445115.67)
# print(buy_order_response)

# sell_order_response = place_order("btc_mxn", "sell", "limit", major=0.00016, price=150000)
# print(sell_order_response)


