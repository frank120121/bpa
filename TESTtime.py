import requests
import os
import json
import time
import hashlib
import hmac
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
KEY = os.environ.get('API_KEY_MFMP')
SECRET = os.environ.get('API_SECRET_MFMP')
BASE_URL = "https://api.binance.com"

# Function to generate signature
def generate_signature(secret_key, data):
    return hmac.new(secret_key.encode(), data.encode(), hashlib.sha256).hexdigest()

# Get server time
response = requests.get("https://api.binance.com/api/v3/time")
server_time = response.json().get("serverTime", 0) // 1000  # Convert to seconds

# Calculate time offset
local_time = int(time.time())
time_offset = local_time - server_time

# Adjusted timestamp in milliseconds
adjusted_timestamp = (int(time.time()) - time_offset) * 1000

# Prepare the data string and signature
data = f'timestamp={adjusted_timestamp}'
signature = generate_signature(SECRET, data)

# Final URL
url = f"{BASE_URL}/sapi/v1/c2c/orderMatch/getUserOrderDetail?{data}&signature={signature}"

# Prepare headers and payload
headers = {
    'X-MBX-APIKEY': KEY,
    'clientType': 'web',
    'Content-Type': 'application/json'
}
payload = {
    "adOrderNo": "20534096348670504960"
}

# Make the POST request
response = requests.post(url, headers=headers, json=payload)
print(response.text)
