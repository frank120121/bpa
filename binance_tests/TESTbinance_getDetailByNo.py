from credentials import credentials_dict
import requests
import hashlib
import hmac
import time
import logging
from logging_config import setup_logging
setup_logging(log_filename='TESTs_logger.log')
logger = logging.getLogger(__name__)

def create_signature(secret_key, query_string):
    return hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

api_key = credentials_dict['account_1']['KEY']
secret_key = credentials_dict['account_1']['SECRET']

api_endpoint = "https://api.binance.com/sapi/v1/c2c/ads/getDetailByNo"
adsNo = "12578448109213659136"
timestamp = str(int(time.time() * 1000)) 
query_string = f"adsNo={adsNo}&timestamp={timestamp}"
signature = create_signature(secret_key, query_string)
headers = {
    'X-MBX-APIKEY': api_key,
    'clientType': 'web',  
}
data = {
    'adsNo': adsNo,
    'timestamp': timestamp,
    'signature': signature
}
response = requests.post(api_endpoint, headers=headers, data=data)
if response.status_code == 200:
    print("Success:", response.json())
else:
    print("Failed:", response.content)
