# binance_endpoints.py
#Base binance endpoint.
BASE_ENDPOINT = "https://api.binance.com"

# Retrive Server timestamp.
TIME_ENDPOINT_V1 = f"{BASE_ENDPOINT}/api/v1/time"
TIME_ENDPOINT_V3 = f"{BASE_ENDPOINT}/api/v3/time"

#Retrieve Chat WSS URL, Listen Key and Token.
GET_CHAT_CREDENTIALS = f"{BASE_ENDPOINT}/sapi/v1/c2c/chat/retrieveChatCredential"

# User data stream endpoint
USER_DATA_STREAM_ENDPOINT = f"{BASE_ENDPOINT}/api/v3/userDataStream"

# Retrieve User Order Detail
USER_ORDER_DETAIL = f"{BASE_ENDPOINT}/sapi/v1/c2c/orderMatch/getUserOrderDetail"

# Search Ads
SEARCH_ADS = f"{BASE_ENDPOINT}/sapi/v1/c2c/ads/search"
