import requests

response = requests.get('https://api.binance.com/api/v3/time')
data = response.json()

# This will give you the server time in milliseconds
server_time = data.get('serverTime')

print("Server Time:", server_time)