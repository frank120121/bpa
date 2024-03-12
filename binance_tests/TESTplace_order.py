from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET
from credentials import credentials_dict

# Select the account for which you want to place an order
selected_account = "account_2"

# Extract API key and secret key from credentials_dict
api_key = credentials_dict[selected_account]['KEY']
secret_key = credentials_dict[selected_account]['SECRET']

# Initialize the Binance client with the extracted API keys
client = Client(api_key, secret_key)

# Define the order parameters
symbol = "BTCUSDC"  # Trading pair symbol for BTC/USDC
side = "BUY"
quantity = 0.00025  # The quantity of BTC you want to buy

# Step 1: Check account balance in USDC
account_info = client.get_account()
balances = {asset['asset']: float(asset['free']) for asset in account_info['balances']}

if "USDC" in balances and balances["USDC"] >= quantity:
    print("Sufficient USDC balance to place the order.")
else:
    print("Insufficient USDC balance to place the order.")
    exit()

# Step 2: Place the actual order
actual_order = client.create_order(
    symbol=symbol,
    side=side,
    type=ORDER_TYPE_MARKET,
    quantity=quantity
)

# Check if the actual order was successful
if actual_order['status'] == "FILLED":
    print("Actual order executed successfully.")

