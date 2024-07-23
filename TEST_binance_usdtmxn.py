import requests

def get_binance_tickers():
    url = 'https://api.binance.com/api/v3/ticker/price'
    response = requests.get(url)
    
    if response.status_code == 200:
        tickers = response.json()
        return tickers
    else:
        print(f"Failed to retrieve tickers. Status code: {response.status_code}")
        return None

def main():
    tickers = get_binance_tickers()
    
    if tickers:
        for ticker in tickers:
            print(f"Symbol: {ticker['symbol']} - Price: {ticker['price']}")

if __name__ == "__main__":
    main()
