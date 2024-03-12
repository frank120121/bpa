import ccxt.pro as ccxtpro
import asyncio
import logging
from logging_config import setup_logging
setup_logging(log_filename='TESTs_logger.log')
logger = logging.getLogger(__name__)
async def connect_to_exchange_order_books(exchange_id):
    exchange_class = getattr(ccxtpro, exchange_id)
    exchange = exchange_class()

    try:
        # Connect to the WebSocket stream for order book updates
        order_book_stream = await exchange.watch_order_book('BTC/USDT', limit=25)  # Adjust 'BTC/USD' and 'limit' as needed

        async for order_book in order_book_stream:
            # Handle incoming order book data for this exchange
            print(f"Received order book data from {exchange_id}: {order_book}")

    except Exception as e:
        print(f"Error connecting to {exchange_id}: {e}")
    finally:
        await exchange.close()

async def main():
    exchanges_to_monitor = [
        'alpaca', 'bequant', 'bitcoincom', 'bitfinex', 'bitmex', 'bitopro', 'bitstamp', 'blockchaincom', 'cex', 'cryptocom', 'exmo', 'gate', 'gateio', 'gemini', 'hitbtc', 'hollaex', 'idex', 'independentreserve', 'krakenfutures', 'luno', 'ndax', 'phemex', 'upbit', 'woo', 'ascendex', 'binancecoinm', 'binanceus', 'binance', 'bitget', 'binanceusdm', 'bitvavo', 'bittrex', 'bitfinex2', 'coinbaseprime', 'coinbase', 'coinbasepro', 'coinex', 'bitmart', 'bitpanda', 'bitrue', 'deribit', 'kraken', 'bybit', 'currencycom', 'kucoin', 'huobijp', 'mexc', 'mexc3', 'okx', 'poloniex', 'poloniexfutures', 'whitebit', 'kucoinfutures', 'probit', 'wazirx', 'huobipro', 'huobi'  # Add more exchanges as needed
    ]

    tasks = [connect_to_exchange_order_books(exchange_id) for exchange_id in exchanges_to_monitor]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
