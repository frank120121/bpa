import aiosqlite
import statistics
import asyncio
import aiohttp
from binance_api import BinanceAPI
from credentials import credentials_dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def update_merchant_id(conn, order_no, merchant_id):
    await conn.execute(
        "UPDATE orders SET merchant_id = ? WHERE order_no = ?",
        (merchant_id, order_no)
    )
    await conn.commit()

async def get_statistics_with_retry(order_no, merchant_id, conn, min_delay):
    if merchant_id is not None:
        accounts_to_try = [f'account_{merchant_id}']
    else:
        accounts_to_try = ['account_1', 'account_2', 'account_3']

    for account in accounts_to_try:
        credentials = credentials_dict[account]
        binance_api = BinanceAPI(credentials['KEY'], credentials['SECRET'], min_delay)
        response = await binance_api.get_counterparty_order_statistics(order_no)
        await binance_api.close_session()  # Ensure session is closed after each use
        if response:
            if response.get("code") != -1000:
                if merchant_id is None:
                    new_merchant_id = int(account.split('_')[1])
                    await update_merchant_id(conn, order_no, new_merchant_id)
                return response
            else:
                logger.error(f"System abnormality with {account}: {response.get('msg')}")
        else:
            logger.error(f"Failed with {account}: {response}")
    return None

async def process_p2p_blacklist_orders(db_path):
    conn = await aiosqlite.connect(db_path)
    cursor = await conn.execute("SELECT order_no, merchant_id FROM orders")
    orders = await cursor.fetchall()

    completedOrderNumOfLatest30day = []
    finishRateLatest30Day = []
    completedOrderNum = []
    finishRate = []
    users_with_less_than_3_orders = 0
    users_with_more_than_3_orders = 0

    for order in orders:
        order_no, merchant_id = order
        response = await get_statistics_with_retry(order_no, merchant_id, conn, min_delay=1)
        if response and response.get("success"):
            data = response.get("data", {})
            completed_orders = data.get("completedOrderNum", 0)
            completedOrderNumOfLatest30day.append(data.get("completedOrderNumOfLatest30day", 0))
            finishRateLatest30Day.append(data.get("finishRateLatest30Day", 0.0))
            completedOrderNum.append(completed_orders)
            finishRate.append(data.get("finishRate", 0.0))

            if completed_orders < 5:
                users_with_less_than_3_orders += 1
            elif completed_orders > 5:
                users_with_more_than_3_orders += 1
        else:
            logger.error(f"Failed to get data for order_no {order_no}: {response}")

    if completedOrderNumOfLatest30day:
        print(f"Median completedOrderNumOfLatest30day: {statistics.median(completedOrderNumOfLatest30day)}")
        print(f"Average completedOrderNumOfLatest30day: {statistics.mean(completedOrderNumOfLatest30day)}")
    if finishRateLatest30Day:
        print(f"Median finishRateLatest30Day: {statistics.median(finishRateLatest30Day)}")
        print(f"Average finishRateLatest30Day: {statistics.mean(finishRateLatest30Day)}")
    if completedOrderNum:
        print(f"Median completedOrderNum: {statistics.median(completedOrderNum)}")
        print(f"Average completedOrderNum: {statistics.mean(completedOrderNum)}")
    if finishRate:
        print(f"Median finishRate: {statistics.median(finishRate)}")
        print(f"Average finishRate: {statistics.mean(finishRate)}")

    print(f"Number of users with less than 3 completed orders: {users_with_less_than_3_orders}")
    print(f"Number of users with more than 3 completed orders: {users_with_more_than_3_orders}")

    await conn.close()

if __name__ == "__main__":
    DB_FILE = "C:/Users/p7016/Documents/bpa/orders_data.db"
    asyncio.run(process_p2p_blacklist_orders(DB_FILE))
