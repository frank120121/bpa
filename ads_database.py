import json
import aiosqlite
import logging
import asyncio
from logging_config import setup_logging
setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
from common_vars import ads_dict
from common_utils_db import print_table_contents, create_connection, add_column_if_not_exists

DB_PATH = 'C:/Users/p7016/Documents/bpa/ads_data.db'

async def create_database():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                advNo TEXT PRIMARY KEY,
                target_spot INTEGER NOT NULL,
                asset_type TEXT NOT NULL,
                price REAL,
                floating_ratio REAL,
                last_updated TIMESTAMP,
                account TEXT NOT NULL,
                surplused_amount REAL DEFAULT 0,
                fiat TEXT NOT NULL DEFAULT 'Unknown',
                transAmount REAL,
                payTypes TEXT NOT NULL DEFAULT '[]',
                `Group` TEXT NOT NULL DEFAULT 'Unknown',
                merchant_id INTEGER REFERENCES merchants(id),
                trade_type TEXT NOT NULL,
                minTransAmount REAL DEFAULT 0.0
            )
        ''')
        await conn.commit()


async def clear_ads_table():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("DELETE FROM ads")
        await conn.commit()


async def fetch_all_ads_from_database(trade_type=None):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        query = "SELECT * FROM ads"
        params = ()
        if trade_type is not None:
            query += " WHERE trade_type = ?"
            params = (trade_type,)
        await c.execute(query, params)
        ads = await c.fetchall()
    return [
        {
            'advNo': ad[0],
            'target_spot': ad[1],
            'asset_type': ad[2],
            'price': ad[3],
            'floating_ratio': ad[4],
            'last_updated': ad[5],
            'account': ad[6],
            'surplused_amount': ad[7],
            'fiat': ad[8],
            'transAmount': ad[9],
            'payTypes': json.loads(ad[10]) if ad[10] and ad[10].strip().startswith('[') else None,
            'Group': ad[11],
            'trade_type': ad[12],
            'minTransAmount': ad[13] if ad[13] is not None else 0.0
        }
        for ad in ads
    ]

async def get_ad_from_database(advNo):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute("SELECT * FROM ads WHERE advNo=?", (advNo,))
        ad = await c.fetchone()
    if ad:
        return {
            'advNo': ad[0],
            'target_spot': ad[1],
            'asset_type': ad[2],
            'price': ad[3],
            'floating_ratio': ad[4],
            'last_updated': ad[5],
            'account': ad[6],
            'surplused_amount': ad[7],
            'fiat': ad[8],
            'transAmount': ad[9],
            'payTypes': json.loads(ad[10]) if ad[10] is not None else None,  # Deserialize or set to None
            'Group': ad[11],
            'trade_type': ad[12],
            'minTransAmount': ad[13] if ad[13] is not None else 0.0
        }
    return None

async def update_ad_in_database(target_spot, advNo, asset_type, floating_ratio, price, surplusAmount, account, fiat, transAmount, minTransAmount):
    logger.debug(f"Attempting to update {advNo} with price: {price}, floating_ratio: {floating_ratio}, asset_type: {asset_type}, target_spot: {target_spot}, fiat: {fiat}, transAmount: {transAmount}, minTransAmount: {minTransAmount}")

    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        try:
            # Update only specific fields without changing payTypes and Group
            await c.execute("""
                UPDATE ads
                SET target_spot = ?, asset_type = ?, price = ?, floating_ratio = ?, last_updated = datetime('now'), account = ?, surplused_amount = ?, fiat = ?, transAmount = ?, minTransAmount = ?
                WHERE advNo = ?""", 
                (target_spot, asset_type, price, floating_ratio, account, surplusAmount, fiat, transAmount, minTransAmount, advNo))
            await conn.commit()

            logger.debug(f"Updated ad {advNo} successfully without modifying payTypes and Group.")
        except Exception as e:
            logger.error(f"Exception during updating ad {advNo}: {e}")


async def insert_initial_ads():
    ads_to_insert = []
    for account_name, ads in ads_dict.items():
        for ad in ads:
            pay_types_serialized = json.dumps(ad.get('payTypes', []))  # Serialize payTypes to JSON
            # Use account_name instead of ad['account']
            ads_to_insert.append((ad['advNo'], ad['target_spot'], ad['asset_type'], account_name,
                                  ad['fiat'], ad['transAmount'], pay_types_serialized, ad['Group'], ad['trade_type'], ad['minTransAmount']))
    await insert_multiple_ads(ads_to_insert)

async def insert_multiple_ads(ads_list):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        for ad in ads_list:
            await c.execute(
                """INSERT OR REPLACE INTO ads (advNo, target_spot, asset_type, account, fiat, transAmount, payTypes, `Group`, trade_type, minTransAmount) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ad  # Pass the tuple directly
            )
        await conn.commit()


async def main():
    conn = await create_connection(DB_PATH)
    if conn is not None:
        # await clear_ads_table()
        # await create_database()
        # await insert_initial_ads()

        await print_table_contents(conn, 'ads')
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())

