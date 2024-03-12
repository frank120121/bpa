import aiosqlite
import asyncio
import logging

DATABASE = "C:/Users/p7016/Documents/bpa/orders_data.db"
logging.basicConfig(level=logging.INFO)

async def check_bot_replied_column_exists(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("PRAGMA table_info(orders)")
        columns = [column[1] for column in await cursor.fetchall()]
        return 'bot_replied' in columns and 'reply_count' in columns

async def upgrade_database():
    async with aiosqlite.connect(DATABASE) as conn:
        if await check_bot_replied_column_exists(conn):
            logging.info("Detected 'bot_replied' and 'reply_count' columns in orders table. Starting upgrade process...")

            # 1. Rename the original orders table
            await conn.execute("ALTER TABLE orders RENAME TO orders_old")
            logging.info("Renamed orders table to orders_old")

            # 2. Create a new orders table with the desired changes
            await conn.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT NOT NULL UNIQUE,
                    buyer_name TEXT,
                    seller_name TEXT,
                    trade_type TEXT,
                    order_status INTEGER,
                    total_price REAL,
                    fiat_unit TEXT,
                    asset TEXT,
                    amount REAL DEFAULT 0,
                    payment_proof TEXT
                )
            """)
            logging.info("Created new orders table with 'asset' and 'amount' columns")

            # 3. Copy data from the old table to the new one
            await conn.execute("""
                INSERT INTO orders (id, order_no, buyer_name, seller_name, trade_type, 
                                    order_status, total_price, fiat_unit, asset, amount, payment_proof)
                SELECT id, order_no, buyer_name, seller_name, trade_type, order_status, 
                       total_price, fiat_unit, 
                       CASE WHEN bot_replied = 0 THEN 'BTC' ELSE 'ETH' END, 
                       CAST(reply_count AS REAL), payment_proof
                FROM orders_old
            """)
            logging.info("Copied data from orders_old to orders")

            # 4. Drop the old table
            await conn.execute("DROP TABLE orders_old")
            logging.info("Dropped orders_old table")

            await conn.commit()
            logging.info("Upgrade completed successfully!")
        else:
            logging.info("No 'bot_replied' or 'reply_count' columns found in orders table. No upgrade needed.")

if __name__ == "__main__":
    asyncio.run(upgrade_database())
