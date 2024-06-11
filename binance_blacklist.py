import asyncio
import aiosqlite
from common_utils_db import create_connection, print_table_contents

DB_FILE = "C:/Users/p7016/Documents/bpa/orders_data.db"

async def initialize_database(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS P2PBlacklist (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            order_no TEXT,
            country TEXT,
            response TEXT DEFAULT NULL,
            anti_fraud_stage INTEGER DEFAULT 0
        )
        """
    )
    await conn.commit()

async def clear_blacklist(conn):
    await conn.execute("DELETE FROM P2PBlacklist")
    await conn.commit()

#modify the function to accept response and anti_fraud_stage, both of which are optional
async def add_to_blacklist(conn, name, order_no, country, response=None, anti_fraud_stage=0):
    try:
        await conn.execute(
            "INSERT OR IGNORE INTO P2PBlacklist (name, order_no, country, response, anti_fraud_stage) VALUES (?, ?, ?, ?, ?)", 
            (name, order_no, country, response, anti_fraud_stage)
        )
        await conn.commit()
    except aiosqlite.IntegrityError:
        pass

async def is_blacklisted(conn, name):
    cursor = await conn.execute("SELECT id FROM P2PBlacklist WHERE name = ?", (name,))
    result = await cursor.fetchone()
    return result is not None

async def remove_from_blacklist(conn, name):
    await conn.execute("DELETE FROM P2PBlacklist WHERE name = ?", (name,))
    await conn.commit()

# make an async function that removes users form the blacklist that country is none
async def remove_from_blacklist_no_country(conn):
    await conn.execute("DELETE FROM P2PBlacklist WHERE country IS NULL")
    await conn.commit()
    
async def get_blacklist_counts_by_country(conn):
    query = """
    SELECT country, COUNT(*) as count
    FROM P2PBlacklist
    GROUP BY country
    """
    async with conn.execute(query) as cursor:
        result = await cursor.fetchall()
        return result
async def remove_from_blacklist_accepted_countries(conn, accepted_countries):
    placeholders = ', '.join('?' for _ in accepted_countries)
    query = f"DELETE FROM P2PBlacklist WHERE country IN ({placeholders})"
    await conn.execute(query, accepted_countries)
    await conn.commit()
    
async def main():
    conn = await create_connection(DB_FILE)
    await remove_from_blacklist_no_country(conn)
    accepted_countries_for_oxxo = ['MX', 'CO', 'VE', 'AR', 'ES', 'CL', 'CA', 'HK', 'PE', 'BE', 'EC', 'RU', 'TH', 'IN', 'UA', 'DE', 'JP', 'US', 'RU', 'FR']
    await remove_from_blacklist_accepted_countries(conn, accepted_countries_for_oxxo)

    await print_table_contents(conn, 'P2PBlacklist')
    # Get blacklist counts by country
    # counts = await get_blacklist_counts_by_country(conn)
    
    # # Print the counts
    # for country, count in counts:
    #     print(f"{country}: {count}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
