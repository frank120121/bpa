import asyncio
import aiosqlite
import logging
from logging_config import setup_logging
setup_logging(log_filename='TESTs_logger.log')
logger = logging.getLogger(__name__)

DB_PATH = 'C:/Users/p7016/Documents/bpa/ads_data.db'

async def test_data_retrieval():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            c = await conn.cursor()
            await c.execute("SELECT * FROM ads")
            all_ads = await c.fetchall()
            print(f"Number of rows retrieved: {len(all_ads)}")
    except Exception as e:
        print(f"An error occurred while retrieving data from the database: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_data_retrieval())
