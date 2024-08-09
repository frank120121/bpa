import aiosqlite

async def check_timestamp_format():
    DB_FILE = 'C:/Users/p7016/Documents/bpa/orders_data.db'
    async with aiosqlite.connect(DB_FILE) as conn:
        cursor = await conn.cursor()

        # Select a sample value from the `order_date` column
        await cursor.execute("SELECT order_date FROM orders LIMIT 1;")
        result = await cursor.fetchone()

        if result:
            sample_value = result[0]
            print(f"Sample value in `order_date`: {sample_value}")
            if isinstance(sample_value, str):
                print("TIMESTAMP is stored as a string.")
            elif isinstance(sample_value, int):
                print("TIMESTAMP is stored as an integer.")
            elif isinstance(sample_value, float):
                print("TIMESTAMP is stored as a real number.")
            else:
                print("Unknown data type for TIMESTAMP.")
        else:
            print("No data found in `order_date` column.")

# Run the function
import asyncio
asyncio.run(check_timestamp_format())
