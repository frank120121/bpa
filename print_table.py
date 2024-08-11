import aiosqlite
import asyncio

async def get_all_table_names(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = await cursor.fetchall()
    return [table[0] for table in tables]

async def fetch_and_print_all_data_from_table(conn, table_name):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(f"SELECT * FROM {table_name};")
            rows = await cursor.fetchall()
            print(f"\nData from {table_name}:")
            for row in rows:
                print(row)
    except aiosqlite.Error as e:
        print(f"Error: {str(e)}")

async def fetch_and_print_all_data_from_all_tables(conn):
    table_names = await get_all_table_names(conn)
    print(f"Found tables: {table_names}")  # Additional log
    for table_name in table_names:
        await fetch_and_print_all_data_from_table(conn, table_name)

async def fetch_and_print_data_for_orders(conn):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM orders;")
            rows = await cursor.fetchall()
            print("\nData from orders:")
            for row in rows:
                print(row)
    except aiosqlite.Error as e:
        print(f"Error: {str(e)}")

async def main():
    print("Starting main function...")
    db_path = "C:/Users/p7016/Documents/bpa/orders_data.db"  # Adjust this path as needed
    print(f"Connecting to {db_path}...")
    conn = await aiosqlite.connect(db_path)

    try:
        print("Fetching and printing data...")
        await fetch_and_print_all_data_from_all_tables(conn)
        await fetch_and_print_data_for_orders(conn)
    finally:
        print("Closing the connection...")
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())