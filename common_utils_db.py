import aiosqlite
import asyncio
import logging
from prettytable import PrettyTable
logger = logging.getLogger(__name__)
async def create_connection(db_file, num_retries=3, delay_seconds=5):
    logger.debug("Inside async_create_connection function")
    conn = None
    retries = 0
    while retries < num_retries:
        try:
            conn = await aiosqlite.connect(db_file)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}. Retrying in {delay_seconds} seconds.")
            await asyncio.sleep(delay_seconds)
            retries += 1
    logger.error("Max retries reached. Could not establish the database connection.")
    return None
def handle_error(e, message_prefix):
    if isinstance(e, Exception):
        logger.error(f"Database error: {e}")
    else:
        logger.error(f"{message_prefix}: {e}")
async def execute_and_commit(conn, sql, params=None):
    try:
        cursor = await conn.execute(sql, params)
        await conn.commit()
        await cursor.close()
    except Exception as e:
        handle_error(e, "Exception in execute_and_commit")

async def print_table_contents(conn, table_name):
    async with conn.cursor() as cursor:
        try:
            await cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = await cursor.fetchall()
            column_names = [column[1] for column in columns_info]
            await cursor.execute(f"SELECT * FROM {table_name}")
            rows = await cursor.fetchall()
            table = PrettyTable()
            table.field_names = column_names
            for row in rows:
                table.add_row(row)
            print(f"\nContents of {table_name}:")
            print(table)
        except Exception as e:
            print(f"Error reading from table {table_name}: {e}")

async def add_column_if_not_exists(conn, table_name, column_name, data_type, default_value):
    try:
        await conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type} DEFAULT {default_value}"
        )
        await conn.commit()
    except aiosqlite.OperationalError:
        pass

async def clear_table(conn, table_name):
    # Validate the table_name to ensure it's a safe and valid identifier
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: {table_name}")

    # Execute the DELETE command
    await conn.execute(f"DELETE FROM {table_name}")
    await conn.commit()

async def remove_from_table(conn, table_name, column_name, value):
    # Validate the table_name and column_name to ensure they're safe and valid identifiers
    if not table_name.isidentifier() or not column_name.isidentifier():
        raise ValueError("Invalid table or column name")

    # Prepare the DELETE command with placeholders for safety
    query = f"DELETE FROM {table_name} WHERE {column_name} = ?"

    # Execute the DELETE command
    await conn.execute(query, (value,))
    await conn.commit()

async def create_table(conn, create_table_sql):
    async with conn.cursor() as cursor:
        await cursor.execute(create_table_sql)