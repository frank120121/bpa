import os
from cryptography.fernet import Fernet
import aiosqlite

# Load the encryption key from an environment variable
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")

fernet = Fernet(ENCRYPTION_KEY)

async def encrypt_message(message):
    return fernet.encrypt(message.encode()).decode()

async def decrypt_message(encrypted_message):
    return fernet.decrypt(encrypted_message.encode()).decode()

async def save_merchant(conn, seller_name, api_key, api_secret):
    encrypted_key = await encrypt_message(api_key)
    encrypted_secret = await encrypt_message(api_secret)
    async with conn.execute("INSERT INTO merchants (sellerName, api_key, api_secret) VALUES (?, ?, ?)", (seller_name, encrypted_key, encrypted_secret)):
        await conn.commit()

async def fetch_merchant_credentials(conn, seller_name):
    async with conn.execute("SELECT api_key, api_secret FROM merchants WHERE sellerName = ?", (seller_name,)) as cursor:
        row = await cursor.fetchone()
        if row:
            decrypted_key = await decrypt_message(row[0])
            decrypted_secret = await decrypt_message(row[1])
            return {'KEY': decrypted_key, 'SECRET': decrypted_secret}
        return None

# Example usage
async def main():
    async with aiosqlite.connect("path_to_your_database.db") as conn:
        # Save a new merchant
        await save_merchant(conn, "seller_name_example", "api_key_example", "api_secret_example")

        # Fetch merchant credentials
        credentials = await fetch_merchant_credentials(conn, "seller_name_example")
        print(credentials)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
