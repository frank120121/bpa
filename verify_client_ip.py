import asyncio
import logging
from fetch_outlook import outlook_fetch_ip
from fetch_gmail import gmail_fetch_ip
from ip_info_io import get_ip_origin
from common_vars import MERCHANTS

logger = logging.getLogger(__name__)

async def fetch_ip(last_four_digits, seller_name):
    if seller_name not in MERCHANTS:
        logger.error(f"Seller({seller_name}) not in MERCHANTS.")
        return

    email_id = MERCHANTS[seller_name]
    ip = None

    for attempt in range(10):
        await asyncio.sleep(3)
        if email_id == 1:
            logger.debug(f"Fetching from outlook.com")
            ip = await outlook_fetch_ip(last_four_digits)
        else:
            logger.debug(f"Fetching from gmail.com")
            ip = await gmail_fetch_ip(last_four_digits)

        if ip:
            break
        else:
            logger.warning(f"Attempt {attempt + 1}: Could not fetch the IP. Retrying...")

    if ip:
        country = get_ip_origin(ip)
        logger.info(f"IP from Country: {country}")
        return country
    else:
        logger.error(f"Failed to fetch the IP address after 10 attempts for the last four digits: {last_four_digits}.")

        
async def main():
    last_four_digits = '1248'
    seller_name = 'MUÑOZ PEREA MARIA FERNANDA'
    logger.info(f'Fetching ip for {last_four_digits}')
    country = await fetch_ip(last_four_digits, seller_name)
    if country and country != "MX":
        logger.warning(f"The IP address originates from {country}. Transaction denied. Seller not from Mexico. Buyer added to blacklist.")
    elif country == "MX":
        logger.info(f"The IP address is from Mexico.")

if __name__ == "__main__":
    asyncio.run(main())