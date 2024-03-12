import asyncio
import aiohttp
import traceback
import re
import json
import os
import datetime
from dotenv import load_dotenv
import logging
load_dotenv("C:/Users/p7016/Documents/bpa/.env.email")
logger = logging.getLogger(__name__)
CLIENT_ID = os.environ.get('MFMP_OUTLOOK_CLIENT_ID')
CLIENT_SECRET = os.environ.get('MFMP_OUTLOOK_SECRET_VALUE')
REDIRECT_URI = os.environ.get('MFMP_OUTLOOK_REDIRECT_URI')
AUTHORIZATION_CODE = os.environ.get('MFMP_OUTLOOK_AUTHORIZATION_CODE')

TOKEN_FILE = 'C:/Users/p7016/Documents/bpa/tokens.json'

async def save_tokens(access_token, refresh_token, expires_in):
    try:
        expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
        with open(TOKEN_FILE, 'w') as f:
            json.dump({
                'access_token': access_token,
                'refresh_token': refresh_token,
                'expiration_time': expiration_time.isoformat()
            }, f)
    except Exception as e:
        logger.error(f"Exception: {e}")

async def load_tokens():
    try:
        with open(TOKEN_FILE, 'r') as f:
            tokens = json.load(f)
            expiration_time = datetime.datetime.fromisoformat(tokens.get('expiration_time'))
            return tokens.get('access_token'), tokens.get('refresh_token'), expiration_time
    except:
        logger.error("Failed to load tokens")
        return None, None, None
    
async def get_access_token(refresh_token=None):
    async with aiohttp.ClientSession() as session:
        try:
            token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
            
            if refresh_token:
                logger.debug(f"REFRESH TOKEN: {refresh_token}")

                data = {
                    "client_id": CLIENT_ID,
                    "scope": "openid offline_access Mail.Read",
                    "refresh_token": refresh_token,
                    "redirect_uri": REDIRECT_URI,
                    "grant_type": "refresh_token",
                    "client_secret": CLIENT_SECRET
                }
            else:
                logger.debug("Gettng ACCESS TOKEN")
                data = {
                    "client_id": CLIENT_ID,
                    "scope": "openid offline_access Mail.Read",
                    "code": AUTHORIZATION_CODE,
                    "redirect_uri": REDIRECT_URI,
                    "grant_type": "authorization_code",
                    "client_secret": CLIENT_SECRET
                }

            response = await session.post(token_url, data=data)
            token_data = await response.json()
            
            access_token = token_data.get('access_token')
            refresh_token = token_data.get('refresh_token')
            expires_in = token_data.get('expires_in')

            if not access_token:
                raise ValueError(f"Failed to get access token. Response: {token_data}")
            
            expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
            logger.warning(f"Token expires in: {expiration_time}")

            await save_tokens(access_token, refresh_token, expires_in)

            return access_token
        except Exception as e:
            logger.error(f"An error occurred: {e}\n{traceback.format_exc()}")

async def outlook_fetch_ip(last_four):
    logger.debug(f"Searching ip for: {last_four}")
    access_token, refresh_token, expiration_time = await load_tokens()
    
    if datetime.datetime.now() >= expiration_time:
        access_token = await get_access_token(refresh_token)

    async with aiohttp.ClientSession() as session:
        if not access_token:
            access_token = await get_access_token(refresh_token)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        try:
            response = await session.get("https://graph.microsoft.com/v1.0/me/messages?$top=2", headers=headers)
            emails_data = await response.json()
            emails = emails_data.get('value', [])
            for email in emails:
                subject = email.get("subject")
                if f"[Binance] Tienes una nueva orden P2P {last_four}" in subject:
                    email_content = email.get("body", {}).get("content", "")
                    ip_match = re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", email_content)
                    logger.debug(f"Ip found: {ip_match.group(0)}")
                    if ip_match:
                        return ip_match.group(0)
                        
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            logger.error(f"Full exception traceback: {traceback.format_exc()}")

async def main():
    access_token, refresh_token, expiration_time = await load_tokens()
    try:
        last_four = '7184'
        ip_info = await outlook_fetch_ip(last_four)
        print(f'IP info:{ip_info}')
        # print("Fetching a new token.")
        # try:
        #     access_token = await get_access_token(refresh_token)
        # except Exception as e:
        #     logger.error(f"Error while fetching new access token: {e}")
        #     logger.error(f"Full exception traceback: {traceback.format_exc()}")
        #     return
    except Exception as e:
        logger.error(f"Error while fetching emails: {e}")
        logger.error(f"Full exception traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    asyncio.run(main())