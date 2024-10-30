import asyncio
import base64
import re
from googleapiclient.discovery import build
import pickle
import os
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set logging to debug level for detailed output
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# Create a logging handler (prints to console)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

load_dotenv('.env.email')
gmail_credentials_json_path = os.getenv('GMAIL_CREDENTIALS_JSON_PATH')
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

async def get_gmail_service():
    creds = None
    token_path = 'C:/Users/p7016/Documents/bpa/token.pickle'
    
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error("Error refreshing credentials, re-authenticating...")
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                gmail_credentials_json_path, SCOPES)
            creds = flow.run_local_server(port=8080)

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)

async def gmail_fetch_ip(last_four):
    max_retries = 5
    retry_count = 0

    service = await get_gmail_service()

    while retry_count < max_retries:
        try:
            # Fetch the 10 most recent emails
            messages_request = await asyncio.to_thread(service.users().messages().list, userId='me', maxResults=10)
            messages_response = messages_request.execute()
            messages = messages_response.get('messages', [])

            logger.debug(f"Fetched {len(messages)} messages")

            for message in messages:
                # Get each message's details
                msg_request = await asyncio.to_thread(service.users().messages().get, userId='me', id=message['id'], format='metadata')
                msg_response = msg_request.execute()
                headers = msg_response['payload']['headers']

                # Log the subject line of each email
                subject_line = next((header['value'] for header in headers if header['name'] == 'Subject'), None)
                logger.debug(f"Checking subject: {subject_line}")

                # Check if the subject line matches
                if subject_line and f"Tienes una nueva orden P2P {last_four}" in subject_line:
                    logger.debug(f"Found matching email with subject: {subject_line}")
                    # Fetch the full email content
                    msg_request = await asyncio.to_thread(service.users().messages().get, userId='me', id=message['id'], format='full')
                    msg_response = msg_request.execute()

                    # Handle multipart emails and log content
                    for part in msg_response['payload'].get('parts', []):
                        if part['mimeType'] == 'text/plain':
                            msg_body = part['body']['data']
                            logger.debug(f"Raw message body (base64): {msg_body[:100]}...")  # Log the first 100 chars of raw message

                            email_content = base64.urlsafe_b64decode(msg_body).decode('utf-8')
                            logger.debug(f"Decoded email content: {email_content[:100]}...")  # Log the first 100 chars of decoded content

                            # Extract IP address
                            desde_match = re.search(r'desde\s+(\d+\.\d+\.\d+\.\d+)', email_content, re.IGNORECASE)
                            if desde_match:
                                ip_address = desde_match.group(1)
                                logger.debug(f"IP found: {ip_address}")
                                return ip_address

            # If not found, increase retry count and wait
            retry_count += 1
            logger.debug(f"Retry {retry_count}/{max_retries} - No matching email yet, sleeping before retry...")
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            # Optional: Add logic to handle specific exceptions

    logger.warning(f"No matching email found after maximum retries for last four digits: {last_four}")
    return None

async def main():
    last_four_digits = '2400'
    ip_address = await gmail_fetch_ip(last_four_digits)
    print("Extracted IP Address:", ip_address)

if __name__ == "__main__":
    asyncio.run(main())
