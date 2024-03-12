from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
from dotenv import load_dotenv

load_dotenv('.env.email')
gmail_credentials_json_path = os.getenv('GMAIL_CREDENTIALS_JSON_PATH')
print(f"GMAIL path: {gmail_credentials_json_path}") 

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If credentials are not valid or do not exist, reauthenticate
    if not creds or not creds.valid:
        # If the credentials are expired and have no refresh token, or the refresh process fails
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print("Error refreshing credentials, re-authenticating...")
                creds = None

        # If we have no credentials or failed to refresh them, start the authentication flow
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                gmail_credentials_json_path, SCOPES)
            creds = flow.run_local_server(port=8080)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

if __name__ == '__main__':
    main()
