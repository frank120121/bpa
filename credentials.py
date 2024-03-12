import os
from dotenv import load_dotenv

load_dotenv()


BITSO_BASE_URL = "https://api.bitso.com"
BASE_URL = "https://api.binance.com"

# BINANCE credentials
credentials_dict = {
    'account_1': {
        'KEY': os.environ.get('API_KEY_MFMP'),
        'SECRET': os.environ.get('API_SECRET_MFMP')
    },
    'account_2': {
        'KEY': os.environ.get('API_KEY_MGL'),
        'SECRET': os.environ.get('API_SECRET_MGL')
    },
    'account_3': {
        'KEY': os.environ.get('API_KEY_FJL'),
        'SECRET': os.environ.get('API_SECRET_FJL')
    }
}

# Bitso credentials
bitso_credentials = {
    'bitso_account_MGL': {
        'KEY': os.environ.get('BITSO_KEY_MGL'),
        'SECRET': os.environ.get('BITSO_SECRET_MGL')
    },
    'bitso_account_FJL': {
        'KEY': os.environ.get('BITSO_KEY_FJL'),
        'SECRET': os.environ.get('BITSO_SECRET_FJL')
    },
    'bitso_account_MFMP': {
        'KEY': os.environ.get('BITSO_KEY_MFMP'),
        'SECRET': os.environ.get('BITSO_SECRET_MFMP')
    }
}
# TRUBIT credentials
trubit_credentials = {
    'trubit_account_FJL': {
        'KEY': os.environ.get('TRUBIT_KEY_FJL'),
        'SECRET': os.environ.get('TRUBIT_SECRET_FJL')
    }
}

bitso_ws_url = 'wss://bitso.com/trade/socket.io/?EIO=4&transport=websocket'
