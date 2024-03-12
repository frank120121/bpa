already_processed = set()
ProhibitedPaymentTypes = "TERCEROS, BANCOPPEL, BANCO AZTECA, STP, MERCADO PAGO, o en EFECTIVO"
ORDER_STATUS_UNDER_REVIEW = 2
FIAT_UNIT_MXN = 'MXN'
FIAT_UNIT_USD = 'USD'
MGL_SPOT = 2
MFM_SPOT = 1
MXN_BTC_AMT = '5000'
MXN_USDT_AMT = '30000'
USD_AMT_1 = '100'
USD_AMT_2 = '500'



ads_dict = {
    'account_1': [
        {'advNo': '12593303119082127360', 'target_spot': MFM_SPOT, 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MXN_BTC_AMT, 'payTypes': None, 'Group': '1'},
        {'advNo': '12593308415142735872', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MXN_USDT_AMT, 'payTypes': ['BBVABank'], 'Group': '2'},
        {'advNo': '12598158630177452032', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':'500', 'payTypes': ['OXXO'], 'Group': '4'},
        # {'advNo': '12578447747234050048', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['Zelle'], 'Group': '3'},
        # {'advNo': '12590565226093010944', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['Zelle'], 'Group': '4'},
        # {'advNo': '12590566284535308288', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['SkrillMoneybookers'], 'Group': '5'},
        # {'advNo': '12590567548383592448', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['SkrillMoneybookers'], 'Group': '6'},
        # {'advNo': '12590568032956669952', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['BANK'], 'Group': '7'},
        # {'advNo': '12590568277293666304', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['BANK'], 'Group': '8'}
    ],
    'account_2': [
        {'advNo': '12593495469168508928', 'target_spot': MGL_SPOT, 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MXN_BTC_AMT, 'payTypes': None, 'Group': '1'},
        {'advNo': '12593490877264977920', 'target_spot': MGL_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MXN_USDT_AMT, 'payTypes': ['BBVABank'], 'Group': '2'},
        {'advNo': '12598150744306384896', 'target_spot': '1', 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount':MXN_BTC_AMT, 'payTypes': None, 'Group': '3'},
    #     {'advNo': '12590585293541416960', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['Zelle'], 'Group': '3'},
    #     {'advNo': '12590585457789411328', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['Zelle'], 'Group': '4'},
    #     {'advNo': '12590585929304309760', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['SkrillMoneybookers'], 'Group': '5'},
    #     {'advNo': '12590586117778108416', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['SkrillMoneybookers'], 'Group': '6'},
    #     {'advNo': '12590586776166993920', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['BANK'], 'Group': '7'},
    #     {'advNo': '12590586951200821248', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['BANK'], 'Group': '8'}
    ]
}

status_map = {
    'seller_merchant_trading': 1,
    'seller_payed': 2,
    'buyer_merchant_trading': 3,
    'seller_completed': 4,
    'be_appeal': 5,
    'seller_cancelled': 6,
    'cancelled_by_system': 7,
    'buyer_payed': 8,
    'submit_appeal': 9
}

temp_ignore = {
    'seller_merchant_trading': 1,
    'seller_payed': 2,
}

SYSTEM_REPLY_FUNCTIONS = {
    1: 'new_order',
    2: 'request_proof',
    3: 'we_are_buying',
    4: 'completed_order',
    5: 'customer_appealed',
    6: 'seller_cancelled',
    7: 'canceled_by_system',
    8: 'we_payed',
    9: 'we_apealed'
}

ANTI_FRAUD_CHECKS = {}

MERCHANTS = {
    'GUERRERO LOPEZ MARTHA': 2, 
    'MUNOZ PEREA MARIA FERNANDA': 1
}

MONTHLY_LIMITS = 2000000.00
DAILY_LIMITS = 90000.00

bank_accounts = [
    {
        "bank_name": "Nvio",
        "beneficiary": "Francisco Javier Lopez",
        "account_number": "710969000007300927",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "BBVA",
        "beneficiary": "Francisco Javier Lopez",
        "account_number": "1532335128",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "STP",
        "beneficiary": "Francisco Javier Lopez",
        "account_number": "646180146099983826",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "Banregio",
        "beneficiary": "Francisco Javier Lopez",
        "account_number": "058597000056476091",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "BBVA",
        "beneficiary": "Maria Fernanda Munoz Perea",
        "account_number": "1593999048",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "Nvio",
        "beneficiary": "Maria Fernanda Munoz Perea",
        "account_number": "710969000016348705",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "BBVA",
        "beneficiary": "Maria Fernanda Munoz Perea",
        "account_number": "0482424657",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": 250000.00
    },
    {
        "bank_name": "Nvio",
        "beneficiary": "Martha Guerrero Lopez",
        "account_number": "710969000015306104",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "Banregio",
        "beneficiary": "Martha Guerrero Lopez",
        "account_number": "058597000054265356",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "Santander",
        "beneficiary": "Martha Guerrero Lopez",
        "account_number": "014761655091416464",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    },
    {
        "bank_name": "STP",
        "beneficiary": "ANBER CAP DE MEXICO SA DE CV",
        "account_number": "646180204200033494",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS
    }
]

NOT_ACCEPTED_BANKS = {"banco azteca", "mercado pago", "stp", "bancoppel", "albo", "azteca", "mercadopago", "oxxo", "coppel"}

ACCEPTED_BANKS = {
    'abc capital', 'actinver', 'afirme', 'alternativos', 'arcus', 'asp integra opc',
    'autofin', 'babien', 'bajio', 'banamex', 'banco covalto', 'banco s3', 'bancomer',
    'bancomext', 'bancrea', 'banjercito', 'bankaool', 'banobras', 'banorte',
    'banregio', 'bansi', 'banxico', 'barclays', 'bbase', 'bbva', 'bbva bancomer',
    'bbva mexico', 'bmonex', 'caja pop mexica', 'cb intercam', 'cibanco', 'compartamos',
    'consubanco', 'cuenca', 'donde', 'finamex', 'gbm', 'hsbc', 'icbc', 'inbursa',
    'indeval', 'intercam banco', 'invercap', 'invex', 'kuspit', 'libertad', 'masari',
    'mifel', 'monex', 'multiva banco', 'nafin', 'nu', 'nu bank', 'nu mexico', 'nvio',
    'pagatodo', 'profuturo', 'sabadell', 'santander', 'scotia', 'scotiabank', 'shinhan',
    'tesored', 'transfer', 'unagra', 'valmex', 'value', 've por mas', 'vector', 'spin'
}

BBVA_BANKS = ['bbva', 'bbva bancomer', 'bancomer', 'bbva mexico']
CUTOFF_DAYS = 1

DB_FILE = 'C:/Users/p7016/Documents/bpa/orders_data.db'

prohibited_countries = [
    "AF", "AL", "AZ", "BY", "BZ", "BA", "BI", "CF", "TD", "CD", "CU", "CY", "ET", "GY",
    "HT", "HN", "IR", "IQ", "XK", "KG", "LA", "LB", "LY", "MK", "ML", "ME", "MZ", "MM",
    "NI", "NG", "KP", "PG", "PY", "PK", "PA", "ST", "RS", "SO", "SS", "SD", "SY",
    "TZ", "TJ", "TT", "TR", "TM", "UA", "UZ", "VU", "YE", "ZW"
]