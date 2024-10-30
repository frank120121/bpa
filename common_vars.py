#bpa/common/vars.py
already_processed = set()
ProhibitedPaymentTypes = "TERCEROS, BANCOPPEL, BANCO AZTECA, STP, MERCADO PAGO, o en EFECTIVO"
ORDER_STATUS_UNDER_REVIEW = 2
FIAT_UNIT_MXN = 'MXN'
FIAT_UNIT_USD = 'USD'
MGL_SPOT = 1
MFM_SPOT = 1
MXN_BTC_AMT = '5000'
MXN_USDT_AMT = '3000'
USD_AMT_1 = '100'
USD_AMT_2 = '500'
MXN_SELL_AMT = '5000'

MGL_SELL_AMT = '50000'
MFM_SELL_AMT = '5000'
MFM_SELL_AMT_MIN = '15000'

MGL_BUY_MIN = '10000'
MGL_BUY_MIN_2 = '2000'
MGL_BUY_TRANS_AMT = '10000'

MFM_BUY_MIN = '50000'
MFM_BUY_MIN_2 = '2000'
MFM_BUY_TRANS_AMT = '15000'

ads_dict = {
    'account_1': [
        #BUY MXN
        {'advNo': '12593303119082127360', 'target_spot': '1', 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount': MFM_BUY_TRANS_AMT, 'payTypes': None, 'Group': '1', 'trade_type': 'BUY', 'minTransAmount': MFM_BUY_MIN},
        {'advNo': '12593308415142735872', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount': MFM_BUY_TRANS_AMT, 'payTypes': None, 'Group': '2', 'trade_type': 'BUY', 'minTransAmount': MFM_BUY_MIN},
        {'advNo': '12633943668966330368', 'target_spot': '1', 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount': MFM_BUY_TRANS_AMT, 'payTypes': None, 'Group': '3', 'trade_type': 'BUY', 'minTransAmount': MFM_BUY_MIN},
        {'advNo': '12598158630177452032', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MFM_BUY_TRANS_AMT, 'payTypes': ['OXXO'], 'Group': '4', 'trade_type': 'BUY', 'minTransAmount': MFM_BUY_MIN},
        {'advNo': '12622081735207518208', 'target_spot': '1', 'asset_type': 'USDC', 'fiat': 'MXN', 'transAmount': MFM_BUY_TRANS_AMT, 'payTypes': None, 'Group': '5', 'trade_type': 'BUY', 'minTransAmount': MFM_BUY_MIN},

        #SELL MXN
        {'advNo': '12684742856008511488', 'target_spot': '1', 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '1', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684742556603133952', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '2', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684743099915677696', 'target_spot': MFM_SPOT, 'asset_type': 'BNB', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '3', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684743277897785344', 'target_spot': MFM_SPOT, 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '4', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771021839237120', 'target_spot': MFM_SPOT, 'asset_type': 'DAI', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '5', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771206348828672', 'target_spot': MFM_SPOT, 'asset_type': 'DOGE', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '6', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771445253165056', 'target_spot': MFM_SPOT, 'asset_type': 'ADA', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '7', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771608146374656', 'target_spot': MFM_SPOT, 'asset_type': 'XRP', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '8', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771783039131648', 'target_spot': MFM_SPOT, 'asset_type': 'FDUSD', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '9', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684743643268935680', 'target_spot': '1', 'asset_type': 'USDC', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '10', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        {'advNo': '12684771978045091840', 'target_spot': '1', 'asset_type': 'WLD', 'fiat': 'MXN', 'transAmount':MFM_SELL_AMT, 'payTypes': None, 'Group': '11', 'trade_type': 'SELL', 'minTransAmount': MFM_SELL_AMT_MIN},
        # {'advNo': '12578447747234050048', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['Zelle'], 'Group': '3'},
        # {'advNo': '12590565226093010944', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['Zelle'], 'Group': '4'},
        # {'advNo': '12590566284535308288', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['SkrillMoneybookers'], 'Group': '5'},
        # {'advNo': '12590567548383592448', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['SkrillMoneybookers'], 'Group': '6'},
        # {'advNo': '12590568032956669952', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['BANK'], 'Group': '7'},
        # {'advNo': '12590568277293666304', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['BANK'], 'Group': '8'}
    ],
    'account_2': [
        #BUY MXN
        {'advNo': '12593495469168508928', 'target_spot': '1', 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MGL_BUY_MIN_2, 'payTypes': None, 'Group': '1', 'trade_type': 'BUY', 'minTransAmount': MGL_BUY_MIN},
        {'advNo': '12593490877264977920', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MGL_BUY_MIN, 'payTypes': ['BBVABank'], 'Group': '2', 'trade_type': 'BUY', 'minTransAmount': MGL_BUY_MIN_2},
        {'advNo': '12598150744306384896', 'target_spot': '1', 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount': MGL_BUY_MIN_2, 'payTypes': None, 'Group': '3', 'trade_type': 'BUY', 'minTransAmount': MGL_BUY_MIN},
        {'advNo': '12601117035243544576', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MGL_BUY_MIN_2, 'payTypes': ['OXXO'], 'Group': '4', 'trade_type': 'BUY', 'minTransAmount': MGL_BUY_MIN_2},
        {'advNo': '12622083293687042048', 'target_spot': '1', 'asset_type': 'USDC', 'fiat': 'MXN', 'transAmount':MGL_BUY_MIN_2, 'payTypes': None, 'Group': '5', 'trade_type': 'BUY', 'minTransAmount': MGL_BUY_MIN},

        #BUY USD
        {'advNo': '12601438033869934592', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':'500', 'payTypes': ['BANK'], 'Group': '6', 'trade_type': 'BUY', 'minTransAmount': '100'},
        {'advNo': '13670543600201879552', 'target_spot': '1', 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':'500', 'payTypes': ['SkrillMoneybookers'], 'Group': '7', 'trade_type': 'BUY', 'minTransAmount': '100'},

        #SELL MXN
        {'advNo': '12601682453428781056', 'target_spot': MGL_SPOT, 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '1', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602501942341144576', 'target_spot': MGL_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '2', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602510869029322752', 'target_spot': MGL_SPOT, 'asset_type': 'BNB', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '3', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602511177784733696', 'target_spot': MGL_SPOT, 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '4', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        # {'advNo': '12602511596171862016', 'target_spot': MGL_SPOT, 'asset_type': 'DAI', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '5', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602512111965536256', 'target_spot': MGL_SPOT, 'asset_type': 'DOGE', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '6', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602513811501113344', 'target_spot': MGL_SPOT, 'asset_type': 'ADA', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '7', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602514074267328512', 'target_spot': MGL_SPOT, 'asset_type': 'XRP', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '8', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12602514264390385664', 'target_spot': MGL_SPOT, 'asset_type': 'FDUSD', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '9', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
        {'advNo': '12622082668750938112', 'target_spot': '1', 'asset_type': 'USDC', 'fiat': 'MXN', 'transAmount':MGL_SELL_AMT, 'payTypes': None, 'Group': '10', 'trade_type': 'SELL', 'minTransAmount': MGL_SELL_AMT},
    #     {'advNo': '12590585293541416960', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['Zelle'], 'Group': '3'},
    #     {'advNo': '12590585457789411328', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['Zelle'], 'Group': '4'},
    #     {'advNo': '12590585929304309760', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['SkrillMoneybookers'], 'Group': '5'},
    #     {'advNo': '12590586117778108416', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['SkrillMoneybookers'], 'Group': '6'},
    #     {'advNo': '12590586776166993920', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_1, 'payTypes': ['BANK'], 'Group': '7'},
    #     {'advNo': '12590586951200821248', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'USD', 'transAmount':USD_AMT_2, 'payTypes': ['BANK'], 'Group': '8'}
    ]
}

sell_ads_dict = {
    'account_1': [
        # {'advNo': '12601682453428781056', 'target_spot': MFM_SPOT, 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '1'},
        # {'advNo': '12602501942341144576', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '2'},
        # {'advNo': '12602510869029322752', 'target_spot': MFM_SPOT, 'asset_type': 'BNB', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '3'},
        # {'advNo': '12602511177784733696', 'target_spot': MFM_SPOT, 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '4'},
        # {'advNo': '12602511596171862016', 'target_spot': MFM_SPOT, 'asset_type': 'DAI', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '5'},
        # {'advNo': '12602512111965536256', 'target_spot': MFM_SPOT, 'asset_type': 'DOGE', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '6'},
        # {'advNo': '12602513811501113344', 'target_spot': MFM_SPOT, 'asset_type': 'ADA', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '7'},
        # {'advNo': '12602514074267328512', 'target_spot': MFM_SPOT, 'asset_type': 'XRP', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '8'},
        # {'advNo': '12602514264390385664', 'target_spot': MFM_SPOT, 'asset_type': 'FDUSD', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '9'},
    ],
    'account_2': [
        {'advNo': '12601682453428781056', 'target_spot': MFM_SPOT, 'asset_type': 'BTC', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '1', 'trade_type': 'SELL'},
        {'advNo': '12602501942341144576', 'target_spot': MFM_SPOT, 'asset_type': 'USDT', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '2', 'trade_type': 'SELL'},
        {'advNo': '12602510869029322752', 'target_spot': MFM_SPOT, 'asset_type': 'BNB', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '3', 'trade_type': 'SELL'},
        {'advNo': '12602511177784733696', 'target_spot': MFM_SPOT, 'asset_type': 'ETH', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '4', 'trade_type': 'SELL'},
        {'advNo': '12602511596171862016', 'target_spot': MFM_SPOT, 'asset_type': 'DAI', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '5', 'trade_type': 'SELL'},
        {'advNo': '12602512111965536256', 'target_spot': MFM_SPOT, 'asset_type': 'DOGE', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '6', 'trade_type': 'SELL'},
        {'advNo': '12602513811501113344', 'target_spot': MFM_SPOT, 'asset_type': 'ADA', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '7', 'trade_type': 'SELL'},
        {'advNo': '12602514074267328512', 'target_spot': MFM_SPOT, 'asset_type': 'XRP', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '8', 'trade_type': 'SELL'},
        {'advNo': '12602514264390385664', 'target_spot': MFM_SPOT, 'asset_type': 'FDUSD', 'fiat': 'MXN', 'transAmount':MXN_SELL_AMT, 'payTypes': None, 'Group': '9', 'trade_type': 'SELL'}
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
    'MUÑOZ PEREA MARIA FERNANDA': 1
}

MONTHLY_LIMITS = 2000000.00
DAILY_LIMITS = 150000.00
OXXO_MONTHLY_LIMIT = 800000.00


#Oxxo Limits
BBVA_OXXO_DAILY_LIMIT = 19000.00
BANAMEX_OXXO_DAILY_LIMIT = 18000.00
SANTANDER_OXXO_DAILY_LIMIT = 10000.00
SCOTIABANK_OXXO_DAILY_LIMIT = 17760.00
INBURSA_OXXO_DAILY_LIMIT = 24000.00
HSBC_OXXO_DAILY_LIMIT = 20000.00
CAJAPOPULAR_OXXO_DAILY_LIMIT = 20000.00
INVEX_OXXO_DAILY_LIMIT = 19000.00
BANREGIO_OXXO_DAILY_LIMIT = 30000.00
SPIN_OXXO_DAILY_LIMIT = 10000.00

#Oxxo Limits too low
AFIRME_OXXO_DAILY_LIMIT = 5000.00
BANCOPPEL_OXXO_DAILY_LIMIT = 5000.00

#Oxxo Monthly Limit
OXXO_MONTHLY_LIMIT = 80000.00

BANREGIO_OXXO_MONTHLY_LIMIT = 900000.00


bank_accounts = [
    {
        "bank_name": "Nvio",
        "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
        "account_number": "710969000007300927",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": 0.00,
    },
    # {
    #     "bank_name": "BBVA",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "account_number": "1532335128",
    #     "card_number": "4152314008762364",
    #     "account_daily_limit": DAILY_LIMITS,
    #     "account_monthly_limit": MONTHLY_LIMITS,
    #     "oxxo_daily_limit": BBVA_OXXO_DAILY_LIMIT
    # },
    {
        "bank_name": "STP",
        "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
        "account_number": "646180146099983826",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": 0.00
    },
    # {
    #     "bank_name": "BBVA",
    #     "beneficiary": "MARIA FERNANDA MUNOZ PEREA",
    #     "account_number": "1593999048",
    #     "card_number": "4152314089865862",
    #     "account_daily_limit": DAILY_LIMITS,
    #     "account_monthly_limit": MONTHLY_LIMITS,
    #     "oxxo_daily_limit": BBVA_OXXO_DAILY_LIMIT
    # },
    {
        "bank_name": "Nvio",
        "beneficiary": "MARIA FERNANDA MUNOZ PEREA",
        "account_number": "710969000016348705",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": 0.00
    },
    # {
    #     "bank_name": "BBVA",
    #     "beneficiary": "MARIA FERNANDA MUNOZ PEREA",
    #     "account_number": "0482424657",
    #     "account_daily_limit": DAILY_LIMITS,
    #     "account_monthly_limit": 250000.00,
    #     "oxxo_daily_limit": BBVA_OXXO_DAILY_LIMIT
    # },
    {
        "bank_name": "Nvio",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "account_number": "710969000015306104",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": 0.00
    },
    {
        "bank_name": "Santander",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "account_number": "65509141646",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": SANTANDER_OXXO_DAILY_LIMIT
    },
    {
        "bank_name": "STP",
        "beneficiary": "ANBER CAP DE MEXICO SA DE CV",
        "account_number": "646180204200033494",
        "account_daily_limit": DAILY_LIMITS,
        "account_monthly_limit": MONTHLY_LIMITS,
        "oxxo_daily_limit": 0.00
    },
    # {
    #     "bank_name": "BBVA",
    #     "beneficiary": "ANBER CAP DE MEXICO SA DE CV",
    #     "account_number": "0122819805",
    #     "account_daily_limit": DAILY_LIMITS,
    #     "account_monthly_limit": MONTHLY_LIMITS,
    #     "oxxo_daily_limit": BBVA_OXXO_DAILY_LIMIT
    # },

    # {
    #     "bank_name": "BBVA",
    #     "beneficiary": "MARIA FERNANDA MUNOZ PEREA",
    #     "account_number": "0486503160",
    #     "account_daily_limit": DAILY_LIMITS,
    #     "account_monthly_limit": MONTHLY_LIMITS,
    #     "oxxo_daily_limit": BBVA_OXXO_DAILY_LIMIT
    # },

    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "account_number": "4347984806135934",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "account_number": "4347984876309005",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "account_number": "4347984866282113",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    # {
    #     "bank_name": "OXXO",
    #     "beneficiary": "MARTHA GUERRERO LOPEZ",
    #     "account_number": "4347984837112696",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": BANREGIO_OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    # {
    #     "bank_name": "OXXO",
    #     "beneficiary": "MARTHA GUERRERO LOPEZ",
    #     "account_number": "4347984868505966",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": BANREGIO_OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    # {
    #     "bank_name": "OXXO",
    #     "beneficiary": "MARTHA GUERRERO LOPEZ",
    #     "account_number": "4347984866288631",
    #     "account_daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "account_monthly_limit": BANREGIO_OXXO_MONTHLY_LIMIT,
    #     "oxxo_daily_limit": BANREGIO_OXXO_DAILY_LIMIT
    # },
    {
        "bank_name": "OXXO",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "account_number": "2242170760065560",
        "account_daily_limit": SPIN_OXXO_DAILY_LIMIT,
        "account_monthly_limit": OXXO_MONTHLY_LIMIT,
        "oxxo_daily_limit": SPIN_OXXO_DAILY_LIMIT
    },
    {
        "bank_name": "OXXO",
        "beneficiary": "MARIA FERNANDA MUNOZ PEREA",
        "account_number": "2242170760324680",
        "account_daily_limit": SPIN_OXXO_DAILY_LIMIT,
        "account_monthly_limit": OXXO_MONTHLY_LIMIT,
        "oxxo_daily_limit": SPIN_OXXO_DAILY_LIMIT
    },
    {
        "bank_name": "OXXO",
        "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
        "account_number": "2242170760314240",
        "account_daily_limit": SPIN_OXXO_DAILY_LIMIT,
        "account_monthly_limit": OXXO_MONTHLY_LIMIT,
        "oxxo_daily_limit": SPIN_OXXO_DAILY_LIMIT
    }

]

#Oxxo debit cards for cash deposits
OXXO_DEBIT_CARDS = [
    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "card_no": "4347984806135934",
    #     "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "monthly_limit": OXXO_MONTHLY_LIMIT
    # },
    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "card_no": "4347984876309005",
    #     "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "monthly_limit": OXXO_MONTHLY_LIMIT
    # },
    # {
    #     "bank_name": "BANREGIO",
    #     "beneficiary": "FRANCISCO JAVIER LOPEZ GUERRERO",
    #     "card_no": "4347984866282113",
    #     "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
    #     "monthly_limit": OXXO_MONTHLY_LIMIT
    # },
    {
        "bank_name": "BANREGIO",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "card_no": "4347984837112696",
        "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
        "monthly_limit": OXXO_MONTHLY_LIMIT
    },
    {
        "bank_name": "BANREGIO",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "card_no": "4347984868505966",
        "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
        "monthly_limit": OXXO_MONTHLY_LIMIT
    },
    {
        "bank_name": "BANREGIO",
        "beneficiary": "MARTHA GUERRERO LOPEZ",
        "card_no": "4347984866288631",
        "daily_limit": BANREGIO_OXXO_DAILY_LIMIT,
        "monthly_limit": OXXO_MONTHLY_LIMIT
    }
]


NOT_ACCEPTED_BANKS = {"banco azteca", "mercado pago", "stp", "bancoppel", "albo", "azteca", "mercadopago", "coppel"}

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
    'tesored', 'transfer', 'unagra', 'valmex', 'value', 've por mas', 'vector', 'spin', 'citibanamex'
}

BBVA_BANKS = ['bbva', 'bbva bancomer', 'bancomer', 'bbva mexico']

def normalize_bank_name(bank_name: str) -> str:
    """Normalizes bank names for consistent lookup"""
    if not bank_name:
        return bank_name
    
    bank_name = bank_name.lower().strip()
    
    # Direct match with SPEI codes
    if bank_name in BANK_SPEI_CODES:
        return bank_name

    # Common variations
    bank_mappings = {
        'bbva': ['bancomer', 'bbva bancomer', 'bbva mexico'],
        'nu': ['nu bank', 'nu mexico', 'nubank'],
        'banamex': ['citibanamex'],
        'scotiabank': ['scotia'],
    }

    # Check if the bank name is a variation of a standard name
    for standard_name, variations in bank_mappings.items():
        if bank_name in variations or any(var in bank_name for var in variations):
            return standard_name

    # If no specific mapping found, return the original name
    return bank_name

CUTOFF_DAYS = 1

DB_FILE = 'C:/Users/p7016/Documents/bpa/orders_data.db'

prohibited_countries = [
    "AF", "AL", "AZ", "BY", "BZ", "BA", "BI", "CF", "TD", "CD", "CU", "CY", "ET", "GY",
    "HT", "HN", "IR", "IQ", "XK", "KG", "LA", "LB", "LY", "MK", "ML", "ME", "MZ", "MM",
    "NI", "NG", "KP", "PG", "PY", "PK", "PA", "ST", "RS", "SO", "SS", "SD", "SY",
    "TZ", "TJ", "TT", "TR", "TM", "UA", "UZ", "VU", "YE", "ZW"
]

prohibited_countries_v2 = [
    "AF", "BY", "BI", "TD", "CD", "KP", "CU", "ER", "IQ", "IR", "LY", "MM", "CF", "SS", "RU", "SY", "SO", "SD", "YE", "VE", "UA"
]

ACCEPTED_COUNTRIES_FOR_OXXO = ['MX', 'CO', 'VE', 'AR', 'ES', 'CL', 'CA', 'HK', 'PE', 'BE', 'EC', 'RU', 'TH', 'IN', 'UA', 'DE', 'JP', 'US', 'RU', 'FR']

BANK_SPEI_CODES = {
    # Most common banks from our analysis
    'nvio': '90710',
    'bbva': '40012',
    'bbva mexico': '40012',
    'bancomer': '40012',
    'bbva bancomer': '40012',
    'banorte': '40072',
    'santander': '40014',
    'banamex': '40002',
    'hsbc': '40021',
    'scotiabank': '40044',
    'nu': '90638',
    'nu mexico': '90638',
    'transfer': '90656',
    'inbursa': '40036',
    'banregio': '40058',
    'bajio': '40030',
    'actinver': '40133',
    
    # Additional major banks from Banxico list that might appear
    'azteca': '40127',
    'bancoppel': '40137',
    'afirme': '40062',
    'compartamos': '40130',
    'mifel': '40042',
    'multiva': '40132',
    'bmonex': '40112',
    'cibanco': '40143',
    'jpmorgan': '40110',
    've por mas': '40113',
    
    # Digital/Fintech banks that are common in crypto
    'spin by oxxo': '90728',
    'mercado pago': '90722',
    'klar': '90661',
}