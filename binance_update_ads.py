#binance_update_ads.py
import asyncio
import aiohttp
import traceback
import logging
from ads_database import update_ad_in_database, fetch_all_ads_from_database, get_ad_from_database
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI, SharedSession
from bitso_wallets import bitso_main
from binance_wallets import BinanceWallets
from asset_balances import total_usd

logger = logging.getLogger(__name__)

# Constants
BUY_PRICE_THRESHOLD = 1.0065
SELL_PRICE_THRESHOLD = 0.9845
PRICE_THRESHOLD_2 = 1.0189
MIN_RATIO = 90.00
MAX_RATIO = 110.00
RATIO_ADJUSTMENT = 0.05
DIFF_THRESHOLD = 0.15

balance_lock = asyncio.Lock()
latest_usd_balance = 0

def adjust_sell_price_threshold(usd_balance):
    global SELL_PRICE_THRESHOLD
    global BUY_PRICE_THRESHOLD
    if usd_balance >= 55000:
        SELL_PRICE_THRESHOLD = 0.9845
        BUY_PRICE_THRESHOLD = 1.0095
        logger.debug("Adjusted sell price threshold to 0.9898 and buy price threshold to 1.0120")
    else:
        adjustment = (55000 - usd_balance) / 1000 * 0.00025
        SELL_PRICE_THRESHOLD = min(0.9909 + adjustment, 0.9992)
        BUY_PRICE_THRESHOLD = min(1.0095 + adjustment, 1.10)
        logger.debug(f"Adjusted sell price threshold to {SELL_PRICE_THRESHOLD} and buy price threshold to {BUY_PRICE_THRESHOLD}")
async def fetch_and_calculate_total_balance():
    while True:
        try:
            await bitso_main()
            binance_wallets = BinanceWallets()
            await binance_wallets.main()
            usd_balance = await total_usd()
            async with balance_lock:
                global latest_usd_balance
                latest_usd_balance = usd_balance
            logger.debug(f"Fetched USD Balance: {usd_balance}")
        except Exception as e:
            logger.error(f"Error fetching balance: {e}")
        await asyncio.sleep(300)  # Fetch balance every 5 minutes

def filter_ads(ads_data, base_price, own_ads, trans_amount_threshold, price_threshold, minTransAmount, is_buy=True):
    own_adv_nos = [ad['advNo'] for ad in own_ads]
    return [ad for ad in ads_data
            if ad['adv']['advNo'] not in own_adv_nos
            and ((float(ad['adv']['price']) >= base_price * price_threshold) if is_buy else (float(ad['adv']['price']) <= base_price * price_threshold))
            and float(ad['adv']['dynamicMaxSingleTransAmount']) >= trans_amount_threshold
            and float(ad['adv']['minSingleTransAmount']) < minTransAmount]

def determine_price_threshold(payTypes, is_buy=True):
    special_payTypes = ['OXXO', 'BANK', 'ZELLE', 'SkrillMoneybookers']
    if payTypes is not None and any(payType in payTypes for payType in special_payTypes):
        return PRICE_THRESHOLD_2 if is_buy else SELL_PRICE_THRESHOLD
    else:
        return BUY_PRICE_THRESHOLD if is_buy else SELL_PRICE_THRESHOLD

def compute_base_price(price: float, floating_ratio: float) -> float:
    return round(price / (floating_ratio / 100), 2)

def check_if_ads_avail(ads_list, adjusted_target_spot):
    if len(ads_list) < adjusted_target_spot:
        adjusted_target_spot = len(ads_list)
        logger.debug("Adjusted the target spot due to insufficient ads after filtering.")
        return adjusted_target_spot
    else:
        return adjusted_target_spot

async def analyze_and_update_ads(ad, api_instance, ads_data, all_ads, is_buy=True):
    advNo = ad['advNo']
    target_spot = ad['target_spot']
    asset_type = ad['asset_type']
    current_priceFloatingRatio = float(ad['floating_ratio'])
    surplusAmount = ad['surplused_amount']
    fiat = ad['fiat']
    transAmount = ad['transAmount']
    minTransAmount = float(ad['minTransAmount'])

    try:
        our_ad_data = next((item for item in ads_data if item['adv']['advNo'] == advNo), None)
        logger.debug(f'Ads_data: {ads_data}')

        if not our_ad_data:
            logger.debug(f"Ad number {advNo} not found in ads data. Fetching details from database...")
            return
            our_ad_data = await get_ad_from_database(advNo)
            if our_ad_data is None:
                logger.debug(f"Ad number {advNo} not found in ads data. Fetching details from API...")
                our_ad_data = await api_instance.get_ad_detail(advNo)
                if our_ad_data is None or 'data' not in our_ad_data:
                    logger.error(f"Failed to get details for ad number {advNo}")
                    return
                await update_ad_in_database(
                    target_spot=target_spot,
                    advNo=advNo,
                    asset_type=our_ad_data['data']['asset'],
                    floating_ratio=our_ad_data['data']['priceFloatingRatio'],
                    price=our_ad_data['data']['price'],
                    surplusAmount=our_ad_data['data']['surplusAmount'],
                    account=ad['account'],
                    fiat=fiat,
                    transAmount=transAmount,
                    minTransAmount=minTransAmount
                )
                our_current_price = float(our_ad_data['data']['price'])
            else:
                our_current_price = float(our_ad_data['price'])
        else:
            our_current_price = float(our_ad_data['adv']['price'])

        base_price = compute_base_price(our_current_price, current_priceFloatingRatio)
        if ad['transAmount'] is  None:
            transAmount = 4000.00
        custom_price_threshold = determine_price_threshold(ad['payTypes'], is_buy)
        filtered_ads = filter_ads(ads_data, base_price, all_ads, transAmount, custom_price_threshold, minTransAmount, is_buy)
        adjusted_target_spot = check_if_ads_avail(filtered_ads, target_spot)
        if not filtered_ads:
            logger.info(f"No filtered ads found for {ad['asset_type']} {ad['fiat']} {ad['transAmount']} {ad['payTypes']}.")
            return
            logger.debug(f"Fileter ads for {advNo} is empty.")
            new_ratio = max(MIN_RATIO, min(MAX_RATIO, round((custom_price_threshold * 100), 2)))
            if new_ratio == current_priceFloatingRatio:
                logger.debug(f"Ratio unchanged")
                return
            else:
                logger.debug(f" No filtered ads found. New ratio: {new_ratio}. old ratio: {current_priceFloatingRatio}.")
                await api_instance.update_ad(advNo, new_ratio)
                await update_ad_in_database(target_spot, advNo, asset_type, new_ratio, our_current_price, surplusAmount, ad['account'], fiat, transAmount, minTransAmount)
            return
            
        competitor_ad = filtered_ads[adjusted_target_spot - 1]
        competitor_price = float(competitor_ad['adv']['price'])
        competitor_ratio = (competitor_price / base_price) * 100


        if (our_current_price >= competitor_price and is_buy) or (our_current_price <= competitor_price and not is_buy):
            new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT if is_buy else competitor_ratio + RATIO_ADJUSTMENT
        else:
            diff_ratio = competitor_ratio - current_priceFloatingRatio if is_buy else current_priceFloatingRatio - competitor_ratio
            if diff_ratio > DIFF_THRESHOLD:
                new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT if is_buy else competitor_ratio + RATIO_ADJUSTMENT
            else:
                logger.debug(f"Competitor ad - spot: {adjusted_target_spot}, price: {competitor_price}, base: {base_price}, ratio: {competitor_ratio}. Not enough diff: {diff_ratio}")
                return

        new_ratio = max(MIN_RATIO, min(MAX_RATIO, round(new_ratio_unbounded, 2)))
        new_diff = abs(new_ratio - current_priceFloatingRatio)
        if new_ratio == current_priceFloatingRatio and new_diff < 0.005:
            logger.debug(f"Ratio unchanged")
            return
        else:
            logger.debug(f"Updating with filter ad: new ratio: {new_ratio}. old ratio: {current_priceFloatingRatio}.")
            await api_instance.update_ad(advNo, new_ratio)
            await update_ad_in_database(target_spot, advNo, asset_type, new_ratio, our_current_price, surplusAmount, ad['account'], fiat, transAmount, minTransAmount)
            logger.debug(f"Ad: {asset_type} - start price: {our_current_price}, ratio: {current_priceFloatingRatio}. Competitor ad - spot: {adjusted_target_spot}, price: {competitor_price}, base: {base_price}, ratio: {competitor_ratio}")

    except Exception as e:
        traceback.print_exc()

async def process_ads(ads_group, api_instances, all_ads, is_buy=True):
    if not ads_group:
        return
    for ad in ads_group:
        account = ad['account']
        api_instance = api_instances[account]
        payTypes_list = ad['payTypes'] if ad['payTypes'] is not None else []
        ads_data = await api_instance.fetch_ads_search('BUY' if is_buy else 'SELL', ad['asset_type'], ad['fiat'], ad['transAmount'], payTypes_list)
        if ads_data is None or ads_data.get('code') != '000000' or 'data' not in ads_data:
            logger.error(f"Failed to fetch ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {payTypes_list}.")
            continue
        current_ads_data = ads_data['data']
        if not isinstance(current_ads_data, list) or not current_ads_data:
            logger.debug(f"No valid ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {payTypes_list}.")
            continue
        await analyze_and_update_ads(ad, api_instance, current_ads_data, all_ads, is_buy)

async def main_loop(api_instances, is_buy=True):
    all_ads = await fetch_all_ads_from_database('BUY' if is_buy else 'SELL')
    logger.debug(f"All ads: {len(all_ads)}")

    grouped_ads = {}
    for ad in all_ads:
        group_key = ad['Group']
        grouped_ads.setdefault(group_key, []).append(ad)
        logger.debug(f"Grouped ads: {group_key} - {len(grouped_ads[group_key])}")

    tasks = []
    for group_key, ads_group in grouped_ads.items():
        tasks.append(asyncio.create_task(process_ads(ads_group, api_instances, all_ads, is_buy)))
    await asyncio.gather(*tasks)

async def start_update_ads(is_buy=True):
    session = await SharedSession.get_session()
    try:
        all_ads = await fetch_all_ads_from_database()
        accounts = set(ad['account'] for ad in all_ads)
        api_instances = {}

        for account in accounts:
            KEY = credentials_dict[account]['KEY']
            SECRET = credentials_dict[account]['SECRET']
            api_instance = await SingletonBinanceAPI.get_instance(account, KEY, SECRET)
            api_instances[account] = api_instance

        while True:
            async with balance_lock:
                usd_balance = latest_usd_balance
            logger.debug(f"Using USD Balance: {usd_balance}")
            adjust_sell_price_threshold(usd_balance)
            await main_loop(api_instances, is_buy)
    finally:
        await SingletonBinanceAPI.close_all()

async def run_ads_update():
    fetch_balances_task = asyncio.create_task(fetch_and_calculate_total_balance())
    await asyncio.sleep(5)  # Ensure balance is fetched initially
    buy_task = asyncio.create_task(start_update_ads(is_buy=True))
    sell_task = asyncio.create_task(start_update_ads(is_buy=False))
    await asyncio.gather(fetch_balances_task, buy_task, sell_task)

if __name__ == "__main__":
    asyncio.run(run_ads_update())