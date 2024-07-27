#binance_update_ads.py
import asyncio
import traceback
import logging
from ads_database import update_ad_in_database, fetch_all_ads_from_database, get_ad_from_database
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI
from binance_share_session import SharedSession
from bitso_wallets import bitso_main
from binance_wallets import BinanceWallets
from asset_balances import total_usd
from TESTbitso_price_listener import shared_data, lowest_ask_lock

logger = logging.getLogger(__name__)

# Constants
SELL_PRICE_THRESHOLD = 0.9945
SELL_PRICE_ADJUSTMENT = 0
PRICE_THRESHOLD_2 = 1.0189
MIN_RATIO = 90.00
MAX_RATIO = 110.00
RATIO_ADJUSTMENT = 0.05
DIFF_THRESHOLD = 0.15
BASE = 0.005


balance_lock = asyncio.Lock()
latest_usd_balance = 0
BUY_PRICE_THRESHOLD = 1.0065  # Default threshold

def adjust_sell_price_threshold(usd_balance):
    max_threshold_balance = 49500
    neutral_balance = 40000
    min_balance = 30000
    
    global SELL_PRICE_ADJUSTMENT
    # Below 30000
    if usd_balance <= min_balance:
        SELL_PRICE_ADJUSTMENT = BASE - 0.01
    # Between 30000 and 40000
    elif min_balance < usd_balance < neutral_balance:
        # Linear interpolation between BASE - 0.01 and 0
        SELL_PRICE_ADJUSTMENT = (usd_balance - min_balance) / (neutral_balance - min_balance) * (0 - (BASE - 0.01)) + (BASE - 0.01)
    # Between 40000 and 49500
    elif neutral_balance <= usd_balance <= max_threshold_balance:
        # Linear interpolation between 0 and BASE + 0.0105
        SELL_PRICE_ADJUSTMENT = (usd_balance - neutral_balance) / (max_threshold_balance - neutral_balance) * ((BASE + 0.0075) - 0)
    # Above 49500
    else:
        SELL_PRICE_ADJUSTMENT = BASE + 0.0105
    


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
        await asyncio.sleep(60)  # Fetch balance every 1 minute

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
    
async def retry_fetch_ads(api_instance, ad, is_buy, page_start=2, page_end=3):
    for page in range(page_start, page_end):
        ads_data = await api_instance.fetch_ads_search('BUY' if is_buy else 'SELL', ad['asset_type'], ad['fiat'], ad['transAmount'], ad['payTypes'], page)
        if ads_data is None or ads_data.get('code') != '000000' or 'data' not in ads_data:
            logger.error(f"Failed to fetch ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {ad['payTypes']} on page {page}.")
            continue

        current_ads_data = ads_data['data']
        if isinstance(current_ads_data, list) and current_ads_data:
            return current_ads_data

    return []
async def is_ad_online(api_instance, advNo):
    try:
        response = await api_instance.get_ad_detail(advNo)
        if response['code'] == '000000' and 'data' in response:
            ad_status = response['data']['advStatus']
            return ad_status == 1  # Return True if ad is online
        else:
            logger.error(f"Failed to get ad details for advNo {advNo}: {response}")
            return False
    except Exception as e:
        logger.error(f"An error occurred while checking ad status for advNo {advNo}: {e}")
        return False
    
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
            if not await is_ad_online(api_instance, advNo):
                logger.debug(f"Ad number {advNo} is not online. Skipping...")
                return
            # Retry fetching ads for pages 2 and 3 if no our_ad_data is found
            ads_data = await retry_fetch_ads(api_instance, ad, is_buy)
            if not ads_data:
                logger.debug(f"No ads data found after retrying up to page 3 for ad number {advNo}.")
                return
            else:
                our_ad_data = next((item for item in ads_data if item['adv']['advNo'] == advNo), None)
        
        if our_ad_data:
            our_current_price = float(our_ad_data['adv']['price'])
        else:
            logger.info(f"No our_ad_data found for ad number {advNo}.")
            return

        base_price = compute_base_price(our_current_price, current_priceFloatingRatio)
        
        # Fetch the lowest ask price from shared_data and calculate BUY_PRICE_THRESHOLD
        async with lowest_ask_lock:
            lowest_ask = shared_data["lowest_ask"]
        if lowest_ask is not None and asset_type == 'USDT':
            logger.debug(f"Lowest ask price for USDT: {lowest_ask}")
            global BUY_PRICE_THRESHOLD
            global SELL_PRICE_THRESHOLD
            
            previous_sell_price_threshold = SELL_PRICE_THRESHOLD
            previous_buy_price_threshold = BUY_PRICE_THRESHOLD
            average_price = (lowest_ask + base_price) / 2
            min_diff = 0.0020
            new_sell_price_threshold = (average_price / base_price) - (SELL_PRICE_ADJUSTMENT)
            new_buy_price_threshold = (average_price * 1.0124) / base_price
            current_diff = 0

            if is_buy:
                current_diff = abs(new_buy_price_threshold - previous_buy_price_threshold)
                if current_diff > min_diff:
                    BUY_PRICE_THRESHOLD = new_buy_price_threshold
                else:
                    logger.debug(f"Diff less than {min_diff}. Not updating BUY_PRICE_THRESHOLD")
            else:
                current_diff = abs(new_sell_price_threshold - previous_sell_price_threshold)
                if current_diff > min_diff:
                    SELL_PRICE_THRESHOLD = new_sell_price_threshold
                else:
                    logger.debug(f"Diff less than {min_diff}. Not updating SELL_PRICE_THRESHOLD")


        custom_price_threshold = determine_price_threshold(ad['payTypes'], is_buy)
        logger.debug(f"Custom price threshold: {custom_price_threshold}")
        filtered_ads = filter_ads(ads_data, base_price, all_ads, transAmount, custom_price_threshold, minTransAmount, is_buy)

        if not filtered_ads:
            logger.debug("No filtered ads found.")
            return
            
        adjusted_target_spot = check_if_ads_avail(filtered_ads, target_spot)
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
            if asset_type == 'USDT':
                logger.debug(f"Updating ad: new ratio: {new_ratio}. old ratio: {current_priceFloatingRatio}. ad number: {advNo}. Base price: {base_price}. Price threshold: {custom_price_threshold}. Is buy: {is_buy}.")
            logger.debug(f"Updating with filter ad: new ratio: {new_ratio}. old ratio: {current_priceFloatingRatio}.")
            await api_instance.update_ad(advNo, new_ratio)
            await update_ad_in_database(target_spot, advNo, asset_type, new_ratio, our_current_price, surplusAmount, ad['account'], fiat, transAmount, minTransAmount)
            logger.debug(f"Ad: {asset_type} - start price: {our_current_price}, ratio: {current_priceFloatingRatio}. Competitor ad - spot: {adjusted_target_spot}, price: {competitor_price}, base: {base_price}, ratio: {competitor_ratio}")

    except Exception as e:
        traceback.print_exc()

async def process_ads(ads_group, api_instances, all_ads, is_buy=True):
    page = 1
    if not ads_group:
        return
    for ad in ads_group:
        account = ad['account']
        api_instance = api_instances[account]
        payTypes_list = ad['payTypes'] if ad['payTypes'] is not None else []
        ads_data = await api_instance.fetch_ads_search('BUY' if is_buy else 'SELL', ad['asset_type'], ad['fiat'], ad['transAmount'], payTypes_list, page)
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
        logger.info(f"Finished updating ads for {'BUY' if is_buy else 'SELL'}.")

async def update_ads_main():
    logger.info("Starting ads update...")
    fetch_balances_task = asyncio.create_task(fetch_and_calculate_total_balance())
    await asyncio.sleep(5)  # Ensure balance is fetched initially
    buy_task = asyncio.create_task(start_update_ads(is_buy=True))
    sell_task = asyncio.create_task(start_update_ads(is_buy=False))
    await asyncio.gather(fetch_balances_task, buy_task, sell_task)

async def main():
    try:
        await update_ads_main()
    finally:
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())