#binance_update_ads.py
import asyncio
import traceback
import logging
from credentials import credentials_dict
from binance_share_data import SharedSession, SharedData
from ads_database import update_ad_in_database
from binance_api import BinanceAPI
from TESTbitso_order_book_cache import reference_prices
logger = logging.getLogger(__name__)

# Constants
SELL_PRICE_THRESHOLD = 0.9945
SELL_PRICE_ADJUSTMENT = 0
BUY_PRICE_THRESHOLD = 1.0065  
PRICE_THRESHOLD_2 = 1.0243
MIN_RATIO = 90.00
MAX_RATIO = 110.00
RATIO_ADJUSTMENT = 0.05
DIFF_THRESHOLD = 0.15
BASE = 0.005

def filter_ads(ads_data, base_price, own_ads, trans_amount_threshold, price_threshold, minTransAmount, is_buy=True):
    own_adv_nos = [ad['advNo'] for ad in own_ads]
    return [ad for ad in ads_data
            if ad['adv']['advNo'] not in own_adv_nos
            and ((float(ad['adv']['price']) > (base_price * price_threshold)) if is_buy else (float(ad['adv']['price']) <= base_price * price_threshold))
            and float(ad['adv']['dynamicMaxSingleTransAmount']) >= trans_amount_threshold
            and float(ad['adv']['minSingleTransAmount']) <= minTransAmount]

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
        return adjusted_target_spot
    else:
        return adjusted_target_spot
    
async def retry_fetch_ads(binance_api, KEY, SECRET, ad, is_buy, page_start=1, page_end=10):
    advNo = ad.get('advNo')
    for page in range(page_start, page_end):
        ads_data = await binance_api.fetch_ads_search(KEY, SECRET, 'BUY' if is_buy else 'SELL', ad['asset_type'], ad['fiat'], ad['transAmount'], ad['payTypes'], page)
        if ads_data is None or ads_data.get('code') != '000000' or 'data' not in ads_data:
            logger.error(f"Failed to fetch ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {ad['payTypes']} on page {page}.")
            continue

        current_ads_data = ads_data['data']
        our_ad_data = next((item for item in current_ads_data if item['adv']['advNo'] == advNo), None)
        if our_ad_data:
            return current_ads_data

    logger.error(f"Failed to fetch ads for ad number {advNo} after checking {page_end - page_start} pages.")
    return []

async def is_ad_online(binance_api, KEY, SECRET, advNo):
    try:
        response = await binance_api.get_ad_detail(KEY, SECRET, advNo)
        if response and response.get('code') == '000000' and 'data' in response:
            ad_status = response['data'].get('advStatus')
            return ad_status == 1 
        else:
            logger.error(f"Failed to get ad details for advNo {advNo}: {response}")
            return False
    except Exception as e:
        logger.error(f"An error occurred while checking ad status for advNo {advNo}: {e}")
        return False

async def analyze_and_update_ads(ad, binance_api, KEY, SECRET, ads_data, all_ads, is_buy=True):
    if not ad:
        logger.error("Ad data is missing. Skipping...")
        return

    advNo = ad.get('advNo')
    target_spot = ad.get('target_spot')
    asset_type = ad.get('asset_type')
    current_priceFloatingRatio = float(ad['floating_ratio'])
    surplusAmount = ad.get('surplused_amount')
    fiat = ad.get('fiat')
    transAmount = ad.get('transAmount')
    minTransAmount = ad.get('minTransAmount')

    try:
        our_ad_data = next((item for item in ads_data if item['adv']['advNo'] == advNo), None)
        if our_ad_data:
            our_current_price = float(our_ad_data['adv']['price'])
        else:
            if not await is_ad_online(binance_api, KEY, SECRET, advNo):
                return
            our_ad_detail = await binance_api.get_ad_detail(KEY, SECRET, advNo)
            if our_ad_detail.get('code') == '000000' and 'data' in our_ad_detail:
                our_ad_data = our_ad_detail['data']
                if our_ad_data['advNo'] == advNo:
                    our_current_price = float(our_ad_data['price'])
                else:
                    logger.error(f"No matching ad data found for ad number {advNo}.")
                    return

        base_price = compute_base_price(our_current_price, current_priceFloatingRatio)

        ask = reference_prices.get("lowest_ask")
        bid = reference_prices.get("highest_bid")

        if ask is not None and bid is not None and asset_type == 'USDT':
            global BUY_PRICE_THRESHOLD
            global SELL_PRICE_THRESHOLD

            previous_sell_price_threshold = SELL_PRICE_THRESHOLD
            previous_buy_price_threshold = BUY_PRICE_THRESHOLD
            min_diff = 0.0005
            new_sell_price_threshold = round(((base_price * 0.9989) / base_price), 4)
            new_buy_price_threshold = round(((ask * 1.017) / base_price), 4)
            current_diff = 0

            if is_buy:
                current_diff = abs(new_buy_price_threshold - previous_buy_price_threshold)
                if current_diff > min_diff:
                    BUY_PRICE_THRESHOLD = new_buy_price_threshold
            else:
                current_diff = abs(new_sell_price_threshold - previous_sell_price_threshold)
                if current_diff > min_diff:
                    SELL_PRICE_THRESHOLD = new_sell_price_threshold

        custom_price_threshold = round(determine_price_threshold(ad['payTypes'], is_buy), 4)

        filtered_ads = filter_ads(ads_data, base_price, all_ads, transAmount, custom_price_threshold, minTransAmount, is_buy)
        if not filtered_ads:
            ads_data = await retry_fetch_ads(binance_api, KEY, SECRET, ad, is_buy)
            if not ads_data:
                return
            filtered_ads = filter_ads(ads_data, base_price, all_ads, transAmount, custom_price_threshold, minTransAmount, is_buy)
            if not filtered_ads:
                return

        adjusted_target_spot = check_if_ads_avail(filtered_ads, target_spot)
        competitor_ad = filtered_ads[adjusted_target_spot - 1]
        competitor_price = float(competitor_ad['adv']['price'])
        competitor_ratio = round(((competitor_price / base_price) * 100), 2)

        if (our_current_price >= competitor_price and is_buy) or (our_current_price <= competitor_price and not is_buy):
            new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT if is_buy else competitor_ratio + RATIO_ADJUSTMENT
        else:
            diff_ratio = competitor_ratio - current_priceFloatingRatio if is_buy else current_priceFloatingRatio - competitor_ratio
            if diff_ratio > DIFF_THRESHOLD:
                new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT if is_buy else competitor_ratio + RATIO_ADJUSTMENT
            else:
                return

        new_ratio = max(MIN_RATIO, min(MAX_RATIO, round(new_ratio_unbounded, 2)))
        new_diff = abs(new_ratio - current_priceFloatingRatio)
        if new_ratio == current_priceFloatingRatio and new_diff < 0.001:
            return
        else:
            await binance_api.update_ad(KEY, SECRET, advNo, new_ratio)
            await update_ad_in_database(
                target_spot=target_spot,
                advNo=advNo,
                asset_type=asset_type,
                floating_ratio=new_ratio,
                price=our_current_price,
                surplusAmount=surplusAmount,
                account=ad['account'],
                fiat=fiat,
                transAmount=transAmount,
                minTransAmount=minTransAmount
            )
            await SharedData.update_ad(
                advNo=advNo,
                target_spot=target_spot,
                asset_type=asset_type,
                floating_ratio=new_ratio,
                price=our_current_price,
                surplusAmount=surplusAmount,
                account=ad['account'],
                fiat=fiat,
                transAmount=transAmount,
                minTransAmount=minTransAmount
            )

    except Exception as e:
        logger.error(f"An error occurred while analyzing and updating ad {advNo}: {e}")
        traceback.print_exc()

async def main_loop(binance_api, is_buy=True):
    all_ads = await SharedData.fetch_all_ads('BUY' if is_buy else 'SELL')

    grouped_ads = {}
    for ad in all_ads:
        group_key = ad['Group']
        grouped_ads.setdefault(group_key, []).append(ad)

    for group_key, ads_group in grouped_ads.items():
        await process_ads(ads_group, binance_api, all_ads, is_buy)

async def process_ads(ads_group, binance_api, all_ads, is_buy=True):
    if not ads_group:
        return
    
    for ad in ads_group:
        account = ad['account']
        KEY = credentials_dict[account]['KEY']
        SECRET = credentials_dict[account]['SECRET']
        payTypes_list = ad['payTypes'] if ad['payTypes'] is not None else []

        ads_data = await binance_api.fetch_ads_search(
            KEY, SECRET,
            'BUY' if is_buy else 'SELL',
            ad['asset_type'], ad['fiat'],
            ad['transAmount'], payTypes_list, page=1
        )
        
        if ads_data is None or ads_data.get('code') != '000000' or 'data' not in ads_data:
            logger.error(f"Failed to fetch ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {payTypes_list}. API response: {ads_data}")
            continue
        
        current_ads_data = ads_data['data']
        
        if not isinstance(current_ads_data, list):
            logger.error(f'Current ads data is not a list: {current_ads_data}. API response: {ads_data}')
            continue
        if not current_ads_data:
            logger.error(f'Current ads data is an empty list: {current_ads_data}. API response: {ads_data}')
            logger.info(f"Parameters used: trade_type={'BUY' if is_buy else 'SELL'}, asset_type={ad['asset_type']}, fiat={ad['fiat']}, transAmount={ad['transAmount']}, payTypes_list={payTypes_list}, page=1")
            continue
        
        await analyze_and_update_ads(ad, binance_api, KEY, SECRET, current_ads_data, all_ads, is_buy)

async def start_update_ads(binance_api, is_buy=True):
    try:
        all_ads = await SharedData.fetch_all_ads()
        if not all_ads:
            return

        while True:
            await main_loop(binance_api, is_buy)
    finally:
        logger.debug(f"Finished updating ads for {'BUY' if is_buy else 'SELL'}.")

async def update_ads_main(binance_api):
    await start_update_ads(binance_api, is_buy=True)
    await start_update_ads(binance_api, is_buy=False)

async def main():
    try:
        binance_api = await BinanceAPI.get_instance()
        await update_ads_main(binance_api)
    finally:
        await BinanceAPI.close_session()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())