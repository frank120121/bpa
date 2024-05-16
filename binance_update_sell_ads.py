import asyncio
import aiohttp
import traceback
import logging
from ads_database import update_ad_in_database, fetch_all_ads_from_database
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI

logger = logging.getLogger(__name__)

PRICE_THRESHOLD = 1.0160
PRICE_THRESHOLD_2 = 1.0180
MIN_RATIO = 101.55
MAX_RATIO = 110
RATIO_ADJUSTMENT = 0.05
DIFF_THRESHOLD = 0.1

def filter_ads(ads_data, base_price, own_ads, trans_amount_threshold, price_threshold):
    own_adv_nos = [ad['advNo'] for ad in own_ads]
    return [ad for ad in ads_data
            if ad['adv']['advNo'] not in own_adv_nos
            and float(ad['adv']['price']) >= base_price * price_threshold
            and float(ad['adv']['dynamicMaxSingleTransAmount']) >= trans_amount_threshold]

def determine_price_threshold(payTypes):
    special_payTypes = ['OXXO', 'BANK', 'ZELLE', 'SkrillMoneybookers']
    if payTypes is not None and any(payType in payTypes for payType in special_payTypes):
        return PRICE_THRESHOLD_2
    else:
        return PRICE_THRESHOLD

def compute_base_price(price: float, floating_ratio: float) -> float:
    return round(price / (floating_ratio / 100), 2)

def check_if_ads_avail(ads_list, adjusted_target_spot):
    if len(ads_list) < adjusted_target_spot:
        adjusted_target_spot = len(ads_list)
        logger.debug("Adjusted the target spot due to insufficient ads after filtering.")
        return adjusted_target_spot
    else:
        return adjusted_target_spot

async def analyze_and_update_ads(ad, api_instance, ads_data, all_ads):
    advNo = ad['advNo']
    target_spot = ad['target_spot']
    asset_type = ad['asset_type']
    current_priceFloatingRatio = float(ad['floating_ratio'])
    surplusAmount = ad['surplused_amount']
    fiat = ad['fiat']
    transAmount = ad['transAmount']

    try:
        our_ad_data = next((item for item in ads_data if item['adv']['advNo'] == advNo), None)
        logger.debug(f'Ads_data: {ads_data}')

        if not our_ad_data:
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
                transAmount=transAmount
            )
            our_current_price = float(our_ad_data['data']['price'])
        else:
            our_current_price = float(our_ad_data['adv']['price'])

        base_price = compute_base_price(our_current_price, current_priceFloatingRatio)
        logger.debug(f"Base Price: {base_price}")
        transAmount_threshold = float(ad['transAmount'])  # Ensure it's a float
        custom_price_threshold = determine_price_threshold(ad['payTypes'])
        filtered_ads = filter_ads(ads_data, base_price, all_ads, transAmount_threshold, custom_price_threshold)
        adjusted_target_spot = check_if_ads_avail(filtered_ads, target_spot)

        if not filtered_ads:
            logger.debug(f"No competitor ads found for {advNo}")
            return

        competitor_ad = filtered_ads[adjusted_target_spot - 1]
        logger.debug(f'Competitor ad: {competitor_ad}')
        competitor_price = float(competitor_ad['adv']['price'])
        competitor_ratio = (competitor_price / base_price) * 100

        if our_current_price >= competitor_price:
            new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT
        else:
            diff_ratio = competitor_ratio - current_priceFloatingRatio
            if diff_ratio > DIFF_THRESHOLD:
                new_ratio_unbounded = competitor_ratio - RATIO_ADJUSTMENT
            else:
                logger.debug(f"Competitor ad - spot: {adjusted_target_spot}, price: {competitor_price}, base: {base_price}, ratio: {competitor_ratio}. Not enough diff: {diff_ratio}")
                return

        new_ratio = max(MIN_RATIO, min(MAX_RATIO, round(new_ratio_unbounded, 2)))
        if new_ratio == current_priceFloatingRatio:
            logger.debug(f"Ratio unchanged")
            return
        else:
            await api_instance.update_ad(advNo, new_ratio)
            await update_ad_in_database(target_spot, advNo, asset_type, new_ratio, our_current_price, surplusAmount, ad['account'], fiat, transAmount)
            logger.debug(f"Ad: {asset_type} - start price: {our_current_price}, ratio: {current_priceFloatingRatio}. Competitor ad - spot: {adjusted_target_spot}, price: {competitor_price}, base: {base_price}, ratio: {competitor_ratio}")

    except Exception as e:
        traceback.print_exc()

async def process_ads(ads_group, api_instances, all_ads):
    if not ads_group:
        return
    for ad in ads_group:
        account = ad['account']
        api_instance = api_instances[account]
        payTypes_list = ad['payTypes'] if ad['payTypes'] is not None else []
        ads_data = await api_instance.fetch_ads_search('BUY', ad['asset_type'], ad['fiat'], ad['transAmount'], payTypes_list)
        if ads_data is None or ads_data.get('code') != '000000' or 'data' not in ads_data:
            logger.error(f"Failed to fetch ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {payTypes_list}.")
            continue
        current_ads_data = ads_data['data']
        if not isinstance(current_ads_data, list) or not current_ads_data:
            logger.debug(f"No valid ads data for asset_type {ad['asset_type']}, fiat {ad['fiat']}, transAmount {ad['transAmount']}, and payTypes {payTypes_list}.")
            continue
        await analyze_and_update_ads(ad, api_instance, current_ads_data, all_ads)

async def main_loop(api_instances):
    all_ads = await fetch_all_ads_from_database('BUY')
    logger.debug(f"All ads: {len(all_ads)}")

    grouped_ads = {}
    for ad in all_ads:
        group_key = ad['Group']
        grouped_ads.setdefault(group_key, []).append(ad)

    tasks = []
    for group_key, ads_group in grouped_ads.items():
        tasks.append(asyncio.create_task(process_ads(ads_group, api_instances, all_ads)))
    await asyncio.gather(*tasks)

async def start_update_sell_ads():
    async with aiohttp.ClientSession() as session:
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
                await main_loop(api_instances)
                await asyncio.sleep(2)  # Adjust sleep time as needed
        finally:
            await SingletonBinanceAPI.close_all()

if __name__ == "__main__":
    asyncio.run(start_update_sell_ads())
