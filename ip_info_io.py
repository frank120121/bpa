import ipinfo
import logging
import requests
import time

logger = logging.getLogger(__name__)

def get_ip_origin(ip_address, max_retries=3, initial_backoff=1, max_backoff=5):
    logger.debug(f"Getting IP details for: {ip_address}")
    access_token = '51a4ceb0f429f2'
    handler = ipinfo.getHandler(access_token)
    retries = 0
    backoff = initial_backoff

    while retries < max_retries:
        try:
            details = handler.getDetails(ip_address)
            country = details.country
            logger.debug(f"Got IP details: {details.all}")
            return country
        except requests.exceptions.Timeout:
            retries += 1
            logger.warning(f"Timeout occurred while getting IP details for: {ip_address}. Retry {retries}/{max_retries} in {backoff} seconds.")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff) 
        except requests.exceptions.RequestException as e:
            retries += 1
            logger.warning(f"RequestException occurred while getting IP details for: {ip_address}: {e}. Retry {retries}/{max_retries} in {backoff} seconds.")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        except ValueError as e:
            logger.error(f"ValueError occurred while processing IP details for: {ip_address}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while getting IP details for: {ip_address}: {e}")
            return None

    logger.error(f"Failed to get IP details for: {ip_address} after {max_retries} retries.")
    return None
