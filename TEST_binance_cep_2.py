#bpa/TEST_binance_cep_2.py
import sys
import asyncio
import pytesseract
import re
from cep import Transferencia
from datetime import datetime
import logging
from abc import ABC, abstractmethod
from requests.exceptions import HTTPError

from binance_share_data import SharedSession
from binance_db_get import get_test_orders_from_db
from common_utils import download_image, retrieve_binance_messages
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def retry_request(func, retries=3, delay=1, backoff=2):
    for attempt in range(retries):
        try:
            result = await asyncio.to_thread(func)
            return result
        except HTTPError as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                # Log the status code and response content
                logger.error(f"Status Code: {e.response.status_code}")
                logger.error(f"Response Content: {e.response.content.decode('utf-8', errors='ignore')}")
            if e.response and e.response.status_code == 500:
                # Log detailed info if it's a 500 error
                logger.error(f"Server Error (500): {e.response.text}")
            if attempt < retries - 1:
                logger.info(f"Retrying after {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= backoff
            else:
                logger.error("All retry attempts failed.")
                return None
        except Exception as e:
            # Log the actual exception message and traceback for better debugging
            import traceback
            logger.error(f"Unexpected error: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if attempt < retries - 1:
                logger.info(f"Retrying after {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= backoff
            else:
                logger.error("All retry attempts failed.")
                return None
    
class BankReceiptHandler(ABC):
    """Abstract base class for bank receipt handlers"""
    
    @abstractmethod
    def extract_clave_de_rastreo(self, image):
        """Extract tracking number from image"""
        pass
    
    @abstractmethod
    def validate_clave_format(self, clave):
        """Validate the format of tracking number"""
        pass

class BBVAReceiptHandler(BankReceiptHandler):
    def extract_clave_de_rastreo(self, image):        
        custom_config = r'--oem 3 --psm 6'
        logger.info(f"Using Tesseract config: {custom_config}")

        raw_text = pytesseract.image_to_string(image, config=custom_config)
        logger.info(f"raw OCR text:\n{raw_text}")
        
        # Log each line separately for better analysis
        logger.info("OCR text by lines:")
        for i, line in enumerate(raw_text.split('\n')):
            logger.info(f"Line {i+1}: {repr(line)}")
        
        # Look for text containing "Clave" and log the search process
        lines = raw_text.split('\n')
        clave_text = None
        for i, line in enumerate(lines):
            if 'lave' in line.lower() and 'rastreo' in line.lower():
                logger.info(f"Found potential clave line at index {i}: {repr(line)}")
                if i + 1 < len(lines):
                    next_line = lines[i+1].strip()
                    logger.info(f"Next line: {repr(next_line)}")
                    if next_line.startswith('MBAN') or next_line.startswith('BNET'):
                        cleaned = next_line.upper().replace('O', '0').replace('I', '1')
                        if self.validate_clave_format(cleaned):
                            logger.info(f"Valid clave found: {cleaned}")
                            return cleaned
                    clave_text = line + next_line
                break
        
        # If not found directly after "Clave de rastreo", try cleaning full text
        logger.info("Attempting fallback search in full text...")
        cleaned_text = raw_text.upper()
        cleaned_text = cleaned_text.replace('O', '0').replace('I', '1')
        cleaned_text = cleaned_text.replace(' ', '')
        
        # Look for both MBAN and BNET patterns
        mban_matches = re.findall(r'MBAN[A-Z0-9]{20}', cleaned_text)
        bnet_matches = re.findall(r'BNET[A-Z0-9]{20}', cleaned_text)
        matches = mban_matches + bnet_matches
        
        logger.info(f"Fallback matches - MBAN: {mban_matches}, BNET: {bnet_matches}")
        
        if matches:
            clave = matches[0]
            if len(clave) == 24:
                if clave.startswith('MBAN'):
                    clave = 'MBAN01' + clave[6:]
                elif clave.startswith('BNET'):
                    clave = 'BNET01' + clave[6:]
                logger.info(f"Final clave from fallback: {clave}")
                return clave
                
        logger.info("No valid clave found in any attempt")
        return None

    def validate_clave_format(self, clave):
        if not clave or len(clave) != 24:
            return False
        # Allow both MBAN and BNET formats
        return bool(re.match(r'^(MBAN|BNET)[A-Za-z0-9]{20}$', clave))

class NUReceiptHandler(BankReceiptHandler):
    def extract_clave_de_rastreo(self, image):
        custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=NUJABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        text = pytesseract.image_to_string(image, config=custom_config)
        matches = re.findall(r'NU[A-Z0-9]{26}', text)
        
        if matches:
            clave = matches[0]
            if self.validate_clave_format(clave):
                return clave
        return None

    def validate_clave_format(self, clave):
        if not clave or len(clave) != 28:
            return False
        return bool(re.match(r'^NU[A-Z0-9]{26}$', clave))

class BanorteReceiptHandler(BankReceiptHandler):
    def extract_clave_de_rastreo(self, image):
        custom_config = r'--oem 3 --psm 6'
        text = pytesseract.image_to_string(image, config=custom_config)
        matches = re.findall(r'[A-Za-z0-9]*CP0[A-Za-z0-9]{2,26}', text)
        
        if matches:
            clave = matches[0]
            if self.validate_clave_format(clave):
                return clave
        return None

    def validate_clave_format(self, clave):
        if not clave:
            return False
        return bool(re.match(r'^.*CP0[A-Z0-9]{2,26}$', clave))

def get_bank_handler(bank):
    """Factory function to get the appropriate bank handler"""
    handlers = {
        "BBVA": BBVAReceiptHandler,
        "NU": NUReceiptHandler,
        "BANORTE": BanorteReceiptHandler
    }
    handler_class = handlers.get(bank)
    if handler_class:
        return handler_class()
    raise ValueError(f"No handler available for bank: {bank}")

async def extract_clave_de_rastreo(image_url, bank):
    img = await download_image(image_url)
    if not img:
        logger.error("Failed to download image")
        return None

    try:
        handler = get_bank_handler(bank)
        logger.info(f"Processing image for bank: {bank}")
        
        clave = handler.extract_clave_de_rastreo(img)
        if clave:
            logger.info(f"Extracted clave using {bank} handler: {clave}")
            return clave
            
        logger.info(f"No clave found for {bank}")
        return None
        
    except ValueError as e:
        logger.warning(f"Bank handler error: {e}")
    except Exception as e:
        logger.error(f"Error processing with bank handler: {e}", exc_info=True)
    
    return None

if __name__ == "__main__":
    async def main():
        sys.path.append('C:\\Users\\p7016\\Documents\\bpa')

        try:
            from credentials import credentials_dict
        except ModuleNotFoundError:
            logger.info("Failed to import credentials. Please check the path and ensure credentials.py is in the specified directory.")
            sys.exit(1)

        api_key = credentials_dict['account_2']['KEY']
        secret_key = credentials_dict['account_2']['SECRET']

        # Get orders from database
        order_details = await get_test_orders_from_db()
        
        if not order_details:
            logger.error("No matching orders found in database")
            sys.exit(1)

        processed_count = 0
        success_count = 0
        failed_count = 0
        no_image_count = 0

        for order_no, details in order_details.items():
            processed_count += 1
            logger.info(f"\nProcessing order {processed_count}/{len(order_details)}")
            logger.info(f"OrderNo: {order_no}")
            logger.info(f"Details: Date: {details['fecha']}, Amount: {details['monto']}, Account: {details['cuenta']}")
            
            # Skip if we already have a clave
            if details['existing_clave']:
                logger.info(f"Skipping order {order_no} - Already has clave: {details['existing_clave']}")
                continue
            
            data = await retrieve_binance_messages(api_key, secret_key, order_no)

            if data['success']:
                messages = data['data']
                image_url = None
                for message in messages:
                    if message['type'] == 'image':
                        image_url = message['imageUrl']
                        break

                if image_url:
                    session = await SharedSession.get_session()
                    clave_de_rastreo = await extract_clave_de_rastreo(image_url, details['bank'])
                    if clave_de_rastreo:
                        logger.info(f"Extracted Clave de Rastreo: {clave_de_rastreo}")
                        logger.info(f'fecha: {details["fecha"]}, clave_rastreo: {clave_de_rastreo}, emisor: {details["emisor"]}, receptor: {details["receptor"]}, cuenta: {details["cuenta"]}, monto: {details["monto"]}')

                        validation_successful = await retry_request(
                            lambda: Transferencia.validar(
                                fecha=details['fecha'],
                                clave_rastreo=clave_de_rastreo,
                                emisor=details['emisor'],
                                receptor=details['receptor'],
                                cuenta=details['cuenta'],
                                monto=details['monto']
                            ),
                            retries=5,
                            delay=2,
                            backoff=2
                        )
                        if validation_successful:
                            success_count += 1
                            logger.info(f"Transfer validation successful. ({success_count} successful so far)")
                        else:
                            failed_count += 1
                            logger.info(f'fecha: {details["fecha"]}, clave_rastreo: {clave_de_rastreo}, emisor: {details["emisor"]}, receptor: {details["receptor"]}, cuenta: {details["cuenta"]}, monto: {details["monto"]}')
                            logger.warning(f"Transfer validation failed. ({failed_count} failed so far)")
                    else:
                        failed_count += 1
                        logger.warning(f"No Clave de Rastreo found. ({failed_count} failed so far)")
                else:
                    no_image_count += 1
                    logger.warning(f"No image message found. ({no_image_count} orders without images so far)")
            else:
                failed_count += 1
                logger.error(f"Error retrieving messages: {data['msg']}")

        # Print summary
        logger.info("\n=== Processing Summary ===")
        logger.info(f"Total orders processed: {processed_count}")
        logger.info(f"Successful validations: {success_count}")
        logger.info(f"Failed validations: {failed_count}")
        logger.info(f"Orders without images: {no_image_count}")
        logger.info("======================")

        await SharedSession.close_session()

    asyncio.run(main())