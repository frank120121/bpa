import sys
import asyncio
import aiohttp
import aiofiles
import time
import hmac
import hashlib
from PIL import Image
from io import BytesIO
import pytesseract
import re
from cep import Transferencia
from datetime import datetime

def correct_ocr_errors(text, bank):
    # Correct specific OCR errors for different banks
    if bank == "BBVA":
        return text.replace('O', '0')
    elif bank == "NU":
        return text.replace('/', 'J')
    else:
        return text

def extract_clave_de_rastreo_from_text(text, bank):
    def clean_and_verify_clave(clave):
        cleaned_clave = re.sub(r'\W+', '', clave)
        print(f"Cleaned Clave: {cleaned_clave}")  # Debug
        return cleaned_clave

    corrected_text = correct_ocr_errors(text, bank)
    print(f"Corrected Text: {corrected_text}")  # Debug

    if bank == "BBVA":
        matches = re.findall(r'MBAN[A-Za-z0-9]{20}', corrected_text)
    elif bank == "NU":
        matches = re.findall(r'NU[A-Za-z0-9]{26}', corrected_text)
    elif bank == "Banorte":
        matches = re.findall(r'[A-Za-z0-9]*CP0[A-Za-z0-9]{2,26}', corrected_text)
    else:
        matches = re.findall(r'\b[A-Za-z0-9]{23,30}\b', corrected_text)
    
    print(f"Matches found: {matches}")  # Debug

    for potential_clave in matches:
        print(f"Potential Clave (Before): {potential_clave}")
        potential_clave = potential_clave.strip()
        print(f"Potential Clave (After): {potential_clave}") 
        print(f"Length of Potential Clave: {len(potential_clave)}")

        clave_de_rastreo = clean_and_verify_clave(potential_clave)
        if clave_de_rastreo:
            return clave_de_rastreo

    return None

async def download_image(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        img_data = await response.read()
        return Image.open(BytesIO(img_data))

async def extract_clave_de_rastreo(image_url, bank):
    async with aiohttp.ClientSession() as session:
        img = await download_image(session, image_url)
        text = pytesseract.image_to_string(img)
        print(f"Extracted Text: {text}")  # Debug
        return extract_clave_de_rastreo_from_text(text, bank)

async def validate_transfer(fecha, clave_rastreo, emisor, receptor, cuenta, monto):
    if isinstance(fecha, str):
        fecha = datetime.strptime(fecha, '%Y-%m-%d').date()
    
    print(f"Validating transfer for Clave: {clave_rastreo}")  # Debug
    try:
        tr = await asyncio.to_thread(Transferencia.validar,
            fecha=fecha,
            clave_rastreo=clave_rastreo,
            emisor=emisor,
            receptor=receptor,
            cuenta=cuenta,
            monto=monto,
        )
        if tr is not None:
            print("Validation successful, downloading PDF...")
            pdf = await asyncio.to_thread(tr.descargar)
            
            file_path = rf"C:\Users\p7016\Downloads\{clave_rastreo}.pdf"
            
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(pdf)
            print(f"PDF saved successfully at {file_path}.")
            return True
        else:
            print("Validation failed, unable to download PDF.")
            return False
    except Exception as e:
        print(f"Transfer validation failed with error: {e}")
        return False

async def retrieve_binance_messages(api_key, secret_key, order_no):
    params = {
        'page': 1,
        'rows': 20,
        'orderNo': order_no,
        'timestamp': int(time.time() * 1000)
    }

    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {
        'X-MBX-APIKEY': api_key,
        'clientType': 'your_client_type',
    }

    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.binance.com/sapi/v1/c2c/chat/retrieveChatMessagesWithPagination', headers=headers, params=params) as response:
            response.raise_for_status()
            return await response.json()

if __name__ == "__main__":
    async def main():
        sys.path.append('C:\\Users\\p7016\\Documents\\bpa')

        try:
            from credentials import credentials_dict
        except ModuleNotFoundError:
            print("Failed to import credentials. Please check the path and ensure credentials.py is in the specified directory.")
            sys.exit(1)

        api_key = credentials_dict['account_1']['KEY']
        secret_key = credentials_dict['account_1']['SECRET']

        order_details = {
            '22643818484843712512': {
                'fecha': datetime.strptime('08-07-2024', '%d-%m-%Y').date(), 
                'emisor': '40012',  # BBVA MEXICO
                'receptor': '90710',  # NVIO
                'cuenta': '710969000015306104',
                'monto': 509.00,
                'bank': 'BBVA'
            },
            '22643806509627179008': {
                'fecha': datetime.strptime('08-07-2024', '%d-%m-%Y').date(),
                'emisor': '40012',  # BBVA MEXICO
                'receptor': '90710',  # NVIO
                'cuenta': '710969000015306104',
                'monto': 842.00,
                'bank': 'BBVA'
            },
            '22643829517634654208': {
                'fecha': datetime.strptime('08-07-2024', '%d-%m-%Y').date(),
                'emisor': '40012',  # BBVA MEXICO
                'receptor': '90710',  # NVIO
                'cuenta': '710969000015306104',
                'monto': 2600.00,
                'bank': 'BBVA'
            }
        }

        for order_no, details in order_details.items():
            print(f"Processing orderNo: {order_no}")
            data = await retrieve_binance_messages(api_key, secret_key, order_no)

            if data['success']:
                messages = data['data']
                image_url = None
                for message in messages:
                    if message['type'] == 'image':
                        image_url = message['imageUrl']
                        break

                if image_url:
                    clave_de_rastreo = await extract_clave_de_rastreo(image_url, details['bank'])
                    if clave_de_rastreo:
                        print(f"Extracted Clave de Rastreo: {clave_de_rastreo}")

                        validation_successful = await validate_transfer(
                            fecha=details['fecha'],
                            clave_rastreo=clave_de_rastreo,
                            emisor=details['emisor'],
                            receptor=details['receptor'],
                            cuenta=details['cuenta'],
                            monto=details['monto']
                        )
                        if validation_successful:
                            print("Transfer validation and PDF download successful.")
                        else:
                            print("Transfer validation failed.")
                    else:
                        print("No Clave de Rastreo found.")
                else:
                    print("No image message found.")
            else:
                print(f"Error: {data['msg']}")

    asyncio.run(main())
