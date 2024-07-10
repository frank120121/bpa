import sys
import os
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
from datetime import date

def extract_clave_de_rastreo_from_text(text):
    def clean_and_verify_clave(clave):
        # Remove any non-alphanumeric characters
        cleaned_clave = re.sub(r'\W+', '', clave)
        # Ensure the clave is exactly 24 characters long and alphanumeric
        return cleaned_clave if len(cleaned_clave) == 24 and cleaned_clave.isalnum() else None

    def correct_ocr_errors(text):
        # Replace commonly misinterpreted characters
        corrected_text = text.replace('O', '0').replace('I', '1').replace('l', '1')
        return corrected_text

    # Correct common OCR errors in the extracted text
    corrected_text = correct_ocr_errors(text)
    print(f"Corrected Text: {corrected_text}")  # Debug: print the corrected text

    # Use regex to find the "clave de rastreo"
    matches = re.findall(r'MBAN[A-Za-z0-9]{20}', corrected_text)
    for potential_clave in matches:
        potential_clave = potential_clave.strip()  # Ensure no leading or trailing whitespace
        print(f"Potential Clave: {potential_clave}")  # Debug: print the potential clave
        print(f"Length of Potential Clave: {len(potential_clave)}")  # Debug: print the length of potential clave

        # Clean and verify the potential clave
        clave_de_rastreo = clean_and_verify_clave(potential_clave)
        if clave_de_rastreo:
            return clave_de_rastreo

    return None

async def download_image(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        img_data = await response.read()
        return Image.open(BytesIO(img_data))

async def extract_clave_de_rastreo(image_url):
    async with aiohttp.ClientSession() as session:
        img = await download_image(session, image_url)
        text = pytesseract.image_to_string(img)
        print(f"Extracted Text: {text}")  # Debug: print the extracted text
        return extract_clave_de_rastreo_from_text(text)

async def validate_transfer(fecha, clave_rastreo, emisor, receptor, cuenta, monto):
    print("Validating transfer...")
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
        
        # Define the full path where the PDF will be saved, using the clave de rastreo
        file_path = rf"C:\Users\p7016\Downloads\{clave_rastreo}.pdf"
        
        # Save the PDF to the specified file
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(pdf)
        print(f"PDF saved successfully at {file_path}.")
        return True
    else:
        print("Validation failed, unable to download PDF.")
        return False

async def retrieve_binance_messages(api_key, secret_key, order_no):
    # Define the parameters
    params = {
        'page': 1,
        'rows': 20,
        'orderNo': order_no,
        'timestamp': int(time.time() * 1000)  # Current timestamp in milliseconds
    }

    # Create the query string
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])

    # Generate the signature
    signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    # Add the signature to the parameters
    params['signature'] = signature

    # Define the headers
    headers = {
        'X-MBX-APIKEY': api_key,
        'clientType': 'your_client_type',  # Replace with your client device type
        # Add other headers if necessary
    }

    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.binance.com/sapi/v1/c2c/chat/retrieveChatMessagesWithPagination', headers=headers, params=params) as response:
            return await response.json()

if __name__ == "__main__":
    async def main():
        # Add the directory containing credentials.py to the Python path
        sys.path.append('C:\\Users\\p7016\\Documents\\bpa')

        try:
            from credentials import credentials_dict
        except ModuleNotFoundError:
            print("Failed to import credentials. Please check the path and ensure credentials.py is in the specified directory.")
            sys.exit(1)

        # Load the credentials for account_1
        api_key = credentials_dict['account_1']['KEY']
        secret_key = credentials_dict['account_1']['SECRET']

        # Retrieve Binance messages
        order_no = '22643438016233922560'
        data = await retrieve_binance_messages(api_key, secret_key, order_no)

        # Extract the clave de rastreo from a payment capture image
        if data['success']:
            messages = data['data']
            image_url = None
            for message in messages:
                if message['type'] == 'image':
                    image_url = message['imageUrl']
                    break

            if image_url:
                clave_de_rastreo = await extract_clave_de_rastreo(image_url)
                if clave_de_rastreo:
                    print(f"Extracted Clave de Rastreo: {clave_de_rastreo}")

                    # Validate the transfer with the extracted clave de rastreo
                    fecha = date.today()
                    emisor = '40012'  # BBVA MEXICO
                    receptor = '90710'  # Nvio
                    cuenta = '710969000016348705'
                    monto = 6000.00

                    validation_successful = await validate_transfer(fecha, clave_de_rastreo, emisor, receptor, cuenta, monto)
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
