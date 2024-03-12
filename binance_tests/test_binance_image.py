import json
import re
import requests
from PIL import Image
from io import BytesIO
import pytesseract

# Set Tesseract command line path
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def extract_clave_rastreo(image_url):
    # Download the image
    response = requests.get(image_url)
    image = Image.open(BytesIO(response.content))
    
    # Use Tesseract to do OCR on the image
    text = pytesseract.image_to_string(image, lang='spa')
    
    # Print raw OCR output for debugging
    print("OCR Output:", text)
    pattern = (
        r'Clave\s*'           # Match the word "Clave"
        r'.*?de\s*'           # Lazily match any characters until "de"
        r'([A-Z0-9]+)\s*'     # Capture the first part of the clave that appears after "de"
        r'.*?rastre\s*'       # Lazily match any characters until "rastre"
        r'([A-Z0-9]+)\s*'     # Capture the second part of the clave that appears after "rastre"
        r'.*?o\s*'            # Lazily match any characters until "o"
        r'([A-Z0-9]*)'        # Capture the continuation of the clave if present
    )
    
    # Use re.DOTALL to match across multiple lines
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        # Join all non-None, non-empty matches to form the complete "Clave de rastreo"
        clave_rastreo_parts = [part for part in match.groups() if part]
        clave_rastreo = ''.join(clave_rastreo_parts)
        return clave_rastreo.strip()  # Strip to remove any leading/trailing whitespace

    return None
# The test section below is for running this script directly.
# If you're importing extract_clave_rastreo into another script, you may remove this part.
if __name__ == '__main__':
    # Provided WebSocket image message
    log_message = "2024-01-26 07:45:15,515:websocket_handlers:INFO:{\"createTime\":1706272620000,\"fromNickname\":\"\",\"height\":1600,\"id\":\"1009649847204426496\",\"imageType\":\"jpg\",\"imageUrl\":\"https://bin.bnbstatic.com/client_upload/c2c/chat/20240126/bcf71be70c1d44beab41a499cb456292_20240126123658.jpg\",\"orderNo\":\"20584174952307261440\",\"self\":false,\"status\":\"read\",\"thumbnailUrl\":\"https://bin.bnbstatic.com/client_upload/c2c/chat/20240126/bcf71be70c1d44beab41a499cb456292_20240126123658.jpg\",\"type\":\"image\",\"uuid\":\"0100a32d-268d-4544-b467-8a9bf28ca7ee\",\"width\":339}"

    # Extract JSON part of the message
    json_start = log_message.find('{')
    json_message = log_message[json_start:]

    # Convert JSON string to dictionary
    message_data = json.loads(json_message)

    # Extract the imageUrl from the message data
    image_url = message_data.get('imageUrl')

    # If imageUrl is present, call the extract function
    if image_url:
        clave_rastreo = extract_clave_rastreo(image_url)
        if clave_rastreo:
            print(f"Extracted Clave rastreo: {clave_rastreo}")
        else:
            print("Clave rastreo not found.")
    else:
        print("No image URL provided.")
