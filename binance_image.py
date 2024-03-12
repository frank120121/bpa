import json
import requests
from PIL import Image
from io import BytesIO

# Your JSON message
json_message = '{"createTime":1704047368866,"height":1600,"id":"972316320597789953","imageType":"jpg","imageUrl":"https://bin.bnbstatic.com/client_upload/c2c/chat/20231231/0cf8fe0d93f649139b9a0957c8e8df17_20231231182926.jpg","orderNo":"20574842532470734848","self":false,"status":"unread","thumbnailUrl":"https://bin.bnbstatic.com/client_upload/c2c/chat/20231231/0cf8fe0d93f649139b9a0957c8e8df17_20231231182926.jpg","type":"image","uuid":"93228adc-c8e9-47d6-a435-996e0d219c6e","width":720}'

# Parse JSON to get the image URL
data = json.loads(json_message)
image_url = data['imageUrl']

# Download the image
response = requests.get(image_url)
image = Image.open(BytesIO(response.content))

# Display the image (this will depend on your environment)
image.show()
