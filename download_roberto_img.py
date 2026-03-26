import requests
import os
from dotenv import load_dotenv

load_dotenv()
WHAPI_TOKEN = os.getenv('WHAPI_TOKEN')

def download_media(media_id, output_name):
    url = f"https://gate.whapi.cloud/media/{media_id}"
    headers = {"Authorization": f"Bearer {WHAPI_TOKEN}"}
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        with open(output_name, 'wb') as f:
            f.write(resp.content)
        print(f"Descargado: {output_name}")
    else:
        print(f"Error: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    media_id = "jpeg-3eb02a39aaca4f25a63574-826d01ab9dd427a4cf9a"
    download_media(media_id, "asignacion_roberto.jpg")
