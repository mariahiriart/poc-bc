import requests
import os
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
WHAPI_TOKEN = os.getenv('WHAPI_TOKEN')

def search_roberto():
    url = "https://gate.whapi.cloud/messages/list"
    headers = {"Authorization": f"Bearer {WHAPI_TOKEN}"}
    
    # Buscar en los últimos mensajes de hoy
    now = datetime.now()
    today_start = int(datetime(now.year, now.month, now.day).timestamp())
    
    params = {
        "count": 100,
        "from": today_start
    }
    
    resp = requests.get(url, params=params, headers=headers)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text}")
        return

    messages = resp.json().get('messages', [])
    print(f"Total mensajes hoy: {len(messages)}")
    
    for m in messages:
        sender_name = m.get('from_name', '')
        text = ""
        if 'text' in m:
             text = m['text'].get('body', '')
        elif 'image' in m:
             text = m['image'].get('caption', '[IMAGEN]')
             
        if "roberto" in sender_name.lower() or "roberto" in text.lower():
            print(f"--- Fila encontrada ---")
            print(f"ChatID: {m.get('chat_id')}")
            print(f"De: {sender_name} ({m.get('from')})")
            print(f"Mensaje: {text}")
            print(f"Hora: {datetime.fromtimestamp(m.get('timestamp')).strftime('%H:%M:%S')}")
            if 'image' in m:
                print(f"ID Imagen: {m['image'].get('id')}")

if __name__ == "__main__":
    search_roberto()
