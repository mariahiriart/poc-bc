import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
WHAPI_TOKEN = os.getenv('WHAPI_TOKEN')

def list_active_chats_today():
    url = "https://gate.whapi.cloud/chats"
    headers = {"Authorization": f"Bearer {WHAPI_TOKEN}"}
    
    # Obtener chats paginados
    all_chats = []
    offset = 0
    count = 100
    
    while True:
        resp = requests.get(url, params={"offset": offset, "count": count}, headers=headers)
        if resp.status_code != 200:
            break
        data = resp.json().get('chats', [])
        if not data:
            break
        all_chats.extend(data)
        if len(data) < count:
            break
        offset += count

    now = datetime.now()
    today_start = int(datetime(now.year, now.month, now.day).timestamp())
    
    print(f"Chats con mensajes hoy (TS >= {today_start}):")
    print(f"{'ID':40s} | {'Last Msg TS':12s} | Nombre")
    print("-" * 80)
    for c in all_chats:
        last_msg = c.get('last_message', {})
        ts = last_msg.get('timestamp') or 0
        if ts >= today_start:
            print(f"{c['id']:40s} | {str(ts):12s} | {c.get('name', '???')}")

if __name__ == "__main__":
    list_active_chats_today()
