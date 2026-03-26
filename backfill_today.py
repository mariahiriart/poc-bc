import requests, json, os, sys
from datetime import datetime, timezone
from dotenv import load_dotenv
import database

load_dotenv()

TOKEN = os.environ.get('WHAPI_TOKEN')
AUTHORIZED_CHAT_IDS = [
    '120363349733984596@g.us',
    '120363419653209546@g.us',
    '120363423645957323@g.us',
    '120363349579190170@g.us',
    '120363400981379542@g.us',
    '120363380129878437@g.us',
]

def backfill_today():
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    print(f"Backfilling messages for {fecha_hoy} with requests...", flush=True)
    
    headers = {
        'Authorization': f'Bearer {TOKEN}', 
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
    }
    total_new = 0
    
    for chat_id in AUTHORIZED_CHAT_IDS:
        url = f'https://gate.whapi.cloud/messages/list/{chat_id}?count=100'
        print(f"Fetching {chat_id}...", flush=True)
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                msgs = resp.json().get('messages', [])
                print(f"  Received {len(msgs)} messages. Checking timestamps...", flush=True)
                for m in msgs:
                    ts = m.get('timestamp', 0)
                    if ts < 1774425600:
                        continue
                        
                    if 'chat_id' not in m:
                        m['chat_id'] = chat_id
                        
                    try:
                        res = database.save_raw_message(m)
                        if res:
                            total_new += 1
                        # even if res is None, we don't count it as error, might already exist
                    except Exception as ex:
                        print(f"  Error saving msg: {ex}", flush=True)
                print(f"  Synced {chat_id}.", flush=True)
            else:
                print(f"  HTTP {resp.status_code} - {resp.text[:100]}", flush=True)
        except Exception as e:
            print(f"  Error fetching {chat_id}: {e}", flush=True)
            
    print(f"\nFinished. New messages saved to DB: {total_new}", flush=True)

if __name__ == "__main__":
    backfill_today()
