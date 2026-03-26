import json, os, urllib.request, re
from datetime import datetime, timezone

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
AUTHORIZED_CHAT_IDS = [
    '120363349733984596@g.us',
    '120363419653209546@g.us',
    '120363423645957323@g.us',
    '120363349579190170@g.us',
    '120363400981379542@g.us',
    '120363380129878437@g.us',
]

def fetch_recent():
    headers = {'Authorization': f'Bearer {TOKEN}', 'Accept': 'application/json'}
    for chat_id in AUTHORIZED_CHAT_IDS:
        url = f'https://gate.whapi.cloud/messages/list/{chat_id}?count=50'
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
                msgs = data.get('messages', [])
                print(f"\n--- Chat: {chat_id} ({len(msgs)} msgs) ---")
                for m in msgs[:5]:
                    ts = m.get('timestamp')
                    dt = datetime.fromtimestamp(ts) if ts else '?'
                    mtype = m.get('type')
                    from_name = m.get('from_name', 'Unknown')
                    text = ""
                    if mtype == 'text':
                        text = m.get('text', {}).get('body', '')
                    elif mtype == 'image':
                        text = m.get('image', {}).get('caption', '[IMAGE]')
                    print(f"[{dt}] {from_name}: {text[:100]}")
        except Exception as e:
            print(f"Error {chat_id}: {e}")

if __name__ == "__main__":
    fetch_recent()
