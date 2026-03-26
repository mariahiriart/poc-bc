import requests, json, sys

sys.stdout.reconfigure(encoding='utf-8')

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
CHAT_ID = "120363349733984596@g.us"
url = f"https://gate.whapi.cloud/messages/list/{CHAT_ID}?count=50"
headers = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    msgs = resp.json().get('messages', [])
    print(f"Total msgs fetched: {len(msgs)}")
    for m in msgs:
        ts = m.get('timestamp')
        who = m.get('from_name', 'Unknown')
        text = ""
        mtype = m.get('type')
        if mtype == 'text':
            text = m.get('text', {}).get('body', '')
        elif mtype == 'image':
            text = m.get('image', {}).get('caption', '[IMG]')
        if text:
            print(f"[{ts}] {who}: {text[:100]}")
else:
    print(f"Error: {resp.status_code}")
