import requests, json

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
CHAT_ID = "120363349579190170@g.us"
url = f"https://gate.whapi.cloud/messages/list/{CHAT_ID}?count=50"
headers = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    msgs = resp.json().get('messages', [])
    for m in msgs:
        if m.get('type') == 'document':
            doc = m.get('document', {})
            print(f"[{m.get('timestamp')}] DOC: {doc.get('filename')} (id={doc.get('id')})")
else:
    print(f"Error: {resp.status_code} {resp.text}")
