import requests, json

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
CHAT_ID = "120363349733984596@g.us"
url = f"https://gate.whapi.cloud/messages/list/{CHAT_ID}?count=50"
headers = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    msgs = resp.json().get('messages', [])
    for m in msgs:
        ts = m.get('timestamp')
        import datetime
        dt = datetime.datetime.fromtimestamp(ts)
        print(f"[{dt}] {m.get('from_name')}: {m.get('type')}")
else:
    print(f"Error: {resp.status_code}")
