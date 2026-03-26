import requests, json

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
AUTHORIZED_CHAT_IDS = [
    '120363349733984596@g.us',
    '120363419653209546@g.us',
    '120363423645957323@g.us',
    '120363349579190170@g.us',
    '120363400981379542@g.us',
    '120363380129878437@g.us',
]

for chat_id in AUTHORIZED_CHAT_IDS:
    url = f"https://gate.whapi.cloud/messages/list/{chat_id}?count=50"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        msgs = resp.json().get('messages', [])
        # Filtra mensajes de hoy (después de 2026-03-26 00:00 CST = 06:00 UTC)
        # ts 1774425600 is 2026-03-26 06:00 UTC
        today_msgs = [m for m in msgs if m.get('timestamp', 0) >= 1774425600]
        print(f"Chat {chat_id}: {len(today_msgs)} today's msgs")
    else:
        print(f"Chat {chat_id}: Error {resp.status_code}")
