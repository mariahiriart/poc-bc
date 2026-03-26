import requests

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
CHAT_ID = "120363349579190170@g.us"
url = f"https://gate.whapi.cloud/messages/list/{CHAT_ID}"
headers = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(url, headers=headers)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text[:500]}")
