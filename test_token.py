import requests
import os

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
url = "https://gate.whapi.cloud/groups"  # Test group list
headers = {"Authorization": f"Bearer {TOKEN}"}

try:
    resp = requests.get(url, headers=headers)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text[:500]}")
except Exception as e:
    print(f"Error: {e}")
