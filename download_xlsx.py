import urllib.request, os

TOKEN = "bHNii3AcUgvtuYc5tQclVHiBW31vIH1s"
DOC_ID = "xlsx-3eb003782572179f2954d3-82e001ab9dd427a4cf9a"
OUT_PATH = "d:/Usuario/Desktop/poc/rutas_26_mzo.xlsx"

url = f"https://gate.whapi.cloud/media/{DOC_ID}"
headers = {'Authorization': f'Bearer {TOKEN}', 'User-Agent': 'Mozilla/5.0'}

try:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
        with open(OUT_PATH, 'wb') as f:
            f.write(data)
        print(f"Downloaded: {OUT_PATH} ({len(data)} bytes)")
except Exception as e:
    print(f"Error: {e}")
