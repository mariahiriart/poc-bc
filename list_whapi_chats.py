"""
list_whapi_chats.py — Lista todos los chats/grupos de WhatsApp desde Whapi.
Incluye paginación para obtener más de 50 chats.
"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

WHAPI_TOKEN = os.environ.get('WHAPI_TOKEN')


def list_chats(max_chats=200):
    """
    Lista chats de Whapi con paginación automática.
    Parámetros:
        max_chats: máximo de chats a traer (default 200)
    """
    url     = "https://gate.whapi.cloud/chats"
    headers = {"Authorization": f"Bearer {WHAPI_TOKEN}"}
    all_chats = []
    offset    = 0
    page_size = 50

    while len(all_chats) < max_chats:
        params = {"count": page_size, "offset": offset}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
        except requests.RequestException as e:
            print(f"Error de conexión: {e}")
            break

        if resp.status_code != 200:
            print(f"Error HTTP {resp.status_code}: {resp.text[:200]}")
            break

        data  = resp.json()
        chats = data.get('chats', [])
        if not chats:
            break

        all_chats.extend(chats)
        print(f"  Página offset={offset}: {len(chats)} chats")

        if len(chats) < page_size:
            break  # última página
        offset += page_size

    print(f"\nTotal chats obtenidos: {len(all_chats)}")
    print(f"{'ID':40s} | {'Tipo':8s} | Nombre")
    print("-" * 80)
    for chat in all_chats:
        c_id   = chat.get('id', '')
        c_name = chat.get('name', 'Sin nombre')
        c_type = 'grupo' if '@g.us' in c_id else 'privado'
        print(f"{c_id:40s} | {c_type:8s} | {c_name}")

    # Filtrar solo grupos (los relevantes para el sistema)
    grupos = [c for c in all_chats if '@g.us' in c.get('id', '')]
    print(f"\nGrupos (@g.us): {len(grupos)}")
    for g in grupos:
        print(f"  {g['id']} — {g.get('name', 'Sin nombre')}")


if __name__ == "__main__":
    list_chats()