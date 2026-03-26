import database
from datetime import datetime

today = datetime.now().strftime('%Y-%m-%d')
msgs = database.load_day_messages(today)
print(f"Total mensajes hoy en DB: {len(msgs)}")
for m in msgs:
    name = m.get('from_name', '?')
    text = m.get('text', {}).get('body', '[MEDIA]')
    ts = m.get('timestamp')
    hora = datetime.fromtimestamp(ts).strftime('%H:%M') if ts else '??:??'
    print(f"{hora} | {name:20s} | {text[:50]}")
