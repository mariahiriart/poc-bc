import psycopg2
import json
import os
import re
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)

GROUP_MAP = {
    "120363349733984596@g.us": "46a53be2-bac1-4c90-a9fd-c566402c7cfa",
    "120363419653209546@g.us": "77d5a479-481a-4914-a529-042af31f946f",
    "120363349579190170@g.us": "57ced152-a970-4f8f-8a48-9fa8c1f1537e",
    "120363400981379542@g.us": "2657cc8f-6ad6-4541-8bd5-6575fcb0d564",
    "120363380129878437@g.us": "a76447de-d3e2-4675-960b-07bee7092a2c",
    "120363423645957323@g.us": "9c146e27-5d25-41e9-9892-669b91e08436",
}

# Mexico = UTC-6 (CST). Cambiar a -5 en verano (CDT).
MEXICO_TZ = timezone(timedelta(hours=-6))


def get_connection():
    return psycopg2.connect(**DB_CFG)


# ── GUARDAR MENSAJE RAW ────────────────────────────────────────────────────────
def save_raw_message(msg_data):
    """
    Guarda el JSON crudo de Whapi en raw_messages.
    Retorna el UUID del registro insertado, o None si ya existia o hubo error.
    """
    chat_id    = msg_data.get('chat_id', '')
    group_uuid = GROUP_MAP.get(chat_id)

    if not group_uuid:
        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.execute("SELECT id FROM whatsapp_groups WHERE chat_id = %s", (chat_id,))
            res = cur.fetchone()
            if res:
                group_uuid = str(res[0])
                GROUP_MAP[chat_id] = group_uuid
            cur.close(); conn.close()
        except Exception as e:
            print(f"[DB] lookup group {chat_id}: {e}")

    if not group_uuid:
        print(f"[DB] Sin group_id para chat {chat_id} — mensaje no guardado.")
        return None

    ts = msg_data.get('timestamp')
    # FIX: guardar siempre UTC timezone-aware
    sent_at = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(tz=timezone.utc)

    mtype = msg_data.get('type', '')
    phone = msg_data.get('from', '')
    name  = msg_data.get('from_name', '')

    if mtype == 'text':
        content = (msg_data.get('text') or {}).get('body', '')
    elif mtype == 'image':
        content = (msg_data.get('image') or {}).get('caption', '')
    elif mtype == 'document':
        content = (msg_data.get('document') or {}).get('filename', '')
    else:
        content = ''

    has_media    = mtype in ('image', 'video', 'audio', 'document', 'sticker')
    has_location = mtype == 'location'
    whapi_msg_id = msg_data.get('id', '')

    conn = get_connection()
    cur  = conn.cursor()
    try:
        if whapi_msg_id:
            cur.execute("SELECT id FROM raw_messages WHERE whapi_message_id = %s LIMIT 1", (whapi_msg_id,))
            if cur.fetchone():
                return None  # ya existe

        cur.execute("""
            INSERT INTO raw_messages (
                sender_phone, sender_name, content, raw_json, sent_at,
                whapi_type, group_id, source, message_type,
                has_media, has_location, whapi_message_id, ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'api_webhook_v2',%s,%s,%s,%s, NOW())
            RETURNING id
        """, (
            phone, name, content, json.dumps(msg_data), sent_at,
            mtype, group_uuid, 'unclassified',
            has_media, has_location, whapi_msg_id
        ))
        row = cur.fetchone()
        conn.commit()
        return str(row[0]) if row else None
    except Exception as e:
        print(f"[DB] save_raw_message error: {e}")
        conn.rollback()
        return None
    finally:
        cur.close(); conn.close()


# ── CARGAR MENSAJES DEL DIA ────────────────────────────────────────────────────
def load_day_messages(date_str):
    """
    Devuelve los mensajes del dia en formato Whapi (listo para procesar_mensaje).
    FIX timezone: filtra por rango CST completo, no por sent_at::date UTC.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT id, raw_json, sender_name
            FROM raw_messages
            WHERE sent_at >= (%s::date + interval '6 hours')
              AND sent_at <  (%s::date + interval '1 day' + interval '6 hours')
              AND whapi_type IN ('text', 'image', 'location', 'document')
            ORDER BY sent_at ASC
        """, (date_str, date_str))
        rows = cur.fetchall()
        msgs = []
        for row_id, raw_json, sender_name in rows:
            if not raw_json:
                continue
            msg = dict(raw_json) if isinstance(raw_json, dict) else json.loads(raw_json)
            if sender_name and not msg.get('from_name'):
                msg['from_name'] = sender_name
            # IMPORTANTE: preservamos el ID real de la base de datos (UUID)
            msg['raw_id'] = str(row_id)
            msgs.append(msg)
        return msgs
    except Exception as e:
        print(f"[DB] load_day_messages {date_str}: {e}")
        return []
    finally:
        cur.close(); conn.close()


# ── GUARDAR REPORTE DEL DRIVER ─────────────────────────────────────────────────
def save_driver_report(raw_message_id, data):
    """
    Persiste un reporte parseado del driver.
    data debe tener: id_bop, ruta, punto, estatus, observaciones, nombre
    Se llama UNA VEZ por BOP (el caller itera si hay multiples BOPs en el mensaje).
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        punto = data.get('punto', '?')
        cur.execute("""
            INSERT INTO driver_reports (
                raw_message_id, route_number, stop_number, idbop,
                status, observations, driver_name, reported_at, parsed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (raw_message_id, idbop) DO UPDATE SET
                status       = EXCLUDED.status,
                observations = EXCLUDED.observations,
                parsed_at    = NOW()
        """, (
            raw_message_id,
            str(data.get('ruta', '')),
            int(punto) if str(punto).isdigit() else None,
            str(data.get('id_bop', '')),
            data.get('estatus', ''),
            data.get('observaciones', ''),
            data.get('nombre', ''),
        ))
        conn.commit()
    except Exception as e:
        print(f"[DB] save_driver_report error: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()


# ── GUARDAR RESPUESTA DEL BO ───────────────────────────────────────────────────
def save_bo_closure(raw_message_id, data):
    """
    Persiste la respuesta/cierre del BO.
    data debe tener: id_bop, codigo_cierre, detalle, instrucciones
    Se llama UNA VEZ por BOP.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bo_closures (
                raw_message_id, idbop, status_code,
                comment_simpliroute, instruction_code, closed_at, parsed_at
            ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (raw_message_id, idbop) DO UPDATE SET
                status_code            = EXCLUDED.status_code,
                comment_simpliroute    = EXCLUDED.comment_simpliroute,
                instruction_code       = EXCLUDED.instruction_code,
                parsed_at              = NOW()
        """, (
            raw_message_id,
            str(data.get('id_bop', '')),
            data.get('codigo_cierre', ''),
            data.get('detalle', ''),
            data.get('instrucciones', ''),
        ))
        conn.commit()
    except Exception as e:
        print(f"[DB] save_bo_closure error: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()


# ── CARGAR RUTAS DESDE DB ─────────────────────────────────────────────────────
def load_routes_from_db(date_str):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT r.vehicle_label, rs.idbop
            FROM routes r
            JOIN route_stops rs ON r.id = rs.route_id
            WHERE r.operation_date = %s AND rs.idbop IS NOT NULL
            ORDER BY r.vehicle_label, rs.stop_number
        """, (date_str,))
        rows = cur.fetchall()
        if not rows:
            return {}, {}
        rutas = {}
        for vehicle, idbop in rows:
            bop = str(idbop).strip()
            if len(bop) == 7:
                rutas.setdefault(vehicle, [])
                if bop not in rutas[vehicle]:
                    rutas[vehicle].append(bop)
        bop_to_ruta = {b: r for r, bops in rutas.items() for b in bops}
        return rutas, bop_to_ruta
    except Exception as e:
        print(f"[DB] load_routes_from_db {date_str}: {e}")
        return {}, {}
    finally:
        cur.close(); conn.close()


# ── HILO CRONOLÓGICO POR BOP ──────────────────────────────────────────────────
BO_PHONES_SET = {'5215568660814', '5215528551646', '5215530313942', '5215580510043'}

def load_bop_thread(bop_id: str, date_str: str = None):
    """
    Devuelve todos los mensajes raw que mencionan un BOP específico,
    ordenados cronológicamente. Incluye texto, imágenes y ubicaciones
    del driver y del backoffice.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        if date_str:
            cur.execute("""
                SELECT
                    rm.id,
                    rm.sender_name,
                    rm.sender_phone,
                    rm.content,
                    rm.whapi_type,
                    rm.has_media,
                    rm.has_location,
                    rm.sent_at,
                    rm.raw_json
                FROM raw_messages rm
                WHERE rm.content ILIKE %s
                  AND rm.sent_at >= (%s::date + interval '6 hours')
                  AND rm.sent_at <  (%s::date + interval '1 day' + interval '6 hours')
                ORDER BY rm.sent_at ASC
            """, (f'%{bop_id}%', date_str, date_str))
        else:
            cur.execute("""
                SELECT
                    rm.id,
                    rm.sender_name,
                    rm.sender_phone,
                    rm.content,
                    rm.whapi_type,
                    rm.has_media,
                    rm.has_location,
                    rm.sent_at,
                    rm.raw_json
                FROM raw_messages rm
                WHERE rm.content ILIKE %s
                ORDER BY rm.sent_at ASC
                LIMIT 200
            """, (f'%{bop_id}%',))

        rows = cur.fetchall()
        thread = []

        for row in rows:
            raw_id, sender_name, sender_phone, content, wtype, has_media, has_loc, sent_at, raw_json = row

            msg = {}
            try:
                msg = json.loads(raw_json) if isinstance(raw_json, str) else dict(raw_json)
            except Exception:
                pass

            # Determinar si el mensaje es del BackOffice
            phone_str = str(sender_phone or '')
            is_bo = phone_str in BO_PHONES_SET
            if not is_bo and msg:
                text_body = (msg.get('text') or {}).get('body', '') or content or ''
                is_bo = bool(re.search(r'IdBop|🧾', text_body or '', re.I))

            # Hora en México
            hora_mx = ''
            if sent_at:
                sent_aware = sent_at if sent_at.tzinfo else sent_at.replace(tzinfo=timezone.utc)
                hora_mx = sent_aware.astimezone(MEXICO_TZ).strftime('%H:%M')

            item = {
                'id':           str(raw_id),
                'sender_name':  sender_name or '',
                'sender_phone': phone_str,
                'type':         wtype or '',
                'sent_at':      sent_at.isoformat() if sent_at else '',
                'hora':         hora_mx,
                'is_bo':        is_bo,
                'content':      content or '',
                'media':        None,
                'location':     None,
            }

            # Extraer media del raw_json
            if wtype == 'image' and msg:
                img = msg.get('image') or {}
                caption = img.get('caption', '') or content or ''
                item['media'] = {
                    'type':    'image',
                    'id':      img.get('id', ''),
                    'preview': img.get('preview', ''),
                    'caption': caption,
                }
                if item['media']['preview'] and len(item['media']['preview']) > 70000:
                    item['media']['preview'] = ''

            elif wtype == 'location' and msg:
                loc = msg.get('location') or {}
                lat = loc.get('latitude') or loc.get('lat')
                lon = loc.get('longitude') or loc.get('lng')
                if lat and lon:
                    item['location'] = {
                        'lat': float(lat),
                        'lon': float(lon),
                        'url': f'https://maps.google.com/?q={lat},{lon}',
                    }

            thread.append(item)

        return thread

    except Exception as e:
        print(f"[DB] load_bop_thread {bop_id}: {e}")
        return []
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

def get_last_bops_for_phone(phone: str, date_str: str) -> list:
    """Retorna los IDs de los últimos BOPs reportados por un teléfono hoy."""
    conn = None
    cur  = None
    try:
        conn = get_connection()
        cur  = conn.cursor()
        query_id = """
            SELECT r.raw_message_id
            FROM driver_reports r
            JOIN raw_messages m ON r.raw_message_id = m.id
            WHERE m.sender_phone = %s 
              AND TO_CHAR(m.sent_at AT TIME ZONE 'UTC' AT TIME ZONE 'CST', 'YYYY-MM-DD') = %s
            ORDER BY m.sent_at DESC
            LIMIT 1
        """
        cur.execute(query_id, (phone, date_str))
        row = cur.fetchone()
        if not row: return []
        
        query_bops = "SELECT idbop FROM driver_reports WHERE raw_message_id = %s"
        cur.execute(query_bops, (row[0],))
        return [str(r[0]) for r in cur.fetchall()]
    except Exception as e:
        print(f"[DB] get_last_bops_for_phone {phone}: {e}")
        return []
    finally:
        if cur: cur.close()
        if conn: conn.close()