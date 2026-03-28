import psycopg2
import psycopg2.extras
import json
import os
import re
import io
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_CFG = dict(
    host=os.environ.get("DB_HOST", ""),
    port=int(os.environ.get("DB_PORT", 5432)),
    database=os.environ.get("DB_NAME", "neondb"),
    user=os.environ.get("DB_USER", ""),
    password=os.environ.get("DB_PASSWORD", ""),
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

MEXICO_TZ = timezone(timedelta(hours=-6))


def get_connection():
    return psycopg2.connect(**DB_CFG)


# ── MIGRACIONES AL ARRANCAR ───────────────────────────────────────────────────
def ensure_tables():
    """
    Ajusta el schema existente sin romper nada:
      1. Agrega columna `content BYTEA` a route_files para guardar el xlsx binario.
      2. Crea tabla driver_assignments (nueva) para persistir mapeo ruta->nombre.
    No toca routes, route_stops, raw_messages ni ninguna tabla existente.
    """
    ddl = """
    ALTER TABLE route_files ADD COLUMN IF NOT EXISTS content BYTEA;
    CREATE TABLE IF NOT EXISTS driver_assignments (
        id          SERIAL PRIMARY KEY,
        fecha       DATE        NOT NULL,
        assignments JSONB       NOT NULL,
        updated_at  TIMESTAMPTZ DEFAULT NOW(),
        CONSTRAINT driver_assignments_fecha_unique UNIQUE (fecha)
    );
    """
    conn = None
    cur  = None
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        print("[DB] Schema verificado: route_files.content + driver_assignments OK.", flush=True)
    except Exception as e:
        print(f"[DB] ensure_tables error: {e}", flush=True)
        if conn:
            try: conn.rollback()
            except: pass
    finally:
        if cur:  cur.close()
        if conn: conn.close()


# ── GUARDAR MENSAJE RAW ────────────────────────────────────────────────────────
def save_raw_message(msg_data):
    chat_id    = msg_data.get("chat_id", "")
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

    ts      = msg_data.get("timestamp")
    sent_at = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(tz=timezone.utc)
    mtype   = msg_data.get("type", "")
    phone   = msg_data.get("from", "")
    name    = msg_data.get("from_name", "")

    if mtype == "text":
        content = (msg_data.get("text") or {}).get("body", "")
    elif mtype == "image":
        content = (msg_data.get("image") or {}).get("caption", "")
    elif mtype == "video":
        content = (msg_data.get("video") or {}).get("caption", "")
    elif mtype == "document":
        content = (msg_data.get("document") or {}).get("filename", "")
    else:
        content = ""

    has_media    = mtype in ("image", "video", "audio", "document", "sticker")
    has_location = mtype == "location"
    whapi_msg_id = msg_data.get("id", "")

    conn = get_connection()
    cur  = conn.cursor()
    try:
        if whapi_msg_id:
            cur.execute("SELECT id FROM raw_messages WHERE whapi_message_id = %s LIMIT 1", (whapi_msg_id,))
            if cur.fetchone():
                return None
        cur.execute("""
            INSERT INTO raw_messages (
                sender_phone, sender_name, content, raw_json, sent_at,
                whapi_type, group_id, source, message_type,
                has_media, has_location, whapi_message_id, ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'api_webhook_v2',%s,%s,%s,%s, NOW())
            RETURNING id
        """, (
            phone, name, content, json.dumps(msg_data), sent_at,
            mtype, group_uuid, "unclassified",
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
    """Incluye video ademas de text/image/location/document."""
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT id, raw_json, sender_name
            FROM raw_messages
            WHERE sent_at >= (%s::date + interval '6 hours')
              AND sent_at <  (%s::date + interval '1 day' + interval '6 hours')
              AND whapi_type IN ('text', 'image', 'video', 'location', 'document')
            ORDER BY sent_at ASC
        """, (date_str, date_str))
        rows = cur.fetchall()
        msgs = []
        for row_id, raw_json, sender_name in rows:
            if not raw_json:
                continue
            msg = dict(raw_json) if isinstance(raw_json, dict) else json.loads(raw_json)
            if sender_name and not msg.get("from_name"):
                msg["from_name"] = sender_name
            msg["raw_id"] = str(row_id)
            msgs.append(msg)
        return msgs
    except Exception as e:
        print(f"[DB] load_day_messages {date_str}: {e}")
        return []
    finally:
        cur.close(); conn.close()


# ── GUARDAR REPORTE DEL DRIVER ─────────────────────────────────────────────────
def save_driver_report(raw_message_id, data):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        punto = data.get("punto", "?")
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
            str(data.get("ruta", "")),
            int(punto) if str(punto).isdigit() else None,
            str(data.get("id_bop", "")),
            data.get("estatus", ""),
            data.get("observaciones", ""),
            data.get("nombre", ""),
        ))
        conn.commit()
    except Exception as e:
        print(f"[DB] save_driver_report error: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()


# ── GUARDAR RESPUESTA DEL BO ───────────────────────────────────────────────────
def save_bo_closure(raw_message_id, data):
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
            str(data.get("id_bop", "")),
            data.get("codigo_cierre", ""),
            data.get("detalle", ""),
            data.get("instrucciones", ""),
        ))
        conn.commit()
    except Exception as e:
        print(f"[DB] save_bo_closure error: {e}")
        conn.rollback()
    finally:
        cur.close(); conn.close()


# ── XLSX EN POSTGRES (via route_files existente) ──────────────────────────────
def save_route_file(fecha_str, filename, content_bytes, raw_message_id=None):
    """
    Guarda el xlsx en route_files (tabla ya existente en el schema).
    Usa ON CONFLICT en operation_date para upsert.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO route_files (
                filename, operation_date, received_at, processed,
                raw_message_id, content
            )
            VALUES (%s, %s::date, NOW(), false, %s, %s)
            ON CONFLICT (operation_date) DO UPDATE SET
                filename       = EXCLUDED.filename,
                content        = EXCLUDED.content,
                received_at    = NOW(),
                processed      = false,
                raw_message_id = COALESCE(EXCLUDED.raw_message_id, route_files.raw_message_id)
        """, (filename, fecha_str, raw_message_id, psycopg2.Binary(content_bytes)))
        conn.commit()
        print(f"[DB] route_files: {filename} guardado para {fecha_str} ({len(content_bytes)} bytes)", flush=True)
    except Exception as e:
        print(f"[DB] save_route_file error: {e}", flush=True)
        conn.rollback()
    finally:
        cur.close(); conn.close()


def load_route_file_bytes(fecha_str):
    """Recupera el xlsx binario desde route_files. Retorna (filename, bytes) o None."""
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT filename, content FROM route_files
            WHERE operation_date = %s::date AND content IS NOT NULL
        """, (fecha_str,))
        row = cur.fetchone()
        if not row:
            return None
        filename, content = row
        raw = bytes(content) if isinstance(content, memoryview) else content
        print(f"[DB] route_files: recuperado {filename} ({len(raw)} bytes) para {fecha_str}", flush=True)
        return filename, raw
    except Exception as e:
        print(f"[DB] load_route_file_bytes error: {e}", flush=True)
        return None
    finally:
        cur.close(); conn.close()


# ── IMPORTAR RUTAS A routes + route_stops ────────────────────────────────────
def import_routes_from_xlsx(fecha_str, xlsx_bytes, filename):
    """
    Parsea el xlsx y popula routes + route_stops para que load_routes_from_db
    devuelva datos reales (antes estas tablas quedaban vacias).

    Columnas xlsx (0-indexed):
      [0] Conductor  [1] Vehiculo (RUTA X)  [2] Parada  [3] Titulo
      [4] Direccion  [5] Id referencia (idbop) [6] Ventana [7] Notas [8] Folio
    """
    import openpyxl
    try:
        wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
        ws = wb.active
    except Exception as e:
        print(f"[DB] import_routes_from_xlsx: error abriendo xlsx: {e}", flush=True)
        return

    # Agrupar por ruta
    rutas_data = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or len(row) < 6:
            continue
        vehicle = str(row[1]).strip() if row[1] is not None else ""
        idbop   = str(row[5]).strip() if row[5] is not None else ""
        if not vehicle or not idbop or len(idbop) < 4:
            continue
        if vehicle not in rutas_data:
            rutas_data[vehicle] = []
        rutas_data[vehicle].append(row)

    if not rutas_data:
        print(f"[DB] import_routes_from_xlsx: sin datos validos en {filename}", flush=True)
        return

    conn = get_connection()
    cur  = conn.cursor()
    try:
        routes_ok = 0
        stops_ok  = 0

        for vehicle_label, rows in rutas_data.items():
            # Upsert route
            cur.execute("""
                INSERT INTO routes (id, operation_date, route_number, vehicle_label, source_file, imported_at, total_stops)
                VALUES (gen_random_uuid(), %s::date, %s, %s, %s, NOW(), 0)
                ON CONFLICT (operation_date, route_number) DO UPDATE SET
                    vehicle_label = EXCLUDED.vehicle_label,
                    source_file   = EXCLUDED.source_file,
                    imported_at   = NOW()
                RETURNING id
            """, (fecha_str, vehicle_label, vehicle_label, filename))
            row_r = cur.fetchone()
            if not row_r:
                cur.execute("SELECT id FROM routes WHERE operation_date=%s::date AND route_number=%s",
                            (fecha_str, vehicle_label))
                row_r = cur.fetchone()
            route_id = str(row_r[0])
            routes_ok += 1

            # Upsert stops
            for row in rows:
                stop_num = row[2] if isinstance(row[2], int) else None
                title    = str(row[3]).strip() if row[3] is not None else None
                address  = str(row[4]).strip() if row[4] is not None else None
                idbop    = str(row[5]).strip() if row[5] is not None else None
                time_win = str(row[6]).strip() if len(row) > 6 and row[6] is not None else None
                notes    = str(row[7]).strip() if len(row) > 7 and row[7] is not None else None
                folio    = str(row[8]).strip() if len(row) > 8 and row[8] is not None else None

                carrier = None
                if title:
                    if "Telefonico" in title or "Telcel" in title:
                        carrier = "Telcel"
                    elif "Movistar" in title:
                        carrier = "Movistar"

                if stop_num is None:
                    cur.execute("SELECT COALESCE(MAX(stop_number),0)+1 FROM route_stops WHERE route_id=%s", (route_id,))
                    stop_num = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO route_stops (id, route_id, stop_number, idbop, title, address, folio, time_window, carrier, notes)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (route_id, stop_number) DO UPDATE SET
                        idbop      = EXCLUDED.idbop,
                        title      = EXCLUDED.title,
                        address    = EXCLUDED.address,
                        folio      = EXCLUDED.folio,
                        time_window= EXCLUDED.time_window,
                        carrier    = EXCLUDED.carrier
                """, (route_id, stop_num, idbop, title, address, folio, time_win, carrier, notes))
                stops_ok += 1

        # Marcar como procesado
        cur.execute("""
            UPDATE route_files SET processed=true, processed_at=NOW(),
                total_routes=%s, total_stops=%s
            WHERE operation_date=%s::date
        """, (routes_ok, stops_ok, fecha_str))

        conn.commit()
        print(f"[DB] import_routes OK: {routes_ok} rutas, {stops_ok} paradas para {fecha_str}", flush=True)

    except Exception as e:
        print(f"[DB] import_routes_from_xlsx error: {e}", flush=True)
        conn.rollback()
    finally:
        cur.close(); conn.close()


# ── CARGAR RUTAS DESDE DB ─────────────────────────────────────────────────────
def load_routes_from_db(date_str):
    """
    Ahora funciona porque import_routes_from_xlsx() popula routes + route_stops.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT r.vehicle_label, rs.idbop
            FROM routes r
            JOIN route_stops rs ON r.id = rs.route_id
            WHERE r.operation_date = %s::date AND rs.idbop IS NOT NULL
            ORDER BY r.vehicle_label, rs.stop_number
        """, (date_str,))
        rows = cur.fetchall()
        if not rows:
            return {}, {}
        rutas = {}
        for vehicle, idbop in rows:
            bop = str(idbop).strip()
            if len(bop) >= 4:
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


# ── DRIVER ASSIGNMENTS ────────────────────────────────────────────────────────
def save_driver_assignments(fecha_str, assignments):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO driver_assignments (fecha, assignments, updated_at)
            VALUES (%s::date, %s, NOW())
            ON CONFLICT (fecha) DO UPDATE SET assignments=EXCLUDED.assignments, updated_at=NOW()
        """, (fecha_str, json.dumps(assignments, ensure_ascii=False)))
        conn.commit()
        print(f"[DB] driver_assignments: {len(assignments)} rutas para {fecha_str}", flush=True)
    except Exception as e:
        print(f"[DB] save_driver_assignments error: {e}", flush=True)
        conn.rollback()
    finally:
        cur.close(); conn.close()


def load_driver_assignments(fecha_str):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT assignments FROM driver_assignments WHERE fecha=%s::date", (fecha_str,))
        row = cur.fetchone()
        if not row:
            return None
        data = row[0]
        result = data if isinstance(data, dict) else json.loads(data)
        print(f"[DB] driver_assignments: {len(result)} rutas recuperadas para {fecha_str}", flush=True)
        return result
    except Exception as e:
        print(f"[DB] load_driver_assignments error: {e}", flush=True)
        return None
    finally:
        cur.close(); conn.close()


# ── HILO CRONOLOGICO POR BOP ──────────────────────────────────────────────────
BO_PHONES_SET = {"5215568660814", "5215528551646", "5215530313942", "5215580510043"}

def load_bop_thread(bop_id, date_str=None):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        if date_str:
            cur.execute("""
                SELECT rm.id, rm.sender_name, rm.sender_phone, rm.content,
                       rm.whapi_type, rm.has_media, rm.has_location, rm.sent_at, rm.raw_json
                FROM raw_messages rm
                WHERE rm.content ILIKE %s
                  AND rm.sent_at >= (%s::date + interval '6 hours')
                  AND rm.sent_at <  (%s::date + interval '1 day' + interval '6 hours')
                ORDER BY rm.sent_at ASC
            """, (f"%{bop_id}%", date_str, date_str))
        else:
            cur.execute("""
                SELECT rm.id, rm.sender_name, rm.sender_phone, rm.content,
                       rm.whapi_type, rm.has_media, rm.has_location, rm.sent_at, rm.raw_json
                FROM raw_messages rm WHERE rm.content ILIKE %s
                ORDER BY rm.sent_at ASC LIMIT 200
            """, (f"%{bop_id}%",))

        rows   = cur.fetchall()
        thread = []
        for row in rows:
            raw_id, sender_name, sender_phone, content, wtype, has_media, has_loc, sent_at, raw_json = row
            msg = {}
            try:
                msg = json.loads(raw_json) if isinstance(raw_json, str) else dict(raw_json)
            except Exception:
                pass

            phone_str = str(sender_phone or "")
            is_bo     = phone_str in BO_PHONES_SET
            if not is_bo and msg:
                text_body = (msg.get("text") or {}).get("body", "") or content or ""
                is_bo     = bool(re.search(r"IdBop|🧾", text_body or "", re.I))

            hora_mx = ""
            if sent_at:
                sent_aware = sent_at if sent_at.tzinfo else sent_at.replace(tzinfo=timezone.utc)
                hora_mx    = sent_aware.astimezone(MEXICO_TZ).strftime("%H:%M")

            item = {
                "id": str(raw_id), "sender_name": sender_name or "",
                "sender_phone": phone_str, "type": wtype or "",
                "sent_at": sent_at.isoformat() if sent_at else "",
                "hora": hora_mx, "is_bo": is_bo,
                "content": content or "", "media": None, "location": None,
            }

            if wtype == "image" and msg:
                img     = msg.get("image") or {}
                caption = img.get("caption", "") or content or ""
                item["media"] = {
                    "type": "image", "id": img.get("id", ""),
                    "preview": img.get("preview", ""), "caption": caption,
                }
                if item["media"]["preview"] and len(item["media"]["preview"]) > 70000:
                    item["media"]["preview"] = ""
            elif wtype == "location" and msg:
                loc = msg.get("location") or {}
                lat = loc.get("latitude") or loc.get("lat")
                lon = loc.get("longitude") or loc.get("lng")
                if lat and lon:
                    item["location"] = {
                        "lat": float(lat), "lon": float(lon),
                        "url": f"https://maps.google.com/?q={lat},{lon}",
                    }
            thread.append(item)
        return thread
    except Exception as e:
        print(f"[DB] load_bop_thread {bop_id}: {e}")
        return []
    finally:
        if "cur" in locals() and cur:  cur.close()
        if "conn" in locals() and conn: conn.close()


def get_last_bops_for_phone(phone, date_str):
    conn = None
    cur  = None
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT r.raw_message_id FROM driver_reports r
            JOIN raw_messages m ON r.raw_message_id = m.id
            WHERE m.sender_phone = %s
              AND TO_CHAR(m.sent_at AT TIME ZONE 'UTC' AT TIME ZONE 'CST', 'YYYY-MM-DD') = %s
            ORDER BY m.sent_at DESC LIMIT 1
        """, (phone, date_str))
        row = cur.fetchone()
        if not row: return []
        cur.execute("SELECT idbop FROM driver_reports WHERE raw_message_id=%s", (row[0],))
        return [str(r[0]) for r in cur.fetchall()]
    except Exception as e:
        print(f"[DB] get_last_bops_for_phone {phone}: {e}")
        return []
    finally:
        if cur:  cur.close()
        if conn: conn.close()