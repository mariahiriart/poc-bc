"""
procesar_whatsapp.py — Genera dashboard_YYYY_MM_DD/data.js leyendo desde PostgreSQL.

Uso:
    python procesar_whatsapp.py [YYYY-MM-DD]
    python procesar_whatsapp.py 2026-03-19 2026-03-20   # múltiples fechas

Si no se pasa fecha, usa el día de hoy (hora México).
Rutas: intenta DB (routes/route_stops) → fallback a xlsx local → fallback sin rutas.

NOTA: Este script comparte toda la lógica de parseo con main_api.py
      a través del módulo bop_parser.py. Cualquier cambio en las reglas
      de parseo solo se hace en bop_parser.py.
"""
import json, sys, os, shutil
import openpyxl
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2

# Importar lógica de parseo compartida
from bop_parser import (
    BO_PHONES, MEXICO_TZ,
    fmt_hour, mexico_now, ts_to_seconds,
    extract_bop, is_bo_fmt, is_driver_msg,
    parse_driver, parse_bo, is_exitoso,
    get_media_for_bop, MEDIA_WINDOW_S,
)

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv()

# ── DB CONFIG ──────────────────────────────────────────────────────────────────
DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

XLSX_MAP = {
    '2026-03-18': 'rutas_18_mzo.xlsx',
    '2026-03-19': 'rutas_19_mzo.xlsx',
    '2026-03-20': 'rutas_20_mzo.xlsx',
    '2026-03-21': 'rutas_21_mzo.xlsx',
    '2026-03-23': 'rutas_23_mzo.xlsx',
    '2026-03-24': 'rutas_24_mzo.xlsx',
}


# ── CARGAR RUTAS ───────────────────────────────────────────────────────────────
def load_rutas_from_db(date_str):
    try:
        conn = psycopg2.connect(**DB_CFG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT r.vehicle_label, rs.idbop
            FROM routes r
            JOIN route_stops rs ON r.id = rs.route_id
            WHERE r.operation_date = %s AND rs.idbop IS NOT NULL
            ORDER BY r.vehicle_label, rs.stop_number
        """, (date_str,))
        rows = cur.fetchall()
        cur.close(); conn.close()
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
        print(f'[DB] load_rutas_from_db {date_str}: {e}')
        return {}, {}


def load_rutas_from_xlsx(date_str):
    fname = XLSX_MAP.get(date_str)
    if not fname:
        return {}, {}
    path = os.path.join(BASE_DIR, fname)
    if not os.path.exists(path):
        return {}, {}
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    rutas = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        vehiculo = row[1]
        bop      = str(row[5]).strip() if row[5] else None
        if not vehiculo or not bop or len(bop) != 7:
            continue
        ruta = vehiculo.strip()
        rutas.setdefault(ruta, [])
        if bop not in rutas[ruta]:
            rutas[ruta].append(bop)
    bop_to_ruta = {b: r for r, bops in rutas.items() for b in bops}
    return rutas, bop_to_ruta


def load_rutas(date_str):
    rutas, b2r = load_rutas_from_db(date_str)
    if rutas:
        total = sum(len(v) for v in rutas.values())
        print(f'  Rutas desde DB: {len(rutas)} rutas, {total} BOPs')
        return rutas, b2r
    rutas, b2r = load_rutas_from_xlsx(date_str)
    if rutas:
        total = sum(len(v) for v in rutas.values())
        print(f'  Rutas desde xlsx: {len(rutas)} rutas, {total} BOPs')
        return rutas, b2r
    print(f'  Sin rutas para {date_str} — solo se listará lo reportado.')
    return {}, {}


# ── QUERY MENSAJES DESDE DB ────────────────────────────────────────────────────
def fetch_messages(date_str):
    """
    Carga mensajes del día en hora México (CST = UTC-6).
    Usa rango horario completo para no perder mensajes del tramo 18:00–23:59 CST
    que en UTC ya son el día siguiente.
    """
    conn = psycopg2.connect(**DB_CFG)
    cur  = conn.cursor()
    cur.execute("""
        SELECT sender_phone, sender_name, whapi_type, sent_at, content, raw_json
        FROM raw_messages
        WHERE sent_at >= (%s::date + interval '6 hours')
          AND sent_at <  (%s::date + interval '1 day' + interval '6 hours')
          AND whapi_type IN ('text', 'image', 'video', 'location')
        ORDER BY sent_at ASC
    """, (date_str, date_str))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


# ── PROCESAR UN DÍA ────────────────────────────────────────────────────────────
def procesar_dia(date_str):
    print(f'\n=== {date_str} ===')
    rows = fetch_messages(date_str)
    print(f'  Mensajes en DB: {len(rows)}')

    rutas_csv, bop_to_ruta = load_rutas(date_str)

    bop_reports    = {}
    bo_responses   = {}
    media_by_phone = {}   # phone → [(ts, media_item)]

    for phone, sender_name, wtype, sent_at, content, raw_json in rows:
        if not phone:
            continue

        raw    = raw_json if isinstance(raw_json, dict) else (json.loads(raw_json) if raw_json else {})
        nombre = raw.get('from_name') or sender_name or str(phone)
        hora   = fmt_hour(sent_at)

        # Ubicación
        if wtype == 'location':
            loc = raw.get('location', {})
            lat = loc.get('latitude') or loc.get('lat')
            lon = loc.get('longitude') or loc.get('lng')
            if lat and lon:
                lat, lon = float(lat), float(lon)
                item = {
                    'type': 'location',
                    'preview': loc.get('preview', ''),
                    'url': f'https://maps.google.com/?q={lat},{lon}',
                    'lat': lat, 'lon': lon,
                }
                media_by_phone.setdefault(str(phone), []).append((sent_at, item))
            continue

        # Imagen
        if wtype == 'image':
            text    = (raw.get('image') or {}).get('caption', '') or content or ''
            preview = (raw.get('image') or {}).get('preview', '')
            item = {
                'type':    'image',
                'preview': preview,
                'id':      (raw.get('image') or {}).get('id', ''),
                'caption': text,
            }
            media_by_phone.setdefault(str(phone), []).append((sent_at, item))
            if not text:
                continue
        # Video con caption (fix: antes se ignoraban y el reporte se perdía)
        elif wtype == 'video':
            text = (raw.get('video') or {}).get('caption', '') or content or ''
            if not text:
                continue
            item_vid = {
                'type': 'video',
                'id': (raw.get('video') or {}).get('id', ''),
                'caption': text,
            }
            media_by_phone.setdefault(str(phone), []).append((sent_at, item_vid))
        elif wtype == 'text':
            text = (raw.get('text') or {}).get('body', '') or content or ''
            if not text:
                continue
        else:
            continue

        phone_str = str(phone)

        # BO
        if phone_str in BO_PHONES or is_bo_fmt(text):
            r = parse_bo(text)
            if r:
                for bop in r['bops']:
                    if bop not in bo_responses:
                        bo_responses[bop] = {
                            'bo_status': r['bo_status'], 'bo_obs': r['bo_obs'],
                            'msgs': [], 'hora': hora,
                        }
                    else:
                        bo_responses[bop]['bo_status'] = r['bo_status']
                        bo_responses[bop]['bo_obs']     = r['bo_obs']
                        bo_responses[bop]['hora']        = hora
                    bo_responses[bop]['msgs'].append(f'{hora} {nombre}: {text[:80]}')
            continue

        # Driver
        if not is_driver_msg(text):
            continue

        r = parse_driver(text)
        if not r:
            continue

        for bop in r['bops']:
            ruta_real = bop_to_ruta.get(bop) or r['ruta'] or '?'
            if bop not in bop_reports:
                bop_reports[bop] = {
                    'phone': phone_str, 'nombre': nombre, 'ruta': ruta_real,
                    'punto': r['punto'], 'status': r['status'], 'obs': r['obs'],
                    'ts': sent_at, 'msgs': [], 'imgs': 0,
                }
            else:
                bop_reports[bop]['status'] = r['status']
                bop_reports[bop]['obs']    = r['obs']
                bop_reports[bop]['ts']     = sent_at
            bop_reports[bop]['msgs'].append(f'{hora} {nombre}: {text[:100]}')
            if wtype == 'image':
                bop_reports[bop]['imgs'] += 1

    # Construir detalle
    detalle  = []
    all_bops = set()
    tiene_rutas = bool(bop_to_ruta)

    for bop, rep in bop_reports.items():
        if tiene_rutas and bop not in bop_to_ruta:
            continue
        bo      = bo_responses.get(bop, {})
        exitoso = is_exitoso(rep['status'])
        media   = get_media_for_bop(rep['phone'], rep['ts'], media_by_phone)
        detalle.append({
            'bop': bop, 'driver': rep['nombre'], 'ruta': rep['ruta'],
            'status_final': 'Exito' if exitoso else 'Fallido / Incidencia',
            'evidencias': rep['imgs'], 'media': media,
            'driver_status': rep['status'] or 'sin estatus',
            'driver_obs': rep['obs'],
            'bo_status': bo.get('bo_status', 'N/A'),
            'bo_obs': bo.get('bo_obs', ''),
            'raw_drv_msgs': rep['msgs'],
            'raw_bo_msgs': bo.get('msgs', []),
            'ultima_hora': fmt_hour(rep['ts']) if rep['ts'] else '',
        })
        all_bops.add(bop)

    for bop, bo in bo_responses.items():
        if bop in all_bops:
            continue
        if tiene_rutas and bop not in bop_to_ruta:
            continue
        detalle.append({
            'bop': bop, 'driver': 'Solo BO',
            'ruta': bop_to_ruta.get(bop, '?'),
            'status_final': 'Fallido / Incidencia', 'evidencias': 0, 'media': [],
            'driver_status': 'N/A', 'driver_obs': '',
            'bo_status': bo.get('bo_status', 'N/A'), 'bo_obs': bo.get('bo_obs', ''),
            'raw_drv_msgs': [], 'raw_bo_msgs': bo.get('msgs', []),
            'ultima_hora': bo.get('hora', ''),
        })
        all_bops.add(bop)

    sin_reporte_vistos = set()
    for ruta, bops_asig in rutas_csv.items():
        for bop in bops_asig:
            if bop not in all_bops and bop not in sin_reporte_vistos:
                detalle.append({
                    'bop': bop, 'driver': '—', 'ruta': ruta,
                    'status_final': 'Sin Reporte', 'evidencias': 0, 'media': [],
                    'driver_status': 'Sin reporte', 'driver_obs': '',
                    'bo_status': 'N/A', 'bo_obs': '',
                    'raw_drv_msgs': [], 'raw_bo_msgs': [], 'ultima_hora': '—',
                })
                sin_reporte_vistos.add(bop)

    detalle.sort(key=lambda x: (x['ruta'], x['bop']))
    total    = len(detalle)
    exitosos = sum(1 for d in detalle if d['status_final'] == 'Exito')
    con_bo   = sum(1 for d in detalle if d['bo_status'] != 'N/A')

    rutas_out = []
    for ruta, bops_asig in sorted(
        rutas_csv.items(),
        key=lambda x: int(x[0].split()[-1]) if x[0].split()[-1].isdigit() else 0
    ):
        reportados = [b for b in bops_asig if b in all_bops]
        faltantes  = [b for b in bops_asig if b not in all_bops]
        driver = next((d['driver'] for d in detalle if d['ruta'] == ruta
                       and d['driver'] not in ('Solo BO', '—')), '?')
        rutas_out.append({
            'ruta': ruta, 'driver': driver,
            'total_asignado':  len(bops_asig),
            'total_reportado': len(reportados),
            'total_faltante':  len(faltantes),
            'faltantes_bops':  faltantes,
        })

    output = {
        'generado_at': mexico_now().strftime('%Y-%m-%d %H:%M:%S'),
        'kpis': {
            'total_asignados':   sum(r['total_asignado']  for r in rutas_out) or total,
            'total_reportados':  sum(r['total_reportado'] for r in rutas_out),
            'total_sin_reporte': sum(r['total_faltante']  for r in rutas_out),
            'total_bops':  total,
            'exitosos':    exitosos,
            'fallidos':    total - exitosos,
            'con_bo':      con_bo,
            'pct_exito':   round(exitosos / total * 100, 1) if total else 0,
        },
        'rutas': rutas_out,
        'detalle_reportados': detalle,
    }

    # Guardar data.js
    out_dir  = os.path.join(BASE_DIR, f'dashboard_{date_str.replace("-", "_")}')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'data.js')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('const dashboardData = ' +
                json.dumps(output, ensure_ascii=False, indent=2, default=str) + ';\n')

    # Copiar index.html de referencia si no existe
    html_src = os.path.join(BASE_DIR, 'dashboard_2026_03_23', 'index.html')
    html_dst = os.path.join(out_dir, 'index.html')
    if not os.path.exists(html_dst) and os.path.exists(html_src):
        shutil.copy2(html_src, html_dst)

    print(f'  BOPs driver={len(bop_reports)} BO={len(bo_responses)} total={total}')
    print(f'  Exitosos={exitosos} Fallidos={total-exitosos} BO={con_bo} %={output["kpis"]["pct_exito"]}%')
    print(f'  Guardado: {out_path}')
    for r in rutas_out:
        bar = '#' * r['total_reportado'] + '.' * r['total_faltante']
        print(f'  {r["ruta"]:8s} [{bar:17s}] {r["total_reportado"]:2d}/{r["total_asignado"]:2d} | {r["driver"]}')

    return output


# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    hoy_mx = mexico_now().strftime('%Y-%m-%d')
    fechas = sys.argv[1:] if len(sys.argv) > 1 else [hoy_mx]
    for fecha in fechas:
        procesar_dia(fecha)
        