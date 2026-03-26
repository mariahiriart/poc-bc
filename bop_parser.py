"""
bop_parser.py — Fuente única de verdad para toda la lógica de parseo de mensajes BOP.

Importar desde main_api.py, procesar_whatsapp.py y procesar_whapi_json.py.
Así cualquier fix o mejora aplica a los tres automáticamente.
"""
from __future__ import annotations
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

# ── CONSTANTES ─────────────────────────────────────────────────────────────────
BO_PHONES = {'5215568660814', '5215528551646', '5215530313942', '5215580510043'}

# Offset fijo CST (UTC-6). Usado solo como fallback cuando no hay tzinfo.
# Para determinar "hoy en México" usar mexico_now() que maneja verano correctamente.
MEXICO_OFFSET_S = -6 * 3600

# Zona horaria México CST (UTC-6). En temporada de verano México usa UTC-5,
# pero las operaciones de esta empresa son estables en CST.
MEXICO_TZ = timezone(timedelta(hours=-6))


# ── HORA MÉXICO ────────────────────────────────────────────────────────────────
def fmt_hour(ts) -> str:
    """
    Convierte un timestamp (int Unix o datetime) a '[HH:MM]' en hora México.
    Maneja correctamente tanto ints (desde Whapi) como datetimes aware/naive (desde DB).
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            # Naive datetime: asumir que viene de PostgreSQL en UTC
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(MEXICO_TZ).strftime('[%H:%M]')
    # Unix timestamp int/float
    return datetime.fromtimestamp(ts, tz=MEXICO_TZ).strftime('[%H:%M]')


def mexico_now() -> datetime:
    """Devuelve datetime actual en zona horaria México (CST = UTC-6)."""
    return datetime.now(tz=MEXICO_TZ)


def ts_to_seconds(ts) -> float:
    """Convierte datetime o int Unix a segundos Unix float (para comparaciones de ventana)."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    return float(ts)


# ── EXTRACCIÓN DE BOP IDs ───────────────────────────────────────────────────────
def extract_bop(text: str) -> list:
    """
    Extrae todos los IDs BOP (7 dígitos) de un texto.

    Estrategia:
    1. Busca el patrón etiquetado 'ID BOP / IdBop / ID Bop' y extrae todos los
       números de 7 dígitos que lo siguen (maneja múltiples BOPs separados por /, ,, y).
    2. Si no hay etiqueta pero el texto contiene 'estatus' o 'status',
       busca cualquier número de 7 dígitos (contexto de reporte de driver o BO).
    3. Si no aplica ninguno, devuelve [].

    El guard en el paso 2 evita falsos positivos en mensajes que solo tienen
    un número de 7 dígitos (ej. teléfonos, folios, etc.).
    """
    if not text:
        return []

    # Paso 1 — patrón etiquetado explícito
    labeled = re.search(
        r'(?:ID\s*BO[BP]|IdBop|ID\s*Bop)[:\s\*\[#\s]*([\d/,\s\-y]+)',
        text, re.I
    )
    if labeled:
        bops = re.findall(r'\d{7}', labeled.group(1))
        if bops:
            return bops

    # Paso 2 — sin etiqueta pero con contexto de reporte
    if re.search(r'estatus|status|IdBop|🧾', text, re.I):
        return re.findall(r'\b(\d{7})\b', text)

    return []


# ── DETECCIÓN DE FORMATO ────────────────────────────────────────────────────────
def is_bo_fmt(text: str) -> bool:
    """True si el texto tiene el formato de respuesta del BackOffice."""
    return bool(re.search(r'IdBop|🧾', text or '', re.I))


def is_driver_msg(text: str) -> bool:
    """
    True si el texto parece un reporte de driver.
    Requiere mención de 'ID BOP/BOB/Bop' Y 'estatus/status'.
    El [BP] cubre typos frecuentes (BOB en lugar de BOP).
    """
    return bool(
        re.search(r'ID\s*BO[BP]|ID\s*Bop', text, re.I) and
        re.search(r'estatus|status', text, re.I)
    )


# ── PARSEO DE MENSAJES ──────────────────────────────────────────────────────────
def parse_driver(text: str) -> Optional[dict]:
    """
    Parsea un mensaje de driver y devuelve un dict con:
      bops, punto, status, obs, ruta
    Devuelve None si no se puede parsear.
    """
    if not text:
        return None
    bops = extract_bop(text)
    if not bops:
        return None

    tc = re.sub(r'\s+', ' ', text.strip())

    # Estatus (todo lo que está en la línea, antes de "observaciones")
    m = re.search(r'(?:estatus|status)[:\s]*([^\n]+)', tc, re.I)
    status = ''
    if m:
        status = m.group(1).strip()
        status = re.split(r'observaciones?|obs\.?', status, flags=re.I)[0].strip().rstrip('|').strip()

    # Observaciones
    m2 = re.search(r'(?:observaciones?|obs\.?)[:\s]*(.+)', tc, re.I | re.DOTALL)
    obs = m2.group(1).strip() if m2 else ''

    # Punto y ruta
    m3 = re.search(r'(?:punto)[:\s]*(\w+)', tc, re.I)
    m4 = re.search(r'(?:ruta)[:\s]*(\w+)', tc, re.I)

    return {
        'bops':   bops,
        'punto':  m3.group(1) if m3 else '?',
        'status': status,
        'obs':    obs,
        'ruta':   f'RUTA {m4.group(1)}' if m4 else None,
    }


def parse_bo(text: str) -> dict | None:
    """
    Parsea un mensaje de BackOffice y devuelve:
      bops, bo_status, bo_obs
    Devuelve None si no se puede parsear.
    """
    if not text:
        return None
    bops = extract_bop(text)
    if not bops:
        return None

    m = re.search(r'Estatus[:\*\s]+\*?([^\n\*]+)\*?', text, re.I)
    bo_status = m.group(1).strip().strip('*').strip() if m else 'N/A'

    m2 = re.search(
        r'(?:Motivo|Comentario)[:\s]*\n(.+?)(?:\n\n|Instrucciones|$)',
        text, re.I | re.DOTALL
    )
    bo_obs = re.sub(r'\*', '', m2.group(1)).strip() if m2 else ''

    return {'bops': bops, 'bo_status': bo_status, 'bo_obs': bo_obs}


def is_exitoso(s: str) -> bool:
    """
    True si el estatus indica entrega exitosa.
    Cubre: exitoso, éxito (con tilde), entregado, entrega, exito, exit, ok.
    Comparación case-insensitive y sin importar tildes parciales.
    """
    if not s:
        return False
    sl = s.lower()
    # Normalizar tilde común
    sl_norm = sl.replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('á', 'a')
    return any(x in sl_norm for x in ['exit', 'entregado', 'entrega', 'exito', 'ok'])


# ── ASOCIACIÓN DE MEDIA A BOPs ──────────────────────────────────────────────────
MEDIA_WINDOW_S = 45 * 60  # 45 minutos en segundos

def get_media_for_bop(phone: str, ts_ref, media_by_phone: dict,
                       window_s: int = MEDIA_WINDOW_S) -> list:
    """
    Devuelve todos los items multimedia del driver en una ventana de ±window_s segundos
    alrededor del timestamp del reporte.

    Parámetros:
        phone:          teléfono del driver
        ts_ref:         timestamp del reporte (int Unix o datetime)
        media_by_phone: dict phone → [(ts, item)]  (ts puede ser int o datetime)
        window_s:       ventana en SEGUNDOS (default: 45 min = 2700 s)
    """
    ref_s = ts_to_seconds(ts_ref)
    items, seen = [], set()
    for mt, item in media_by_phone.get(phone, []):
        mt_s = ts_to_seconds(mt)
        if abs(mt_s - ref_s) <= window_s:
            key = item.get('url') or item.get('id') or str(mt_s)
            if key not in seen:
                seen.add(key)
                items.append(item)
    return items