"""
fix_migrations.py — Migraciones de DB necesarias antes de encender el sistema.

Ejecutar UNA SOLA VEZ antes del deploy.
Cada migración verifica si ya existe antes de aplicarla — es seguro re-ejecutar.

Migraciones incluidas:
  1. UNIQUE constraint en raw_messages.whapi_message_id (ya tenías esto)
  2. UNIQUE constraint en driver_reports (raw_message_id, idbop)
  3. UNIQUE constraint en bo_closures (raw_message_id, idbop)
  4. Verificación del CHECK constraint de message_type
  5. Fix del CHECK constraint de message_type si los valores no coinciden
"""

import psycopg2

DB_CFG = dict(
    host="ep-royal-fog-amdxgnd5-pooler.c-5.us-east-1.aws.neon.tech",
    port=5432,
    database="neondb",
    user="neondb_owner",
    password="npg_MtVIvY13mjGw",
    sslmode="require"
)

# Valores de message_type que el código escribe realmente
# (deben coincidir con el CHECK constraint de la tabla)
VALID_MESSAGE_TYPES = (
    'driver_report',
    'bo_closure',
    'media',
    'location',
    'coordination',
    'system',
    'other',
)


def run_migration(cur, name: str, check_sql: str, apply_sql: str):
    """
    Ejecuta una migración solo si el check indica que no existe todavía.
    check_sql debe devolver al menos una fila si la migración YA fue aplicada.
    """
    cur.execute(check_sql)
    already_exists = cur.fetchone() is not None
    if already_exists:
        print(f"  ⏭  {name} — ya existe, saltando.")
        return False
    cur.execute(apply_sql)
    print(f"  ✓  {name} — aplicada correctamente.")
    return True


def migrate():
    conn = psycopg2.connect(**DB_CFG)
    cur  = conn.cursor()
    changes = 0

    print("\n=== MIGRACIONES OpenClaw / Brightcell / JCR ===\n")

    try:
        # ── MIGRACIÓN 1 ────────────────────────────────────────────────────────
        # UNIQUE en raw_messages.whapi_message_id
        # (puede que ya exista de tu script anterior — no falla si es así)
        print("[ 1 ] UNIQUE raw_messages.whapi_message_id")
        ok = run_migration(
            cur,
            name="raw_messages_whapi_message_id_unique",
            check_sql="""
                SELECT 1 FROM pg_constraint
                WHERE conname = 'raw_messages_whapi_message_id_unique'
                  AND conrelid = 'raw_messages'::regclass
            """,
            apply_sql="""
                ALTER TABLE raw_messages
                ADD CONSTRAINT raw_messages_whapi_message_id_unique
                UNIQUE (whapi_message_id)
            """
        )
        if ok: changes += 1

        # ── MIGRACIÓN 2 ────────────────────────────────────────────────────────
        # UNIQUE en driver_reports (raw_message_id, idbop)
        # Necesario para que ON CONFLICT (raw_message_id, idbop) funcione
        print("\n[ 2 ] UNIQUE driver_reports (raw_message_id, idbop)")
        ok = run_migration(
            cur,
            name="dr_msg_bop_unique",
            check_sql="""
                SELECT 1 FROM pg_constraint
                WHERE conname = 'dr_msg_bop_unique'
                  AND conrelid = 'driver_reports'::regclass
            """,
            apply_sql="""
                ALTER TABLE driver_reports
                ADD CONSTRAINT dr_msg_bop_unique
                UNIQUE (raw_message_id, idbop)
            """
        )
        if ok: changes += 1

        # ── MIGRACIÓN 3 ────────────────────────────────────────────────────────
        # UNIQUE en bo_closures (raw_message_id, idbop)
        print("\n[ 3 ] UNIQUE bo_closures (raw_message_id, idbop)")
        ok = run_migration(
            cur,
            name="bo_msg_bop_unique",
            check_sql="""
                SELECT 1 FROM pg_constraint
                WHERE conname = 'bo_msg_bop_unique'
                  AND conrelid = 'bo_closures'::regclass
            """,
            apply_sql="""
                ALTER TABLE bo_closures
                ADD CONSTRAINT bo_msg_bop_unique
                UNIQUE (raw_message_id, idbop)
            """
        )
        if ok: changes += 1

        # ── VERIFICACIÓN 4 ─────────────────────────────────────────────────────
        # Chequear si existe un CHECK constraint en message_type y qué valores permite
        print("\n[ 4 ] Verificando CHECK constraint de raw_messages.message_type")
        cur.execute("""
            SELECT con.conname, pg_get_constraintdef(con.oid) AS definition
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            WHERE rel.relname = 'raw_messages'
              AND con.contype = 'c'
              AND pg_get_constraintdef(con.oid) ILIKE '%message_type%'
        """)
        check_row = cur.fetchone()

        if check_row is None:
            print("  ℹ  No existe CHECK constraint en message_type.")
            print("     El código inserta 'unclassified' — esto NO rompe nada.")
            print("     Si querés agregar el CHECK con los valores correctos, descomentá la Migración 5.")
        else:
            conname, definition = check_row
            print(f"  ✓  Existe CHECK: {conname}")
            print(f"     Definición: {definition}")

            # Detectar si 'unclassified' está permitido
            if 'unclassified' not in definition:
                print("\n  ⚠  ALERTA: 'unclassified' NO está en el CHECK constraint.")
                print("     El código actual inserta 'unclassified' → cada INSERT falla.")
                print("     Aplicando Migración 5 automáticamente...")

                # ── MIGRACIÓN 5 (auto) ──────────────────────────────────────
                # Reemplazar el CHECK con los valores que el código realmente usa
                cur.execute(f"ALTER TABLE raw_messages DROP CONSTRAINT {conname}")
                cur.execute(f"""
                    ALTER TABLE raw_messages
                    ADD CONSTRAINT {conname}
                    CHECK (message_type IN {VALID_MESSAGE_TYPES + ('unclassified',)})
                """)
                print(f"  ✓  CHECK constraint actualizado con 'unclassified' incluido.")
                changes += 1
            else:
                print("  ✓  'unclassified' está permitido — no se necesita cambio.")

        # ── COMMIT ─────────────────────────────────────────────────────────────
        conn.commit()
        print(f"\n=== {changes} migraciones aplicadas. Commit realizado. ===\n")

    except Exception as e:
        conn.rollback()
        print(f"\n✗ ERROR — rollback ejecutado: {e}\n")
        raise

    finally:
        # ── REPORTE FINAL ──────────────────────────────────────────────────────
        print("[ Estado final de constraints en tablas operativas ]\n")
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT
                rel.relname   AS tabla,
                con.conname   AS constraint_name,
                con.contype   AS tipo,
                pg_get_constraintdef(con.oid) AS definicion
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            WHERE rel.relname IN ('raw_messages', 'driver_reports', 'bo_closures')
              AND con.contype IN ('u', 'c')
            ORDER BY rel.relname, con.contype, con.conname
        """)
        rows = cur2.fetchall()
        current_table = None
        for tabla, name, tipo, definition in rows:
            if tabla != current_table:
                print(f"  {tabla}")
                current_table = tabla
            tipo_label = 'UNIQUE' if tipo == 'u' else 'CHECK '
            print(f"    [{tipo_label}] {name}")
            print(f"           {definition}")
        cur2.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    migrate()