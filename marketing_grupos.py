"""
marketing_grupos.py — Piezas compartidas entre el bot de marketing (recurrente/bot_marketing.py)
y el panel de Campañas (views/campanas.py).

  • Columna mensajes.origen_bot: marca QUÉ tarea del bot envió cada mensaje.
        'G1'..'G4'  → mensajes directos a cada grupo (Recompra, Escribió sin comprar, ...)
        'Z1'..'Z3'  → reactivación zombie (nivel del cliente al enviar)
        NULL        → mensaje saliente que no salió del bot (escrito desde el teléfono, etc.)
                      o enviado antes de que existiera esta columna.
  • Tabla bot_grupos_limites: límite diario de mensajes directos por grupo y por cuenta,
    configurable en el panel (Campañas → Mensajes).
"""
from sqlalchemy import text

NOMBRE_GRUPO = {1: "Recompra", 2: "Escribió sin comprar", 3: "Cliente antiguo", 4: "Contacto frío"}

# Cuentas de WhatsApp del bot: sesión WAHA → nombre visible
SESIONES = {"principal": "Principal", "default": "Lentes"}

# Valores iniciales al crear la tabla (se cambian desde el panel)
LIMITES_INICIALES = {1: 50, 2: 100, 3: 10, 4: 10}


def origen_grupo(grupo):
    return f"G{int(grupo)}"


def origen_zombie(nivel):
    return f"Z{int(nivel)}"


def asegurar_esquema(conn):
    """Crea la columna y la tabla si no existen. Barato: solo consulta el catálogo cuando ya existen."""
    tiene_col = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'mensajes' AND column_name = 'origen_bot'
    """)).scalar()
    if not tiene_col:
        conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS origen_bot VARCHAR(10)"))

    if not conn.execute(text("SELECT to_regclass('public.bot_grupos_limites')")).scalar():
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bot_grupos_limites (
                grupo INTEGER PRIMARY KEY,
                nombre TEXT,
                activo BOOLEAN DEFAULT TRUE,
                limite_principal INTEGER DEFAULT 10,
                limite_default INTEGER DEFAULT 10
            )
        """))
        for g, nombre in NOMBRE_GRUPO.items():
            conn.execute(text("""
                INSERT INTO bot_grupos_limites (grupo, nombre, activo, limite_principal, limite_default)
                VALUES (:g, :n, TRUE, :l, :l) ON CONFLICT (grupo) DO NOTHING
            """), {"g": g, "n": nombre, "l": LIMITES_INICIALES[g]})


def leer_limites(conn):
    """{grupo: {"nombre", "activo", "principal", "default"}}"""
    filas = conn.execute(text(
        "SELECT grupo, nombre, activo, limite_principal, limite_default FROM bot_grupos_limites ORDER BY grupo"
    )).fetchall()
    return {
        int(r.grupo): {
            "nombre": r.nombre or NOMBRE_GRUPO.get(int(r.grupo), f"Grupo {r.grupo}"),
            "activo": bool(r.activo),
            "principal": int(r.limite_principal or 0),
            "default": int(r.limite_default or 0),
        }
        for r in filas
    }


def enviados_hoy_por_grupo(conn, sesion):
    """{grupo: teléfonos distintos contactados hoy por mensajes directos de esa sesión}"""
    filas = conn.execute(text("""
        SELECT origen_bot, COUNT(DISTINCT telefono)
        FROM mensajes
        WHERE tipo = 'SALIENTE_BOT'
          AND origen_bot LIKE 'G%'
          AND TRIM(COALESCE(session_name, 'default')) = :sess
          AND fecha >= (NOW() AT TIME ZONE 'America/Lima')::date
        GROUP BY origen_bot
    """), {"sess": sesion}).fetchall()
    return {int(o[1:]): int(n) for o, n in filas if o and o[1:].isdigit()}


# ==============================================================================
# 🧟 VENTANAS DE TIEMPO ZOMBIE (días calendario desde ultimo_msg_zombie)
#   Lvl 1: el mismo día              (día 0)
#   Lvl 2: desde el día siguiente    (días 1 a 6)
#   Lvl 3: desde el día 7            (días 7 a 13)
#   Día 14 en adelante: deja de ser zombie (nivel 0)
# Las ventanas no se cruzan: el nivel se CALCULA siempre a partir de los días, nunca se arrastra.
# ==============================================================================
ZOMBIE_INICIO_LVL2 = 1
ZOMBIE_INICIO_LVL3 = 7
ZOMBIE_DIA_SALIDA = 14

ZOMBIE_VENTANAS = {
    1: "Mismo día (día 0)",
    2: f"Días {ZOMBIE_INICIO_LVL2} a {ZOMBIE_INICIO_LVL3 - 1}",
    3: f"Días {ZOMBIE_INICIO_LVL3} a {ZOMBIE_DIA_SALIDA - 1}",
}


def dias_zombie(ultimo_msg_zombie, hoy=None):
    """Días calendario (no bloques de 24 h) desde que el cliente quedó como zombie."""
    from datetime import date
    hoy = hoy or date.today()
    return (hoy - ultimo_msg_zombie.date()).days


def nivel_zombie_por_dias(dias):
    """1, 2 o 3 según la ventana; 0 = ya pasó el día 14 y deja de ser zombie."""
    if dias >= ZOMBIE_DIA_SALIDA:
        return 0
    if dias >= ZOMBIE_INICIO_LVL3:
        return 3
    if dias >= ZOMBIE_INICIO_LVL2:
        return 2
    return 1


def fecha_inicio_para_nivel(nivel):
    """Al marcar un nivel a mano desde el chat: cuántos días atrás debe quedar ultimo_msg_zombie
    para que el bot lo trate en ese nivel (Lvl 1 → hoy, Lvl 2 → hace 1 día, Lvl 3 → hace 7 días)."""
    return {1: 0, 2: ZOMBIE_INICIO_LVL2, 3: ZOMBIE_INICIO_LVL3}.get(int(nivel), 0)
