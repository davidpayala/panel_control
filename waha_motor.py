"""
waha_motor.py — Elige qué motor de WAHA (GOWS, NOWEB o WEBJS) usa cada acción de WhatsApp.

La elección se guarda en la tabla `waha_motores` y se cambia desde Opciones → Motor WhatsApp.
Si la tabla no existe o la base no responde, todas las acciones usan MOTOR_POR_DEFECTO.

Uso:
    from waha_motor import url_waha, headers_waha
    requests.post(f"{url_waha('enviar_texto')}/api/sendText", json=..., headers=headers_waha())
"""
import os
import threading
import time

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# URL de cada contenedor. WAHA_URL siempre apuntó al contenedor WEBJS (puerto 3000).
#   GOWS  (3002): motor en Go (whatsmeow). Liviano y estable con contactos LID.
#   NOWEB (3001): motor en Node (Baileys). Con la 7.0.0-rc14 perdía mensajes entrantes de la sesión principal.
#   WEBJS (3000): navegador Chromium. Mucha RAM.
MOTORES = {
    "GOWS": (os.getenv("WAHA_URL_GOWS") or "http://localhost:3002").rstrip("/"),
    "NOWEB": (os.getenv("WAHA_URL_NOWEB") or os.getenv("WAHA_URL_ESTADOS") or "http://localhost:3001").rstrip("/"),
    "WEBJS": (os.getenv("WAHA_URL_WEBJS") or os.getenv("WAHA_URL") or "http://localhost:3000").rstrip("/"),
}
# Nombre del contenedor Docker de cada motor (para chequeos de salud y reinicios)
CONTENEDORES = {"GOWS": "waha-gows", "NOWEB": "waha-estados", "WEBJS": "waha"}

MOTOR_POR_DEFECTO = "GOWS"  # si la base no responde, se usa este (NOWEB y WEBJS están apagados)

# Acción -> descripción que se muestra en Opciones
ACCIONES = {
    "enviar_texto": "Enviar mensajes de texto (chat del panel, bot de marketing, respuestas automáticas)",
    "enviar_imagen": "Enviar imágenes y archivos (bot de marketing y campañas)",
    "estados": "Publicar estados de WhatsApp",
    "marcar_leido": "Marcar chats como leídos",
    "contactos": "Consultar contactos, nombres, LIDs y verificar si un número tiene WhatsApp",
    "descargar_media": "Descargar fotos/audios recibidos (respaldo si el webhook no trae el archivo)",
}

_CACHE_SEG = 60
_cache = {"t": 0.0, "datos": {}}
_lock = threading.Lock()


def _engine():
    from database import engine  # import tardío: evita ciclos y no conecta si no se usa
    return engine


def _crear_tabla(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS waha_motores (
            accion TEXT PRIMARY KEY,
            motor TEXT NOT NULL,
            actualizado TIMESTAMP DEFAULT NOW()
        )
    """))
    # Tablas creadas antes de GOWS tenían CHECK (motor IN ('NOWEB','WEBJS')): se quita, la validación la hace MOTORES.
    conn.execute(text("ALTER TABLE waha_motores DROP CONSTRAINT IF EXISTS waha_motores_motor_check"))


def configuracion(forzar=False):
    """{accion: motor} para todas las acciones (lee la base como máximo una vez por minuto)."""
    with _lock:
        if not forzar and time.time() - _cache["t"] < _CACHE_SEG and _cache["datos"]:
            return dict(_cache["datos"])
    datos = {a: MOTOR_POR_DEFECTO for a in ACCIONES}
    try:
        with _engine().connect() as conn:
            filas = conn.execute(text("SELECT accion, motor FROM waha_motores")).fetchall()
        datos.update({a: m for a, m in filas if a in ACCIONES and m in MOTORES})
    except Exception:
        pass  # tabla aún no creada o base caída: se usan los valores por defecto
    with _lock:
        _cache.update(t=time.time(), datos=datos)
    return dict(datos)


def guardar_configuracion(nueva):
    """Guarda {accion: motor} en la base y refresca la caché."""
    with _engine().begin() as conn:
        _crear_tabla(conn)
        for accion, motor in nueva.items():
            if accion in ACCIONES and motor in MOTORES:
                conn.execute(text("""
                    INSERT INTO waha_motores (accion, motor, actualizado) VALUES (:a, :m, NOW())
                    ON CONFLICT (accion) DO UPDATE SET motor = EXCLUDED.motor, actualizado = NOW()
                """), {"a": accion, "m": motor})
    return configuracion(forzar=True)


def motor_de(accion):
    return configuracion().get(accion, MOTOR_POR_DEFECTO)


def url_waha(accion):
    """URL base del contenedor WAHA que debe atender esta acción."""
    return MOTORES[motor_de(accion)]


def url_motor(motor):
    return MOTORES.get(str(motor).upper(), MOTORES[MOTOR_POR_DEFECTO])


def motores_en_uso():
    """Motores que al menos una acción necesita (solo esos se vigilan y reinician)."""
    return sorted(set(configuracion().values()))


def headers_waha(json=True):
    h = {"Accept": "application/json"}
    if json:
        h["Content-Type"] = "application/json"
    key = os.getenv("WAHA_KEY")
    if key:
        h["X-Api-Key"] = key
    return h


def url_media(media_url, motor=None):
    """Traduce la URL de un archivo que anuncia WAHA (siempre 'localhost:3000' dentro del contenedor)
    a la URL real del contenedor que lo recibió."""
    if not media_url:
        return None
    base = url_motor(motor) if motor else url_waha("descargar_media")
    if not media_url.startswith("http"):
        return f"{base}/{media_url.lstrip('/')}"
    if "/api/" in media_url and ("localhost" in media_url or "127.0.0.1" in media_url or "waha" in media_url):
        return f"{base}/api/{media_url.split('/api/', 1)[1]}"
    return media_url
