"""📱 Videos automáticos → estados de WhatsApp.

Cada día, después de generar los videos (recurrente/videos_diarios.py), `planificar()` sortea el orden:
1 video en la ventana de la mañana y 2 en la de la tarde (separados al menos `separacion_min`), para
cada sesión elegida en el panel (Campañas → Estados → Videos automáticos).
El bot de marketing (Tarea 2) llama a `publicar_pendientes()` en cada pasada y sube los que ya tocan.

Tabla videos_estados: un registro por video y sesión.
"""
import base64
import datetime
import json
import random
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

LIMA = ZoneInfo('America/Lima')
CLAVE = 'videos_auto'

DEFECTO_ESTADOS = {
    'estados_activo': True,
    'estados_sesiones': ['default', 'principal'],
    'fotos_lentes': False,          # estados con FOTO en la sesión Lentes (default). Principal sigue igual.
    'manana': ['09:30', '12:30'],
    'tarde': ['14:30', '20:00'],
    'separacion_min': 90,
    'texto_estado': '🎄✨ Esta Navidad dale brillo a tu mirada. Escríbenos y elige tus lentes 👀💫',
}
NOMBRE_SESION = {'default': 'Lentes', 'principal': 'Principal'}


def asegurar_tabla(conn):
    conn.execute(text("""CREATE TABLE IF NOT EXISTS videos_estados (
        id SERIAL PRIMARY KEY,
        fecha DATE NOT NULL,
        id_video INTEGER NOT NULL,
        sesion TEXT NOT NULL,
        programado TIMESTAMP NOT NULL,
        publicado TIMESTAMP,
        intentos INTEGER DEFAULT 0,
        resultado TEXT,
        UNIQUE (id_video, sesion))"""))


def config(conn):
    v = conn.execute(text("SELECT valor FROM ajustes WHERE clave = :k"), {'k': CLAVE}).scalar()
    d = dict(DEFECTO_ESTADOS)
    if v:
        try:
            d.update(json.loads(v))
        except Exception:
            pass
    return d


def _hm(s, fecha):
    h, m = (int(x) for x in str(s).split(':')[:2])
    return datetime.datetime.combine(fecha, datetime.time(h, m))


def _sortear_hora(rnd, ini, fin, ocupados, sep):
    total = int((fin - ini).total_seconds() // 60)
    if total <= 0:
        return ini
    for _ in range(200):
        t = ini + datetime.timedelta(minutes=rnd.randint(0, total))
        if all(abs((t - o).total_seconds()) >= sep * 60 for o in ocupados):
            return t
    return ini + datetime.timedelta(minutes=rnd.randint(0, total))


def planificar(conn, fecha, ids_videos, cfg=None, rnd=None):
    """Programa los videos del día: el primero (sorteado) en la mañana, el resto en la tarde."""
    asegurar_tabla(conn)
    cfg = cfg or config(conn)
    rnd = rnd or random.Random()
    if not cfg.get('estados_activo') or not ids_videos:
        return []
    ids = list(ids_videos)
    rnd.shuffle(ids)
    sep = int(cfg.get('separacion_min', 90))
    m_ini, m_fin = _hm(cfg['manana'][0], fecha), _hm(cfg['manana'][1], fecha)
    t_ini, t_fin = _hm(cfg['tarde'][0], fecha), _hm(cfg['tarde'][1], fecha)
    horas, ocupados = [], []
    for i, _ in enumerate(ids):
        ini, fin = (m_ini, m_fin) if i == 0 else (t_ini, t_fin)
        h = _sortear_hora(rnd, ini, fin, ocupados, sep)
        ocupados.append(h)
        horas.append(h)
    plan = []
    for vid, h in zip(ids, horas):
        for k, ses in enumerate(cfg.get('estados_sesiones') or []):
            hs = h + datetime.timedelta(minutes=3 * k)  # la 2.ª cuenta unos minutos después
            conn.execute(text("""INSERT INTO videos_estados (fecha, id_video, sesion, programado)
                VALUES (:f, :v, :s, :p)
                ON CONFLICT (id_video, sesion) DO UPDATE SET programado = EXCLUDED.programado
                WHERE videos_estados.publicado IS NULL"""), {'f': fecha, 'v': vid, 's': ses, 'p': hs})
            plan.append((vid, ses, hs))
    return plan


def subir_estado_video(sesion, ruta, caption='', contactos=None, timeout=240):
    from utils import WAHA_KEY
    from waha_motor import url_waha
    with open(ruta, 'rb') as f:
        data = base64.b64encode(f.read()).decode()
    payload = {'file': {'mimetype': 'video/mp4', 'filename': 'estado.mp4', 'data': data},
               'convert': False, 'caption': caption or ''}
    if contactos:
        payload['contacts'] = contactos
    r = requests.post(f"{url_waha('estados')}/api/{sesion}/status/video", json=payload,
                      headers={'X-Api-Key': WAHA_KEY or '', 'Content-Type': 'application/json'}, timeout=timeout)
    return r.status_code in (200, 201), f'{r.status_code} {r.text[:200]}'


def publicar_pendientes(engine, log=print, sesiones_ok=None, max_intentos=3):
    """Sube los estados de video programados para hoy cuya hora ya pasó. Devuelve cuántos publicó."""
    ahora = datetime.datetime.now(LIMA).replace(tzinfo=None)
    with engine.begin() as conn:
        asegurar_tabla(conn)
        cfg = config(conn)
        if not cfg.get('estados_activo'):
            log('🎬 Videos en estados: desactivado en el panel.')
            return 0
        filas = conn.execute(text("""
            SELECT e.id, e.sesion, e.programado, e.intentos, v.archivo, v.caso
            FROM videos_estados e JOIN videos_generados v ON v.id = e.id_video
            WHERE e.fecha = :hoy AND e.publicado IS NULL AND e.programado <= :ahora AND e.intentos < :mx
            ORDER BY e.programado"""), {'hoy': ahora.date(), 'ahora': ahora, 'mx': max_intentos}).fetchall()
    if not filas:
        log('🎬 Videos en estados: nada pendiente por ahora.')
        return 0
    hechos = 0
    for f in filas:
        if sesiones_ok is not None and f.sesion not in sesiones_ok:
            continue
        try:
            ok, msg = subir_estado_video(f.sesion, f.archivo, cfg.get('texto_estado', ''))
        except Exception as e:
            ok, msg = False, str(e)[:200]
        with engine.begin() as conn:
            if ok:
                conn.execute(text("UPDATE videos_estados SET publicado = :t, intentos = intentos + 1, resultado = :r WHERE id = :i"),
                             {'t': datetime.datetime.now(LIMA).replace(tzinfo=None), 'r': msg, 'i': f.id})
                conn.execute(text("INSERT INTO Historial_Estados (sku, session_name, fecha_publicacion) VALUES (:s, :ses, NOW())"),
                             {'s': f'VIDEO:{f.caso}', 'ses': f.sesion})
                hechos += 1
                log(f'🎬 ✅ Video «{f.caso}» publicado en estados ({NOMBRE_SESION.get(f.sesion, f.sesion)}), programado {f.programado:%H:%M}.')
            else:
                conn.execute(text("UPDATE videos_estados SET intentos = intentos + 1, resultado = :r WHERE id = :i"),
                             {'r': msg, 'i': f.id})
                log(f'🎬 ❌ No se pudo publicar el video «{f.caso}» en {f.sesion}: {msg}')
    return hechos
