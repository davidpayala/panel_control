"""🎬 Videos diarios para estados (3 por día, formato vertical).

Casos:
  1. naturales  — lentes naturales aleatorios con stock en tienda (stock_interno > 0).
  2. grupo      — todos los colores de un mismo modelo (NL018A, NL018B, …) con stock en tienda y/o
                  proveedor. La duración depende de cuántos colores tenga (mín. 3, máx. 12 por defecto).
  3. mixto      — 50 % naturales y 50 % fantasía aleatorios con stock en tienda, intercalados.

Solo variantes sin medida (la foto es la misma) y con precio ≤ precio base de la promo, para que el
precio del video coincida con los packs. Evita repetir los SKUs usados en los últimos días.
La plantilla y los parámetros se leen de ajustes → 'videos_auto' (JSON); la promo, de 'promo_lentes'.

Salida: ~/videos_auto/AAAA-MM-DD/caso{1,2,3}_*.mp4 y registro en la tabla videos_generados.
Las carpetas con más de `conservar_dias` se borran.

Uso:
    .venv/bin/python recurrente/videos_diarios.py                 # genera los 3 de hoy
    .venv/bin/python recurrente/videos_diarios.py --caso=2        # solo uno
    .venv/bin/python recurrente/videos_diarios.py --capturas      # además guarda JPG de revisión
    .venv/bin/python recurrente/videos_diarios.py --rehacer       # vuelve a generar aunque ya existan
    .venv/bin/python recurrente/videos_diarios.py --sin-estados   # no programa estados (pruebas)
"""
import datetime
import json
import os
import random
import re
import shutil
import sys
import time
from collections import defaultdict
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text  # noqa: E402

from database import engine  # noqa: E402
from videos import musica as musica_mod  # noqa: E402
from videos.motor import descargar_imagen  # noqa: E402
from videos.plantillas import PLANTILLAS, disponibles  # noqa: E402
from videos import estados as estados_mod  # noqa: E402

CLAVE = 'videos_auto'
DEFECTO = {
    'activo': True,
    'plantillas': ['navidad'],        # activas; cada video usa una al azar entre las vigentes
    'formato': 'vertical',
    'segundos_foto': 2.0,
    'fotos_naturales': 10,
    'fotos_mixto': 10,
    'grupo_min': 3,
    'grupo_max': 12,
    'dias_sin_repetir': 7,
    'conservar_dias': 14,
    'precio_max_sin_promo': 60,
}
SALIDA = os.path.expanduser('~/videos_auto')
LIMA = ZoneInfo('America/Lima')
NOMBRES = {'1': 'naturales', '2': 'grupo', '3': 'mixto'}


def log(*a):
    print(datetime.datetime.now(LIMA).strftime('%H:%M:%S'), *a, flush=True)


def leer_json(conn, clave, defecto=None):
    v = conn.execute(text("SELECT valor FROM ajustes WHERE clave = :k"), {'k': clave}).scalar()
    d = dict(defecto or {})
    if v:
        try:
            d.update(json.loads(v))
        except Exception:
            pass
    return d


def asegurar_tablas(conn):
    conn.execute(text("CREATE TABLE IF NOT EXISTS ajustes (clave TEXT PRIMARY KEY, valor TEXT)"))
    conn.execute(text("""INSERT INTO ajustes (clave, valor) VALUES (:k, :v) ON CONFLICT (clave) DO NOTHING"""),
                 {'k': CLAVE, 'v': json.dumps(DEFECTO, ensure_ascii=False)})
    conn.execute(text("""CREATE TABLE IF NOT EXISTS videos_generados (
        id SERIAL PRIMARY KEY,
        fecha DATE NOT NULL,
        caso TEXT NOT NULL,
        plantilla TEXT,
        formato TEXT,
        archivo TEXT,
        skus TEXT[],
        grupo TEXT,
        duracion NUMERIC,
        musica TEXT,
        creado TIMESTAMP DEFAULT NOW(),
        UNIQUE (fecha, caso, formato))"""))
    conn.execute(text("ALTER TABLE videos_generados ADD COLUMN IF NOT EXISTS musica TEXT"))


def alertar_panel(mensaje):
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) "
                              "VALUES ('videos', 'ALERTA_CRITICA', :p)"),
                         {'p': json.dumps({'mensaje': f'🎬 Videos diarios: {mensaje}', 'clave': 'videos'}, ensure_ascii=False)})
    except Exception as e:
        log('no se pudo guardar la alerta:', e)


SQL_CANDIDATOS = """
SELECT var.sku, COALESCE(NULLIF(TRIM(var.nombre_variante), ''), NULLIF(TRIM(p.color_principal), ''), '') AS color,
       COALESCE(NULLIF(TRIM(p.marca), ''), '') AS marca, COALESCE(NULLIF(TRIM(p.modelo), ''), '') AS modelo,
       p.categoria, COALESCE(var.stock_interno, 0) AS st_int, COALESCE(var.stock_externo, 0) AS st_ext,
       COALESCE(NULLIF(var.precio_rebajado, 0), var.precio) AS precio,
       COALESCE(NULLIF(TRIM(var.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS img
FROM variantes var JOIN productos p ON p.id_producto = var.id_producto
WHERE p.categoria IN ('Estilo Natural', 'Estilo Fantasía')
  AND COALESCE(TRIM(var.medida), '') IN ('', 'nan', '0', '0.00', '-0.00', '+0.00')
  AND COALESCE(NULLIF(var.precio_rebajado, 0), var.precio) > 0
  AND COALESCE(NULLIF(var.precio_rebajado, 0), var.precio) <= :pmax
  AND COALESCE(NULLIF(TRIM(var.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) IS NOT NULL
"""


def candidatos(conn, pmax):
    filas = [dict(r._mapping) for r in conn.execute(text(SQL_CANDIDATOS), {'pmax': pmax})]
    # una entrada por foto (misma foto = mismo color); se suma el stock
    por_img = {}
    for f in filas:
        k = f['img']
        if k in por_img:
            por_img[k]['st_int'] += f['st_int']
            por_img[k]['st_ext'] += f['st_ext']
        else:
            f['sku'] = re.sub(r'-\d{4}$', '', f['sku'])
            por_img[k] = f
    return list(por_img.values())


def usados_recientes(conn, dias):
    filas = conn.execute(text("SELECT skus, grupo FROM videos_generados WHERE fecha >= CURRENT_DATE - :d"), {'d': dias}).fetchall()
    skus, grupos = set(), set()
    for s, g in filas:
        skus.update(s or [])
        if g:
            grupos.add(g)
    return skus, grupos


def a_producto(c, encabezado):
    nombre = ' '.join(x for x in [c['marca'], c['modelo']] if x).strip() or c['sku']
    color = c['color'][:1].upper() + c['color'][1:]
    return {'sku': c['sku'], 'nombre': nombre, 'color': color, 'precio': float(c['precio']),
            'encabezado': encabezado, 'url': c['img']}


def con_imagen(lista, n, rnd, usados):
    """Elige hasta n con foto descargable; primero los no usados recientemente."""
    nuevos = [c for c in lista if c['sku'] not in usados]
    viejos = [c for c in lista if c['sku'] in usados]
    rnd.shuffle(nuevos)
    rnd.shuffle(viejos)
    out = []
    for c in nuevos + viejos:
        if len(out) >= n:
            break
        im = descargar_imagen(c['img'])
        if im is not None:
            c = dict(c)
            c['imagen'] = im
            out.append(c)
    return out


def base_grupo(sku):
    m = re.match(r'^([A-Z]+(?:-[A-Z]+)?\d+)([A-Z]?)', sku.upper())
    return m.group(1) if m else sku


def elegir(caso, cands, cfg, rnd, usados, grupos_usados):
    nat = [c for c in cands if c['categoria'] == 'Estilo Natural']
    fan = [c for c in cands if c['categoria'] == 'Estilo Fantasía']
    if caso == '1':
        el = con_imagen([c for c in nat if c['st_int'] > 0], cfg['fotos_naturales'], rnd, usados)
        return [dict(a_producto(c, 'Lentes naturales'), imagen=c['imagen']) for c in el], None, \
            'Tenemos cientos de modelos naturales'
    if caso == '3':
        mitad = cfg['fotos_mixto'] // 2
        a = con_imagen([c for c in nat if c['st_int'] > 0], mitad, rnd, usados)
        b = con_imagen([c for c in fan if c['st_int'] > 0], cfg['fotos_mixto'] - mitad, rnd, usados)
        a = [dict(a_producto(c, 'Lentes naturales'), imagen=c['imagen']) for c in a]
        b = [dict(a_producto(c, 'Lentes de fantasía'), imagen=c['imagen']) for c in b]
        mezcla = []
        for i in range(max(len(a), len(b))):
            mezcla += ([a[i]] if i < len(a) else []) + ([b[i]] if i < len(b) else [])
        return mezcla, None, 'Cientos de modelos naturales y de fantasía'
    # caso 2: mismo grupo de colores
    grupos = defaultdict(list)
    for c in nat:
        if (c['st_int'] > 0 or c['st_ext'] > 0) and c['color']:
            grupos[(base_grupo(c['sku']), c['modelo'].lower())].append(c)
    validos = [(k, v) for k, v in grupos.items() if len(v) >= cfg['grupo_min']]
    rnd.shuffle(validos)
    validos.sort(key=lambda kv: kv[0][0] in grupos_usados)  # primero los no usados
    for (base, _), lista in validos:
        lista = sorted(lista, key=lambda c: c['sku'])[: cfg['grupo_max']]
        el = []
        for c in lista:
            im = descargar_imagen(c['img'])
            if im is not None:
                el.append(dict(c, imagen=im))
        if len(el) < cfg['grupo_min']:
            continue
        modelo = el[0]['modelo'] or base
        enc = f'{len(el)} tonos de {modelo}'
        return [dict(a_producto(c, enc), imagen=c['imagen']) for c in el], base, 'Tenemos cientos de modelos naturales'
    return [], None, ''


def limpiar_viejos(dias):
    if not os.path.isdir(SALIDA):
        return
    limite = (datetime.datetime.now(LIMA).date() - datetime.timedelta(days=dias)).isoformat()
    for d in os.listdir(SALIDA):
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', d) and d < limite:
            shutil.rmtree(os.path.join(SALIDA, d), ignore_errors=True)
            log('borrada carpeta vieja', d)


def main():
    args = sys.argv[1:]
    casos = ['1', '2', '3']
    for a in args:
        if a.startswith('--caso='):
            casos = a.split('=', 1)[1].split(',')
    capturas = '--capturas' in args
    rehacer = '--rehacer' in args

    with engine.begin() as conn:
        asegurar_tablas(conn)
        cfg = leer_json(conn, CLAVE, DEFECTO)
        promo = leer_json(conn, 'promo_lentes')
    if not cfg.get('activo'):
        log('videos_auto desactivado; nada que hacer')
        return
    hoy = datetime.datetime.now(LIMA).date()
    activas = cfg.get('plantillas') or ([cfg['plantilla']] if cfg.get('plantilla') else [])
    claves_ok = disponibles(activas, hoy)
    if not claves_ok:
        msg = (f"ninguna plantilla activa está vigente hoy ({', '.join(activas) or 'ninguna elegida'}). "
               "Elige o crea otra en Campañas → Estados → Videos automáticos.")
        log('⚠️', msg)
        alertar_panel(msg)
        return
    plantilla = PLANTILLAS[claves_ok[0]]
    carpeta = os.path.join(SALIDA, hoy.isoformat())
    os.makedirs(carpeta, exist_ok=True)
    promo_ok = plantilla.promo_vigente(promo, hoy) if hasattr(plantilla, 'promo_vigente') else False
    pmax = float(promo.get('precio_base', 55)) if promo_ok else float(cfg['precio_max_sin_promo'])

    with engine.connect() as conn:
        cands = candidatos(conn, pmax)
        usados, grupos_usados = usados_recientes(conn, int(cfg['dias_sin_repetir']))
        # música: no repetir la de hoy ni la de ayer
        musicas_recientes = {r[0] for r in conn.execute(text(
            "SELECT musica FROM videos_generados WHERE fecha >= CURRENT_DATE - 1 AND fecha < :h AND musica IS NOT NULL"), {'h': hoy})}
    log(f'{len(cands)} lentes candidatos (precio ≤ {pmax:g}); {len(usados)} SKUs usados en los últimos {cfg["dias_sin_repetir"]} días')

    rnd = random.Random(f'{hoy.isoformat()}')
    errores = []
    for caso in casos:
        nombre = NOMBRES.get(caso)
        if not nombre:
            continue
        salida = os.path.join(carpeta, f'caso{caso}_{nombre}.mp4')
        if os.path.exists(salida) and not rehacer:
            log(f'caso {caso}: ya existe {salida}')
            continue
        try:
            clave_pl = rnd.choice(claves_ok)
            plantilla = PLANTILLAS[clave_pl]
            prods, grupo, texto_modelos = elegir(caso, cands, cfg, rnd, usados, grupos_usados)
            if len(prods) < 2:
                raise RuntimeError('no hay suficientes lentes con stock y foto')
            t0 = time.time()
            cancion = musica_mod.aleatoria(rnd, evitar=musicas_recientes)
            musicas_recientes.add(cancion)
            res = plantilla.generar(prods, salida, formato=cfg['formato'], segundos_foto=float(cfg['segundos_foto']),
                                    texto_modelos=texto_modelos, promo=promo, musica=cancion,
                                    capturas=[1, 4, int(dur_est(prods, cfg, promo_ok)) - 2] if capturas else None)
            dur = res['duracion']
            skus = [p['sku'] for p in prods]
            usados.update(skus)
            with engine.begin() as conn:
                conn.execute(text("""INSERT INTO videos_generados (fecha, caso, plantilla, formato, archivo, skus, grupo, duracion, musica)
                    VALUES (:f, :c, :p, :fo, :a, :s, :g, :d, :m)
                    ON CONFLICT (fecha, caso, formato) DO UPDATE SET plantilla = EXCLUDED.plantilla, archivo = EXCLUDED.archivo,
                      skus = EXCLUDED.skus, grupo = EXCLUDED.grupo, duracion = EXCLUDED.duracion, musica = EXCLUDED.musica,
                      creado = NOW()"""),
                             {'f': hoy, 'c': nombre, 'p': clave_pl, 'fo': cfg['formato'], 'a': salida,
                              's': skus, 'g': grupo, 'd': dur, 'm': cancion})
            log(f'caso {caso} ({nombre}): {len(prods)} fotos, {dur:.0f} s, música {cancion}, {time.time() - t0:.0f} s de render → {salida}')
            log('   ', ', '.join(skus))
        except Exception as e:
            errores.append(f'caso {caso} ({nombre}): {e}')
            log(f'❌ caso {caso}: {e}')

    # 📱 horario de estados: 1 video en la mañana y 2 en la tarde (solo si hoy aún no hay plan)
    if '--sin-estados' in args:
        log('📱 --sin-estados: no se programan estados')
    try:
        if '--sin-estados' in args:
            raise StopIteration
        with engine.begin() as conn:
            estados_mod.asegurar_tabla(conn)
            ya = conn.execute(text("SELECT COUNT(*) FROM videos_estados WHERE fecha = :h"), {'h': hoy}).scalar()
            if not ya:
                ids = [r[0] for r in conn.execute(text(
                    "SELECT id FROM videos_generados WHERE fecha = :h AND formato = :fo ORDER BY caso"), {'h': hoy, 'fo': cfg['formato']})]
                plan = estados_mod.planificar(conn, hoy, ids, rnd=random.Random(f'plan-{hoy}'))
                for vid, ses, hora in sorted(plan, key=lambda x: x[2]):
                    log(f'📱 estado programado: video {vid} → {ses} a las {hora:%H:%M}')
    except StopIteration:
        pass
    except Exception as e:
        errores.append(f'plan de estados: {e}')
        log('❌ plan de estados:', e)

    limpiar_viejos(int(cfg['conservar_dias']))
    if errores:
        alertar_panel('; '.join(errores))


def dur_est(prods, cfg, promo_ok):
    return 3 + len(prods) * float(cfg['segundos_foto']) + (4 if promo_ok else 0) + 3


if __name__ == '__main__':
    main()
