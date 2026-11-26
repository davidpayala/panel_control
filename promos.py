"""
🎁 Promoción de packs de lentes (p. ej. Navidad: 1 par S/55 · 2 pares S/99 · 3 pares S/139).

La configuración se guarda en la tabla ajustes (clave 'promo_lentes', JSON) y se edita en
Campañas → 🎁 Promociones. Desde ahí se publica en kmlentes.pe (opción km_promo, snippet 'KM: promociones').
utils.generar_texto_producto_ia la usa para que los mensajes, estados y posts mencionen la promo.
"""
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from database import engine

CLAVE = "promo_lentes"
CATEGORIAS_LENTE = ("estilo natural", "estilo fantasía", "estilo fantasia")

DEFECTO = {
    "activo": True,
    "nombre": "Promo Navidad",
    "desde": "2026-11-26",
    "hasta": "2026-12-25",
    "precio_base": 55,
    "packs": [{"n": 2, "precio": 99}, {"n": 3, "precio": 139}],
    "regalo": "Estuche de regalo con cada par",
    "envio": "Envío gratis a todo el Perú y en Lima pagas al recibir (contraentrega)",
    "barra": "Navidad: 2 pares S/99 · 3 pares S/139 · envío gratis",
    "usar_mensajes": True,
    "usar_estados": True,
    "usar_facebook": True,
    "frase_ia": "",
    # Registro sanitario: solo marcas con número confirmado (foto del empaque).
    "registro_sanitario": {"FreshLady": "R.S. DM23897E (importado por Droguería Zigma S.A.C.)",
                           "Meetone": "registro sanitario"},
}


def _asegurar_tabla(conn):
    conn.execute(text("CREATE TABLE IF NOT EXISTS ajustes (clave TEXT PRIMARY KEY, valor TEXT)"))


def leer_promo():
    try:
        with engine.connect() as c:
            v = c.execute(text("SELECT valor FROM ajustes WHERE clave = :k"), {"k": CLAVE}).scalar()
    except Exception:
        v = None
    datos = dict(DEFECTO)
    if v:
        try:
            datos.update(json.loads(v))
        except Exception:
            pass
    return datos


def guardar_promo(datos):
    with engine.begin() as c:
        _asegurar_tabla(c)
        c.execute(text("""INSERT INTO ajustes (clave, valor) VALUES (:k, :v)
                          ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor"""),
                  {"k": CLAVE, "v": json.dumps(datos, ensure_ascii=False)})


def soles(v):
    v = float(v)
    return f"S/{int(v)}" if v == int(v) else f"S/{v:.2f}"


def vigente(datos=None):
    d = datos or leer_promo()
    if not d.get("activo") or not d.get("packs"):
        return False
    hoy = datetime.now(ZoneInfo("America/Lima")).strftime("%Y-%m-%d")
    if d.get("desde") and hoy < d["desde"]:
        return False
    if d.get("hasta") and hoy > d["hasta"]:
        return False
    return True


def packs_ordenados(d):
    return sorted(({"n": int(p["n"]), "precio": float(p["precio"])} for p in d.get("packs", [])
                   if int(p.get("n", 0)) >= 2 and float(p.get("precio", 0)) > 0), key=lambda p: p["n"])


def frase_sugerida(d):
    """Texto base de la promo (lo que la IA debe decir, sin inventar otros precios)."""
    partes = [f"1 par {soles(d['precio_base'])}"] + [f"{p['n']} pares {soles(p['precio'])}" for p in packs_ordenados(d)]
    fin = ""
    if d.get("hasta"):
        meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre",
                 "octubre", "noviembre", "diciembre"]
        f = datetime.strptime(d["hasta"], "%Y-%m-%d")
        fin = f" Válido hasta el {f.day} de {meses[f.month - 1]}."
    extras = ". ".join(x for x in (d.get("regalo", ""), d.get("envio", "")) if x)
    return (f"🎄 {d.get('nombre', 'Promo')}: {' · '.join(partes)} (puedes combinar modelos y colores). "
            f"{extras}.{fin}").replace("..", ".")


def frase(d=None):
    d = d or leer_promo()
    return (d.get("frase_ia") or "").strip() or frase_sugerida(d)


def aplica_a_producto(producto, d=None):
    """¿Este producto entra en la promo? Lentes naturales/fantasía con precio <= precio base."""
    d = d or leer_promo()
    if str(producto.get("macro_categoria") or "").strip().lower() != "lentes":
        return False
    if str(producto.get("categoria") or "").strip().lower() not in CATEGORIAS_LENTE:
        return False
    try:
        return 0 < float(producto.get("precio") or 0) <= float(d["precio_base"]) + 0.001
    except (TypeError, ValueError):
        return False


def registro_sanitario(producto, d=None):
    d = d or leer_promo()
    marca = str(producto.get("marca") or "").lower()
    for m, txt in (d.get("registro_sanitario") or {}).items():
        if m.lower() in marca and txt:
            return f"{m} con {txt}"
    return ""


def contexto_ia(producto, canal):
    """Instrucciones extra para la IA. canal: 'mensajes' | 'estados' | 'facebook'.
    Devuelve (instruccion_para_prompt, linea_para_texto_de_reserva)."""
    try:
        d = leer_promo()
    except Exception:
        return "", ""
    partes, reserva = [], []
    if vigente(d) and d.get(f"usar_{canal}", True) and aplica_a_producto(producto, d):
        f = frase(d)
        partes.append(f"PROMOCIÓN VIGENTE (menciónala de forma clara, con estos precios exactos y sin inventar otros "
                      f"descuentos ni precios): {f}")
        reserva.append(f)
    rs = registro_sanitario(producto, d)
    if rs:
        partes.append(f"DATO DE CONFIANZA (puedes mencionarlo): lentes {rs}.")
    else:
        partes.append("PROHIBIDO decir que este producto tiene registro sanitario.")
    return "\n        ".join(partes), "\n\n".join(reserva)


def publicar_en_web(d=None):
    """Envía la configuración a kmlentes.pe (opción km_promo). Devuelve (ok, detalle)."""
    d = d or leer_promo()
    datos = {k: d.get(k) for k in ("activo", "nombre", "desde", "hasta", "precio_base", "packs", "regalo", "barra")}
    try:
        r = requests.post(f"{os.getenv('WOO_URL', 'https://kmlentes.pe').rstrip('/')}/wp-json/km/v1/promo", json=datos,
                          auth=(os.getenv("WP_USER"), os.getenv("WP_APP_PASSWORD")), timeout=60)
        if r.status_code == 200:
            return True, r.json()
        return False, f"HTTP {r.status_code}: {r.text[:300]}"
    except Exception as e:
        return False, str(e)


def frase_corta(d=None):
    """Versión corta para estados (máx. ~12 palabras)."""
    d = d or leer_promo()
    packs = " · ".join(f"{p['n']} pares {soles(p['precio'])}" for p in packs_ordenados(d))
    return f"🎄 {d.get('nombre', 'Promo')}: {packs} · envío gratis"


def asegurar_en_texto(texto, producto, canal, corta=False):
    """Si la promo aplica y la IA no la mencionó (no aparece el precio del pack), la agrega al final."""
    try:
        d = leer_promo()
        if not (vigente(d) and d.get(f"usar_{canal}", True) and aplica_a_producto(producto, d)):
            return texto
        marcas = [soles(p["precio"]) for p in packs_ordenados(d)]
        plano = str(texto or "").replace(" ", "")
        if marcas and all(m.replace(" ", "") in plano for m in marcas):
            return texto
        extra = frase_corta(d) if corta else frase(d)
        return f"{str(texto or '').rstrip()}\n\n{extra}"
    except Exception:
        return texto
