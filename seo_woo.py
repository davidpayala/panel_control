"""
SEO automático para kmlentes.pe (WooCommerce + Rank Math).

Qué hace por cada producto de la tienda de lentes:
  • Título SEO (rank_math_title) en español y con la palabra clave
    "Lentes de Contacto" delante (hoy la mitad empieza por "Lens ...").
  • Meta descripción (rank_math_description) corta (≤155) y con datos útiles.
  • Texto alternativo (alt) en las imágenes que no lo tienen.
  • Robots: los productos ocultos del catálogo (agotados, sin tránsito) pasan a
    "noindex" para que no compitan como páginas huérfanas en Google, y vuelven a
    "index" cuando la sincronización de stock los hace visibles otra vez.

Reglas de seguridad:
  • Por defecto NO escribe nada (modo simulación). Solo escribe con aplicar=True.
  • Nunca pisa un título/descripción que se haya puesto a mano en Rank Math:
    solo rellena campos vacíos o los que este mismo script generó antes
    (marcados con la meta km_seo_auto = "1").
  • Solo rellena alts vacíos; no cambia imágenes ni su orden.

Uso:
  python seo_woo.py                 -> simulación, imprime resumen y ejemplos
  python seo_woo.py --aplicar       -> aplica los cambios en kmlentes.pe
  python seo_woo.py --sku NL001A    -> limita a uno o varios SKUs (coma)
"""
import os
import re
import sys
import html as _html

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

MARCA_WEB = "KM Lentes"
MAX_TITULO = 60
MAX_DESC = 155
META_AUTO = "km_seo_auto"
META_ROBOTS = "rank_math_robots"


# ------------------------------------------------------------------------------
# Robots (se usa también desde la sincronización de stock)
# ------------------------------------------------------------------------------
def robots_meta(visible):
    """meta_data de Rank Math para indexar (visible) o no indexar (oculto)."""
    return {"key": META_ROBOTS, "value": ["index"] if visible else ["noindex"]}


def robots_para_sync(visible):
    """Lo que la sincronización de stock agrega en meta_data. Solo actúa si en .env está
    SEO_NOINDEX_OCULTOS=1 (así se activa de forma consciente); si no, lista vacía = no cambia nada."""
    if os.getenv("SEO_NOINDEX_OCULTOS", "0").strip() == "1":
        return [robots_meta(visible)]
    return []


# ------------------------------------------------------------------------------
# Construcción de textos
# ------------------------------------------------------------------------------
def _limpiar(txt):
    txt = _html.unescape(re.sub(r"<[^>]+>", " ", txt or ""))
    return " ".join(txt.split())


def _atributo(prod, nombre):
    for a in prod.get("attributes", []):
        if a.get("name", "").strip().lower() == nombre.lower():
            return ", ".join(o.strip() for o in a.get("options", []) if o.strip())
    return ""


def _tipo(prod):
    cats = " ".join(c.get("name", "") for c in prod.get("categories", [])).lower()
    if "accesorio" in cats:
        return "accesorio"
    if "fantas" in cats:
        return "fantasia"
    return "natural"


def nombre_comercial(prod):
    """'Lens FreshLady Cleopatra Hazel (NL001A)' -> 'FreshLady Cleopatra Hazel'."""
    nombre = _limpiar(prod.get("name", ""))
    nombre = re.sub(r"\s*\([^)]*\)\s*$", "", nombre)            # quita "(SKU)"
    nombre = re.sub(r"^(lens|lente|lentes)\s+", "", nombre, flags=re.I)
    nombre = nombre.replace(" – ", " ").replace(" - ", " ")
    return " ".join(nombre.split())


def _recortar(txt, maximo):
    if len(txt) <= maximo:
        return txt
    corte = txt[:maximo - 1].rsplit(" ", 1)[0].rstrip(",.;:- ")
    return corte + "…"


def _primero_que_quepa(opciones, maximo):
    for o in opciones:
        if len(o) <= maximo:
            return o
    return _recortar(opciones[-1], maximo)


def _armar_desc(principal, extras, cierre):
    """Añade los extras (en orden de prioridad) mientras quepan; nunca corta a la mitad."""
    desc = principal
    for e in extras:
        if len(desc) + len(e) + 2 + len(cierre) <= MAX_DESC:
            desc += ", " + e
    desc += "."
    for c in cierre.split("|"):
        if c and len(desc) + len(c) <= MAX_DESC:
            desc += c
    return _recortar(desc, MAX_DESC)


def construir_seo(prod, con_sku=False):
    """Devuelve {'titulo', 'descripcion', 'alt'} para un producto de WooCommerce.
    con_sku=True añade el código al título (para desempatar nombres repetidos)."""
    tipo = _tipo(prod)
    nombre = nombre_comercial(prod)
    sku = (prod.get("sku") or "").strip()
    color = re.split(r"[,–-]", _atributo(prod, "Color"))[0].strip()
    diametro = _atributo(prod, "Diametro")
    contorno = _atributo(prod, "Contorno").lower()
    con_medida = bool(_atributo(prod, "Medida Miopía"))
    sufijo = f" | {MARCA_WEB}"
    if con_sku and sku:
        nombre_t = f"{nombre} {sku}"
    else:
        nombre_t = nombre

    # No repite el color si ya está escrito en español en el nombre
    color_txt = f" {color}" if color and color.lower() not in nombre.lower() else ""
    cierre = f" Envío a todo el Perú.| Cód. {sku}." if sku else " Envío a todo el Perú."

    if tipo == "accesorio":
        titulo = _primero_que_quepa([
            f"{nombre_t} para Lentes de Contacto{sufijo}",
            f"{nombre_t} para Lentes de Contacto",
            f"{nombre_t}{sufijo}",
        ], MAX_TITULO)
        desc = _armar_desc(f"{nombre}: accesorio para lentes de contacto", [f"compra en {MARCA_WEB}"], cierre)
        alt = f"{nombre} - accesorio para lentes de contacto"
    elif tipo == "fantasia":
        titulo = _primero_que_quepa([
            f"Lentes de Contacto Fantasía{color_txt} {nombre_t}{sufijo}",
            f"Lentes de Contacto Fantasía {nombre_t}{sufijo}",
            f"Lentes Fantasía {nombre_t}{sufijo}",
            f"Lentes Fantasía {nombre_t}",
        ], MAX_TITULO)
        principal = f"Lentes de contacto de fantasía {nombre}" + (f" color {color.lower()}" if color else "")
        extras = ["ideales para cosplay, anime y maquillaje"] + ([f"diámetro {diametro}"] if diametro else [])
        desc = _armar_desc(principal, extras, cierre)
        alt = f"Lentes de contacto de fantasía{color_txt} {nombre}"
    else:
        titulo = _primero_que_quepa([
            f"Lentes de Contacto{color_txt} {nombre_t}{sufijo}",
            f"Lentes de Contacto {nombre_t}{sufijo}",
            f"Lentes de Contacto {nombre_t}",
        ], MAX_TITULO)
        principal = f"Lentes de contacto de color{(' ' + color.lower()) if color else ''} {nombre}"
        extras = (["también con medida para miopía"] if con_medida else []) + \
                 ([f"diámetro {diametro}"] if diametro else []) + ([contorno] if contorno else [])
        desc = _armar_desc(principal, extras, cierre)
        alt = f"Lentes de contacto{color_txt} {nombre}"

    return {"titulo": titulo, "descripcion": desc, "alt": alt}


# ------------------------------------------------------------------------------
# Cálculo de cambios
# ------------------------------------------------------------------------------
def _meta(prod, clave):
    for m in prod.get("meta_data", []):
        if m.get("key") == clave:
            return m.get("value")
    return None


def calcular_cambios(prod, con_sku=False):
    """Devuelve (payload_update o None, resumen_dict) sin escribir nada."""
    seo = construir_seo(prod, con_sku=con_sku)
    es_auto = str(_meta(prod, META_AUTO) or "") == "1"
    meta = []
    resumen = {"id": prod["id"], "sku": prod.get("sku"), "nombre_web": prod.get("name"),
               "visible": prod.get("catalog_visibility") != "hidden"}

    titulo_actual = _meta(prod, "rank_math_title") or ""
    desc_actual = _meta(prod, "rank_math_description") or ""
    if (not titulo_actual or es_auto) and titulo_actual != seo["titulo"]:
        meta.append({"key": "rank_math_title", "value": seo["titulo"]})
        resumen["titulo_nuevo"] = seo["titulo"]
    if (not desc_actual or es_auto) and desc_actual != seo["descripcion"]:
        meta.append({"key": "rank_math_description", "value": seo["descripcion"]})
        resumen["descripcion_nueva"] = seo["descripcion"]
    if meta:
        meta.append({"key": META_AUTO, "value": "1"})

    # Robots según visibilidad en el catálogo
    deseado = robots_meta(resumen["visible"])
    actual = _meta(prod, META_ROBOTS)
    actual = actual if isinstance(actual, list) else ([actual] if actual else [])
    if sorted(actual) != sorted(deseado["value"]):
        meta.append(deseado)
        resumen["robots_nuevo"] = deseado["value"][0]

    payload = {"id": prod["id"]}
    if meta:
        payload["meta_data"] = meta

    # Alt de imágenes: solo los vacíos; se reenvía la lista completa en el mismo orden
    imagenes = prod.get("images", [])
    if imagenes and any(not (i.get("alt") or "").strip() for i in imagenes):
        nuevas = []
        for n, img in enumerate(imagenes):
            alt = (img.get("alt") or "").strip() or (seo["alt"] if n == 0 else f"{seo['alt']} - foto {n + 1}")
            nuevas.append({"id": img["id"], "alt": alt})
        payload["images"] = nuevas
        resumen["alts_nuevos"] = sum(1 for i in imagenes if not (i.get("alt") or "").strip())

    return (payload if len(payload) > 1 else None), resumen


# ------------------------------------------------------------------------------
# Ejecución contra WooCommerce
# ------------------------------------------------------------------------------
def conectar_woo():
    from woocommerce import API
    url, key, sec = os.getenv("WOO_LENTES_URL"), os.getenv("WOO_LENTES_KEY"), os.getenv("WOO_LENTES_SECRET")
    if not (url and key and sec):
        raise RuntimeError("Faltan WOO_LENTES_URL / WOO_LENTES_KEY / WOO_LENTES_SECRET en .env")
    return API(url=url, consumer_key=key, consumer_secret=sec, version="wc/v3", timeout=60)


def descargar_productos(wcapi):
    productos, pagina = [], 1
    while True:
        r = wcapi.get("products", params={"per_page": 100, "page": pagina, "status": "publish"})
        if r.status_code != 200:
            raise RuntimeError(f"WooCommerce respondió HTTP {r.status_code}: {r.text[:200]}")
        lote = r.json()
        if not lote:
            break
        productos += lote
        pagina += 1
    return productos


def _con_desempate(productos):
    """Títulos que saldrían repetidos (mismo nombre en varios productos) llevan el SKU."""
    conteo = {}
    for p in productos:
        t = construir_seo(p)["titulo"]
        conteo[t] = conteo.get(t, 0) + 1
    return [(p, conteo[construir_seo(p)["titulo"]] > 1) for p in productos]


def ejecutar(aplicar=False, skus=None, log=print):
    """Calcula (y opcionalmente aplica) los cambios SEO. Devuelve la lista de resúmenes."""
    wcapi = conectar_woo()
    productos = descargar_productos(wcapi)
    if skus:
        skus = {s.strip().upper() for s in skus}
        productos = [p for p in productos if (p.get("sku") or "").upper() in skus]
    log(f"📥 {len(productos)} productos leídos de kmlentes.pe")

    payloads, resumenes = [], []
    for p, con_sku in _con_desempate(productos):
        payload, resumen = calcular_cambios(p, con_sku=con_sku)
        if payload:
            payloads.append(payload)
            resumenes.append(resumen)

    log(f"🧮 {len(payloads)} productos con mejoras pendientes "
        f"(títulos: {sum('titulo_nuevo' in r for r in resumenes)}, "
        f"descripciones: {sum('descripcion_nueva' in r for r in resumenes)}, "
        f"noindex: {sum(r.get('robots_nuevo') == 'noindex' for r in resumenes)}, "
        f"index: {sum(r.get('robots_nuevo') == 'index' for r in resumenes)}, "
        f"alts: {sum(r.get('alts_nuevos', 0) for r in resumenes)})")

    if not aplicar:
        log("🔎 Modo simulación: no se escribió nada en la web.")
        return resumenes

    import time
    tam = 10   # lotes pequeños: cada update con imágenes es pesado y Hostinger corta peticiones largas
    for i in range(0, len(payloads), tam):
        lote = payloads[i:i + tam]
        for intento in range(3):
            try:
                r = wcapi.post("products/batch", {"update": lote})
                if r.status_code in (200, 201) and r.headers.get("content-type", "").startswith("application/json"):
                    for e in (u for u in r.json().get("update", []) if "error" in u):
                        log(f"❌ Producto {e.get('id')}: {e['error'].get('message')}")
                    break
                log(f"⚠️ Lote {i // tam + 1}: respuesta HTTP {r.status_code} no válida, reintento {intento + 1}/3")
            except Exception as e:
                log(f"⚠️ Lote {i // tam + 1}: error de red ({e}), reintento {intento + 1}/3")
            time.sleep(5 * (intento + 1))
        time.sleep(1.5)
        if (i // tam) % 10 == 9:
            log(f"   … {min(i + tam, len(payloads))}/{len(payloads)} enviados")

    # Verificación real: se vuelve a leer la tienda (una respuesta 307 del CDN no significa que falló)
    releidos = [p for p in descargar_productos(wcapi) if not skus or (p.get("sku") or "").upper() in skus]
    restantes = sum(1 for p, c in _con_desempate(releidos) if calcular_cambios(p, con_sku=c)[0])
    log(f"✅ SEO aplicado. Productos que aún quedan con cambios pendientes: {restantes} de {len(payloads)}.")
    return resumenes


if __name__ == "__main__":
    args = sys.argv[1:]
    skus = None
    if "--sku" in args:
        skus = args[args.index("--sku") + 1].split(",")
    res = ejecutar(aplicar="--aplicar" in args, skus=skus)
    for r in res[:15]:
        print("-", r.get("sku"), "|", r.get("titulo_nuevo", "(título sin cambio)"), "|",
              r.get("descripcion_nueva", ""), "| robots:", r.get("robots_nuevo", "="), "| alts:", r.get("alts_nuevos", 0))
