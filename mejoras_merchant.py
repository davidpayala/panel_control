"""
Mejoras de catálogo de kmlentes.pe pensadas para Google Merchant Center (Shopping)
y que también suben el SEO de las fichas:

  1. Marca para Google (meta _wc_gla_brand del plugin "Google for WooCommerce"),
     tomada de la columna marca de la base de datos. Genérico/Otros -> "KM Lentes".
  2. Nombre del producto en español: "Lens FreshLady X (NL001A)" ->
     "Lentes de Contacto FreshLady X (NL001A)"; "Lente Ghost (FL027I)" ->
     "Lentes de Contacto Fantasía Ghost (FL027I)". La URL no cambia.
  3. Descripción para los productos visibles que no tienen ninguna (Google los rechaza).

Por defecto solo simula. Con --aplicar escribe en la web.
  python mejoras_merchant.py [--aplicar] [--sku NL001A,EL064]
"""
import re
import sys
import time

from sqlalchemy import text

from database import engine
from seo_woo import conectar_woo, descargar_productos, nombre_comercial, _tipo, _limpiar

META_MARCA = "_wc_gla_brand"
MARCAS_GENERICAS = {"", "genérico", "generico", "otros", "eliminar", "none", "nan"}
OPCIONES_VACIAS = {"mix", "mixted", "mixto"}


# ------------------------------------------------------------------------------
def marcas_db():
    with engine.connect() as conn:
        filas = conn.execute(text("""
            SELECT v.sku, p.marca FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
            WHERE COALESCE(v.sku, '') <> ''
        """)).fetchall()
    mapa = {}
    for sku, marca in filas:
        m = (marca or "").strip()
        m = "KM Lentes" if m.lower() in MARCAS_GENERICAS else m
        m = {"eyeshare": "EyeShare", "freshlady": "FreshLady", "ovolook": "OvoLook", "freshgo": "FreshGo",
             "meetone": "Meetone", "fancylook": "Fancylook"}.get(m.lower(), m)
        mapa[sku.strip().upper()] = m
        base = sku.strip().upper().rsplit("-", 1)[0]
        mapa.setdefault(base, m)
    return mapa


def nombre_nuevo(prod):
    tipo = _tipo(prod)
    actual = " ".join((prod.get("name") or "").split())
    if tipo == "accesorio" or actual.lower().startswith("lentes de contacto"):
        return None
    m = re.search(r"\(([^)]*)\)\s*$", actual)
    codigo = f" ({m.group(1).strip()})" if m else (f" ({prod['sku']})" if prod.get("sku") else "")
    base = nombre_comercial(prod)
    prefijo = "Lentes de Contacto Fantasía" if tipo == "fantasia" else "Lentes de Contacto"
    return f"{prefijo} {base}{codigo}"


def _opciones(prod):
    ops = []
    for a in prod.get("attributes", []):
        ops += [o.strip() for o in a.get("options", []) if o.strip() and o.strip().lower() not in OPCIONES_VACIAS]
    return ops


def descripcion_accesorio(prod):
    nombre = _limpiar(prod.get("name"))
    n = nombre.lower()
    ops = _opciones(prod)
    disponibles = f"<p><strong>Disponible en:</strong> {', '.join(ops)}.</p>" if ops else ""
    pack = re.search(r"pack\s*x\s*(\d+)", n)

    if n.startswith("solución") or n.startswith("solucion"):
        cuerpo = (f"<p>{nombre} es una solución multipropósito para lentes de contacto: sirve para limpiar, "
                  "enjuagar y guardar tus lentes en su estuche. Es un básico para que tus lentes de color se "
                  "mantengan cómodos e higiénicos durante todo su tiempo de uso.</p>"
                  "<p><strong>Consejos:</strong> cambia la solución del estuche cada día, no la mezcles con agua "
                  "y no la reutilices.</p>")
    elif "lavador" in n:
        cuerpo = (f"<p>{nombre}: accesorio para limpiar tus lentes de contacto de forma práctica. Colocas los "
                  "lentes con tu solución multipropósito y te ayuda a retirar residuos de maquillaje y polvo "
                  "sin frotarlos de más con los dedos.</p>")
    elif pack:
        cuerpo = (f"<p>Pack de {pack.group(1)} estuches para lentes de contacto. Ideal para tener un estuche en "
                  "casa, otro en la cartera y uno de repuesto, o para guardar por separado tus lentes de color "
                  "y los de medida.</p>")
    elif "estuche" in n and "aplicador" in n:
        cuerpo = (f"<p>{nombre}: estuche de guardado para lentes de contacto que incluye aplicador, para "
                  "ponerte y quitarte los lentes sin tocarlos con las uñas. Su diseño compacto entra en "
                  "cualquier cartera o mochila.</p>")
    elif "estuche" in n:
        cuerpo = (f"<p>{nombre}: estuche de guardado para lentes de contacto, con un compartimento para cada "
                  "ojo. Mantiene tus lentes protegidos y sumergidos en solución cuando no "
                  "los usas.</p>")
    else:  # kits, mini kits, galletitas
        cuerpo = (f"<p>{nombre}: kit portátil con diseño para guardar y llevar tus lentes de contacto y sus "
                  "accesorios a todas partes. Revisa las fotos para ver todas las piezas que incluye el "
                  "diseño que elijas.</p>"
                  "<p>Es un regalo ideal para quienes usan lentes de contacto de color y quieren tener todo "
                  "ordenado en un solo lugar.</p>")

    cierre = ("<p>Compra en KM Lentes con envío a todo el Perú: gratis a las agencias Shalom y dentro de "
              "nuestra cobertura en Lima.</p>")
    corta = re.sub(r"<[^>]+>", "", cuerpo.split("</p>")[0])
    return cuerpo + disponibles + cierre, corta


# ------------------------------------------------------------------------------
def calcular(prod, marcas):
    payload, cambios = {"id": prod["id"]}, {}
    sku = (prod.get("sku") or "").strip().upper()

    marca = marcas.get(sku) or marcas.get(sku.rsplit("-", 1)[0]) or "KM Lentes"
    actual = next((m["value"] for m in prod.get("meta_data", []) if m["key"] == META_MARCA), None)
    if actual != marca:
        payload["meta_data"] = [{"key": META_MARCA, "value": marca}]
        cambios["marca"] = marca

    nuevo = nombre_nuevo(prod)
    if nuevo and nuevo != prod.get("name"):
        payload["name"] = nuevo
        cambios["nombre"] = nuevo

    if (prod.get("catalog_visibility") != "hidden" and not _limpiar(prod.get("description"))
            and not _limpiar(prod.get("short_description"))):
        larga, corta = descripcion_accesorio(prod)
        payload["description"], payload["short_description"] = larga, corta
        cambios["descripcion"] = corta

    return (payload if len(payload) > 1 else None), cambios


def ejecutar(aplicar=False, skus=None, log=print):
    wcapi = conectar_woo()
    productos = descargar_productos(wcapi)
    if skus:
        skus = {s.strip().upper() for s in skus}
        productos = [p for p in productos if (p.get("sku") or "").upper() in skus]
    marcas = marcas_db()

    lote_total, resumen = [], []
    for p in productos:
        payload, cambios = calcular(p, marcas)
        if payload:
            lote_total.append(payload)
            resumen.append({"sku": p.get("sku"), "antes": p.get("name"), **cambios})
    log(f"🧮 {len(lote_total)} productos con cambios: marca {sum('marca' in r for r in resumen)}, "
        f"nombre {sum('nombre' in r for r in resumen)}, descripción {sum('descripcion' in r for r in resumen)}")
    if not aplicar:
        log("🔎 Simulación: no se escribió nada.")
        return resumen

    for i in range(0, len(lote_total), 20):
        lote = lote_total[i:i + 20]
        for intento in range(3):
            try:
                r = wcapi.post("products/batch", {"update": lote})
                if r.status_code in (200, 201) and r.headers.get("content-type", "").startswith("application/json"):
                    for e in (u for u in r.json().get("update", []) if "error" in u):
                        log(f"❌ Producto {e.get('id')}: {e['error'].get('message')}")
                    break
                log(f"⚠️ Lote {i // 20 + 1}: HTTP {r.status_code}, reintento {intento + 1}/3")
            except Exception as e:
                log(f"⚠️ Lote {i // 20 + 1}: {e}, reintento {intento + 1}/3")
            time.sleep(5 * (intento + 1))
        time.sleep(1.5)

    restantes = sum(1 for p in descargar_productos(wcapi)
                    if (not skus or (p.get("sku") or "").upper() in skus) and calcular(p, marcas)[0])
    log(f"✅ Aplicado. Quedan con cambios pendientes: {restantes}.")
    return resumen


if __name__ == "__main__":
    a = sys.argv[1:]
    res = ejecutar(aplicar="--aplicar" in a, skus=a[a.index("--sku") + 1].split(",") if "--sku" in a else None)
    for r in res[:12]:
        print(r)
