"""
🌐 Publica en kmlentes.pe (WooCommerce) los lentes nuevos del proveedor creados en el panel.

Mismo formato que los productos existentes:
  · Nombre "Lentes de Contacto {Marca} {Modelo} {Color} ({SKU})" (fantasía: "Lentes de Contacto Fantasía …")
  · Producto simple, S/ precio del panel, peso 50 g, stock = interno + proveedor, oculto si no hay stock
  · Categoría Naturales (148) o Fantasía (307) · marca (taxonomía) · etiquetas Lente/Natural|Fantasía/
    "Lente de Contacto {Marca}"/"Lente de Contacto {Modelo}"
  · Atributos globales: Color (pa_color), Contorno (pa_contorno), Diametro (pa_diametro; vacío → 14.20 mm)
  · Descripción larga y corta con las plantillas de mejorar_descripciones.py, SEO de Rank Math con
    seo_woo.construir_seo, marca para Google (_wc_gla_brand)
  · Imagen: /home/panel-control/imagenes_km/<SKU>_1.webp subida a la biblioteca de medios
Sin imagen apta → se crea como BORRADOR (no se ve en la tienda) hasta tener foto.
Al terminar guarda url_imagen y url_tienda en el panel y un registro en respaldos_web/.

Uso:
    .venv/bin/python recurrente/publicar_productos_web.py                 # simulación
    .venv/bin/python recurrente/publicar_productos_web.py --aplicar
    .venv/bin/python recurrente/publicar_productos_web.py --aplicar --sku NL193A,NL194A
"""
import json
import os
import re
import sys
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

import requests
from openpyxl import load_workbook
from sqlalchemy import text

from database import engine
import mejorar_descripciones as md
import seo_woo

PROPUESTA = os.path.join(_raiz, "documentos", "codigos_propuestos.xlsx")
IMAGENES = "/home/panel-control/imagenes_km"
WP = "https://kmlentes.pe/wp-json/wp/v2"
EN_ESPERA = {"NL222A", "NL270A", "NL271A"}   # posibles duplicados: esperan confirmación del usuario

CAT = {"Estilo Natural": 148, "Estilo Fantasía": 307}
MARCA_ID = {"FreshLady": 311, "Meetone": 334}
TAG_LENTE, TAG_NATURAL, TAG_FANTASIA = 22, 96, 129
COLOR_WEB = {"Verde": "Verde", "Gris": "Gris – Blanco", "Azul": "Azul – Celeste", "Chocolate": "Marrón – Chocolate",
             "Marrón": "Marrón – Chocolate", "Miel": "Miel – Amarillo – Dorado", "Negro": "Negro",
             "Morado": "Rosado - Morado"}
INCLUYE = ["Un (1) par de lentes de contacto", "Estuche de guardado para las lentillas", "Garantía de fábrica"]


def propuesta():
    ws = load_workbook(PROPUESTA, read_only=True, data_only=True).worksheets[0]
    filas = list(ws.iter_rows(values_only=True))
    cab = list(filas[0])
    out = {}
    for r in filas[1:]:
        d = dict(zip(cab, r))
        if d["Acción"] in ("MODELO NUEVO", "NUEVO COLOR"):
            out.setdefault(d["SKU KM propuesto"], d)
    return out


def productos_panel(skus):
    with engine.connect() as c:
        filas = c.execute(text("""
            SELECT v.sku, p.id_producto, p.marca, p.modelo, p.nombre, p.categoria, p.color_principal, p.diametro,
                   p.detalles_tecnicos, v.precio, COALESCE(v.stock_interno,0) + COALESCE(v.stock_externo,0) AS stock
            FROM Variantes v JOIN Productos p USING (id_producto) WHERE v.sku = ANY(:s)"""), {"s": list(skus)}).mappings()
        return {f["sku"]: dict(f) for f in filas}


def construir(p):
    fantasia = p["categoria"] == "Estilo Fantasía"
    diam = p["diametro"] if p["diametro"] not in (None, "", "N/A", "0.0") else "14.2"
    diam_web = f"{float(diam):.2f} mm"
    contorno = (p["detalles_tecnicos"] or {}).get("contorno", "Con Contorno")
    if contorno not in ("Con Contorno", "Sin Contorno"):
        contorno = "Con Contorno"
    base = f"{p['marca']} {p['modelo']} {p['nombre']}"
    nombre = f"Lentes de Contacto {'Fantasía ' if fantasia else ''}{base} ({p['sku']})"
    atributos = []
    if p["color_principal"] in COLOR_WEB:
        atributos.append({"id": 2, "position": 0, "visible": True, "variation": False,
                          "options": [COLOR_WEB[p["color_principal"]]]})
    atributos += [{"id": 10, "position": 1, "visible": True, "variation": False, "options": [contorno]},
                  {"id": 11, "position": 2, "visible": True, "variation": False, "options": [diam_web]}]
    prod = {"name": nombre, "sku": p["sku"], "categories": [{"id": CAT[p["categoria"]]}],
            "brands": [{"id": MARCA_ID[p["marca"]], "name": p["marca"]}], "attributes": [
                {**a, "name": {2: "Color", 10: "Contorno", 11: "Diametro"}[a["id"]]} for a in atributos]}
    d = {"nombre": base, "titulo": f"Lentes de Contacto {base}", "sku": p["sku"], "marca": p["marca"],
         "modelo": p["modelo"], "color": p["color_principal"] if p["color_principal"] in COLOR_WEB else "",
         "diametro": f"{float(diam):.1f} mm", "contorno": contorno.capitalize(), "acuoso": "38% – 40%",
         "duracion": "un año", "medida": md._medida({}), "personajes": [], "incluye": INCLUYE, "fantasia": fantasia}
    seo = seo_woo.construir_seo(prod)
    visible = (p["stock"] or 0) > 0
    return {
        "name": nombre, "slug": p["sku"].lower(), "type": "simple", "sku": p["sku"], "regular_price": f"{float(p['precio']):.0f}",
        "weight": "50", "manage_stock": True, "stock_quantity": int(p["stock"] or 0),
        "catalog_visibility": "visible" if visible else "hidden",
        "categories": prod["categories"], "brands": [{"id": MARCA_ID[p["marca"]]}],
        "tags": [{"id": TAG_LENTE}, {"id": TAG_FANTASIA if fantasia else TAG_NATURAL}],
        "attributes": [{k: v for k, v in a.items() if k != "name"} for a in atributos],
        "description": md.descripcion_lente(d), "short_description": md.corta_lente(d),
        "meta_data": [{"key": "rank_math_title", "value": seo["titulo"]},
                      {"key": "rank_math_description", "value": seo["descripcion"]},
                      {"key": seo_woo.META_AUTO, "value": "1"}, seo_woo.robots_meta(visible),
                      {"key": "_wc_gla_brand", "value": p["marca"]}],
        "_alt": seo["alt"], "_tags_texto": [f"Lente de Contacto {p['marca']}", f"Lente de Contacto {p['modelo']}"],
    }


class Etiquetas:
    def __init__(self, wcapi):
        self.w, self.cache = wcapi, {}

    def id(self, nombre, crear):
        k = nombre.lower()
        if k not in self.cache:
            r = self.w.get("products/tags", params={"search": nombre, "per_page": 100}).json()
            hit = next((t["id"] for t in r if t["name"].lower() == k), None)
            if hit is None and crear:
                hit = self.w.post("products/tags", {"name": nombre}).json().get("id")
            self.cache[k] = hit
        return self.cache[k]


def subir_imagen(sesion, ruta, alt, titulo):
    with open(ruta, "rb") as f:
        r = sesion.post(f"{WP}/media", data=f.read(), timeout=120, headers={
            "Content-Disposition": f'attachment; filename="{os.path.basename(ruta)}"', "Content-Type": "image/webp"})
    r.raise_for_status()
    m = r.json()
    sesion.post(f"{WP}/media/{m['id']}", json={"alt_text": alt, "title": titulo}, timeout=60)
    return m["id"], m["source_url"]


def main():
    aplicar = "--aplicar" in sys.argv
    solo = next((a.split("=", 1)[1] if "=" in a else sys.argv[sys.argv.index(a) + 1]
                 for a in sys.argv[1:] if a.startswith("--sku")), "")
    solo = {s.strip() for s in solo.split(",") if s.strip()}
    prop = propuesta()
    skus = [s for s in prop if s not in EN_ESPERA and (not solo or s in solo)]
    panel = productos_panel(skus)
    wcapi = seo_woo.conectar_woo()
    en_web = {p["sku"] for p in seo_woo.descargar_productos(wcapi)}

    plan = []
    for s in skus:
        if s in en_web:
            print(f"↷ {s} ya existe en la web, se omite")
            continue
        if s not in panel:
            print(f"⚠️ {s} no está en el panel")
            continue
        foto = os.path.join(IMAGENES, f"{s}_1.webp")
        plan.append((s, construir(panel[s]), foto if os.path.exists(foto) else None))

    con_foto = [x for x in plan if x[2]]
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'} · {len(plan)} productos: {len(con_foto)} publicados con foto, "
          f"{len(plan) - len(con_foto)} como borrador sin foto · en espera: {sorted(EN_ESPERA)}")
    print(f"   visibles (con stock): {sum(1 for _, p, f in plan if f and p['catalog_visibility'] == 'visible')}")
    if plan:
        s, p, f = plan[0]
        print("   ejemplo:", p["name"], "|", p["regular_price"], "|", p["attributes"], "|", p["_tags_texto"])
        print("   corta:", p["short_description"])
    if not aplicar:
        print("ℹ️ Simulación: no se cambió nada (usa --aplicar)")
        return

    sesion = requests.Session()
    sesion.auth = (os.getenv("WP_USER"), os.getenv("WP_APP_PASSWORD"))
    tags = Etiquetas(wcapi)
    registro = []
    os.makedirs(os.path.join(_raiz, "respaldos_web"), exist_ok=True)
    ruta_reg = os.path.join(_raiz, "respaldos_web", f"productos_nuevos_{datetime.now():%Y%m%d_%H%M%S}.json")
    for s, p, foto in plan:
        try:
            datos = {k: v for k, v in p.items() if not k.startswith("_")}
            datos["tags"] += [{"id": i} for i in (tags.id(t, crear=True) for t in p["_tags_texto"]) if i]
            media_id, url = None, None
            if foto:
                media_id, url = subir_imagen(sesion, foto, p["_alt"], p["name"])
                datos["images"] = [{"id": media_id, "alt": p["_alt"]}]
                datos["status"] = "publish"
            else:
                datos["status"] = "draft"
            r = wcapi.post("products", datos).json()
            if not r.get("id"):
                raise RuntimeError(str(r)[:300])
            registro.append({"sku": s, "id": r["id"], "media_id": media_id, "status": datos["status"]})
            with engine.begin() as c:
                c.execute(text("""UPDATE Productos SET url_imagen = COALESCE(:u, url_imagen), url_tienda = :t
                                  WHERE id_producto = (SELECT id_producto FROM Variantes WHERE sku = :s)"""),
                          {"u": url, "t": r.get("permalink") if datos["status"] == "publish" else None, "s": s})
            print(f"✅ {s} → id {r['id']} ({datos['status']})")
        except Exception as e:
            print(f"❌ {s}: {e}")
        json.dump(registro, open(ruta_reg, "w"), ensure_ascii=False, indent=1)
    print(f"📄 Registro: {ruta_reg} ({len(registro)} creados)")


if __name__ == "__main__":
    main()
