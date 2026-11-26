"""
🔎 Compara los LENTES del panel con kmlentes.pe: precio, precio rebajado, contorno, color, marca y diámetro.

Panel:  Variantes.precio / precio_rebajado · Productos.marca / color_principal / diametro /
        detalles_tecnicos.contorno   (solo macro_categoria = 'Lentes')
Web:    regular_price / sale_price (de la variación si el lente tiene medidas) · marca (taxonomía) ·
        atributos globales Color (familias), Contorno y Diametro

Uso:
    .venv/bin/python recurrente/comparar_lentes_web.py [--excel=documentos/comparacion_lentes.xlsx]
"""
import os
import re
import sys
from collections import Counter, defaultdict

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from sqlalchemy import text
from database import engine
import seo_woo

# color del panel → familia de color de la web (pa_color)
FAMILIA = {"verde": "Verde", "turquesa": "Azul – Celeste", "azul": "Azul – Celeste", "celeste": "Azul – Celeste",
           "gris": "Gris – Blanco", "blanco": "Gris – Blanco", "marrón": "Marrón – Chocolate",
           "marron": "Marrón – Chocolate", "chocolate": "Marrón – Chocolate", "miel": "Miel – Amarillo – Dorado",
           "dorado": "Miel – Amarillo – Dorado", "amarillo": "Miel – Amarillo – Dorado", "negro": "Negro",
           "rojo": "Rojo – Naranja", "naranja": "Rojo – Naranja", "rosado": "Rosado - Morado",
           "morado": "Rosado - Morado", "multicolor": "Multicolor"}
CAMPOS = ["precio", "rebajado", "contorno", "color", "marca", "diametro"]


def marcas_set(v):
    """'UYAAI, Meetone' → {'uyaai', 'meetone'} (un lente de fantasía puede venir de varias marcas)."""
    return {m.strip().lower().replace(" ", "") for m in str(v or "").split(",") if m.strip()}


def num(v):
    try:
        return round(float(str(v).replace(",", ".").split()[0]), 2)
    except (ValueError, IndexError, TypeError):
        return None


def atributo(p, nombre):
    for a in p.get("attributes", []):
        if a.get("name", "").lower() == nombre.lower():
            return ", ".join(a.get("options") or [])
    return ""


def web_lentes(w):
    """{sku: datos} de la web, con las variaciones (lentes con medida) por su propio SKU."""
    out = {}
    for p in seo_woo.descargar_productos(w):
        cats = " ".join(c["slug"] for c in p["categories"])
        if "accesorio" in cats or "lente" not in cats:
            continue
        base = {"id": p["id"], "tipo": p["type"], "nombre": p["name"], "status": p["status"],
                "marca": ", ".join(b["name"] for b in p.get("brands", [])),
                "color": atributo(p, "Color"), "contorno": atributo(p, "Contorno"),
                "diametro": atributo(p, "Diametro")}
        if p["type"] == "variable":
            pag = 1
            while True:
                vs = w.get(f"products/{p['id']}/variations", params={"per_page": 100, "page": pag}).json()
                if not vs:
                    break
                for v in vs:
                    if v.get("sku"):
                        out[v["sku"].strip()] = {**base, "var_id": v["id"], "precio": v.get("regular_price"),
                                                 "rebajado": v.get("sale_price")}
                pag += 1
        elif p.get("sku"):
            out[p["sku"].strip()] = {**base, "precio": p.get("regular_price"), "rebajado": p.get("sale_price")}
    return out


def panel_lentes():
    with engine.connect() as c:
        filas = c.execute(text("""
            SELECT v.sku, v.precio, v.precio_rebajado, p.marca, p.color_principal, p.diametro, p.detalles_tecnicos,
                   p.id_producto, p.modelo, p.nombre
            FROM Variantes v JOIN Productos p USING (id_producto) WHERE p.macro_categoria = 'Lentes'""")).mappings()
        return {f["sku"].strip(): dict(f) for f in filas}


def comparar():
    w = seo_woo.conectar_woo()
    web, panel = web_lentes(w), panel_lentes()
    comunes = sorted(set(web) & set(panel))
    difs = []
    for s in comunes:
        p, x = panel[s], web[s]
        cont_p = (p["detalles_tecnicos"] or {}).get("contorno", "")
        fam_p = FAMILIA.get((p["color_principal"] or "").strip().lower(), "")
        pares = {
            "precio": (num(p["precio"]), num(x["precio"])),
            "rebajado": (num(p["precio_rebajado"]), num(x["rebajado"])),
            "contorno": (cont_p, x["contorno"]),
            "color": (fam_p or (p["color_principal"] or ""), x["color"]),
            "marca": ((p["marca"] or "").strip(), x["marca"]),
            "diametro": (num(p["diametro"]), num(x["diametro"])),
        }
        for campo, (a, b) in pares.items():
            if campo == "marca" and marcas_set(a) == marcas_set(b):
                continue
            if a != b and not (a in ("", None) and b in ("", None)):
                difs.append({"sku": s, "campo": campo, "panel": a, "web": b, "web_nombre": x["nombre"],
                             "web_tipo": x["tipo"], "panel_color_original": p["color_principal"]})
    return web, panel, comunes, difs


def main():
    salida = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--excel=")),
                  os.path.join(_raiz, "documentos", "comparacion_lentes.xlsx"))
    web, panel, comunes, difs = comparar()
    print(f"Lentes en web: {len(web)} · en panel: {len(panel)} · en ambos: {len(comunes)}")
    print(f"Solo en web: {len(set(web) - set(panel))} · solo en panel: {len(set(panel) - set(web))}")
    por = defaultdict(list)
    for d in difs:
        por[d["campo"]].append(d)
    for campo in CAMPOS:
        ds = por[campo]
        tipos = Counter("vacío en panel" if d["panel"] in ("", None) else "vacío en web" if d["web"] in ("", None)
                        else "distinto" for d in ds)
        print(f"\n■ {campo}: {len(ds)} diferencias {dict(tipos)}")
        for d in ds[:6]:
            print(f"   {d['sku']}: panel={d['panel']!r} web={d['web']!r}")
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Diferencias"
    ws.append(["SKU", "Campo", "Panel", "Web", "Nombre en la web", "Tipo web", "Color panel (original)"])
    for d in difs:
        ws.append([d["sku"], d["campo"], str(d["panel"]), str(d["web"]), d["web_nombre"], d["web_tipo"],
                   d["panel_color_original"]])
    ws2 = wb.create_sheet("Solo en un lado")
    ws2.append(["SKU", "Dónde"])
    for s in sorted(set(web) - set(panel)):
        ws2.append([s, "solo web"])
    for s in sorted(set(panel) - set(web)):
        ws2.append([s, "solo panel"])
    wb.save(salida)
    print(f"\n📄 {salida}")


if __name__ == "__main__":
    main()
