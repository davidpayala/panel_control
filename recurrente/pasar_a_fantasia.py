"""
🎭 Pasa lentes de "Naturales" a "Fantasía" en kmlentes.pe y en el panel.

En la web: categoría Naturales (148) → Fantasía (307), etiqueta Natural → Fantasía, nombre
"Lentes de Contacto Fantasía …", color "Rosado - Morado" (opcional), descripción larga/corta y SEO
regenerados como fantasía. La dirección (slug = código) no cambia.
En el panel: Productos.categoria = 'Estilo Fantasía'.
Guarda respaldo de los productos originales en respaldos_web/.

Uso:
    .venv/bin/python recurrente/pasar_a_fantasia.py SKU1,SKU2 [--color-morado]            # simulación
    .venv/bin/python recurrente/pasar_a_fantasia.py SKU1,SKU2 [--color-morado] --aplicar
"""
import json
import os
import re
import sys
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from sqlalchemy import text
from database import engine
import mejorar_descripciones as md
import seo_woo

CAT_NAT, CAT_FAN, TAG_NAT, TAG_FAN = 148, 307, 96, 129
COLOR_MORADO = "Rosado - Morado"


def nuevo_producto(p, color_morado):
    base = re.sub(r"^Lentes de Contacto\s+(Fantas[ií]a\s+)?", "", p["name"]).strip()
    prod = dict(p)
    prod["name"] = f"Lentes de Contacto Fantasía {base}"
    prod["categories"] = [{"id": CAT_FAN, "slug": "lentes-de-contacto-de-fantasia",
                           "name": "Lentes de Contacto de Fantasía"}] + \
        [c for c in p["categories"] if c["id"] not in (CAT_NAT, CAT_FAN)]
    prod["tags"] = [t for t in p["tags"] if t["id"] != TAG_NAT]
    if not any(t["id"] == TAG_FAN for t in prod["tags"]):
        prod["tags"].append({"id": TAG_FAN})
    attrs = []
    for a in p["attributes"]:
        a = {k: a[k] for k in ("id", "name", "position", "visible", "variation", "options")}
        if a["name"] == "Color" and color_morado:
            a["options"] = [COLOR_MORADO]
        attrs.append(a)
    prod["attributes"] = attrs
    d = md.datos_lente(prod)
    d["fantasia"] = True
    seo = seo_woo.construir_seo(prod)
    payload = {
        "name": prod["name"], "categories": [{"id": c["id"]} for c in prod["categories"]],
        "tags": [{"id": t["id"]} for t in prod["tags"]], "attributes": attrs,
        "description": md.descripcion_lente(d), "short_description": md.corta_lente(d),
        "meta_data": [{"key": "rank_math_title", "value": seo["titulo"]},
                      {"key": "rank_math_description", "value": seo["descripcion"]},
                      {"key": seo_woo.META_AUTO, "value": "1"}],
    }
    return payload


def main():
    aplicar = "--aplicar" in sys.argv
    color_morado = "--color-morado" in sys.argv
    skus = [s.strip() for a in sys.argv[1:] if not a.startswith("--") for s in a.split(",") if s.strip()]
    w = seo_woo.conectar_woo()
    web = {p["sku"]: p for p in seo_woo.descargar_productos(w) if p["sku"] in skus}
    faltan = [s for s in skus if s not in web]
    if faltan:
        print("⚠️ no están en la web:", faltan)
    cambios = {s: nuevo_producto(p, color_morado) for s, p in web.items()}
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'} · {len(cambios)} productos")
    for s, c in list(cambios.items())[:2]:
        print(f"   {s}: {web[s]['name']} → {c['name']}")
        print("     corta:", re.sub("<[^>]+>", "", c["short_description"])[:200])
    if not aplicar:
        return
    ruta = os.path.join(_raiz, "respaldos_web", f"a_fantasia_{datetime.now():%Y%m%d_%H%M%S}.json")
    json.dump([web[s] for s in cambios], open(ruta, "w"), ensure_ascii=False)
    print("📄 Respaldo:", ruta)
    for s, c in cambios.items():
        r = w.put(f"products/{web[s]['id']}", c).json()
        print(("✅" if r.get("id") else "❌"), s, r.get("name") or str(r)[:200])
    with engine.begin() as conn:
        n = conn.execute(text("""UPDATE Productos SET categoria = 'Estilo Fantasía'
            WHERE id_producto IN (SELECT id_producto FROM Variantes WHERE regexp_replace(sku, '-[0-9]{4}$', '') = ANY(:s))"""),
            {"s": skus}).rowcount
    print(f"🗂️ Panel: {n} productos pasados a Estilo Fantasía")


if __name__ == "__main__":
    main()
