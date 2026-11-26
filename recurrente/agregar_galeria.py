"""
🖼️ Agrega a la galería de cada producto de kmlentes.pe las fotos extra <SKU>_2.webp, <SKU>_3.webp
de /home/panel-control/imagenes_km (detrás de la foto principal, sin quitar nada).

Solo toca productos publicados; omite las fotos que ya están en la galería (mismo nombre de archivo).

Uso:
    .venv/bin/python recurrente/agregar_galeria.py                    # simulación
    .venv/bin/python recurrente/agregar_galeria.py --aplicar [--sku NL193A,NL194A]
"""
import glob
import json
import os
import re
import sys
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import seo_woo
from publicar_productos_web import IMAGENES, subir_imagen


def main():
    aplicar = "--aplicar" in sys.argv
    solo = next((a.split("=", 1)[1] if "=" in a else sys.argv[sys.argv.index(a) + 1]
                 for a in sys.argv[1:] if a.startswith("--sku")), "")
    solo = {s.strip() for s in solo.split(",") if s.strip()}
    extras = {}
    for ruta in sorted(glob.glob(os.path.join(IMAGENES, "*_[2-9].webp"))):
        sku = re.sub(r"_\d\.webp$", "", os.path.basename(ruta))
        if not solo or sku in solo:
            extras.setdefault(sku, []).append(ruta)
    w = seo_woo.conectar_woo()
    web = {p["sku"]: p for p in seo_woo.descargar_productos(w) if p["sku"] in extras}
    plan = []
    for sku, rutas in extras.items():
        p = web.get(sku)
        if not p or p["status"] != "publish":
            continue
        ya = {os.path.basename(i["src"]).rsplit("-", 1)[0].split(".")[0] for i in p["images"]}
        nuevas = [r for r in rutas if os.path.basename(r).split(".")[0] not in ya]
        if nuevas:
            plan.append((p, nuevas))
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'} · {len(plan)} productos, "
          f"{sum(len(n) for _, n in plan)} fotos nuevas")
    if not aplicar:
        return
    s = requests.Session()
    s.auth = (os.getenv("WP_USER"), os.getenv("WP_APP_PASSWORD"))
    registro = []
    ruta_reg = os.path.join(_raiz, "respaldos_web", f"galeria_{datetime.now():%Y%m%d_%H%M%S}.json")
    for p, nuevas in plan:
        try:
            alt = (p["images"][0].get("alt") if p["images"] else "") or p["name"]
            ids = []
            for n, r in enumerate(nuevas, len(p["images"]) + 1):
                mid, _ = subir_imagen(s, r, f"{alt} - foto {n}", p["name"])
                ids.append(mid)
            imagenes = [{"id": i["id"]} for i in p["images"]] + [{"id": i} for i in ids]
            r = w.put(f"products/{p['id']}", {"images": imagenes}).json()
            if "id" not in r:
                raise RuntimeError(str(r)[:200])
            registro.append({"sku": p["sku"], "id": p["id"], "antes": [i["id"] for i in p["images"]], "nuevas": ids})
            print(f"✅ {p['sku']}: +{len(ids)}")
        except Exception as e:
            print(f"❌ {p['sku']}: {e}")
        json.dump(registro, open(ruta_reg, "w"), indent=1)
    print(f"📄 Registro: {ruta_reg}")


if __name__ == "__main__":
    main()
