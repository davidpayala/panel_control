"""
✂️ Separa los productos de LENTES que tienen varias variantes sin medida: cada color debe ser su propio
producto (solo los lentes con medida -XXXX comparten producto). Accesorios y pelucas no se tocan: en la
web son productos variables y ahí sí corresponde tener varias variantes.

Por cada producto con varios colores:
  · se queda en el producto original la variante cuyo color coincide con el nombre del producto (o la primera)
  · las demás pasan a un producto nuevo, copia del original, con:
      - modelo y nombre (color) tomados del nombre del producto en la web
      - color, contorno, diámetro, marca y categoría iguales a la web (así sync_lentes_web no cambia nada),
        salvo CORREGIR_WEB: valores que la sincronización copió por error de un color hermano y se
        devuelven a lo que tenía la web antes (sync_lentes_web los envía en la siguiente corrida)
      - foto y enlace de la tienda de su propio producto en la web
Guarda respaldo en respaldos_web/separar_variantes_<fecha>.json

Uso:
    .venv/bin/python recurrente/separar_variantes.py             # simulación
    .venv/bin/python recurrente/separar_variantes.py --aplicar
"""
import json
import os
import re
import sys
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from database import engine
import seo_woo
from comparar_lentes_web import FAMILIA

# familia de la web → color del panel (cuando el del producto original no corresponde)
COLOR_PANEL = {"Verde": "Verde", "Azul – Celeste": "Azul", "Gris – Blanco": "Gris", "Marrón – Chocolate": "Marrón",
               "Miel – Amarillo – Dorado": "Miel", "Negro": "Negro", "Rojo – Naranja": "Rojo",
               "Rosado - Morado": "Rosado", "Multicolor": "Multicolor"}
# Valores que la web tenía antes del 25-nov y que la sincronización pisó al compartir producto con otro color
CORREGIR_WEB = {
    "NL169": {"contorno": "Con Contorno"},
    "FL172": {"contorno": "Sin Contorno"},
    "KL037": {"contorno": "Sin Contorno"},
    "NL-MC040": {"contorno": "Con Contorno", "color": "Marrón – Chocolate"},
    "NL-MC014": {"color": "Marrón – Chocolate"},
}


def _n(s):
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def nombre_desde_web(nombre_web, sku, marca, modelo):
    """'Lentes de Contacto Fantasía Meetone Element Strom Browm (CL008)' → ('Element Strom', 'Browm')."""
    n = re.sub(r"\s*\(" + re.escape(sku) + r"\)\s*$", "", nombre_web, flags=re.I)
    n = re.sub(r"\s*\([^)]*\)\s*$", "", n)
    n = re.sub(r"^Lentes de Contacto\s+(de\s+)?(Fantas[ií]a\s+)?", "", n, flags=re.I).strip()
    for m in [x.strip() for x in (marca or "").split(",") if x.strip()]:
        if _n(n).startswith(_n(m) + " "):
            n = n[len(m):].strip()
    if modelo and _n(n).startswith(_n(modelo) + " "):
        return modelo, n[len(modelo):].strip()
    return None, n          # no se pudo separar modelo/color: se deja el modelo del panel y se avisa


def web_por_sku(w):
    out = {}
    for p in seo_woo.descargar_productos(w):
        if p.get("sku") and p["type"] == "simple":
            out[p["sku"].strip()] = p
    return out


def atributo(p, nombre):
    return next((", ".join(a.get("options") or []) for a in p.get("attributes", []) if a["name"] == nombre), "")


def plan():
    with engine.connect() as c:
        grupos = c.execute(text("""
            SELECT p.*, array_agg(v.sku ORDER BY v.sku) AS skus, array_agg(v.nombre_variante ORDER BY v.sku) AS nvs
            FROM Productos p JOIN Variantes v USING (id_producto)
            WHERE p.categoria IN ('Estilo Natural', 'Estilo Fantasía')
            GROUP BY p.id_producto
            HAVING COUNT(*) > 1 AND NOT bool_and(v.sku ~ '-[0-9]{4}$')""")).mappings().all()
    w = seo_woo.conectar_woo()
    web = web_por_sku(w)
    acciones, avisos = [], []
    for g in grupos:
        skus = list(g["skus"])
        queda = next((s for s, nv in zip(skus, g["nvs"]) if _n(nv) == _n(g["nombre"])), skus[0])
        for s in skus:
            x = web.get(s)
            if not x:
                avisos.append(f"{s}: no está en la web (producto #{g['id_producto']}), no se toca")
                continue
            modelo, color = nombre_desde_web(x["name"], s, g["marca"], g["modelo"])
            if modelo is None:
                avisos.append(f"{s}: no pude separar modelo/color de «{x['name']}»; queda modelo «{g['modelo']}», color «{color}»")
                modelo = g["modelo"]
            fam = CORREGIR_WEB.get(s, {}).get("color") or atributo(x, "Color")
            color_p = g["color_principal"] if FAMILIA.get(_n(g["color_principal"])) == fam else COLOR_PANEL.get(fam, g["color_principal"])
            contorno = CORREGIR_WEB.get(s, {}).get("contorno") or atributo(x, "Contorno") or (g["detalles_tecnicos"] or {}).get("contorno")
            diam = re.sub(r"\s*mm$", "", atributo(x, "Diametro")) or g["diametro"]
            try:
                diam = f"{float(diam):.1f}"
            except ValueError:
                pass
            acciones.append({
                "sku": s, "id_producto": g["id_producto"], "nuevo": s != queda,
                "modelo": modelo, "nombre": color, "color_principal": color_p, "contorno": contorno, "diametro": diam,
                "url_imagen": (x.get("images") or [{}])[0].get("src") or g["url_imagen"],
                "url_tienda": x.get("permalink"),
                "antes": f"{g['modelo']} | {g['nombre']} | {g['color_principal']}",
            })
    return grupos, acciones, avisos


def main():
    aplicar = "--aplicar" in sys.argv
    grupos, acciones, avisos = plan()
    nuevos = [a for a in acciones if a["nuevo"]]
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'} · {len(grupos)} productos con varios colores → "
          f"{len(nuevos)} productos nuevos")
    for a in acciones:
        print(f"  {'➕' if a['nuevo'] else '= '} {a['sku']:9} #{a['id_producto']:<5} {a['antes']:40} → "
              f"{a['modelo']} | {a['nombre']} | {a['color_principal']} | {a['contorno']} | {a['diametro']}")
    for x in avisos:
        print("  ⚠️", x)
    if not aplicar:
        return
    with engine.connect() as c:
        resp = [{k: str(v) for k, v in dict(g).items()} for g in grupos]
    ruta = os.path.join(_raiz, "respaldos_web", f"separar_variantes_{datetime.now():%Y%m%d_%H%M%S}.json")
    json.dump(resp, open(ruta, "w"), ensure_ascii=False, indent=1)
    print("📄 Respaldo:", ruta)
    with engine.begin() as c:
        for a in acciones:
            datos = {"m": a["modelo"], "n": a["nombre"], "cp": a["color_principal"], "d": a["diametro"],
                     "ct": a["contorno"], "img": a["url_imagen"], "url": a["url_tienda"], "id": a["id_producto"]}
            if a["nuevo"]:
                nid = c.execute(text("""
                    INSERT INTO Productos (categoria, marca, modelo, color_principal, diametro, url_imagen, url_compra, nombre,
                                           importacion, detalles_tecnicos, macro_categoria, video_youtube, url_tienda, nombre_proveedor)
                    SELECT categoria, marca, :m, :cp, :d, :img, url_compra, :n, importacion,
                           COALESCE(detalles_tecnicos, '{}'::jsonb) || jsonb_build_object('contorno', CAST(:ct AS text)),
                           macro_categoria, NULL, :url, NULL
                    FROM Productos WHERE id_producto = :id RETURNING id_producto"""), datos).scalar()
                c.execute(text("UPDATE Variantes SET id_producto = :nid, nombre_variante = :n WHERE sku = :s"),
                          {"nid": nid, "n": a["nombre"], "s": a["sku"]})
                print(f"  ➕ {a['sku']} → producto #{nid}")
            else:
                c.execute(text("""UPDATE Productos SET color_principal = :cp, diametro = :d,
                                  detalles_tecnicos = COALESCE(detalles_tecnicos, '{}'::jsonb) || jsonb_build_object('contorno', CAST(:ct AS text)),
                                  url_tienda = COALESCE(url_tienda, :url)
                                  WHERE id_producto = :id"""), datos)
    print("✅ listo")


if __name__ == "__main__":
    main()
