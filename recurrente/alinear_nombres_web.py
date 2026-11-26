"""
🏷️ Alineación inicial de nombres panel ← kmlentes.pe (26-nov-2026).

El nombre de la web se arma desde el panel con comparar_lentes_web.nombre_lente():
    Naturales: Lentes de Contacto {Marca} {Modelo} {Color} ({SKU sin medida})
    Fantasía:  Lentes de Contacto Fantasía {Modelo} {Color} ({SKU sin medida})
Antes de que el panel mande, se copia al panel el modelo/color que hoy muestra la web, cuando difieren.
Lo que solo difiere en la marca (fantasías con marca, naturales sin marca) no cambia el panel: la
sincronización corrige el nombre en la web según la regla.

Uso:
    .venv/bin/python recurrente/alinear_nombres_web.py             # simulación
    .venv/bin/python recurrente/alinear_nombres_web.py --aplicar
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
from comparar_lentes_web import comparar, sku_base, MARCAS_SIN_NOMBRE


# Recién corregidos por el usuario en el panel (limpiar_productos.py): manda el panel
MANDA_PANEL = {"CL008", "NL-MC134"}


def _mayusculas(s):
    return sum(ch.isupper() for ch in s)


def _n(s):
    return " ".join((s or "").split()).lower()


def resto_web(nombre_web, sku, marcas):
    """'Lentes de Contacto Fantasía FreshLady Kimetsu Akasa (FL017D)' → 'Kimetsu Akasa'."""
    n = re.sub(r"\s*\(" + re.escape(sku_base(sku)) + r"\)\s*$", "", nombre_web, flags=re.I)
    n = re.sub(r"^Lentes de Contacto\s+(Fantas[ií]a\s+)?", "", n, flags=re.I).strip()
    for m in marcas:
        if _n(n).startswith(_n(m) + " "):
            n = n[len(m):].strip()
            break
    return " ".join(n.split())


def propuesta(p, nombre_web, sku):
    """(modelo, color) que debe tener el panel según la web, o None si no se puede separar."""
    marcas = [m.strip() for m in (p["marca"] or "").split(",") if m.strip().lower() not in MARCAS_SIN_NOMBRE]
    r = resto_web(nombre_web, sku, marcas)
    modelo, color = (p["modelo"] or "").strip(), (p["nombre"] or "").strip()
    actual = " ".join(f"{modelo} {color}".split())
    if _n(r).replace(" ", "") == _n(actual).replace(" ", ""):
        # misma palabra con otras mayúsculas/espacios (o solo difiere la marca): gana la mejor escrita
        if _n(r) != _n(actual) or _mayusculas(actual) >= _mayusculas(r):
            return modelo, color
        return r[:len(modelo)].strip(), r[len(modelo):].strip()
    if modelo and _n(r).startswith(_n(modelo) + " "):
        return r[:len(modelo)].strip(), r[len(modelo):].strip()
    if color and _n(r).endswith(" " + _n(color)):
        return r[:-len(color)].strip(), r[-len(color):].strip()
    return None


def main():
    aplicar = "--aplicar" in sys.argv
    web, panel, comunes, difs = comparar()
    cambios, dudas, solo_web = {}, [], []
    for d in (d for d in difs if d["campo"] == "nombre"):
        s, p = d["sku"], panel[d["sku"]]
        prop = None if s in MANDA_PANEL else propuesta(p, d["web"], s)
        if s in MANDA_PANEL:
            solo_web.append((s, d["web"], d["panel"]))
        elif prop is None:
            dudas.append((s, d["web"], d["panel"]))
        elif prop != ((p["modelo"] or "").strip(), (p["nombre"] or "").strip()):
            cambios[p["id_producto"]] = (s, p["modelo"], p["nombre"], *prop)
        else:
            solo_web.append((s, d["web"], d["panel"]))
    # si con el cambio dos SKUs quedarían con el mismo modelo+color, se respeta el panel
    from collections import Counter
    finales = Counter()
    for s, p in panel.items():
        c = cambios.get(p["id_producto"])
        finales[_n(f"{c[3]} {c[4]}" if c else f"{p['modelo']} {p['nombre']}")] += 1
    for idp, c in list(cambios.items()):
        p = panel[c[0]]
        if finales[_n(f"{c[3]} {c[4]}")] > 1 and _n(f"{c[3]} {c[4]}") != _n(f"{c[1]} {c[2]}"):
            dudas.append((c[0], f"web lo deja igual a otro: {c[3]} {c[4]}", f"se queda {c[1]} {c[2]}"))
            del cambios[idp]
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'} · nombres distintos: "
          f"{sum(1 for d in difs if d['campo'] == 'nombre')}")
    print(f"\n✏️ panel toma modelo/color de la web: {len(cambios)}")
    for s, m0, n0, m1, n1 in sorted(cambios.values()):
        print(f"   {s:10} {m0} | {n0}  →  {m1} | {n1}")
    print(f"\n🌐 solo cambia la marca en el nombre de la web: {len(solo_web)}")
    for x in solo_web:
        print(f"   {x[0]:10} {x[1]}  →  {x[2]}")
    print(f"\n❓ no se pudo separar (la web tomará el nombre del panel): {len(dudas)}")
    for x in dudas:
        print(f"   {x[0]:10} web: {x[1]}  |  panel: {x[2]}")
    if not aplicar:
        return
    ruta = os.path.join(_raiz, "respaldos_web", f"alinear_nombres_{datetime.now():%Y%m%d_%H%M%S}.json")
    json.dump({"cambios": list(cambios.values()), "dudas": dudas, "solo_web": solo_web}, open(ruta, "w"),
              ensure_ascii=False, indent=1)
    with engine.begin() as c:
        for idp, (s, m0, n0, m1, n1) in cambios.items():
            c.execute(text("UPDATE Productos SET modelo = :m, nombre = :n WHERE id_producto = :i"), {"m": m1, "n": n1, "i": idp})
    print(f"✅ {len(cambios)} productos actualizados en el panel · respaldo {ruta}")


if __name__ == "__main__":
    main()
