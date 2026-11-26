"""
🔧 Alineación inicial panel ↔ kmlentes.pe de los LENTES (decisiones del usuario, 25-nov-2026).

  · Contorno vacío en el panel → se copia de la web
  · Precio rebajado 0 en el panel → vacío (= sin rebaja)
  · Precio distinto → manda la web; si la web no tiene precio → se le pone el del panel
  · Diámetro: manda la web; si la web está vacía → se le pone el del panel
  · Marca "Otros" en el panel → la marca de la web · marca web "VisuaShow" → "VisualShow"
  · Marca "Eliminar" en el panel y sin stock → se borra (web: a la papelera; panel: si no tiene historial)
  · Conflicto de marca entre marcas que no son FreshLady ni Meetone y sin stock → se borra igual
  · El resto de conflictos de marca se listan para que decida el usuario

Uso:
    .venv/bin/python recurrente/alinear_lentes_web.py             # simulación
    .venv/bin/python recurrente/alinear_lentes_web.py --aplicar
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from database import engine
import seo_woo
from comparar_lentes_web import comparar, num

MARCAS_REG = {"freshlady", "meetone"}


def diam_web(v):
    return f"{float(v):.2f} mm"


def main():
    aplicar = "--aplicar" in sys.argv
    web, panel, comunes, difs = comparar()
    w = seo_woo.conectar_woo()
    acc_panel, acc_web, borrar, lista_marcas = [], defaultdict(dict), set(), []

    with engine.connect() as c:
        stock = {r[0]: r[1] for r in c.execute(text(
            "SELECT sku, COALESCE(stock_interno,0)+COALESCE(stock_externo,0)+COALESCE(stock_transito,0) FROM Variantes"))}

    for d in difs:
        s, campo, p, x = d["sku"], d["campo"], d["panel"], d["web"]
        idp = panel[s]["id_producto"]
        if campo == "contorno" and not p and x:
            acc_panel.append(("contorno", idp, s, x))
        elif campo == "rebajado" and p == 0 and x is None:
            acc_panel.append(("rebajado_null", idp, s, None))
        elif campo == "precio":
            if x is None:
                acc_web[s]["regular_price"] = f"{p:.0f}"
            else:
                acc_panel.append(("precio", idp, s, x))
        elif campo == "diametro":
            if x is None and p:
                acc_web[s]["diametro"] = diam_web(p)
            elif x is not None:
                acc_panel.append(("diametro", idp, s, f"{x:.1f}"))
        elif campo == "marca":
            pm, wm = (p or "").strip(), (x or "").strip()
            if pm == "Eliminar":
                (borrar.add(s) if stock.get(s, 0) <= 0 else lista_marcas.append((s, pm, wm, stock.get(s), d["web_nombre"])))
            elif pm.lower() == "otros" and wm and "," not in wm and wm.lower() != "otros":
                acc_panel.append(("marca", idp, s, wm))
            elif pm.lower().replace(" ", "") in ("visualshow",) and wm == "VisuaShow":
                pass  # se corrige renombrando la marca en la web
            else:
                ambas = {pm.lower()} | {m.strip().lower() for m in wm.split(",")}
                if not (ambas & MARCAS_REG) and stock.get(s, 0) <= 0:
                    borrar.add(s)
                else:
                    lista_marcas.append((s, pm, wm, stock.get(s), d["web_nombre"]))

    resumen = defaultdict(int)
    for a in acc_panel:
        resumen["panel:" + a[0]] += 1
    for s, cambios in acc_web.items():
        for k in cambios:
            resumen["web:" + k] += 1
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'}")
    for k, v in sorted(resumen.items()):
        print(f"   {k}: {v}")
    print(f"   borrar (sin stock): {len(borrar)} → {sorted(borrar)}")
    print(f"   conflictos de marca para el usuario: {len(lista_marcas)}")
    for x in lista_marcas:
        print("     ", x)
    json.dump(lista_marcas, open(os.path.join(_raiz, "documentos", "marcas_por_decidir.json"), "w"), ensure_ascii=False, indent=1)
    if not aplicar:
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tocados = set(acc_web) | borrar
    json.dump([w.get(f"products/{web[s]['id']}").json() for s in tocados],
              open(os.path.join(_raiz, "respaldos_web", f"alinear_lentes_{ts}.json"), "w"), ensure_ascii=False)
    with engine.connect() as c:
        filas = c.execute(text("""SELECT v.*, p.marca, p.diametro, p.detalles_tecnicos FROM Variantes v
                                  JOIN Productos p USING (id_producto) WHERE p.macro_categoria = 'Lentes'""")).mappings()
        json.dump([{k: str(v) for k, v in f.items()} for f in filas],
                  open(os.path.join(_raiz, "respaldos_web", f"alinear_lentes_panel_{ts}.json"), "w"), ensure_ascii=False)

    with engine.begin() as c:
        for tipo, idp, s, val in acc_panel:
            if tipo == "contorno":
                c.execute(text("""UPDATE Productos SET detalles_tecnicos = COALESCE(detalles_tecnicos,'{}'::jsonb)
                                  || jsonb_build_object('contorno', CAST(:v AS text)) WHERE id_producto = :i"""), {"v": val, "i": idp})
            elif tipo == "rebajado_null":
                c.execute(text("UPDATE Variantes SET precio_rebajado = NULL WHERE sku = :s"), {"s": s})
            elif tipo == "precio":
                c.execute(text("UPDATE Variantes SET precio = :v WHERE sku = :s"), {"v": val, "s": s})
            elif tipo == "diametro":
                c.execute(text("UPDATE Productos SET diametro = :v WHERE id_producto = :i"), {"v": val, "i": idp})
            elif tipo == "marca":
                c.execute(text("UPDATE Productos SET marca = :v WHERE id_producto = :i"), {"v": val, "i": idp})
    print("✅ panel actualizado")

    for s, cambios in acc_web.items():
        x = web[s]
        if "regular_price" in cambios:
            ruta = f"products/{x['id']}/variations/{x['var_id']}" if x.get("var_id") else f"products/{x['id']}"
            w.put(ruta, {"regular_price": cambios["regular_price"]})
        if "diametro" in cambios:
            p = w.get(f"products/{x['id']}").json()
            attrs = [{k: a[k] for k in ("id", "name", "position", "visible", "variation", "options")} for a in p["attributes"]]
            if any(a["name"] == "Diametro" for a in attrs):
                for a in attrs:
                    if a["name"] == "Diametro":
                        a["options"] = [cambios["diametro"]]
            else:
                attrs.append({"id": 11, "name": "Diametro", "position": len(attrs), "visible": True, "variation": False,
                              "options": [cambios["diametro"]]})
            w.put(f"products/{x['id']}", {"attributes": attrs})
    print(f"✅ web actualizada ({len(acc_web)} productos)")

    r = w.put("products/brands/495", {"name": "VisualShow", "slug": "visualshow"})
    print("marca VisuaShow →", r.json().get("name"))

    for s in sorted(borrar):
        x = web[s]
        if x.get("var_id"):
            w.delete(f"products/{x['id']}/variations/{x['var_id']}", params={"force": True})
        else:
            w.delete(f"products/{x['id']}")          # a la papelera (se puede recuperar 30 días)
        with engine.begin() as c:
            usos = sum(c.execute(text(f"SELECT COUNT(*) FROM {t} WHERE sku = :s"), {"s": s}).scalar()
                       for t in ("detalleventa", "movimientos", "stock_ubicaciones"))
            if usos == 0:
                idp = panel[s]["id_producto"]
                c.execute(text("DELETE FROM proveedor_mapeo WHERE sku = :s"), {"s": s})
                c.execute(text("DELETE FROM Variantes WHERE sku = :s"), {"s": s})
                if not c.execute(text("SELECT 1 FROM Variantes WHERE id_producto = :i"), {"i": idp}).fetchone():
                    c.execute(text("DELETE FROM Productos WHERE id_producto = :i"), {"i": idp})
                print(f"🗑️ {s}: web a la papelera, borrado del panel")
            else:
                print(f"🗑️ {s}: web a la papelera; en el panel tiene historial ({usos}), se deja")


if __name__ == "__main__":
    main()
