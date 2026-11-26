"""
🔄 Sincroniza LENTES panel → kmlentes.pe: precio, precio rebajado, contorno, color, marca, diámetro,
categoría (Naturales / Fantasía) y el sello de registro sanitario.

El panel manda: si cambias uno de esos datos en el panel, en la próxima corrida se copia a la web.
Lo llama sync_woo.py después de sincronizar el stock (5 veces al día); también se puede correr solo.

  · precio / precio rebajado → en el producto simple o en la variación de cada medida
  · color (familia), contorno, diámetro y marca → en el producto (para los lentes con medida, en el padre)
  · categoría → categoría + etiqueta + nombre ("Lentes de Contacto Fantasía …") + descripciones y SEO
  · registro sanitario → meta km_registro_sanitario: solo si TODO el stock (estantes con su marca,
    stock sin repartir y stock del proveedor) es FreshLady y/o Meetone (ver ubicaciones_marca.py)
  · si el dato está vacío en el panel no se borra en la web
  · freno: si hay más de MAX_CAMBIOS cambios en una corrida no envía nada y avisa (panel + WhatsApp)

Uso:
    .venv/bin/python recurrente/sync_lentes_web.py             # simulación
    .venv/bin/python recurrente/sync_lentes_web.py --aplicar
"""
import os
import sys
from collections import defaultdict

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import seo_woo
from comparar_lentes_web import comparar, FAMILIA, META_RS
from pasar_a_fantasia import nuevo_producto
import ubicaciones_marca as um
from database import engine

MAX_CAMBIOS = 150
# Lentes cuya marca está en conflicto panel/web y espera decisión del usuario: no se sincroniza la marca
PENDIENTES_MARCA = os.path.join(_raiz, "documentos", "marcas_por_decidir.json")


def marcas_pendientes():
    try:
        import json
        return {x[0] for x in json.load(open(PENDIENTES_MARCA))}
    except (OSError, ValueError):
        return set()
ATR = {"color": (2, "Color"), "contorno": (10, "Contorno"), "diametro": (11, "Diametro")}


def valor_web(campo, p):
    """Valor que debe tener la web según la fila del panel (None = no tocar)."""
    if campo == "precio":
        return f"{float(p['precio']):.2f}".rstrip("0").rstrip(".") if p["precio"] is not None else None
    if campo == "rebajado":
        v = p["precio_rebajado"]
        return "" if v in (None, 0) else f"{float(v):.2f}".rstrip("0").rstrip(".")
    if campo == "color":
        return FAMILIA.get((p["color_principal"] or "").strip().lower())
    if campo == "contorno":
        v = (p["detalles_tecnicos"] or {}).get("contorno")
        return v if v in ("Con Contorno", "Sin Contorno") else None
    if campo == "diametro":
        try:
            n = float(str(p["diametro"]).replace(",", "."))
            return f"{n:.2f} mm" if n > 0 else None
        except (TypeError, ValueError):
            return None
    if campo == "categoria":
        v = (p.get("categoria") or "").strip()
        return v if v in ("Estilo Natural", "Estilo Fantasía") else None
    if campo == "marca":
        v = [m.strip() for m in (p["marca"] or "").split(",") if m.strip()]
        return v if v and not any(m.lower() in ("otros", "eliminar") for m in v) else None


NOMBRE_MARCA = {"freshlady": "FreshLady", "meetone": "Meetone"}


def registro_deseado(web, panel, comunes):
    """{product_id: valor de META_RS} que debe tener cada producto de la web."""
    with engine.begin() as c:
        um.asegurar_columna(c)
        stock = um.marcas_con_stock(c)
    por_padre = defaultdict(lambda: {"stock": set(), "producto": set()})
    for s in comunes:
        d = por_padre[web[s]["id"]]
        d["stock"] |= stock.get(s, set())
        d["producto"] |= {m.strip().lower().replace(" ", "") for m in um.separar_marcas(panel[s]["marca"])}
    out = {}
    for pid, d in por_padre.items():
        if um.tiene_registro(d["stock"], d["producto"]):
            base = d["stock"] or d["producto"]
            out[pid] = " y ".join(NOMBRE_MARCA[m] for m in sorted(base, key=["freshlady", "meetone"].index))
        else:
            out[pid] = "no"
    return out


def sincronizar(aplicar=False, log=print):
    web, panel, comunes, difs = comparar()
    w = seo_woo.conectar_woo()
    marcas = {b["name"].lower(): b["id"] for b in w.get("products/brands", params={"per_page": 100}).json()}

    precios_simples, precios_var = [], defaultdict(list)   # lotes
    por_padre = defaultdict(dict)                          # {product_id: {campo: valor}}
    omitidos = []
    pendientes = marcas_pendientes()
    for d in difs:
        if d["campo"] == "marca" and d["sku"] in pendientes:
            continue
        s, campo = d["sku"], d["campo"]
        nuevo = valor_web(campo, panel[s])
        if nuevo is None:
            continue
        x = web[s]
        if campo in ("precio", "rebajado"):
            clave = "regular_price" if campo == "precio" else "sale_price"
            if x.get("var_id"):
                precios_var[x["id"]].append({"id": x["var_id"], clave: nuevo})
            else:
                precios_simples.append({"id": x["id"], clave: nuevo})
        elif campo == "marca":
            por_padre[x["id"]]["marca"] = nuevo          # lista de nombres; los ids se resuelven al enviar
        elif campo == "categoria":
            if x["categoria"]:                           # solo lentes que ya están en Naturales o Fantasía
                por_padre[x["id"]]["categoria"] = nuevo
        else:
            por_padre[x["id"]][campo] = nuevo

    # registro sanitario: solo cambia la meta → va por lotes
    rs_actual = {}
    for s in comunes:
        rs_actual[web[s]["id"]] = web[s].get("rs")
    rs_lote = [{"id": pid, "meta_data": [{"key": META_RS, "value": v}]}
               for pid, v in registro_deseado(web, panel, comunes).items() if rs_actual.get(pid) != v]

    total = len(precios_simples) + sum(len(v) for v in precios_var.values()) + len(por_padre) + len(rs_lote)
    n_cat = sum(1 for c in por_padre.values() if "categoria" in c)
    log(f"🔄 Lentes panel→web: {len(precios_simples)} precios simples, "
        f"{sum(len(v) for v in precios_var.values())} precios de medidas, {len(por_padre)} productos con atributos/marca/categoría "
        f"({n_cat} cambian de categoría), {len(rs_lote)} sellos de registro sanitario")
    for o in omitidos[:10]:
        log("   ⚠️ " + o)
    if total > MAX_CAMBIOS:
        msg = (f"⛔ La sincronización de lentes panel→web encontró {total} cambios (máximo {MAX_CAMBIOS}) y no envió "
               "nada por seguridad. Si fue un cambio masivo intencional, córrela a mano: recurrente/sync_lentes_web.py --aplicar --sin-freno")
        log(msg)
        if aplicar and "--sin-freno" not in sys.argv:
            try:
                from stock_proveedor import alertar
                alertar("sync_lentes_freno", msg, aplicar=True)
            except Exception as e:
                log(f"   no se pudo avisar: {e}")
            return 0
    if not aplicar:
        return total

    hechos = 0
    for i in range(0, len(precios_simples), 50):
        r = w.post("products/batch", {"update": precios_simples[i:i + 50]})
        hechos += len(r.json().get("update", [])) if r.status_code in (200, 201) else 0
    for pid, lote in precios_var.items():
        r = w.post(f"products/{pid}/variations/batch", {"update": lote})
        hechos += len(r.json().get("update", [])) if r.status_code in (200, 201) else 0
    for i in range(0, len(rs_lote), 50):
        r = w.post("products/batch", {"update": rs_lote[i:i + 50]})
        hechos += len(r.json().get("update", [])) if r.status_code in (200, 201) else 0
        if r.status_code not in (200, 201):
            log(f"   ❌ registro sanitario: {r.text[:200]}")
    for pid, cambios in por_padre.items():
        prod = w.get(f"products/{pid}").json()
        attrs = [{k: a[k] for k in ("id", "name", "position", "visible", "variation", "options")}
                 for a in prod.get("attributes", [])]
        for campo, (aid, nombre) in ATR.items():
            if campo not in cambios:
                continue
            a = next((a for a in attrs if a["id"] == aid or a["name"] == nombre), None)
            if a:
                a["options"] = [cambios[campo]]
            else:
                attrs.append({"id": aid, "name": nombre, "position": len(attrs), "visible": True,
                              "variation": False, "options": [cambios[campo]]})
        datos = {"attributes": attrs}
        if "categoria" in cambios:
            prod["attributes"] = attrs
            datos = nuevo_producto(prod, color_morado=False, fantasia=cambios["categoria"] == "Estilo Fantasía")
            log(f"   🗂️ {prod.get('sku') or pid}: → {cambios['categoria']} ({datos['name']})")
        if "marca" in cambios:
            ids = []
            for nombre in cambios["marca"]:
                clave = nombre.lower().replace(" ", "")
                bid = next((i for n, i in marcas.items() if n.replace(" ", "") == clave), None)
                if bid is None:                      # marca nueva: se crea en la web
                    r_m = w.post("products/brands", {"name": nombre})
                    bid = r_m.json().get("id")
                    if bid:
                        marcas[nombre.lower()] = bid
                        log(f"   ➕ marca creada en la web: {nombre}")
                if bid:
                    ids.append({"id": bid})
            datos["brands"] = ids
        r = w.put(f"products/{pid}", datos)
        hechos += 1 if r.status_code in (200, 201) else 0
        if r.status_code not in (200, 201):
            log(f"   ❌ producto {pid}: {r.text[:200]}")
    log(f"✅ Lentes panel→web: {hechos} actualizaciones enviadas")
    return hechos


if __name__ == "__main__":
    sincronizar(aplicar="--aplicar" in sys.argv)
