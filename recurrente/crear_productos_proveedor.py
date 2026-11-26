"""
➕ Crea en el panel los lentes nuevos del proveedor a partir de documentos/codigos_propuestos.xlsx
   y los conecta al stock diario del proveedor (tabla proveedor_mapeo).

Por cada fila:
  · MODELO NUEVO / NUEVO COLOR → Productos (un producto por color, como el resto del catálogo) + Variantes
        marca, modelo, nombre = color, categoría, color_principal = color filtro, diámetro,
        detalles_tecnicos = {"contorno": "Con Contorno"/"Sin Contorno", "revisar_color": true?}
        precio S/55 sin rebaja (lentes con medida S/100), medida 0.00, stock interno 0
  · VINCULAR → no crea nada, solo conecta el modelo del proveedor con el SKU existente
  · Todas → proveedor_mapeo (y sale de proveedor_no_mapeados)
Además pasa a "Estilo Fantasía" los SKUs existentes que la propuesta marca así (NL-MC113).

No toca la web: sync_woo solo actualiza productos que ya existen en WooCommerce.
Las imágenes se suben después (url_imagen queda vacío).

Uso:
    .venv/bin/python recurrente/crear_productos_proveedor.py             # simulación
    .venv/bin/python recurrente/crear_productos_proveedor.py --aplicar
"""
import json
import os
import re
import sys

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

from openpyxl import load_workbook
from sqlalchemy import text
from database import engine

PROPUESTA = os.path.join(_raiz, "documentos", "codigos_propuestos.xlsx")
PRECIO, PRECIO_MEDIDA = 55, 100
COLORES_OK = {"Verde", "Gris", "Azul", "Chocolate", "Marrón", "Miel", "Negro", "Morado"}


def norm(v):
    return re.sub(r"\s+", " ", str(v if v is not None else "").strip()).lower()


def filas():
    ws = load_workbook(PROPUESTA, read_only=True, data_only=True).worksheets[0]
    datos = list(ws.iter_rows(values_only=True))
    cab = list(datos[0])
    return [dict(zip(cab, r)) for r in datos[1:] if r[cab.index("SKU KM propuesto")]]


def titulo(s):
    s = str(s or "").strip()
    return " ".join(w.capitalize() if w.isupper() and len(w) > 3 else w[:1].upper() + w[1:] for w in s.split())


def main():
    aplicar = "--aplicar" in sys.argv
    props = filas()
    with engine.begin() as conn:
        existentes = {r[0] for r in conn.execute(text("SELECT sku FROM Variantes"))}
        sin = {}
        for t, clave, cod, mod, col in conn.execute(text(
                "SELECT tipo, clave, codigo, modelo, color FROM proveedor_no_mapeados")):
            sin[(t, norm(cod), norm(mod), norm(col))] = clave

        creados, vinculos, fantasia, problemas = [], [], [], []
        for p in props:
            sku = str(p["SKU KM propuesto"]).strip()
            tipo = "freshlady" if p["Lista proveedor"] == "FreshLady" else "meetone"
            clave = sin.get((tipo, norm(p["Código proveedor"]), norm(p["Modelo proveedor"]), norm(p["Color proveedor"])))
            if clave is None:
                problemas.append(f"{sku}: no encontré el modelo en proveedor_no_mapeados ({p['Modelo proveedor']} {p['Color proveedor']})")
            else:
                vinculos.append(dict(tipo=tipo, clave=clave, sku=sku, codigo=p["Código proveedor"] or "",
                                     modelo=p["Modelo proveedor"] or "", color=p["Color proveedor"] or ""))

            if p["Acción"] == "VINCULAR":
                if p.get("Categoría") == "Estilo Fantasía":
                    fantasia.append(sku)
                continue
            if sku in existentes or any(s.startswith(sku + "-") for s in existentes):
                problemas.append(f"{sku}: ya existe en Variantes, no se crea")
                continue
            if sku in {c["sku"] for c in creados}:
                continue
            color_f = p.get("Color filtro") if p.get("Color filtro") in COLORES_OK else None
            detalles = {"contorno": {"Sí": "Con Contorno", "No": "Sin Contorno"}.get(p.get("Contorno"), "Por confirmar")}
            if p.get("Revisar color luego") == "Sí" or not color_f:
                detalles["revisar_color"] = True
            diam = p.get("Diámetro (mm)")
            creados.append(dict(
                sku=sku, marca=p["Lista proveedor"], modelo=titulo(p["Modelo KM"]), nombre=titulo(p["Color KM"]),
                categoria=p.get("Categoría") or "Estilo Natural", color=color_f,
                diametro=f"{float(diam):.1f}" if diam not in (None, "") else "N/A",
                detalles=json.dumps(detalles, ensure_ascii=False),
                precio=PRECIO_MEDIDA if re.search(r"-\d{4}$", sku) else PRECIO))

        print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'}")
        print(f"➕ Productos nuevos: {len(creados)}  (FreshLady {sum(c['marca'] == 'FreshLady' for c in creados)}, "
              f"Meetone {sum(c['marca'] == 'Meetone' for c in creados)}, "
              f"Fantasía {sum(c['categoria'] == 'Estilo Fantasía' for c in creados)})")
        print(f"🔗 Modelos del proveedor que quedan conectados: {len(vinculos)}")
        print(f"🎭 Pasan a Estilo Fantasía (ya existían): {fantasia}")
        sin_color = [c["sku"] for c in creados if not c["color"]]
        print(f"🎨 Sin color filtro (por confirmar): {sin_color}")
        for c in creados[:3]:
            print("   ej.", c)
        for pr in problemas:
            print("⚠️", pr)

        if not aplicar:
            print("ℹ️ Simulación: no se cambió nada (usa --aplicar)")
            return

        for c in creados:
            idp = conn.execute(text("""
                INSERT INTO Productos (marca, modelo, nombre, macro_categoria, categoria, color_principal, diametro, detalles_tecnicos)
                VALUES (:marca, :modelo, :nombre, 'Lentes', :categoria, :color, :diametro, CAST(:detalles AS JSONB))
                RETURNING id_producto"""), c).scalar()
            conn.execute(text("""
                INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, precio_rebajado, stock_externo)
                VALUES (:sku, :idp, :nombre, '0.00', 0, :precio, NULL, 0)"""), {**c, "idp": idp})
        for s in fantasia:
            conn.execute(text("""UPDATE Productos SET categoria = 'Estilo Fantasía'
                                 WHERE id_producto IN (SELECT id_producto FROM Variantes WHERE sku = :s)"""), {"s": s})
        for v in vinculos:
            conn.execute(text("""
                INSERT INTO proveedor_mapeo (tipo, clave, sku, codigo, modelo, color)
                VALUES (:tipo, :clave, :sku, :codigo, :modelo, :color)
                ON CONFLICT (tipo, clave) DO UPDATE SET sku = EXCLUDED.sku, actualizado = CURRENT_TIMESTAMP"""), v)
            conn.execute(text("DELETE FROM proveedor_no_mapeados WHERE tipo = :tipo AND clave = :clave"), v)
        print("✅ Listo. Corre recurrente/stock_proveedor.py --aplicar para cargar el stock del proveedor.")


if __name__ == "__main__":
    main()
