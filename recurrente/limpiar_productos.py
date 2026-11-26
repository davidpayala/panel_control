"""
🧹 Limpieza puntual del catálogo del panel (26-nov-2026, decisiones del usuario):

  1. Duplicados: se elimina uno y su historial (ventas, compras, movimientos, publicaciones, ventas
     anuales, stock mensual, ubicaciones, mapeo del proveedor) pasa al que se queda. En la web el
     eliminado va a la papelera.
         NL147 → NL063      (NL063 tiene 2 fotos, ventas y ubicación)
         NL-MC099 → NL169   (NL169 tiene foto con su código; el proveedor pasa a NL169)
  2. CL008: "Browm" → "Brown"
  3. NL-MC134: Meetone W Lolite (el proveedor lo llama Lolite Lolite; el mapeo no cambia)
  4. Productos sin ninguna variante: se eliminan (eran restos sin SKU)

Uso:
    .venv/bin/python recurrente/limpiar_productos.py            # simulación
    .venv/bin/python recurrente/limpiar_productos.py --aplicar
"""
import json
import os
import sys
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from sqlalchemy import text
from database import engine
import seo_woo

FUSIONES = {"NL147": "NL063", "NL-MC099": "NL169"}
RENOMBRES = {"CL008": {"nombre": "Brown"}, "NL-MC134": {"modelo": "W", "nombre": "Lolite"}}
MOVER_SKU = ["detalleventa", "movimientos", "historial_estados", "historial_facebook", "proveedor_mapeo"]
BORRAR_SKU = ["woo_permalinks", "auditoria_skus_woo", "ubicacion_marca"]


def fusionar(c, quita, queda, log):
    for t in MOVER_SKU:
        n = c.execute(text(f"UPDATE {t} SET sku = :k WHERE sku = :q"), {"k": queda, "q": quita}).rowcount
        if n:
            log(f"   {t}: {n} filas {quita} → {queda}")
    h = c.execute(text("SELECT v2023, v2024, v2025 FROM historialanual WHERE sku = :q"), {"q": quita}).fetchone()
    if h:
        if c.execute(text("SELECT 1 FROM historialanual WHERE sku = :k"), {"k": queda}).fetchone():
            c.execute(text("""UPDATE historialanual SET v2023 = COALESCE(v2023,0) + :a, v2024 = COALESCE(v2024,0) + :b,
                              v2025 = COALESCE(v2025,0) + :c WHERE sku = :k"""),
                      {"a": h[0] or 0, "b": h[1] or 0, "c": h[2] or 0, "k": queda})
            c.execute(text("DELETE FROM historialanual WHERE sku = :q"), {"q": quita})
        else:
            c.execute(text("UPDATE historialanual SET sku = :k WHERE sku = :q"), {"k": queda, "q": quita})
        log(f"   historialanual: ventas {tuple(h)} sumadas a {queda}")
    for mes, stock in c.execute(text("SELECT mes, stock FROM stock_mensual_historico WHERE sku = :q"), {"q": quita}).fetchall():
        n = c.execute(text("UPDATE stock_mensual_historico SET stock = COALESCE(stock,0) + :s WHERE sku = :k AND mes = :m"),
                      {"s": stock or 0, "k": queda, "m": mes}).rowcount
        if not n:
            c.execute(text("INSERT INTO stock_mensual_historico (mes, sku, stock) VALUES (:m, :k, :s)"),
                      {"m": mes, "k": queda, "s": stock or 0})
    c.execute(text("DELETE FROM stock_mensual_historico WHERE sku = :q"), {"q": quita})
    for idu, cant in c.execute(text("SELECT id_ubicacion, cantidad FROM stock_ubicaciones WHERE sku = :q"), {"q": quita}).fetchall():
        n = c.execute(text("UPDATE stock_ubicaciones SET cantidad = cantidad + :c WHERE sku = :k AND id_ubicacion = :u"),
                      {"c": cant, "k": queda, "u": idu}).rowcount
        if not n:
            c.execute(text("INSERT INTO stock_ubicaciones (sku, id_ubicacion, cantidad, fecha_actualizacion) VALUES (:k, :u, :c, NOW())"),
                      {"k": queda, "u": idu, "c": cant})
    c.execute(text("DELETE FROM stock_ubicaciones WHERE sku = :q"), {"q": quita})
    c.execute(text("""UPDATE Variantes k SET stock_interno = COALESCE(k.stock_interno,0) + COALESCE(q.stock_interno,0),
                          stock_externo = COALESCE(k.stock_externo,0) + COALESCE(q.stock_externo,0),
                          stock_transito = COALESCE(k.stock_transito,0) + COALESCE(q.stock_transito,0)
                      FROM Variantes q WHERE k.sku = :k AND q.sku = :q"""), {"k": queda, "q": quita})
    for t in BORRAR_SKU:
        c.execute(text(f"DELETE FROM {t} WHERE sku = :q"), {"q": quita})
    idp = c.execute(text("SELECT id_producto FROM Variantes WHERE sku = :q"), {"q": quita}).scalar()
    c.execute(text("DELETE FROM Variantes WHERE sku = :q"), {"q": quita})
    if idp and not c.execute(text("SELECT 1 FROM Variantes WHERE id_producto = :i"), {"i": idp}).fetchone():
        c.execute(text("DELETE FROM Productos WHERE id_producto = :i"), {"i": idp})
    log(f"   🗑️ {quita} eliminado del panel")


def main():
    aplicar = "--aplicar" in sys.argv
    log = print
    with engine.connect() as c:
        huerfanos = c.execute(text("""SELECT * FROM Productos p WHERE NOT EXISTS
                                      (SELECT 1 FROM Variantes v WHERE v.id_producto = p.id_producto)""")).mappings().all()
        afectados = c.execute(text("""SELECT v.*, p.* FROM Variantes v JOIN Productos p USING (id_producto)
                                      WHERE v.sku = ANY(:s)"""), {"s": list(FUSIONES) + list(FUSIONES.values()) + list(RENOMBRES)}).mappings().all()
    print(f"{'APLICANDO' if aplicar else 'SIMULACIÓN'}")
    print(f"  fusiones: {FUSIONES}")
    print(f"  renombres: {RENOMBRES}")
    print(f"  productos sin variantes a eliminar: {len(huerfanos)}")
    if not aplicar:
        return
    ruta = os.path.join(_raiz, "respaldos_web", f"limpiar_productos_{datetime.now():%Y%m%d_%H%M%S}.json")
    json.dump({"huerfanos": [{k: str(v) for k, v in dict(h).items()} for h in huerfanos],
               "afectados": [{k: str(v) for k, v in dict(a).items()} for a in afectados]},
              open(ruta, "w"), ensure_ascii=False, indent=1)
    print("📄 Respaldo:", ruta)

    w = seo_woo.conectar_woo()
    with engine.begin() as c:
        for quita, queda in FUSIONES.items():
            log(f"🔀 {quita} → {queda}")
            fusionar(c, quita, queda, log)
        for sku, campos in RENOMBRES.items():
            sets = ", ".join(f"{k} = :{k}" for k in campos)
            c.execute(text(f"UPDATE Productos SET {sets} WHERE id_producto = (SELECT id_producto FROM Variantes WHERE sku = :s)"),
                      {**campos, "s": sku})
            c.execute(text("UPDATE Variantes SET nombre_variante = :n WHERE sku = :s"), {"n": campos["nombre"], "s": sku})
            log(f"✏️ {sku}: {campos}")
        n = c.execute(text("""DELETE FROM Productos p WHERE NOT EXISTS
                              (SELECT 1 FROM Variantes v WHERE v.id_producto = p.id_producto)""")).rowcount
        log(f"🗑️ {n} productos sin variantes eliminados")
    for quita in FUSIONES:
        for p in w.get("products", params={"sku": quita}).json():
            r = w.delete(f"products/{p['id']}")          # a la papelera (recuperable 30 días)
            log(f"🌐 {quita}: web a la papelera ({r.status_code})")


if __name__ == "__main__":
    main()
