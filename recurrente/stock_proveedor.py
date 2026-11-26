"""
📦 Stock del proveedor → stock_externo del panel (reemplaza el copiar/pegar del Excel "Stock Lentes").

Descarga la hoja del proveedor ("STOCK LENTES,", enlace público) y la traduce a SKUs KM con la misma
lógica que tenía el Excel:

  · lentes de color, bloque MEETONE   (Modelo, Color, Estado)          clave = "modelo - color"
  · lentes de color, bloque FRESHLADY (Código, Modelo, Color, Estado)  clave = "código - color"
  · CosplayHalloween                  (Nombre, Estado)                 clave = "nombre"
      DISPONIBLE → 10 · AGOTADO / DISCONTINUADO / VER EN MEETONE → 0 · número → ese número (máx. 10)
  · medida con color: tabla medida × modelo. Cada columna se reconoce por su nombre (fila 3), así que
      no importa si el proveedor la mueve de lugar.  SKU = <código KM>-<medida×100 con 4 cifras>
      (−1.25 → NL032C-0125).  DISPONIBLE → 1, AGOTADO / No hacen → 0.
      La variante sin medida (-0000) toma el stock del mismo modelo en la lista de colores.
  · fijos: SKUs que no están en la hoja del proveedor pero siempre hay (solución de líquido).

El diccionario proveedor → SKU KM vive en la tabla proveedor_mapeo (se llena la primera vez con
recurrente/datos/mapeo_proveedor_inicial.json, sacado de las columnas BB:BU de 'Lente 2').
Los modelos del proveedor que no tienen SKU quedan en proveedor_no_mapeados, para asignarlos desde
el panel (Inventario → Importar Stock Externo).

Es el único proveedor: los SKUs que su hoja no lista (o sin SKU en el diccionario) quedan con stock_externo = 0.
Si la descarga falla o la hoja cambió de forma (faltan pestañas, columnas movidas, muchos menos productos),
no actualiza nada y avisa en el panel (alertas de la barra lateral) y por WhatsApp. También avisa cuando
el proveedor agrega modelos que todavía no tienen código KM.

Uso:
    .venv/bin/python recurrente/stock_proveedor.py             # simulación: muestra qué cambiaría
    .venv/bin/python recurrente/stock_proveedor.py --aplicar   # actualiza la base de datos
    .venv/bin/python recurrente/stock_proveedor.py --archivo=prov.xlsx   # usa un archivo ya descargado
    Cron: todos los días 07:40, antes del sync_woo de las 08:00.
"""
import io
import json
import os
import re
import sys
import urllib.request
from datetime import datetime

_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from dotenv import load_dotenv
load_dotenv(os.path.join(_raiz, ".env"))

from openpyxl import load_workbook
from sqlalchemy import text
from database import engine

HOJA_PROVEEDOR = "19zQzq5KWhsbA8NgNsWF0QySsX_AcD8gXjQfdW9kxd-0"
URL_EXPORT = f"https://docs.google.com/spreadsheets/d/{HOJA_PROVEEDOR}/export?format=xlsx"
SEMILLA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datos", "mapeo_proveedor_inicial.json")

# "Freshlady" en la lista Meetone = el proveedor sugiere un similar de la otra marca → para nosotros es agotado
ESTADOS = {"DISPONIBLE": 10, "AGOTADO": 0, "DISCONTINUADO": 0, "VER EN MEETONE": 0, "FRESHLADY": 0}
MAX_STOCK = 10
FIJOS = {"7751030000173": 10, "7751030000494": 10}  # solución de líquido
MINIMO_SKUS = 200  # con menos que esto la hoja vino rota: no se toca nada
CAIDA_MAXIMA = 0.30          # si salen 30 % menos SKUs que en la última corrida buena, algo cambió en la hoja
MAX_ESTADOS_RAROS = 15       # muchos estados que no se entienden = columnas movidas
ALERTA_TEL = "51986203398"   # WhatsApp que recibe los avisos (desde la sesión "principal")
ALERTA_SESION = "principal"


class HojaRota(Exception):
    """La hoja del proveedor cambió de forma o vino incompleta: no se actualiza nada."""


def alertar(clave, mensaje, aplicar, repetir_cada_horas=20):
    """Aviso en el panel (bandeja de alertas de la barra lateral) + WhatsApp.
    El mismo aviso (misma clave) no se repite antes de `repetir_cada_horas`."""
    print(f"🚨 {mensaje}")
    if not aplicar:
        print("   (simulación: no se envía el aviso)")
        return
    texto = f"📦 *Stock del proveedor de lentes*\n{mensaje}"
    try:
        with engine.begin() as conn:
            ya = conn.execute(text("""
                SELECT 1 FROM webhook_logs WHERE session_name = 'proveedor' AND payload LIKE :c
                  AND fecha > NOW() - make_interval(hours => :h) LIMIT 1"""),
                {"c": f'%"clave": "{clave}"%', "h": repetir_cada_horas}).fetchone()
            if ya:
                print("   (ya se avisó hace poco; no se repite)")
                return
            conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) "
                              "VALUES ('proveedor', 'ALERTA_CRITICA', :p)"),
                         {"p": json.dumps({"mensaje": texto.replace("*", ""), "clave": clave}, ensure_ascii=False)})
    except Exception as e:
        print(f"   no se pudo guardar el aviso en el panel: {e}")
    try:
        from utils import enviar_mensaje_whatsapp
        ok = enviar_mensaje_whatsapp(ALERTA_TEL, texto, session=ALERTA_SESION)
        print("   WhatsApp enviado" if ok else "   ⚠️ WhatsApp no se pudo enviar")
    except Exception as e:
        print(f"   ⚠️ WhatsApp no se pudo enviar: {e}")


def norm(v):
    return re.sub(r"\s+", " ", str(v if v is not None else "").strip()).lower()


def txt(v):
    return re.sub(r"\s+", " ", str(v if v is not None else "").strip())


def stock_de(estado):
    """Estado del proveedor → unidades. None si no se entiende (fila se ignora y se reporta)."""
    e = txt(estado).upper()
    if e in ESTADOS:
        return ESTADOS[e]
    try:
        return max(0, min(int(float(e)), MAX_STOCK))
    except ValueError:
        return None


def sku_medida(base, medida):
    return f"{base}-{int(round(abs(medida) * 100)):04d}"


# ---------------------------------------------------------------- base de datos
def preparar_tablas(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS proveedor_mapeo (
            tipo VARCHAR(20) NOT NULL,          -- meetone | freshlady | halloween | medida
            clave VARCHAR(200) NOT NULL,        -- texto normalizado con que se busca en la hoja
            sku VARCHAR(60) NOT NULL,
            codigo VARCHAR(60), modelo VARCHAR(120), color VARCHAR(120),
            actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tipo, clave))
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS proveedor_no_mapeados (
            tipo VARCHAR(20) NOT NULL, clave VARCHAR(200) NOT NULL,
            codigo VARCHAR(60), modelo VARCHAR(120), color VARCHAR(120), estado VARCHAR(60),
            visto TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tipo, clave))
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS proveedor_sync_log (
            id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            aplicado BOOLEAN, skus_proveedor INT, actualizados INT, sin_mapeo INT,
            sku_inexistentes INT, detalle TEXT)
    """))
    vacia = conn.execute(text("SELECT COUNT(*) FROM proveedor_mapeo")).scalar() == 0
    if vacia and os.path.exists(SEMILLA):
        filas = json.load(open(SEMILLA, encoding="utf-8"))
        for f in filas:
            conn.execute(text("""
                INSERT INTO proveedor_mapeo (tipo, clave, sku, codigo, modelo, color)
                VALUES (:tipo, :clave, :sku, :codigo, :modelo, :color) ON CONFLICT DO NOTHING
            """), f)
        print(f"🌱 Diccionario inicial cargado: {len(filas)} modelos")


def cargar_mapeo(conn):
    mapa = {}
    for tipo, clave, sku in conn.execute(text("SELECT tipo, clave, sku FROM proveedor_mapeo")):
        if sku and sku.strip():
            mapa[(tipo, clave)] = sku.strip()
    return mapa


# ---------------------------------------------------------------- lectura de la hoja
def descargar(archivo=None):
    if archivo:
        return load_workbook(archivo, read_only=True, data_only=True)
    req = urllib.request.Request(URL_EXPORT, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        datos = r.read()
    if not datos.startswith(b"PK"):
        raise RuntimeError("Google no devolvió un Excel (¿el proveedor quitó el acceso por enlace?)")
    return load_workbook(io.BytesIO(datos), read_only=True, data_only=True)


def filas(wb, nombre):
    for ws in wb.worksheets:
        if ws.title.strip().lower() == nombre:
            return [list(r) for r in ws.iter_rows(values_only=True)]
    raise RuntimeError(f"No encontré la pestaña '{nombre}' en la hoja del proveedor")


def col(r, i):
    return r[i] if i < len(r) else None


def leer_proveedor(wb):
    """Devuelve lista de dicts {tipo, clave, codigo, modelo, color, estado, medida?}."""
    items = []
    # lentes de color: B=#, C=código, D=modelo, E=color, F=estado | H=modelo, I=color, J=estado (Meetone)
    for r in filas(wb, "lentes de color")[2:]:
        cod, mod, colr, est = txt(col(r, 2)), txt(col(r, 3)), txt(col(r, 4)), col(r, 5)
        if colr and est is not None:
            items.append(dict(tipo="freshlady", clave=f"{norm(cod)} - {norm(colr)}",
                              codigo=cod, modelo=mod, color=colr, estado=txt(est)))
        mod, colr, est = txt(col(r, 7)), txt(col(r, 8)), col(r, 9)
        if colr and est is not None:
            items.append(dict(tipo="meetone", clave=f"{norm(mod)} - {norm(colr)}",
                              codigo="", modelo=mod, color=colr, estado=txt(est)))
    for r in filas(wb, "cosplayhalloween")[1:]:
        nom = txt(col(r, 0))
        if nom:
            items.append(dict(tipo="halloween", clave=norm(nom), codigo="", modelo=nom, color="",
                              estado=txt(col(r, 1))))
    # medida con color: fila 2 = código proveedor, fila 3 = nombre, col A = medida
    mc = filas(wb, "medida con color")
    codigos, nombres = mc[1], mc[2]
    for c in range(1, len(nombres)):
        nom = txt(nombres[c])
        if not nom:
            continue
        for r in mc[3:]:
            try:
                medida = float(col(r, 0))
            except (TypeError, ValueError):
                continue
            items.append(dict(tipo="medida", clave=norm(nom), codigo=txt(col(codigos, c)), modelo=nom,
                              color="", estado=txt(col(r, c)), medida=medida))
    return items


# ---------------------------------------------------------------- traducción
def traducir(items, mapa):
    stock = {}          # sku → unidades
    sin_mapeo = {}      # (tipo, clave) → item
    raros = []          # estados que no se entienden
    bases_medida = set()
    for it in items:
        sku = mapa.get((it["tipo"], it["clave"]))
        if not sku:
            sin_mapeo.setdefault((it["tipo"], it["clave"]), it)
            continue
        if it["tipo"] == "medida":
            bases_medida.add(sku)
            stock[sku_medida(sku, it["medida"])] = 1 if it["estado"].upper() == "DISPONIBLE" else 0
            continue
        u = stock_de(it["estado"])
        if u is None:
            raros.append(f"{it['modelo']} {it['color']} → '{it['estado']}'")
            continue
        stock.setdefault(sku, u)   # como BUSCARV: gana la primera fila
    for base in bases_medida:      # sin medida = stock de la lista de colores
        stock[f"{base}-0000"] = stock.pop(base, 0)
    for sku, u in FIJOS.items():
        stock.setdefault(sku, u)
    return stock, sin_mapeo, raros


def main():
    aplicar = "--aplicar" in sys.argv
    try:
        ejecutar(aplicar)
    except Exception as e:
        motivo = str(e) if isinstance(e, HojaRota) else f"{type(e).__name__}: {e}"
        alertar("roto", "⛔ El actualizador de inventario NO pudo leer la hoja del proveedor, así que hoy no se "
                        f"actualizó el stock externo.\nMotivo: {motivo[:300]}\n"
                        "Revisa si el proveedor cambió pestañas o columnas.", aplicar)
        if aplicar:
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO proveedor_sync_log (aplicado, skus_proveedor, detalle) "
                                  "VALUES (FALSE, 0, :d)"), {"d": motivo[:1000]})
        sys.exit(1)


def ejecutar(aplicar):
    archivo = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--archivo=")), None)
    print(f"🕐 {datetime.now():%Y-%m-%d %H:%M} · {'APLICANDO' if aplicar else 'SIMULACIÓN'}")

    with engine.begin() as conn:
        preparar_tablas(conn)
        mapa = cargar_mapeo(conn)

    items = leer_proveedor(descargar(archivo))
    stock, sin_mapeo, raros = traducir(items, mapa)
    print(f"📄 Filas leídas del proveedor: {len(items)} · SKUs KM resultantes: {len(stock)}")

    # ---- ¿la hoja sigue teniendo la forma que conocemos? ----
    if len(stock) < MINIMO_SKUS:
        raise HojaRota(f"solo salieron {len(stock)} productos reconocidos (mínimo {MINIMO_SKUS}); la hoja vino incompleta")
    claves_hoja = {(it["tipo"], it["clave"]) for it in items}
    conocidas = [k for k in mapa if k[0] != "medida"]
    perdidas = [k for k in conocidas if k not in claves_hoja]
    if conocidas and len(perdidas) / len(conocidas) > CAIDA_MAXIMA:
        raise HojaRota(f"{len(perdidas)} de {len(conocidas)} modelos conocidos ya no aparecen donde estaban "
                       f"(ej.: {', '.join(k[1] for k in perdidas[:3])}); parece que movieron columnas")
    with engine.connect() as conn:
        previo = conn.execute(text("SELECT skus_proveedor FROM proveedor_sync_log WHERE aplicado "
                                   "ORDER BY id DESC LIMIT 1")).scalar()
    if previo and len(stock) < previo * (1 - CAIDA_MAXIMA):
        raise HojaRota(f"salieron {len(stock)} productos y en la última corrida {previo}; faltan demasiados")
    if len(raros) > MAX_ESTADOS_RAROS:
        alertar("estados", f"⚠️ La hoja del proveedor trae {len(raros)} estados que no se entienden "
                           f"(ej.: {'; '.join(raros[:3])}). El stock se actualizó, pero revisa si cambió el formato.",
                aplicar)

    with engine.begin() as conn:
        actuales = {s: e for s, e in conn.execute(text("SELECT sku, stock_externo FROM Variantes"))}
        inexistentes = sorted(s for s in stock if s not in actuales)
        cambios = [(s, u) for s, u in stock.items() if s in actuales and (actuales[s] or 0) != u]
        # Único proveedor: lo que no aparece en su hoja queda en 0
        a_cero = sorted(s for s, e in actuales.items() if s not in stock and (e or 0) != 0)
        cambios += [(s, 0) for s in a_cero]

        print(f"🔁 Cambian: {len(cambios)} (de ellos {len(a_cero)} a 0 por no estar en la hoja del proveedor) · "
              f"ya estaban igual: {len(stock) - len(cambios) + len(a_cero) - len(inexistentes)}")
        for s, u in sorted(cambios)[:40]:
            print(f"   {s}: {actuales[s] or 0} → {u}")
        if len(cambios) > 40:
            print(f"   … y {len(cambios) - 40} más")
        if inexistentes:
            print(f"⚠️ {len(inexistentes)} SKUs del diccionario no existen en Variantes: {', '.join(inexistentes[:25])}"
                  + (" …" if len(inexistentes) > 25 else ""))
        print(f"❓ Modelos del proveedor sin SKU KM: {len(sin_mapeo)}")
        for r in raros:
            print(f"   estado no reconocido (se ignoró): {r}")

        previos = {(t, c) for t, c in conn.execute(text("SELECT tipo, clave FROM proveedor_no_mapeados"))}
        nuevos = [it for k, it in sin_mapeo.items() if k not in previos]
        if nuevos:
            lineas = "\n".join(f"• {it['tipo'].capitalize()}: {it['modelo']} {it['color']} ({it['estado']})".strip()
                               for it in nuevos[:15])
            alertar("nuevo:" + ",".join(sorted(f"{it['tipo']}|{it['clave']}" for it in nuevos))[:200],
                    f"🆕 El proveedor agregó {len(nuevos)} modelo(s) que no tienen código KM:\n{lineas}"
                    + (f"\n… y {len(nuevos) - 15} más" if len(nuevos) > 15 else "")
                    + "\nAsígnales código en el panel: Inventario → Importar Stock Externo.", aplicar,
                    repetir_cada_horas=24 * 30)

        if aplicar:
            for s, u in cambios:
                conn.execute(text("UPDATE Variantes SET stock_externo = :u WHERE sku = :s"), {"u": u, "s": s})
            conn.execute(text("DELETE FROM proveedor_no_mapeados"))
            for (tipo, clave), it in sin_mapeo.items():
                conn.execute(text("""
                    INSERT INTO proveedor_no_mapeados (tipo, clave, codigo, modelo, color, estado)
                    VALUES (:tipo, :clave, :codigo, :modelo, :color, :estado)
                """), {k: it.get(k, "") for k in ("tipo", "clave", "codigo", "modelo", "color", "estado")})
            detalle = json.dumps({"inexistentes": inexistentes[:200], "estados_raros": raros[:50]}, ensure_ascii=False)
            conn.execute(text("""
                INSERT INTO proveedor_sync_log (aplicado, skus_proveedor, actualizados, sin_mapeo, sku_inexistentes, detalle)
                VALUES (TRUE, :n, :c, :m, :i, :d)
            """), {"n": len(stock), "c": len(cambios), "m": len(sin_mapeo), "i": len(inexistentes), "d": detalle})
    print("✅ Listo" if aplicar else "ℹ️ Simulación: no se cambió nada (usa --aplicar)")


if __name__ == "__main__":
    main()
