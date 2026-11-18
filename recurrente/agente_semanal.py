"""
agente_semanal.py — Agente de análisis semanal para K&M.

Qué hace (una vez por semana):
  1. Lee tus ventas, clientes, stock y mensajes (SOLO LECTURA sobre tus tablas).
  2. Calcula: avance de la meta de lentes, semana vs semana anterior, clientes en
     ventana de recompra, stock por reponer, stock dormido, top productos y
     resultados del bot de marketing.
  3. Guarda el resultado en dos tablas nuevas (agente_reportes y agente_recompra)
     para que lo veas en el ERP (módulo REPORTE).
  4. Opcional: pide a Ollama (IA local) un plan de acciones en lenguaje natural.

NO envía ningún mensaje de WhatsApp. NO modifica ventas ni stock.
Además prepara los GRUPOS del bot de marketing (tabla bot_grupos) y, solo para el grupo 4,
marca «sin publicidad» a quien ya recibió 3 mensajes sin responder (se puede apagar con AUTO_SIN_MKT=0).

Uso:
    python agente_semanal.py              # análisis completo (usa Ollama si está encendido)
    python agente_semanal.py --sin-ia     # solo números, sin Ollama
    python agente_semanal.py --prueba     # no guarda nada, solo muestra el resultado

Parámetros opcionales en tu archivo .env (todos tienen valor por defecto):
    META_LENTES_MES=75          unidades de lentes al mes que necesitas
    PRECIO_LENTE=55             precio de venta (para escenarios)
    COSTO_LENTE=27              costo por unidad con envío (para margen estimado)
    RECOMPRA_MIN_DIAS=45        desde cuántos días sin comprar se considera "toca recomprar"
    RECOMPRA_MAX_DIAS=180       hasta cuántos días dura esa ventana
    DORMIDO_MAX_DIAS=540        pasado esto el cliente se considera "perdido"
    COBERTURA_MIN_DIAS=30       alerta de reposición si el stock dura menos de esto
    TASA_RECOMPRA_SUPUESTA=0.10 % de clientes en ventana que se asume recompran (escenario)
    ESPERA_G1_DIAS=30  ESPERA_G2_DIAS=90  ESPERA_G3_DIAS=180  ESPERA_G4_DIAS=240   (días mínimos entre mensajes del bot)
    MAX_SIN_RESPUESTA_G4=3      mensajes sin respuesta tras los cuales el grupo 4 pasa a «sin publicidad»
    AUTO_SIN_MKT=1              0 = no marcar automáticamente a nadie como «sin publicidad»
    AUTO_RESPUESTA_MIN_TELEFONOS=3  mismo texto largo desde N teléfonos distintos = respuesta automática
    OLLAMA_URL / OLLAMA_MODEL   ya los usa tu bot de marketing
"""
import argparse
import calendar
import json
import os
import re
import sys
import traceback
import unicodedata

# Permite ejecutar este archivo desde cualquier carpeta (por ejemplo "recurrente/"):
# busca database.py en esta carpeta y en la carpeta superior.
_AQUI = os.path.dirname(os.path.abspath(__file__))
for _carpeta in (_AQUI, os.path.dirname(_AQUI)):
    if os.path.exists(os.path.join(_carpeta, "database.py")) and _carpeta not in sys.path:
        sys.path.insert(0, _carpeta)
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

from database import engine  # database.py ya carga tu .env


# ==============================================================================
# CONFIGURACIÓN (se puede ajustar desde .env sin tocar el código)
# ==============================================================================
def _env_num(nombre, defecto, tipo=float):
    try:
        return tipo(os.getenv(nombre, defecto))
    except (TypeError, ValueError):
        return tipo(defecto)


META_LENTES_MES = _env_num("META_LENTES_MES", 75, int)
PRECIO_LENTE = _env_num("PRECIO_LENTE", 55)
COSTO_LENTE = _env_num("COSTO_LENTE", 27)
RECOMPRA_MIN_DIAS = _env_num("RECOMPRA_MIN_DIAS", 45, int)
RECOMPRA_MAX_DIAS = _env_num("RECOMPRA_MAX_DIAS", 180, int)
DORMIDO_MAX_DIAS = _env_num("DORMIDO_MAX_DIAS", 540, int)
ANTIGUO_MAX_DIAS = _env_num("ANTIGUO_MAX_DIAS", 3650, int)  # más allá de esto: "Perdido" (no se lista)
COBERTURA_MIN_DIAS = _env_num("COBERTURA_MIN_DIAS", 30, int)
TASA_RECOMPRA_SUPUESTA = _env_num("TASA_RECOMPRA_SUPUESTA", 0.10)
ESPERA_GRUPO = {1: _env_num("ESPERA_G1_DIAS", 30, int), 2: _env_num("ESPERA_G2_DIAS", 90, int),
                3: _env_num("ESPERA_G3_DIAS", 180, int), 4: _env_num("ESPERA_G4_DIAS", 240, int)}
MAX_SIN_RESPUESTA_G4 = _env_num("MAX_SIN_RESPUESTA_G4", 3, int)
AUTO_SIN_MKT = os.getenv("AUTO_SIN_MKT", "1").strip() != "0"
AUTO_MIN_TELEFONOS = _env_num("AUTO_RESPUESTA_MIN_TELEFONOS", 3, int)
VENTA_RECIENTE_DIAS = 60     # quien compró hace menos de esto no entra a ningún grupo (lo atiende la post-venta)
DIAS_ANO = 365
MAX_CANDIDATOS_GUARDADOS = 3000
REPORTES_A_CONSERVAR = 12

# Textos sugeridos para contactar (edítalos a tu gusto). Se envían A MANO desde el
# ERP con un clic (enlace de WhatsApp), así no arriesgas el número con envíos masivos.
PLANTILLAS = {
    "Recompra probable": (
        "{saludo} Hace un tiempo llevaste {producto}. ¿Cómo te fue? "
        "Si quieres renovar o probar otro color, te cuento qué tengo disponible 😊"
    ),
    "Dormido": (
        "{saludo} Hace tiempo que no hablamos. "
        "¿Te cuento qué hay disponible ahora en lentes y pelucas? 😊"
    ),
    "Antiguo": (
        "{saludo} Soy de K&M: hace tiempo nos compraste lentes. "
        "¿Te cuento qué hay disponible ahora en lentes y pelucas? 😊"
    ),
    "Historial": (
        "{saludo} Soy de K&M: hace tiempo nos compraste lentes. "
        "¿Te cuento qué hay disponible ahora en lentes y pelucas? 😊"
    ),
}
SEGMENTOS_CONTACTO = ["Recompra probable", "Dormido", "Antiguo", "Historial"]

# Instrucción que el bot agrega al pedido de texto a la IA según el grupo del cliente
INSTRUCCION_GRUPO = {
    1: "CONTEXTO DEL CLIENTE: ya nos compró lentes antes{producto} y le toca renovar. Recuérdaselo con naturalidad "
       "y ofrécele renovar o probar otro color.",
    2: "CONTEXTO DEL CLIENTE: nos escribió antes preguntando por productos pero aún no compra. Retoma la conversación "
       "con calidez, sin presionar, e invítalo a contarte qué le interesa.",
    3: "CONTEXTO DEL CLIENTE: fue cliente hace tiempo y no hemos hablado hace meses. Salúdalo como a un cliente "
       "conocido y cuéntale las novedades, sin presión.",
    4: "CONTEXTO DEL CLIENTE: contacto que aún no nos ha comprado. Preséntate en una frase como K&M y muestra el "
       "producto de forma amable, sin presión ni insistencia.",
}
NOMBRE_GRUPO = {1: "Recompra", 2: "Escribió sin comprar", 3: "Cliente antiguo", 4: "Contacto frío"}

MESES = ["", "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "setiembre", "octubre", "noviembre", "diciembre"]


LOG_EN_BD = True  # se apaga con --prueba


def log(msg):
    """Imprime y guarda el mensaje en la tabla logs_agente (se ve en Diagnóstico → Logs Agente Semanal),
    igual que hacen tus otras rutinas. Así no hace falta ningún archivo de log."""
    texto = str(msg).lstrip("\n")
    print(texto, flush=True)
    if not LOG_EN_BD or not texto.strip():
        return
    try:
        with engine.begin() as c:
            c.execute(text("CREATE TABLE IF NOT EXISTS logs_agente ("
                           "id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT NOW(), mensaje TEXT)"))
            c.execute(text("INSERT INTO logs_agente (fecha, mensaje) VALUES (NOW(), :m)"), {"m": texto})
    except Exception as e:
        print(f"(no se pudo guardar el log en la base: {e})", flush=True)


# ==============================================================================
# SQL BASE: una fila por línea de venta, con la misma clasificación Lentes /
# Pelucas / Otros que usa tu módulo de Estadísticas.
# ==============================================================================
SQL_LINEAS = """
WITH lineas AS (
    SELECT
        v.id_venta,
        v.id_cliente,
        v.fecha_venta::date AS dia,
        d.sku,
        COALESCE(d.cantidad, 0) AS cantidad,
        COALESCE(d.subtotal, 0) AS subtotal,
        CASE
            WHEN prod.macro_categoria ILIKE 'peluca%' OR var.sku ILIKE 'WB-%'
                 OR var.sku ILIKE 'WIG-%' OR d.descripcion ILIKE '[Pelucas]%' THEN 'Pelucas'
            WHEN d.descripcion ILIKE '[Otros]%' OR d.sku IS NULL THEN 'Otros'
            ELSE 'Lentes'
        END AS macro,
        COALESCE(prod.categoria, 'Otros') AS categoria,
        COALESCE(prod.nombre || ' - ' || var.nombre_variante, d.descripcion, d.sku, 'Sin nombre') AS producto
    FROM ventas v
    JOIN detalleventa d ON d.id_venta = v.id_venta
    LEFT JOIN variantes var ON var.sku = d.sku
    LEFT JOIN productos prod ON prod.id_producto = var.id_producto
    WHERE COALESCE(v.anulado, FALSE) = FALSE
      AND v.fecha_venta IS NOT NULL
)
"""


def _leer(conn, sql, params=None):
    return pd.read_sql(text(sql), conn, params=params or {})


# ==============================================================================
# FECHAS
# ==============================================================================
def calcular_fechas():
    """Lima es UTC-5 todo el año (sin horario de verano)."""
    hoy = (datetime.now(timezone.utc) - timedelta(hours=5)).date()
    fin = hoy - timedelta(days=1)  # el análisis llega hasta ayer (día completo)
    return {
        "hoy": hoy,
        "fin": fin,
        "s_ini": fin - timedelta(days=6),
        "p_ini": fin - timedelta(days=13),
        "p_fin": fin - timedelta(days=7),
        "m_ini": fin.replace(day=1),
        "d30": fin - timedelta(days=30),
        "d90": fin - timedelta(days=90),
        "d180": fin - timedelta(days=180),
        "d365": fin - timedelta(days=365),
        "tend_ini": (fin.replace(day=1) - timedelta(days=330)).replace(day=1),
    }


# ==============================================================================
# CONSULTAS
# ==============================================================================
def consultar_ventas(conn, f):
    sql = SQL_LINEAS + """
    SELECT macro,
        COALESCE(SUM(cantidad) FILTER (WHERE dia BETWEEN :s_ini AND :fin), 0)  AS uds_sem,
        COALESCE(SUM(subtotal) FILTER (WHERE dia BETWEEN :s_ini AND :fin), 0)  AS ing_sem,
        COALESCE(SUM(cantidad) FILTER (WHERE dia BETWEEN :p_ini AND :p_fin), 0) AS uds_prev,
        COALESCE(SUM(subtotal) FILTER (WHERE dia BETWEEN :p_ini AND :p_fin), 0) AS ing_prev,
        COALESCE(SUM(cantidad) FILTER (WHERE dia BETWEEN :m_ini AND :fin), 0)  AS uds_mes,
        COALESCE(SUM(subtotal) FILTER (WHERE dia BETWEEN :m_ini AND :fin), 0)  AS ing_mes
    FROM lineas
    WHERE dia BETWEEN :min_ini AND :fin
    GROUP BY macro
    """
    params = {k: f[k] for k in ("s_ini", "p_ini", "p_fin", "m_ini", "fin")}
    params["min_ini"] = min(f["p_ini"], f["m_ini"])
    df = _leer(conn, sql, params)
    out = {}
    for macro in ("Lentes", "Pelucas", "Otros"):
        fila = df[df["macro"] == macro]
        if fila.empty:
            out[macro] = dict(uds_sem=0, ing_sem=0, uds_prev=0, ing_prev=0, uds_mes=0, ing_mes=0)
        else:
            out[macro] = {k: float(fila.iloc[0][k]) for k in
                          ("uds_sem", "ing_sem", "uds_prev", "ing_prev", "uds_mes", "ing_mes")}
    return out


def consultar_tendencia(conn, f):
    sql = SQL_LINEAS + """
    SELECT date_trunc('month', dia)::date AS mes, macro,
           SUM(cantidad) AS uds, SUM(subtotal) AS ingresos
    FROM lineas
    WHERE dia >= :tend_ini AND dia <= :fin AND macro IN ('Lentes', 'Pelucas')
    GROUP BY 1, 2 ORDER BY 1, 2
    """
    return _leer(conn, sql, {"tend_ini": f["tend_ini"], "fin": f["fin"]})


def consultar_clientes(conn, f):
    sql = SQL_LINEAS + """
    , primera AS (
        SELECT id_cliente, MIN(dia) AS primera_compra
        FROM lineas WHERE id_cliente IS NOT NULL GROUP BY id_cliente
    )
    SELECT
        COUNT(DISTINCT l.id_cliente) FILTER (WHERE p.primera_compra >= :m_ini) AS nuevos,
        COUNT(DISTINCT l.id_cliente) FILTER (WHERE p.primera_compra <  :m_ini) AS recurrentes
    FROM lineas l JOIN primera p ON p.id_cliente = l.id_cliente
    WHERE l.dia BETWEEN :m_ini AND :fin
    """
    mes = _leer(conn, sql, {"m_ini": f["m_ini"], "fin": f["fin"]}).iloc[0]

    sql_base = SQL_LINEAS + """
    SELECT
        (SELECT COUNT(*) FROM clientes WHERE COALESCE(activo, TRUE) = TRUE) AS clientes_activos,
        (SELECT COUNT(DISTINCT id_cliente) FROM lineas WHERE id_cliente IS NOT NULL) AS compradores_historicos,
        (SELECT COUNT(DISTINCT id_cliente) FROM lineas
          WHERE id_cliente IS NOT NULL AND dia > :d365 AND dia <= :fin) AS compradores_12m
    """
    base = _leer(conn, sql_base, {"d365": f["d365"], "fin": f["fin"]}).iloc[0]
    return (
        {"nuevos": int(mes["nuevos"] or 0), "recurrentes": int(mes["recurrentes"] or 0)},
        {k: int(base[k] or 0) for k in ("clientes_activos", "compradores_historicos", "compradores_12m")},
    )


def _norm_tel(valor):
    if not valor:
        return None
    s = "".join(ch for ch in str(valor) if ch.isdigit())
    if len(s) == 9:
        return "51" + s
    if len(s) == 11 and s.startswith("51"):
        return s
    if 7 <= len(s) <= 15:
        return s
    return None


def _segmentar(df, fin):
    df = df.copy()
    df["dias_desde_compra"] = (pd.Timestamp(fin) - pd.to_datetime(df["ultima_compra"])).dt.days.astype(int)
    ciclo = pd.to_numeric(df["ciclo_dias"], errors="coerce")
    n = pd.to_numeric(df["n_compras"], errors="coerce").fillna(0)
    ciclo_valido = (n >= 2) & ciclo.between(20, 400)
    ini = np.where(ciclo_valido, ciclo * 0.8, RECOMPRA_MIN_DIAS)
    fin_ventana = np.where(ciclo_valido, ciclo * 2.0, RECOMPRA_MAX_DIAS)
    d = df["dias_desde_compra"].to_numpy()
    limite_dormido = np.maximum(fin_ventana, DORMIDO_MAX_DIAS)
    df["segmento"] = np.select(
        [d < ini, d <= fin_ventana, d <= limite_dormido, d <= np.maximum(limite_dormido, ANTIGUO_MAX_DIAS)],
        ["Reciente", "Recompra probable", "Dormido", "Antiguo"],
        default="Perdido",
    )
    df["ciclo_propio"] = ciclo_valido
    return df


NOMBRES = {}  # sku -> {"completo": "Lente L3 - Gris (LC-003)", "corto": "Lente L3 - Gris"}


def _gs(row, key):
    v = row.get(key)
    if v is None or (isinstance(v, float) and v != v):
        return ""
    t = str(v).strip()
    return "" if t.lower() in ("nan", "none") else t


def nombre_comercial(row, con_sku=True):
    """Mismo criterio que utils.generar_nombre_inteligente del ERP (pantalla Productos)."""
    macro, cat = _gs(row, "macro"), _gs(row, "categoria")
    marca, modelo, nombre, sku = _gs(row, "marca"), _gs(row, "modelo"), _gs(row, "nombre"), _gs(row, "sku")
    prefijo = ""
    if macro == "Pelucas":
        c = cat.lower()
        if "natural" in c:
            prefijo = marca if marca.lower() not in ("pelucat", "genérico", "generico", "") else "Peluca"
        elif any(k in c for k in ("fantas", "cosplay", "lace")):
            prefijo = "Peluca"
    else:
        if cat == "Estilo Natural":
            prefijo = marca
        elif cat == "Estilo Fantasía":
            prefijo = "Lente"
    partes = [prefijo] if prefijo else []
    if modelo:
        if prefijo and modelo.lower().startswith(prefijo.lower()):
            partes = [modelo]
        else:
            partes.append(modelo)
    inicio = " ".join(partes).strip()
    if inicio and nombre:
        base = f"{inicio} - {nombre}"
    elif inicio:
        base = inicio
    else:
        base = nombre or "Artículo sin título"
    return f"{base} ({sku})".strip() if (sku and con_sku) else base.strip()


def cargar_nombres(conn):
    """Carga el nombre comercial de cada SKU (igual al de la pantalla Productos)."""
    NOMBRES.clear()
    df = _leer(conn, """
        SELECT var.sku, prod.categoria, prod.marca, prod.modelo, prod.nombre,
               CASE WHEN prod.macro_categoria ILIKE 'peluca%' OR var.sku ILIKE 'WB-%' OR var.sku ILIKE 'WIG-%'
                    THEN 'Pelucas' ELSE 'Lentes' END AS macro
        FROM variantes var LEFT JOIN productos prod ON prod.id_producto = var.id_producto""")
    for r in df.to_dict("records"):
        NOMBRES[r["sku"]] = {"completo": nombre_comercial(r, True), "corto": nombre_comercial(r, False)}


def nombre_sku(sku, fallback="", clave="completo"):
    d = NOMBRES.get(sku)
    return d[clave] if d else (fallback or sku or "Sin nombre")


def hay_historico(conn):
    """¿Existe la tabla ventas_historicas (creada por importar_historico.py)?"""
    return conn.execute(text("SELECT to_regclass('public.ventas_historicas')")).scalar() is not None


def resumen_historico(conn):
    if not hay_historico(conn):
        return None
    r = conn.execute(text(
        "SELECT COUNT(*) FILTER (WHERE lentes AND id_cliente IS NOT NULL) AS pedidos_lentes, "
        "COUNT(*) FILTER (WHERE lentes AND id_cliente IS NULL) AS pedidos_lentes_sin_cliente, "
        "COUNT(DISTINCT id_cliente) FILTER (WHERE lentes AND id_cliente IS NOT NULL) AS clientes_lentes, "
        "MIN(fecha) AS desde, MAX(fecha) AS hasta FROM ventas_historicas")).fetchone()
    return {"pedidos_lentes": int(r.pedidos_lentes or 0),
            "pedidos_lentes_sin_cliente": int(r.pedidos_lentes_sin_cliente or 0),
            "clientes_lentes": int(r.clientes_lentes or 0), "desde": r.desde, "hasta": r.hasta}


def analizar_recompra(conn, f):
    historico = hay_historico(conn)
    # Compras de lentes: las del ERP y, si se importó, las de los Excel antiguos (ventas_historicas)
    compras_hist = """
        UNION ALL
        SELECT id_cliente, fecha AS dia, 1 AS uds, COALESCE(monto, 0) AS monto
        FROM ventas_historicas
        WHERE lentes AND id_cliente IS NOT NULL AND fecha <= :fin
    """ if historico else ""
    producto_hist = """,
           (SELECT LEFT(SPLIT_PART(h.orden, CHR(10), 1), 60) FROM ventas_historicas h
             WHERE h.id_cliente = r.id_cliente AND h.lentes
             ORDER BY h.fecha DESC LIMIT 1)""" if historico else ""
    sql = SQL_LINEAS + f"""
    , compras_raw AS (
        SELECT id_cliente, dia, SUM(cantidad) AS uds, SUM(subtotal) AS monto
        FROM lineas
        WHERE macro = 'Lentes' AND id_cliente IS NOT NULL AND dia <= :fin
        GROUP BY id_cliente, dia
        {compras_hist}
    ),
    compras AS (
        SELECT id_cliente, dia, SUM(uds) AS uds, SUM(monto) AS monto
        FROM compras_raw GROUP BY id_cliente, dia
    ),""" + f"""
    resumen AS (
        SELECT id_cliente, MAX(dia) AS ultima_compra, COUNT(*) AS n_compras,
               SUM(monto) AS gasto_total, SUM(uds) AS uds_total
        FROM compras GROUP BY id_cliente
    ),
    ciclos AS (
        SELECT id_cliente, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap) AS ciclo_dias
        FROM (
            SELECT id_cliente, (dia - LAG(dia) OVER (PARTITION BY id_cliente ORDER BY dia)) AS gap
            FROM compras
        ) g
        WHERE gap > 0
        GROUP BY id_cliente
    )
    SELECT r.id_cliente, c.nombre_corto, c.nombre_ia, r.ultima_compra, r.n_compras,
           r.gasto_total, r.uds_total, ci.ciclo_dias,
           COALESCE(
               (SELECT t.telefono FROM telefonoscliente t
                 WHERE t.id_cliente = r.id_cliente AND COALESCE(t.activo, TRUE) = TRUE
                 ORDER BY t.es_principal DESC NULLS LAST LIMIT 1),
               c.telefono
           ) AS telefono,
           (SELECT l.sku FROM lineas l
             WHERE l.id_cliente = r.id_cliente AND l.macro = 'Lentes'
             ORDER BY l.dia DESC LIMIT 1) AS ultimo_sku,
           COALESCE(
               (SELECT l.producto FROM lineas l
                 WHERE l.id_cliente = r.id_cliente AND l.macro = 'Lentes'
                 ORDER BY l.dia DESC LIMIT 1){producto_hist}
           ) AS ultimo_producto
    FROM resumen r
    JOIN clientes c ON c.id_cliente = r.id_cliente
    LEFT JOIN ciclos ci ON ci.id_cliente = r.id_cliente
    WHERE COALESCE(c.activo, TRUE) = TRUE
      AND COALESCE(c.excluir_publicidad, FALSE) = FALSE
      AND COALESCE(c.estado, '') NOT ILIKE 'Sin WhatsApp'
    """
    df = _leer(conn, sql, {"fin": f["fin"]})
    if df.empty:
        return df
    df = _segmentar(df, f["fin"])
    df["telefono"] = df["telefono"].map(_norm_tel)
    # Nombre comercial del producto (sin el SKU: va en mensajes a clientes)
    df["ultimo_producto"] = [nombre_sku(sk, fb, "corto") if isinstance(sk, str) and sk else fb
                             for sk, fb in zip(df["ultimo_sku"], df["ultimo_producto"])]
    return df


def contactos_historial(conn, f, recompra):
    """Compradores de lentes de los Excel antiguos cuyo teléfono NO está en el ERP (segmento «Historial»).
    Se agrupan por teléfono; no se crean como clientes."""
    if not hay_historico(conn):
        return pd.DataFrame()
    df = _leer(conn, """
        SELECT telefono, MAX(fecha) AS ultima_compra, COUNT(DISTINCT fecha) AS n_compras,
               SUM(COALESCE(monto, 0)) AS gasto_total,
               (ARRAY_AGG(NULLIF(TRIM(nombre), '') ORDER BY fecha DESC))[1] AS nombre_corto,
               (ARRAY_AGG(LEFT(SPLIT_PART(orden, CHR(10), 1), 60) ORDER BY fecha DESC))[1] AS ultimo_producto
        FROM ventas_historicas
        WHERE lentes AND id_cliente IS NULL AND telefono IS NOT NULL AND fecha <= :fin
        GROUP BY telefono""", {"fin": f["fin"]})
    if df.empty:
        return df
    df["telefono"] = df["telefono"].map(_norm_tel)
    df = df[df["telefono"].notna()]
    if not recompra.empty:  # no repetir a quien ya sale como cliente del ERP
        df = df[~df["telefono"].isin(set(recompra["telefono"].dropna()))]
    df["id_cliente"] = None
    df["nombre_ia"] = None
    df["ciclo_dias"] = np.nan
    df["uds_total"] = df["n_compras"]
    df["dias_desde_compra"] = (pd.Timestamp(f["fin"]) - pd.to_datetime(df["ultima_compra"])).dt.days.astype(int)
    df = df[df["dias_desde_compra"] <= ANTIGUO_MAX_DIAS]
    df["segmento"] = "Historial"
    return df


# ==============================================================================
# GRUPOS PARA EL BOT DE MARKETING
# ==============================================================================
_PAT_AUTO_FUERTE = re.compile(
    r"respuesta automatica|mensaje automatico|mensaje de bienvenida|horario de atencion|fuera de horario|"
    r"no estamos disponibles|no nos encontramos disponibles|en este momento no (podemos|estamos|nos)")
_PAT_AUTO_DEBIL = re.compile(
    r"gracias por (comunicarte|comunicarse|contactarnos|contactar|escribirnos|escribir|tu mensaje|su mensaje)|"
    r"te responderemos|le responderemos|nos pondremos en contacto|en breve (te|le) (atender|respond)|"
    r"un asesor (te|le) (atender|respond)")


def _norm_texto(t):
    t = unicodedata.normalize("NFD", str(t or "").lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r"[^a-z ]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def marcar_automaticos(df):
    """Marca como respuesta automática (de una empresa) los mensajes entrantes que:
       - repiten el MISMO texto largo desde 3+ teléfonos distintos Y llegan como respuesta a un mensaje nuestro
         (un mensaje igual que llega sin que les hayamos escrito, por ejemplo el texto prearmado del botón de
         WhatsApp de tu web, es un cliente real y NO se marca), o
       - lo repiten 3+ veces desde un mismo teléfono, o
       - contienen frases típicas de auto-respuesta (horario de atención, «este es un mensaje automático»...).
    Devuelve (serie booleana alineada a df, lista de textos detectados para auditar)."""
    if df.empty:
        return pd.Series([], dtype=bool), []
    norm = df["contenido"].map(_norm_texto)
    largo = norm.str.len() >= 25
    n_tel = df[largo].groupby(norm[largo])["telefono"].nunique()
    n_rep = df[largo].groupby([df.loc[largo, "telefono"], norm[largo]]).size()
    tras = df["tras_saliente"].fillna(False).astype(bool) if "tras_saliente" in df.columns else pd.Series(False, index=df.index)
    repetido_global = (norm.map(n_tel).fillna(0) >= AUTO_MIN_TELEFONOS) & tras
    repetido_tel = pd.Series(
        [n_rep.get((t, n), 0) >= 3 if l else False for t, n, l in zip(df["telefono"], norm, largo)], index=df.index)
    fuerte = norm.map(lambda x: bool(_PAT_AUTO_FUERTE.search(x)))
    debil = norm.map(lambda x: len(x) >= 60 and bool(_PAT_AUTO_DEBIL.search(x)))
    auto = (repetido_global | repetido_tel | fuerte | debil) & (norm != "")
    ejemplos = (df.loc[auto, "contenido"].astype(str).str.slice(0, 80).value_counts().head(8))
    return auto, [f"{n}x «{t}»" for t, n in ejemplos.items()]


def _col_existe(conn, tabla, columna):
    return bool(conn.execute(text(
        "SELECT 1 FROM information_schema.columns WHERE table_name = :t AND column_name = :c LIMIT 1"),
        {"t": tabla.lower(), "c": columna.lower()}).scalar())


def calcular_grupos(conn, f, recompra):
    """Asigna cada cliente a UN grupo (1 a 4) o a ninguno:
       1 Recompra            = «Recompra probable» del agente
       2 Escribió sin comprar= escribió (mensaje real) en el último año y no compró en ese período
       3 Cliente antiguo     = no escribió en el último año pero alguna vez compró (ERP o Excel antiguos)
       4 Contacto frío       = no escribió en el último año y nunca compró
    Quedan fuera: inactivos, sin publicidad, sin WhatsApp, zombies, compra de los últimos 60 días, y quien
    escribió y compró en el último año sin estar en ventana de recompra (ya es cliente activo)."""
    fin = pd.Timestamp(f["fin"])
    ini_ano = fin - pd.Timedelta(days=DIAS_ANO)
    zombie = "COALESCE(c.nivel_zombie, 0)" if _col_existe(conn, "clientes", "nivel_zombie") else "0"
    base = _leer(conn, f"""
        SELECT c.id_cliente, c.estado, COALESCE(c.activo, TRUE) AS activo,
               COALESCE(c.excluir_publicidad, FALSE) AS excl, {zombie} AS zombie,
               EXISTS (SELECT 1 FROM telefonoscliente t WHERE t.id_cliente = c.id_cliente
                        AND COALESCE(t.activo, TRUE) AND t.es_principal AND LENGTH(t.telefono) > 6) AS tiene_tel
        FROM clientes c""")
    if base.empty:
        return pd.DataFrame(), {}
    tmap = """WITH tmap AS (
        SELECT telefono AS k, id_cliente FROM telefonoscliente WHERE telefono IS NOT NULL
        UNION SELECT lid, id_cliente FROM telefonoscliente WHERE lid IS NOT NULL
        UNION SELECT telefono, id_cliente FROM clientes WHERE telefono IS NOT NULL)"""
    # «tras_saliente»: ya le habíamos escrito a ese teléfono antes (se compara por id de mensaje, no por hora)
    ent = _leer(conn, tmap + """
        SELECT COALESCE(m.id_cliente, tm.id_cliente) AS id_cliente, m.telefono, m.fecha, LEFT(m.contenido, 300) AS contenido,
               (s.min_sal IS NOT NULL AND s.min_sal < m.id_mensaje) AS tras_saliente
        FROM mensajes m
        LEFT JOIN tmap tm ON tm.k = m.telefono
        LEFT JOIN (SELECT telefono, MIN(id_mensaje) AS min_sal FROM mensajes
                    WHERE tipo LIKE 'SALIENTE%' GROUP BY telefono) s ON s.telefono = m.telefono
        WHERE m.tipo = 'ENTRANTE'""")
    auto_ejemplos, n_auto = [], 0
    if not ent.empty:
        auto, auto_ejemplos = marcar_automaticos(ent)
        n_auto = int(auto.sum())
        ent = ent[~auto]
    ent = ent[ent["id_cliente"].notna()].copy()
    ent["id_cliente"] = ent["id_cliente"].astype("int64")
    ent["fecha"] = pd.to_datetime(ent["fecha"])
    ult_real = ent.groupby("id_cliente")["fecha"].max()

    bot = _leer(conn, tmap + """
        SELECT COALESCE(m.id_cliente, tm.id_cliente) AS id_cliente, m.fecha
        FROM mensajes m LEFT JOIN tmap tm ON tm.k = m.telefono
        WHERE m.tipo = 'SALIENTE_BOT'""")
    bot = bot[bot["id_cliente"].notna()].copy()
    bot["id_cliente"] = bot["id_cliente"].astype("int64")
    bot = bot.drop_duplicates()
    bot["fecha"] = pd.to_datetime(bot["fecha"])
    bot["ult_real"] = ult_real.reindex(bot["id_cliente"]).to_numpy()
    sin_resp = bot[bot["ult_real"].isna() | (bot["fecha"] > bot["ult_real"])].groupby("id_cliente").size()

    ventas = _leer(conn, """
        SELECT id_cliente, MAX(fecha_venta::date) AS ult FROM ventas
        WHERE COALESCE(anulado, FALSE) = FALSE AND id_cliente IS NOT NULL AND fecha_venta::date <= :fin
        GROUP BY id_cliente""", {"fin": f["fin"]})
    if hay_historico(conn):
        h = _leer(conn, "SELECT id_cliente, MAX(fecha) AS ult FROM ventas_historicas "
                        "WHERE id_cliente IS NOT NULL AND fecha <= :fin GROUP BY id_cliente", {"fin": f["fin"]})
        ventas = pd.concat([ventas, h], ignore_index=True)
    ventas["ult"] = pd.to_datetime(ventas["ult"])
    ult_venta = ventas.groupby("id_cliente")["ult"].max()

    prod_g1 = {}
    if not recompra.empty:
        r1 = recompra[recompra["segmento"] == "Recompra probable"]
        prod_g1 = dict(zip(r1["id_cliente"], r1["ultimo_producto"]))

    d = base.copy()
    d["ult_venta"] = ult_venta.reindex(d["id_cliente"]).to_numpy()
    d["ult_real"] = ult_real.reindex(d["id_cliente"]).to_numpy()
    d["sin_respuesta"] = sin_resp.reindex(d["id_cliente"]).fillna(0).astype(int).to_numpy()
    elegible = (d["activo"] & ~d["excl"] & d["tiene_tel"] & (d["zombie"] == 0)
                & ~d["estado"].fillna("").str.contains("sin whatsapp", case=False))
    dias_venta = (fin - d["ult_venta"]).dt.days
    reciente = dias_venta < VENTA_RECIENTE_DIAS
    escribio = d["ult_real"] >= ini_ano
    vendio_ano = d["ult_venta"] >= ini_ano
    g1 = d["id_cliente"].isin(prod_g1.keys())
    grupo = pd.Series(0, index=d.index)
    grupo[~g1 & ~escribio & d["ult_venta"].isna()] = 4
    grupo[~g1 & ~escribio & d["ult_venta"].notna()] = 3
    grupo[~g1 & escribio & ~vendio_ano] = 2
    grupo[g1] = 1
    grupo[~elegible | reciente.fillna(False)] = 0
    d["grupo"] = grupo

    # Grupo 4: tras N mensajes sin respuesta pasa a «sin publicidad»
    sin_mkt = d[(d["grupo"] == 4) & (d["sin_respuesta"] >= MAX_SIN_RESPUESTA_G4)]
    d = d[(d["grupo"] > 0) & ~d["id_cliente"].isin(sin_mkt["id_cliente"])].copy()

    d["escribio_real"] = d["ult_real"].notna()   # alguna vez escribió un mensaje real (sin contar respuestas automáticas)
    d["nombre_grupo"] = d["grupo"].map(NOMBRE_GRUPO)
    d["espera_dias"] = d["grupo"].map(ESPERA_GRUPO)
    d["prioridad"] = d["grupo"]
    d["ultimo_producto"] = d["id_cliente"].map(prod_g1)
    d["instruccion_ia"] = [
        INSTRUCCION_GRUPO[g].format(producto=(f" ({_txt(p)})" if _txt(p) else ""))
        for g, p in zip(d["grupo"], d["ultimo_producto"])]
    info = {
        "por_grupo": {int(k): int(v) for k, v in d["grupo"].value_counts().sort_index().items()},
        "sin_mkt_nuevos": [int(x) for x in sin_mkt["id_cliente"]][:500],
        "n_sin_mkt_nuevos": int(len(sin_mkt)),
        "mensajes_automaticos": n_auto, "ejemplos_automaticos": auto_ejemplos,
        "esperas_dias": ESPERA_GRUPO,
    }
    return d[["id_cliente", "grupo", "nombre_grupo", "espera_dias", "prioridad", "ultimo_producto",
              "instruccion_ia", "escribio_real"]], info


def guardar_grupos(conn, grupos, info):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bot_grupos (
            id_cliente INTEGER PRIMARY KEY, grupo INTEGER, nombre_grupo TEXT, espera_dias INTEGER,
            prioridad INTEGER, ultimo_producto TEXT, instruccion_ia TEXT, escribio_real BOOLEAN DEFAULT FALSE,
            actualizado TIMESTAMP DEFAULT NOW())"""))
    conn.execute(text("ALTER TABLE bot_grupos ADD COLUMN IF NOT EXISTS escribio_real BOOLEAN DEFAULT FALSE"))
    conn.execute(text("DELETE FROM bot_grupos"))
    filas = [{"c": int(r.id_cliente), "g": int(r.grupo), "ng": r.nombre_grupo, "e": int(r.espera_dias),
              "p": int(r.prioridad), "u": _txt(r.ultimo_producto) or None, "i": r.instruccion_ia,
              "w": bool(r.escribio_real)}
             for r in grupos.itertuples(index=False)]
    for i in range(0, len(filas), 1000):
        conn.execute(text("INSERT INTO bot_grupos (id_cliente, grupo, nombre_grupo, espera_dias, prioridad, "
                          "ultimo_producto, instruccion_ia, escribio_real) "
                          "VALUES (:c, :g, :ng, :e, :p, :u, :i, :w)"), filas[i:i + 1000])
    n_sin = 0
    if AUTO_SIN_MKT and info.get("sin_mkt_nuevos"):
        r = conn.execute(text("UPDATE clientes SET excluir_publicidad = TRUE "
                              "WHERE id_cliente = ANY(:ids) AND COALESCE(excluir_publicidad, FALSE) = FALSE"),
                         {"ids": info["sin_mkt_nuevos"]})
        n_sin = r.rowcount
    return len(filas), n_sin



def _txt(valor):
    """Texto seguro: None o NaN (campo vacío en la base) se vuelven ''."""
    if valor is None or (isinstance(valor, float) and valor != valor):
        return ""
    return str(valor).strip()


def _nombre_corto(row):
    nombre = _txt(row.get("nombre_ia")) or _txt(row.get("nombre_corto"))
    return nombre.split()[0] if nombre else ""


def preparar_candidatos(df):
    """Filtra a quienes conviene contactar y les arma un mensaje sugerido."""
    if df.empty:
        return df, 0
    cand = df[df["segmento"].isin(SEGMENTOS_CONTACTO)].copy()
    sin_tel = int(cand["telefono"].isna().sum())
    cand = cand[cand["telefono"].notna()].copy()
    if cand.empty:
        return cand, sin_tel

    def armar(row):
        nombre = _nombre_corto(row)
        saludo = f"Hola {nombre} 👋" if nombre else "Hola 👋"
        producto = _txt(row.get("ultimo_producto")) or "tus lentes"
        return PLANTILLAS[row["segmento"]].format(saludo=saludo, producto=producto)

    cand["mensaje_sugerido"] = cand.apply(armar, axis=1)
    cand["nombre"] = cand.apply(lambda r: _txt(r.get("nombre_corto")) or _txt(r.get("nombre_ia")), axis=1)
    cand["ultimo_producto"] = cand["ultimo_producto"].map(_txt)
    # Dentro de cada segmento, primero los que más veces compraron y más gastaron
    orden = {"Recompra probable": 0, "Dormido": 1, "Antiguo": 2, "Historial": 3}
    cand["_o"] = cand["segmento"].map(orden)
    cand = cand.sort_values(["_o", "n_compras", "gasto_total"], ascending=[True, False, False]).drop(columns="_o")
    return cand.head(MAX_CANDIDATOS_GUARDADOS), sin_tel


def analizar_stock(conn, f):
    sql = SQL_LINEAS + """
    , ventas_sku AS (
        SELECT sku,
               SUM(cantidad) FILTER (WHERE dia > :d90 AND dia <= :fin) AS uds90,
               MAX(dia) AS ultima_venta
        FROM lineas WHERE sku IS NOT NULL GROUP BY sku
    )
    SELECT var.sku,
           COALESCE(NULLIF(TRIM(CONCAT_WS(' ', prod.marca, prod.modelo, prod.nombre, var.nombre_variante)), ''), var.sku) AS producto,
           CASE WHEN prod.macro_categoria ILIKE 'peluca%' OR var.sku ILIKE 'WB-%' OR var.sku ILIKE 'WIG-%'
                THEN 'Pelucas' ELSE 'Lentes' END AS macro,
           COALESCE(prod.categoria, 'Otros') AS categoria,
           COALESCE(var.stock_interno, 0) + COALESCE(var.stock_externo, 0) AS stock,
           COALESCE(var.stock_transito, 0) AS transito,
           COALESCE(vs.uds90, 0) AS uds90,
           vs.ultima_venta
    FROM variantes var
    LEFT JOIN productos prod ON prod.id_producto = var.id_producto
    LEFT JOIN ventas_sku vs ON vs.sku = var.sku
    """
    df = _leer(conn, sql, {"d90": f["d90"], "fin": f["fin"]})
    if df.empty:
        return [], [], {}
    for col in ("stock", "transito", "uds90"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["producto"] = [nombre_sku(sk, pr) for sk, pr in zip(df["sku"], df["producto"])]
    df["ritmo_dia"] = df["uds90"] / 90.0
    df["cobertura_dias"] = np.where(df["ritmo_dia"] > 0, df["stock"] / df["ritmo_dia"], np.nan)

    # Resumen por línea de negocio
    por_macro = []
    for macro, g in df.groupby("macro"):
        ritmo = g["ritmo_dia"].sum()
        por_macro.append({
            "macro": macro,
            "stock_uds": float(g["stock"].sum()),
            "transito_uds": float(g["transito"].sum()),
            "vendido_90d": float(g["uds90"].sum()),
            "cobertura_dias": float(g["stock"].sum() / ritmo) if ritmo > 0 else None,
        })

    # Reponer pronto: se vende de verdad y el stock dura poco
    rep = df[(df["uds90"] >= 3) & (df["cobertura_dias"] < COBERTURA_MIN_DIAS)].copy()
    rep = rep.sort_values("cobertura_dias").head(15)
    reponer = rep[["sku", "producto", "macro", "stock", "transito", "uds90", "cobertura_dias"]]
    reponer = reponer.to_dict("records")

    # Stock dormido: hay unidades y no se vende hace 180 días
    ult = pd.to_datetime(df["ultima_venta"])
    dorm = df[(df["stock"] > 0) & (ult.isna() | (ult < pd.Timestamp(f["d180"])))].copy()
    dormido = {
        "skus": int(len(dorm)),
        "unidades": float(dorm["stock"].sum()),
        "por_macro": {m: float(g["stock"].sum()) for m, g in dorm.groupby("macro")},
        "top": dorm.sort_values("stock", ascending=False).head(10)[["sku", "producto", "macro", "stock"]].to_dict("records"),
    }
    return por_macro, reponer, dormido


def top_productos(conn, f):
    sql = SQL_LINEAS + """
    SELECT macro, sku, producto,
           SUM(cantidad) FILTER (WHERE dia > :d30) AS uds30,
           SUM(cantidad) AS uds90,
           SUM(subtotal) AS ing90
    FROM lineas
    WHERE dia > :d90 AND dia <= :fin AND macro IN ('Lentes', 'Pelucas')
    GROUP BY macro, sku, producto
    """
    df = _leer(conn, sql, {"d30": f["d30"], "d90": f["d90"], "fin": f["fin"]})
    if not df.empty:
        df["producto"] = [nombre_sku(sk, pr) if isinstance(sk, str) and sk else pr
                          for sk, pr in zip(df["sku"], df["producto"])]
        df = df.groupby(["macro", "producto"], as_index=False)[["uds30", "uds90", "ing90"]].sum()
    out = {}
    for macro in ("Lentes", "Pelucas"):
        g = df[df["macro"] == macro].fillna(0).sort_values("uds90", ascending=False).head(10)
        out[macro] = g[["producto", "uds30", "uds90", "ing90"]].to_dict("records")
    return out


def efectividad_bot(conn, f):
    """Compras dentro de 7 días de un mensaje del bot (solo envíos con ventana ya cumplida)."""
    try:
        sql = """
        WITH envios AS (
            SELECT DISTINCT m.id_cliente, m.fecha::date AS dia_envio
            FROM mensajes m
            WHERE m.tipo = 'SALIENTE_BOT' AND m.id_cliente IS NOT NULL
              AND m.fecha::date >= :ini AND m.fecha::date <= :fin7
        )
        SELECT COUNT(*) AS envios,
               COUNT(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM ventas v
                   WHERE v.id_cliente = e.id_cliente
                     AND COALESCE(v.anulado, FALSE) = FALSE
                     AND v.fecha_venta::date BETWEEN e.dia_envio AND e.dia_envio + 7
               )) AS con_compra
        FROM envios e
        """
        r = _leer(conn, sql, {"ini": f["fin"] - timedelta(days=37), "fin7": f["fin"] - timedelta(days=7)}).iloc[0]
        envios, comp = int(r["envios"] or 0), int(r["con_compra"] or 0)
        return {"envios": envios, "con_compra": comp, "pct": round(100.0 * comp / envios, 1) if envios else None}
    except Exception as e:  # el reporte no debe caerse por esto
        log(f"⚠️ No se pudo medir el bot de marketing: {e}")
        return None


# ==============================================================================
# ALERTAS EN TEXTO (calculadas por código, sin IA: los números son exactos)
# ==============================================================================
def _f0(x):
    return f"{x:,.0f}"


def construir_alertas(f, ventas, tendencia, clientes_mes, segmentos, n_sin_tel, stock_macro, reponer, dormido, bot,
                      historico=None, grupos=None):
    alertas = []
    L = ventas["Lentes"]
    fin = f["fin"]
    dias_mes = calendar.monthrange(fin.year, fin.month)[1]
    proy = L["uds_mes"] / fin.day * dias_mes
    mes_txt = MESES[fin.month]
    mes_cerrado = fin.day == dias_mes
    ing = L["ing_mes"]
    margen = ing - L["uds_mes"] * COSTO_LENTE

    # 1) Meta de lentes
    if mes_cerrado:
        dif = L["uds_mes"] - META_LENTES_MES
        alertas.append(
            f"🎯 Lentes en {mes_txt} (mes cerrado): {_f0(L['uds_mes'])} uds vs meta {META_LENTES_MES} "
            f"({'superaste por' if dif >= 0 else 'faltaron'} {_f0(abs(dif))}). Margen estimado: S/ {_f0(margen)}."
        )
    else:
        dif = proy - META_LENTES_MES
        alertas.append(
            f"🎯 Lentes en {mes_txt}: {_f0(L['uds_mes'])} uds al día {fin.day}. Al ritmo actual cerrarías en "
            f"~{_f0(proy)} (meta {META_LENTES_MES}: {'te sobran' if dif >= 0 else 'te faltan'} ~{_f0(abs(dif))}). "
            f"Margen estimado hasta hoy: S/ {_f0(margen)}."
        )
        if dif < 0:
            faltan_hoy = max(0, META_LENTES_MES - L["uds_mes"])
            alertas.append(f"   Para llegar a la meta necesitas vender {_f0(faltan_hoy)} unidades más antes de fin de mes.")

    # 2) Semana vs semana previa
    for macro in ("Lentes", "Pelucas"):
        v = ventas[macro]
        delta = v["uds_sem"] - v["uds_prev"]
        alertas.append(
            f"📈 {macro}: {_f0(v['uds_sem'])} uds en los últimos 7 días ({delta:+.0f} vs la semana anterior, "
            f"S/ {_f0(v['ing_sem'])} de ingresos)."
        )

    # 3) Recompra
    n_rec = segmentos.get("Recompra probable", 0)
    n_dor = segmentos.get("Dormido", 0)
    if n_rec or n_dor:
        esc_uds = round(n_rec * TASA_RECOMPRA_SUPUESTA)  # (los «antiguos» se tratan aparte)
        alertas.append(
            f"🔁 {_f0(n_rec)} clientes de lentes están en su ventana de recompra y {_f0(n_dor)} están dormidos. "
            f"Escenario (supuesto, no dato): si recompra el {int(TASA_RECOMPRA_SUPUESTA * 100)}% de los primeros, "
            f"serían ~{esc_uds} unidades ≈ S/ {_f0(esc_uds * PRECIO_LENTE)}."
        )
    n_ant = segmentos.get("Antiguo", 0)
    if n_ant:
        alertas.append(
            f"🕰️ {_f0(n_ant)} clientes antiguos (compraron lentes hace más de {DORMIDO_MAX_DIAS // 30} meses) para "
            f"reactivar de a poco, empezando por los que más veces compraron. Se recompra poco con el tiempo: "
            f"úsalos para recuperar contacto, no esperes una conversión alta."
        )
    if grupos and grupos.get("por_grupo"):
        pg = grupos["por_grupo"]
        esp = grupos.get("esperas_dias", {})
        alertas.append(
            "🎯 Grupos del bot de marketing: "
            + " | ".join(f"G{g} {NOMBRE_GRUPO[g]}: {_f0(pg.get(g, 0))} (espera {esp.get(g, esp.get(str(g), '?'))} d)"
                         for g in (1, 2, 3, 4)))
        if grupos.get("n_sin_mkt_nuevos"):
            alertas.append(f"🚫 {_f0(grupos['n_sin_mkt_nuevos'])} contactos fríos (grupo 4) ya recibieron "
                           f"{MAX_SIN_RESPUESTA_G4} mensajes sin responder"
                           + (": quedaron en «sin publicidad»." if AUTO_SIN_MKT else " (AUTO_SIN_MKT=0: no se marcaron)."))
        if grupos.get("mensajes_automaticos"):
            alertas.append(f"🤖 {_f0(grupos['mensajes_automaticos'])} mensajes entrantes se detectaron como respuestas "
                           f"automáticas de empresas y NO cuentan como que el cliente escribió.")
    n_his = segmentos.get("Historial", 0)
    if n_his:
        alertas.append(
            f"📇 {_f0(n_his)} contactos del historial (compraron lentes en 2020-2025, su teléfono no está en tu ERP). "
            f"Son los más fríos: escríbeles de a pocos y, si responden, regístralos como clientes."
        )
    if historico:
        sin_cli = historico.get("pedidos_lentes_sin_cliente", 0)
        alertas.append(
            f"📚 Historial importado ({historico['desde']} a {historico['hasta']}): "
            f"{_f0(historico['pedidos_lentes'])} pedidos de lentes de {_f0(historico['clientes_lentes'])} clientes de tu ERP. "
            + (f"Otros {_f0(sin_cli)} pedidos de lentes no se pudieron vincular (su teléfono no está en tu lista de clientes). "
               if sin_cli else "")
            + "Faltan en el historial feb a nov 2024 y abr a dic 2025, así que algunos «antiguos» pueden haber "
              "comprado en esos meses."
        )
    if n_sin_tel:
        alertas.append(f"   {n_sin_tel} clientes candidatos no tienen un teléfono válido y quedaron fuera de la lista.")

    # 4) Clientes del mes
    tot = clientes_mes["nuevos"] + clientes_mes["recurrentes"]
    if tot:
        alertas.append(
            f"👥 Compradores en {mes_txt}: {tot} ({clientes_mes['recurrentes']} ya eran clientes, "
            f"{clientes_mes['nuevos']} nuevos)."
        )

    # 5) Stock
    if reponer:
        top = ", ".join(
            f"{r['producto']} ({'agotado' if r['stock'] <= 0 else format(r['cobertura_dias'], '.0f') + ' días'})"
            for r in reponer[:3]
        )
        alertas.append(
            f"📦 {len(reponer)} productos están agotados o se agotan en menos de {COBERTURA_MIN_DIAS} días "
            f"al ritmo de los últimos 90 días. Los más urgentes: {top}."
        )
    for m in stock_macro:
        if m["macro"] == "Pelucas" and m["vendido_90d"] > 0:
            ritmo_sem = m["vendido_90d"] / 90 * 7
            semanas = (m["stock_uds"] / ritmo_sem) if ritmo_sem > 0 else None
            if semanas is not None:
                alertas.append(
                    f"💇 Pelucas: {_f0(m['stock_uds'])} uds en stock, ritmo ~{ritmo_sem:.1f}/semana → "
                    f"te duran ~{semanas:.0f} semanas."
                )
    if dormido and dormido.get("unidades"):
        alertas.append(
            f"🧊 Stock dormido (sin ventas hace 180+ días): {_f0(dormido['unidades'])} unidades en {dormido['skus']} productos."
        )

    # 6) Bot
    if bot and bot.get("envios"):
        alertas.append(
            f"🤖 Bot de marketing: de {bot['envios']} envíos (hace 37 a 7 días), {bot['con_compra']} "
            f"({bot['pct']}%) compraron dentro de 7 días. Es correlación, no prueba que el mensaje causó la compra."
        )
    return alertas


# ==============================================================================
# RESUMEN CON IA LOCAL (Ollama) — opcional
# ==============================================================================
_RECHAZO = re.compile(r"\b(no puedo|no me es posible|lo siento|no estoy en condiciones|no puedo ayudarte|"
                      r"i can't|i cannot|i'm sorry)\b", re.I)


def _parece_rechazo(texto):
    """Los modelos pequeños a veces rechazan sin motivo. Un plan real tiene varias líneas con guion."""
    lineas_plan = [l for l in texto.splitlines() if l.strip().startswith(("-", "•", "*", "1", "2"))]
    return bool(_RECHAZO.search(texto[:200])) and len(lineas_plan) < 2


def resumen_ollama(alertas):
    url = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/") + "/api/chat"
    modelo = os.getenv("OLLAMA_MODEL", "llama3.1")
    datos = "\n".join(f"- {a.strip()}" for a in alertas)
    sistema = (
        "Eres un asistente de gestión para K&M, una tienda online peruana pequeña y legítima que vende lentes de "
        "contacto cosméticos, pelucas y accesorios por WhatsApp y web. Tu trabajo es leer el reporte semanal de "
        "ventas e inventario del dueño y proponerle tareas de gestión (reponer stock, dar seguimiento a clientes "
        "que ya compraron antes, revisar metas). Es una consulta interna de administración, totalmente normal. "
        "Responde siempre en español, sin rechazos ni advertencias."
    )
    consulta = (
        "Con SOLO los datos de abajo, escribe una lista de 4 a 5 tareas concretas y priorizadas para esta semana "
        "(máximo 130 palabras en total, una tarea por línea, cada línea empieza con un guion). "
        "No inventes cifras, promociones ni datos que no estén abajo.\n\nDATOS:\n" + datos
    )
    for intento in (1, 2):
        try:
            r = requests.post(url, json={"model": modelo, "stream": False, "options": {"temperature": 0.3},
                                         "messages": [{"role": "system", "content": sistema},
                                                      {"role": "user", "content": consulta}]}, timeout=180)
        except requests.exceptions.RequestException as e:
            log(f"⚠️ Ollama no disponible ({type(e).__name__}). Se omite el resumen con IA.")
            return None
        if r.status_code != 200:
            log(f"⚠️ Ollama respondió HTTP {r.status_code}. Se omite el resumen con IA.")
            return None
        texto = ((r.json().get("message") or {}).get("content") or "").strip()
        if texto and not _parece_rechazo(texto):
            return texto
        if intento == 1:
            consulta += "\n\nRecuerda: responde solo con la lista de tareas en español, empezando cada línea con un guion."
    log("⚠️ La IA local no entregó un plan útil (rechazó o respondió vacío). Se omite; el resumen de arriba sigue siendo válido.")
    return None


# ==============================================================================
# GUARDADO
# ==============================================================================
DDL = [
    """
    CREATE TABLE IF NOT EXISTS agente_reportes (
        id SERIAL PRIMARY KEY,
        fecha_generacion TIMESTAMP DEFAULT NOW(),
        periodo_fin DATE,
        kpis JSONB,
        alertas JSONB,
        resumen_ia TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agente_recompra (
        id SERIAL PRIMARY KEY,
        id_reporte INTEGER REFERENCES agente_reportes(id) ON DELETE CASCADE,
        id_cliente INTEGER,
        nombre TEXT,
        telefono TEXT,
        segmento TEXT,
        ultima_compra DATE,
        dias_desde_compra INTEGER,
        n_compras INTEGER,
        gasto_total NUMERIC,
        ciclo_dias NUMERIC,
        ultimo_producto TEXT,
        mensaje_sugerido TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_agente_recompra_rep ON agente_recompra(id_reporte)",
]


def _limpiar(o):
    """Convierte cualquier valor a algo que JSON acepte (NaN -> None, Decimal -> float...)."""
    if isinstance(o, dict):
        return {str(k): _limpiar(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_limpiar(v) for v in o]
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (pd.Timestamp, datetime, date)):
        return None if pd.isna(o) else o.isoformat()[:10]
    if o is pd.NaT:
        return None
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        return None if (o != o) else float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    return o


def guardar(conn, f, kpis, alertas, resumen, candidatos):
    for ddl in DDL:
        conn.execute(text(ddl))
    id_rep = conn.execute(
        text("INSERT INTO agente_reportes (periodo_fin, kpis, alertas, resumen_ia) "
             "VALUES (:fin, CAST(:k AS jsonb), CAST(:a AS jsonb), :r) RETURNING id"),
        {"fin": f["fin"], "k": json.dumps(_limpiar(kpis)), "a": json.dumps(_limpiar(alertas)), "r": resumen},
    ).scalar()

    if candidatos is not None and not candidatos.empty:
        filas = []
        for _, r in candidatos.iterrows():
            filas.append({
                "rep": id_rep, "cli": None if pd.isna(r["id_cliente"]) else int(r["id_cliente"]), "nom": r["nombre"], "tel": r["telefono"],
                "seg": r["segmento"], "ult": r["ultima_compra"], "dias": int(r["dias_desde_compra"]),
                "n": int(r["n_compras"]), "gasto": float(r["gasto_total"] or 0),
                "ciclo": None if pd.isna(r["ciclo_dias"]) else float(r["ciclo_dias"]),
                "prod": r["ultimo_producto"], "msg": r["mensaje_sugerido"],
            })
        conn.execute(text(
            "INSERT INTO agente_recompra (id_reporte, id_cliente, nombre, telefono, segmento, ultima_compra, "
            "dias_desde_compra, n_compras, gasto_total, ciclo_dias, ultimo_producto, mensaje_sugerido) "
            "VALUES (:rep, :cli, :nom, :tel, :seg, :ult, :dias, :n, :gasto, :ciclo, :prod, :msg)"), filas)

    # Conservar solo los últimos reportes (los candidatos se borran en cascada)
    conn.execute(text(
        "DELETE FROM agente_reportes WHERE id NOT IN "
        "(SELECT id FROM agente_reportes ORDER BY id DESC LIMIT :n)"), {"n": REPORTES_A_CONSERVAR})
    return id_rep


# ==============================================================================
# ORQUESTADOR
# ==============================================================================
def generar_reporte(usar_ia=True, guardar_en_bd=True):
    f = calcular_fechas()
    log(f"🤖 Agente semanal — análisis hasta {f['fin']} (7 días: {f['s_ini']} a {f['fin']})")

    # Todas las lecturas van en UNA transacción con tiempo límite ampliado (SET LOCAL
    # se revierte solo al terminar, no afecta al resto del ERP).
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = 120000"))
        cargar_nombres(conn)
        ventas = consultar_ventas(conn, f)
        tendencia = consultar_tendencia(conn, f)
        clientes_mes, base = consultar_clientes(conn, f)
        recompra = analizar_recompra(conn, f)
        recompra = pd.concat([recompra, contactos_historial(conn, f, recompra)], ignore_index=True)
        historico = resumen_historico(conn)
        grupos, info_grupos = calcular_grupos(conn, f, recompra)
        stock_macro, reponer, dormido = analizar_stock(conn, f)
        tops = top_productos(conn, f)
        bot = efectividad_bot(conn, f)

    segmentos = {}
    if not recompra.empty:
        segmentos = {k: int(v) for k, v in recompra["segmento"].value_counts().items()}
    candidatos, n_sin_tel = preparar_candidatos(recompra)

    alertas = construir_alertas(f, ventas, tendencia, clientes_mes, segmentos, n_sin_tel,
                                stock_macro, reponer, dormido, bot, historico, info_grupos)

    L = ventas["Lentes"]
    dias_mes = calendar.monthrange(f["fin"].year, f["fin"].month)[1]
    kpis = {
        "fechas": {k: f[k] for k in ("fin", "s_ini", "m_ini")},
        "meta": {
            "meta_lentes_mes": META_LENTES_MES,
            "lentes_mes_uds": L["uds_mes"],
            "proyeccion_mes": L["uds_mes"] / f["fin"].day * dias_mes,
            "ingresos_lentes_mes": L["ing_mes"],
            "margen_estimado_mes": L["ing_mes"] - L["uds_mes"] * COSTO_LENTE,
        },
        "ventas": ventas,
        "tendencia": tendencia.to_dict("records"),
        "clientes_mes": clientes_mes,
        "base": base,
        "segmentos": segmentos,
        "candidatos_sin_telefono": n_sin_tel,
        "stock_macro": stock_macro,
        "reponer": reponer,
        "stock_dormido": dormido,
        "top_productos": tops,
        "bot": bot,
        "historico": historico,
        "bot_grupos": info_grupos,
        "parametros": {
            "recompra_min_dias": RECOMPRA_MIN_DIAS, "recompra_max_dias": RECOMPRA_MAX_DIAS,
            "dormido_max_dias": DORMIDO_MAX_DIAS, "antiguo_max_dias": ANTIGUO_MAX_DIAS,
            "cobertura_min_dias": COBERTURA_MIN_DIAS,
            "tasa_recompra_supuesta": TASA_RECOMPRA_SUPUESTA,
        },
    }

    resumen = resumen_ollama(alertas) if usar_ia else None

    log("\n".join(["", "=== RESUMEN ==="] + alertas))
    if resumen:
        log("\n=== PLAN SUGERIDO (IA local) ===\n" + resumen)
    if info_grupos.get("ejemplos_automaticos"):
        log("\n🔎 Textos detectados como respuesta automática (revísalos por si alguno fuera de un cliente real):\n   "
            + "\n   ".join(info_grupos["ejemplos_automaticos"]))

    if guardar_en_bd:
        with engine.begin() as conn:
            id_rep = guardar(conn, f, kpis, alertas, resumen, candidatos)
            if grupos is not None and not grupos.empty:
                n_g, n_sin = guardar_grupos(conn, grupos, info_grupos)
                log(f"🎯 Grupos del bot guardados: {n_g} clientes."
                    + (f" {n_sin} clientes del grupo 4 pasaron a «sin publicidad»." if n_sin else ""))
        log(f"\n✅ Reporte #{id_rep} guardado. Candidatos a contactar: {0 if candidatos is None else len(candidatos)}")
    else:
        log("\nℹ️ Modo prueba: no se guardó nada en la base de datos.")
    return kpis, alertas, resumen, candidatos


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Agente de análisis semanal de K&M")
    ap.add_argument("--sin-ia", action="store_true", help="No consultar a Ollama")
    ap.add_argument("--prueba", action="store_true", help="No guardar nada, solo mostrar el resultado")
    args = ap.parse_args()
    if args.prueba:
        LOG_EN_BD = False
    try:
        generar_reporte(usar_ia=not args.sin_ia, guardar_en_bd=not args.prueba)
    except Exception:
        traceback.print_exc()
        log("❌ El agente semanal falló:\n" + traceback.format_exc()[-1500:])
        sys.exit(1)