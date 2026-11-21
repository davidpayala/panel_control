"""
views/reporte_seo.py — Pestaña "SEO web" del módulo REPORTE.
Muestra el último reporte semanal de recurrente/reporte_seo.py (tabla seo_reportes):
indicadores vs semana anterior, acciones para Claude y para ti, y el análisis.
"""
import json

import pandas as pd
import streamlit as st
from sqlalchemy import text

from database import engine

ETIQUETAS = {
    "productos_visibles": "Productos visibles",
    "productos_en_sitemap": "Productos en el sitemap",
    "google_activos": "Activos en Google Shopping",
    "google_rechazados": "Rechazados en Google",
    "shopping_clics_7d": "Clics en Shopping (7 días)",
    "shopping_impresiones_7d": "Impresiones en Shopping (7 días)",
    "organico_clics_7d": "Clics orgánicos (7 días)",
    "organico_impresiones_7d": "Impresiones orgánicas (7 días)",
    "organico_posicion": "Posición media en Google",
    "resenas_aprobadas": "Reseñas aprobadas",
    "articulos_publicados": "Artículos del blog",
    "enlaces_rotos": "Enlaces internos rotos",
}
MENOS_ES_MEJOR = {"google_rechazados", "enlaces_rotos", "organico_posicion"}


def _dict(v):
    if isinstance(v, dict):
        return v
    try:
        return json.loads(v or "{}")
    except Exception:
        return {}


def _cargar(n=8):
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('public.seo_reportes')")).scalar():
            return []
        return conn.execute(text(
            "SELECT id, fecha, kpis, acciones, analisis, detalle FROM seo_reportes ORDER BY fecha DESC LIMIT :n"
        ), {"n": n}).fetchall()


def render_reporte_seo():
    filas = _cargar()
    if not filas:
        st.info("Todavía no hay reportes SEO. Se generan cada sábado (o con: python recurrente/reporte_seo.py).")
        return

    ult = filas[0]
    k = _dict(ult.kpis)
    ant = k.pop("_semana_anterior", {}) or {}
    st.caption(f"Reporte del {ult.fecha:%d/%m/%Y %H:%M}")

    cols = st.columns(4)
    for i, (clave, etiqueta) in enumerate(ETIQUETAS.items()):
        valor = k.get(clave)
        previo = ant.get(clave)
        delta = None
        if isinstance(valor, (int, float)) and isinstance(previo, (int, float)):
            delta = round(valor - previo, 1)
        cols[i % 4].metric(etiqueta, "—" if valor is None else valor, delta,
                           delta_color="inverse" if clave in MENOS_ES_MEJOR else "normal")

    acc = _dict(ult.acciones)
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🤖 Acciones de Claude")
        for a in acc.get("claude", []) or ["Nada pendiente"]:
            st.markdown(f"- {a}")
    with c2:
        st.subheader("🙋 Acciones para ti")
        for a in acc.get("usuario", []) or ["Nada pendiente"]:
            st.markdown(f"- {a}")

    if ult.analisis:
        st.subheader("📝 Análisis de la semana")
        st.markdown(ult.analisis)

    with st.expander("📊 Evolución semanal"):
        hist = []
        for f in reversed(filas):
            kk = _dict(f.kpis)
            hist.append({"Semana": f.fecha.strftime("%d/%m"), **{ETIQUETAS[c]: kk.get(c) for c in ETIQUETAS}})
        st.dataframe(pd.DataFrame(hist).set_index("Semana").T, use_container_width=True)

    det = _dict(ult.detalle)
    with st.expander("🔍 Detalle técnico"):
        rotos = (det.get("sitio") or {}).get("enlaces_rotos") or []
        if rotos:
            st.markdown("**Enlaces internos rotos**")
            st.dataframe(pd.DataFrame(rotos), hide_index=True, use_container_width=True)
        rech = (det.get("merchant") or {}).get("rechazados") or []
        if rech:
            st.markdown("**Rechazados en Google Shopping**")
            st.write(rech)
        sc = det.get("search_console") or {}
        if sc.get("disponible") and sc.get("top_busquedas"):
            st.markdown("**Búsquedas principales en Google**")
            st.dataframe(pd.DataFrame(sc["top_busquedas"]), hide_index=True, use_container_width=True)
        top = (det.get("merchant") or {}).get("shopping_top_28d") or []
        if top:
            st.markdown("**Productos con más clics en Shopping (28 días)**")
            st.dataframe(pd.DataFrame(top), hide_index=True, use_container_width=True)
