"""
views/reporte_semanal.py — Módulo REPORTE del ERP.
Muestra el último análisis que dejó el agente semanal (agente_semanal.py).
Solo lee las tablas agente_reportes y agente_recompra.
"""
import json
from urllib.parse import quote

import pandas as pd
import streamlit as st
from sqlalchemy import text

from database import engine


def _num(x, dec=0):
    try:
        return f"{float(x):,.{dec}f}"
    except (TypeError, ValueError):
        return "—"


def _cargar_ultimo():
    with engine.connect() as conn:
        existe = conn.execute(text("SELECT to_regclass('public.agente_reportes')")).scalar()
        if not existe:
            return None, None
        fila = conn.execute(text(
            "SELECT id, fecha_generacion, periodo_fin, kpis, alertas, resumen_ia "
            "FROM agente_reportes ORDER BY id DESC LIMIT 1")).fetchone()
        if not fila:
            return None, None
        cand = pd.read_sql(text(
            "SELECT nombre, telefono, segmento, ultima_compra, dias_desde_compra, n_compras, gasto_total, "
            "ciclo_dias, ultimo_producto, mensaje_sugerido "
            "FROM agente_recompra WHERE id_reporte = :r "
            "ORDER BY CASE segmento WHEN 'Recompra probable' THEN 0 WHEN 'Dormido' THEN 1 WHEN 'Antiguo' THEN 2 ELSE 3 END, id"),
            conn, params={"r": fila.id})
    return fila, cand


def _como_dict(valor):
    if isinstance(valor, (dict, list)):
        return valor
    return json.loads(valor) if valor else {}


def render_reporte_semanal():
    if "REPORTE" not in st.session_state.get("modulos", []) and st.session_state.get("rol") != "Admin":
        st.error("No tienes acceso a este módulo.")
        return

    fila, cand = _cargar_ultimo()

    # ---- Botón para generar el análisis ahora (solo Admin) ----
    if st.session_state.get("rol") == "Admin":
        c1, c2 = st.columns([1, 3])
        if c1.button("🔄 Generar análisis ahora", help="Solo números (sin IA). Puede tardar unos segundos."):
            with st.spinner("Analizando ventas, clientes y stock..."):
                try:
                    import os, sys
                    raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    for sub in ("", "recurrente"):  # donde hayas guardado agente_semanal.py
                        ruta = os.path.join(raiz, sub)
                        if os.path.exists(os.path.join(ruta, "agente_semanal.py")) and ruta not in sys.path:
                            sys.path.insert(0, ruta)
                    from agente_semanal import generar_reporte
                    generar_reporte(usar_ia=False, guardar_en_bd=True)
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo generar el análisis: {e}")
        c2.caption("El agente también corre solo cada semana (ver instrucciones de programación).")

    if fila is None:
        st.info("Todavía no hay ningún reporte. Pulsa «Generar análisis ahora» o ejecuta `python agente_semanal.py`.")
        return

    kpis, alertas = _como_dict(fila.kpis), _como_dict(fila.alertas)
    meta = kpis.get("meta", {})
    ventas = kpis.get("ventas", {})
    seg = kpis.get("segmentos", {})

    st.caption(f"Reporte #{fila.id} · generado {fila.fecha_generacion:%d/%m/%Y %H:%M} · datos hasta {fila.periodo_fin:%d/%m/%Y}")

    # ---- Indicadores principales ----
    c1, c2, c3, c4 = st.columns(4)
    uds_mes = meta.get("lentes_mes_uds", 0)
    meta_mes = meta.get("meta_lentes_mes", 0)
    c1.metric("Lentes este mes", _num(uds_mes), f"meta {_num(meta_mes)}", delta_color="off")
    c2.metric("Proyección de cierre", _num(meta.get("proyeccion_mes")),
              f"{meta.get('proyeccion_mes', 0) - meta_mes:+.0f} vs meta")
    c3.metric("Lentes últimos 7 días", _num(ventas.get("Lentes", {}).get("uds_sem")),
              f"{ventas.get('Lentes', {}).get('uds_sem', 0) - ventas.get('Lentes', {}).get('uds_prev', 0):+.0f} vs semana previa")
    c4.metric("Clientes en ventana de recompra", _num(seg.get("Recompra probable", 0)),
              f"{_num(seg.get('Dormido', 0))} dormidos · {_num(seg.get('Antiguo', 0))} antiguos · {_num(seg.get('Historial', 0))} del historial", delta_color="off")
    if meta_mes:
        st.progress(min(1.0, uds_mes / meta_mes), text=f"Avance de la meta mensual: {uds_mes / meta_mes:.0%}")

    if fila.resumen_ia:
        st.subheader("🧠 Plan sugerido (IA local)")
        st.info(fila.resumen_ia)
        st.caption("Redactado por Ollama a partir de los datos de abajo; revisa siempre las cifras en las pestañas.")

    st.subheader("📝 Resumen de la semana")
    for a in alertas:
        st.markdown(a.strip())

    tab_rec, tab_stock, tab_prod, tab_tend = st.tabs(
        ["🔁 Recompra", "📦 Stock", "🏆 Más vendidos", "📈 Tendencia"])

    # ---- Recompra ----
    with tab_rec:
        df = None
        if cand is None or cand.empty:
            st.info("No hay clientes candidatos en este reporte.")
        else:
            st.caption("Cada enlace abre WhatsApp con el mensaje ya escrito: lo envías tú, uno por uno, "
                       "sin automatizar (así cuidas tu número). Puedes editar el texto antes de enviar.")
            st.caption("Orden: dentro de cada segmento, primero quienes más veces compraron. "
                       "«Antiguo» = cliente del ERP que compró lentes hace más de 18 meses. "
                       "«Historial» = compró lentes en 2020-2025 según tus Excel y su teléfono no está en el ERP: "
                       "son los más fríos, úsalos de a pocos.")
            segs = st.multiselect("Segmento", ["Recompra probable", "Dormido", "Antiguo", "Historial"], default=["Recompra probable"])
            buscar = st.text_input("Buscar por nombre o teléfono")
            df = cand[cand["segmento"].isin(segs)].copy()
            if buscar:
                b = buscar.strip().lower()
                df = df[df["nombre"].fillna("").str.lower().str.contains(b) | df["telefono"].fillna("").str.contains(b)]
            if df.empty:
                st.info("No hay clientes con ese filtro. Prueba con otro segmento o borra la búsqueda.")
                df = None
        if df is not None and not df.empty:
            df["WhatsApp"] = [f"https://wa.me/{t}?text={quote(m or '')}"
                              for t, m in zip(df["telefono"], df["mensaje_sugerido"])]
            st.write(f"{len(df)} clientes")
            st.dataframe(
                df[["nombre", "segmento", "ultima_compra", "dias_desde_compra", "ultimo_producto",
                    "n_compras", "gasto_total", "WhatsApp"]],
                column_config={
                    "nombre": "Cliente", "segmento": "Segmento", "ultima_compra": "Última compra",
                    "dias_desde_compra": "Días sin comprar", "ultimo_producto": "Último producto",
                    "n_compras": "Compras", "gasto_total": st.column_config.NumberColumn("Gasto total (S/)", format="%.0f"),
                    "WhatsApp": st.column_config.LinkColumn("WhatsApp", display_text="Abrir chat"),
                },
                hide_index=True, use_container_width=True)
            st.download_button("⬇️ Descargar lista (CSV)",
                               df.drop(columns=["WhatsApp"]).to_csv(index=False).encode("utf-8-sig"),
                               file_name="recompra_lentes.csv", mime="text/csv")

    # ---- Stock ----
    with tab_stock:
        sm = pd.DataFrame(kpis.get("stock_macro", []))
        if not sm.empty:
            sm = sm.rename(columns={"macro": "Línea", "stock_uds": "Stock", "transito_uds": "En camino",
                                    "vendido_90d": "Vendido 90 d", "cobertura_dias": "Cobertura (días)"})
            st.dataframe(sm, hide_index=True, use_container_width=True)
        rep = pd.DataFrame(kpis.get("reponer", []))
        st.markdown("**Reponer pronto** (se venden y les queda poco)")
        if rep.empty:
            st.success("Ningún producto con rotación está por agotarse.")
        else:
            st.dataframe(rep.rename(columns={"sku": "SKU", "producto": "Producto", "macro": "Línea", "stock": "Stock",
                                             "transito": "En camino", "uds90": "Vendido 90 d",
                                             "cobertura_dias": "Cobertura (días)"}),
                         hide_index=True, use_container_width=True)
        dorm = kpis.get("stock_dormido", {})
        if dorm.get("unidades"):
            st.markdown(f"**Stock dormido** (sin ventas hace 180+ días): {_num(dorm['unidades'])} unidades "
                        f"en {dorm.get('skus', 0)} productos")
            st.dataframe(pd.DataFrame(dorm.get("top", [])), hide_index=True, use_container_width=True)

    # ---- Más vendidos ----
    with tab_prod:
        tops = kpis.get("top_productos", {})
        for macro in ("Lentes", "Pelucas"):
            st.markdown(f"**{macro}** — últimos 90 días")
            t = pd.DataFrame(tops.get(macro, []))
            if t.empty:
                st.caption("Sin ventas en el período.")
            else:
                st.dataframe(t.rename(columns={"producto": "Producto", "uds30": "Uds 30 d", "uds90": "Uds 90 d",
                                               "ing90": "Ingresos 90 d (S/)"}),
                             hide_index=True, use_container_width=True)

    # ---- Tendencia ----
    with tab_tend:
        t = pd.DataFrame(kpis.get("tendencia", []))
        if t.empty:
            st.info("Sin datos de tendencia.")
        else:
            piv = t.pivot_table(index="mes", columns="macro", values="uds", aggfunc="sum").fillna(0)
            st.markdown("**Unidades vendidas por mes**")
            st.bar_chart(piv)
            if meta_mes and "Lentes" in piv.columns:
                st.caption(f"Tu meta de lentes es {meta_mes} unidades al mes.")
