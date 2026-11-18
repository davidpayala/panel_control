import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

# ==========================================================================
# 🧩 MÓDULOS DE PESTAÑAS (Independientes y aislados)
# ==========================================================================
def tab_auditoria_woo():
    st.markdown("### ⚖️ Conciliación de Catálogo (PostgreSQL vs WordPress)")
    st.info("Muestra las discrepancias atrapadas durante la última ejecución de la sincronización de stock en segundo plano.")
    
    col_btn_aud, col_vacio = st.columns([2, 8])
    if col_btn_aud.button("🔄 Refrescar Auditoría", key="btn_ref_aud"):
        st.rerun()
        
    try:
        with engine.connect() as conn:
            df_aud = pd.read_sql(text("SELECT tienda, tipo_error, sku, detalle, fecha_deteccion FROM auditoria_skus_woo ORDER BY tienda, tipo_error, sku"), conn)
    except Exception:
        df_aud = pd.DataFrame() 
        
    if df_aud.empty:
        st.success("✨ **¡Catálogo Inmaculado!** No existen SKUs huérfanos ni discrepancias entre tu PostgreSQL local y WooCommerce en ninguna de tus tiendas.")
    else:
        tiendas_afectadas = df_aud['tienda'].unique()
        for t in tiendas_afectadas:
            st.markdown(f"#### 🏪 Instancia: `{t}`")
            df_t = df_aud[df_aud['tienda'] == t]
            
            c_db_woo, c_woo_db = st.columns(2)
            
            with c_db_woo:
                df_faltan_woo = df_t[df_t['tipo_error'] == 'FALTA_EN_WOO']
                st.error(f"🔴 En tu BD pero NO en WordPress ({len(df_faltan_woo)})")
                st.caption("Falta crear estos ítems en tu página web:")
                if not df_faltan_woo.empty:
                    st.dataframe(df_faltan_woo[['sku', 'detalle']], hide_index=True, use_container_width=True)
                else:
                    st.write("Todo en orden aquí ✔️")
                    
            with c_woo_db:
                df_faltan_db = df_t[df_t['tipo_error'] == 'FALTA_EN_DB']
                st.warning(f"🟡 En WordPress pero NO en tu BD ({len(df_faltan_db)})")
                st.caption("Falta registrar este SKU en tu inventario de Streamlit:")
                if not df_faltan_db.empty:
                    st.dataframe(df_faltan_db[['sku', 'detalle']], hide_index=True, use_container_width=True)
                else:
                    st.write("Todo en orden aquí ✔️")
            st.divider()

    # =========================================================================
    # NUEVA SECCIÓN INTEGRADA: LOGS DE SINCRONIZACIÓN
    # =========================================================================
    st.markdown("### 📡 Logs de Sincronización WOO")
    st.info("Visualiza en tiempo real las operaciones y el estado de tu motor de actualización de WordPress.")
    
    c_filtro_woo, c_btn_woo = st.columns([8, 2])
    filtro_tiempo_woo = c_filtro_woo.radio("⏳ Mostrar eventos de:", ["Últimas 2 horas", "Hoy", "Últimas 24h"], horizontal=True, key="flt_woo")
    
    if c_btn_woo.button("🔄 Refrescar Logs WOO", key="btn_logs_woo"):
        st.rerun()

    condicion_tiempo_woo = "fecha >= NOW() - INTERVAL '2 hours'" if filtro_tiempo_woo == "Últimas 2 horas" else \
                           "fecha::date = CURRENT_DATE" if filtro_tiempo_woo == "Hoy" else \
                           "fecha >= NOW() - INTERVAL '24 hours'"
    
    try:
        # Aseguramos que la tabla exista en caso de que aún no corras el script
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_woo (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text(f"SELECT fecha, mensaje FROM logs_woo WHERE {condicion_tiempo_woo} ORDER BY id DESC"), conn)
        
        if not df_logs.empty:
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros de sincronización WOO generados para el filtro: {filtro_tiempo_woo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos de logs: {e}")

def tab_logs_marketing():
    st.markdown("### 📈 Auditoría del Motor de Marketing")
    st.info("Visualiza en tiempo real las operaciones del bot de prospección.")
    
    c_filtro, c_btn_log = st.columns([8, 2])
    filtro_tiempo = c_filtro.radio("⏳ Mostrar eventos de:", ["Últimas 2 horas", "Hoy", "Últimas 24h"], horizontal=True, key="flt_mkt")
    
    if c_btn_log.button("🔄 Refrescar Logs", key="btn_mkt"):
        st.rerun()

    condicion_tiempo = "fecha >= NOW() - INTERVAL '2 hours'" if filtro_tiempo == "Últimas 2 horas" else \
                       "fecha::date = CURRENT_DATE" if filtro_tiempo == "Hoy" else \
                       "fecha >= NOW() - INTERVAL '24 hours'"
    
    try:
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_marketing (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text(f"SELECT fecha, mensaje FROM logs_marketing WHERE {condicion_tiempo} ORDER BY id DESC"), conn)
        
        if not df_logs.empty:
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros de marketing generados para el filtro: {filtro_tiempo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos: {e}")

def tab_logs_postventa():
    st.markdown("### 📦 Auditoría de Rutina Post-Venta")
    st.info("Visualiza en tiempo real los mensajes de agradecimiento y seguimiento logístico enviados.")
    
    c_filtro, c_btn_log = st.columns([8, 2])
    filtro_tiempo = c_filtro.radio("⏳ Mostrar eventos de:", ["Últimas 2 horas", "Hoy", "Últimas 24h"], horizontal=True, key="flt_pv")
    
    if c_btn_log.button("🔄 Refrescar Logs", key="btn_pv"):
        st.rerun()

    condicion_tiempo = "fecha >= NOW() - INTERVAL '2 hours'" if filtro_tiempo == "Últimas 2 horas" else \
                       "fecha::date = CURRENT_DATE" if filtro_tiempo == "Hoy" else \
                       "fecha >= NOW() - INTERVAL '24 hours'"
    
    try:
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_postventa (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text(f"SELECT fecha, mensaje FROM logs_postventa WHERE {condicion_tiempo} ORDER BY id DESC"), conn)
        
        if not df_logs.empty:
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros de post-venta generados para el filtro: {filtro_tiempo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos: {e}")

def tab_logs_catalogos():
    st.markdown("### 📑 Logs de Catálogos PDF")
    st.info("Visualiza en tiempo real la generación y subida de catálogos a Google Drive (Cron de las 4 AM).")
    
    c_filtro, c_btn_log = st.columns([8, 2])
    filtro_tiempo = c_filtro.radio("⏳ Mostrar eventos de:", ["Últimas 2 horas", "Hoy", "Últimas 24h"], horizontal=True, key="flt_cat")
    
    if c_btn_log.button("🔄 Refrescar Logs Catálogos", key="btn_cat"):
        st.rerun()

    condicion_tiempo = "fecha >= NOW() - INTERVAL '2 hours'" if filtro_tiempo == "Últimas 2 horas" else \
                       "fecha::date = CURRENT_DATE" if filtro_tiempo == "Hoy" else \
                       "fecha >= NOW() - INTERVAL '24 hours'"
    
    try:
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_catalogos (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text(f"SELECT fecha, mensaje FROM logs_catalogos WHERE {condicion_tiempo} ORDER BY id DESC"), conn)
        
        if not df_logs.empty:
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros de catálogos generados para el filtro: {filtro_tiempo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos: {e}")

def tab_logs_agente():
    st.markdown("### 🧠 Logs del Agente Semanal")
    st.info("Lo que hizo el agente de análisis semanal (domingos 7:00 am). El reporte completo está en «Reporte semanal».")

    c_filtro, c_btn_log = st.columns([8, 2])
    filtro_tiempo = c_filtro.radio("⏳ Mostrar eventos de:", ["Últimas 24h", "Últimos 7 días", "Últimos 30 días"],
                                   horizontal=True, key="flt_agente")

    if c_btn_log.button("🔄 Refrescar Logs Agente", key="btn_agente"):
        st.rerun()

    condicion_tiempo = "fecha >= NOW() - INTERVAL '24 hours'" if filtro_tiempo == "Últimas 24h" else \
                       "fecha >= NOW() - INTERVAL '7 days'" if filtro_tiempo == "Últimos 7 días" else \
                       "fecha >= NOW() - INTERVAL '30 days'"

    try:
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_agente (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text(f"SELECT fecha, mensaje FROM logs_agente WHERE {condicion_tiempo} ORDER BY id DESC"), conn)

        if not df_logs.empty:
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros del agente semanal para el filtro: {filtro_tiempo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos: {e}")

def tab_logs_respaldo():
    st.markdown("### 💾 Logs del Respaldo Semanal")
    st.info("Respaldo de la base de datos y sesiones de WhatsApp hacia el Synology (domingos 4:00 am). "
            "En el NAS se conservan 8 semanas de la base y 2 de WhatsApp.")

    c_filtro, c_btn_log = st.columns([8, 2])
    filtro_tiempo = c_filtro.radio("⏳ Mostrar eventos de:", ["Últimos 7 días", "Últimos 30 días", "Últimos 90 días"],
                                   horizontal=True, key="flt_respaldo")

    if c_btn_log.button("🔄 Refrescar Logs Respaldo", key="btn_respaldo"):
        st.rerun()

    dias = {"Últimos 7 días": 7, "Últimos 30 días": 30, "Últimos 90 días": 90}[filtro_tiempo]

    try:
        with engine.begin() as conn_init:
            conn_init.execute(text("""
                CREATE TABLE IF NOT EXISTS logs_respaldo (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    mensaje TEXT
                )
            """))

        with engine.connect() as conn:
            df_logs = pd.read_sql(text("SELECT fecha, mensaje FROM logs_respaldo "
                                       "WHERE fecha >= NOW() - make_interval(days => :d) ORDER BY id DESC"),
                                  conn, params={"d": dias})

        if not df_logs.empty:
            ultimo_ok = df_logs[df_logs['mensaje'].str.startswith("✅")]
            if not ultimo_ok.empty:
                st.success(f"Último respaldo completo: {pd.to_datetime(ultimo_ok.iloc[0]['fecha']).strftime('%Y-%m-%d %H:%M')}")
            df_logs['fecha'] = pd.to_datetime(df_logs['fecha'])
            lineas_log = [f"[{row['fecha'].strftime('%Y-%m-%d %H:%M:%S')}] {row['mensaje']}" for _, row in df_logs.iterrows()]
            st.code("\n".join(lineas_log), language="log")
        else:
            st.warning(f"Aún no hay registros de respaldo para el filtro: {filtro_tiempo}")

    except Exception as e:
        st.error(f"❌ Error leyendo la base de datos: {e}")

# ==========================================================================
# 🏗️ RENDERIZADOR PRINCIPAL (Estructura Dinámica)
# ==========================================================================
def render_diagnostico():
    st.title("🛠️ Centro de Diagnóstico")
    
    pestanas = {
        "⚖️ Auditoría WOO": tab_auditoria_woo,
        "📈 Logs Marketing": tab_logs_marketing,
        "📦 Logs Post-Venta": tab_logs_postventa,
        "📑 Logs Catálogos": tab_logs_catalogos,
        "🧠 Logs Agente Semanal": tab_logs_agente,
        "💾 Logs Respaldo": tab_logs_respaldo
    }
    
    tabs_obj = st.tabs(list(pestanas.keys()))
    
    for tab, nombre_pestana in zip(tabs_obj, pestanas.keys()):
        with tab:
            pestanas[nombre_pestana]()