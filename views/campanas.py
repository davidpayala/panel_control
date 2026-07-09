import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime

def inicializar_tabla_bot():
    """Asegura que la tabla base y las columnas de probabilidad por número existan en BD"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Configuracion_Campanas (
                    id SERIAL PRIMARY KEY,
                    bot_activo BOOLEAN DEFAULT FALSE,
                    max_mensajes_dia INTEGER DEFAULT 10,
                    hora_inicio TIME DEFAULT '09:00:00',
                    hora_fin TIME DEFAULT '20:00:00',
                    estados_activo BOOLEAN DEFAULT FALSE,
                    prob_sesion_lentes INTEGER DEFAULT 100,
                    prob_sesion_principal INTEGER DEFAULT 50
                );
            """))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS excluir_publicidad BOOLEAN DEFAULT FALSE"))
            
            # --- COLUMNAS DE PROBABILIDAD INDEPENDIENTES POR NÚMERO ---
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_msg_default INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_msg_principal INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_est_default INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_est_principal INTEGER DEFAULT 100"))
            
            # Limpieza de nulos por seguridad
            conn.execute(text("""
                UPDATE Subcategorias_Sistema 
                SET prob_msg_default = COALESCE(prob_msg_default, 100),
                    prob_msg_principal = COALESCE(prob_msg_principal, 100),
                    prob_est_default = COALESCE(prob_est_default, 100),
                    prob_est_principal = COALESCE(prob_est_principal, 100)
            """))
    except Exception:
        pass

def mostrar_indicador_suma(df, col_pri, col_len):
    """Muestra un indicador visual en vivo de la suma de porcentajes"""
    suma_pri = df[col_pri].sum()
    suma_len = df[col_len].sum()
    
    col1, col2 = st.columns(2)
    with col1:
        if suma_pri == 100:
            st.success(f"🟢 **Suma Principal: {suma_pri}%** (Perfecto)")
        else:
            st.warning(f"⚠️ **Suma Principal: {suma_pri}%** (Recomendado: 100%)")
            
    with col2:
        if suma_len == 100:
            st.success(f"🟢 **Suma Lentes: {suma_len}%** (Perfecto)")
        else:
            st.warning(f"⚠️ **Suma Lentes: {suma_len}%** (Recomendado: 100%)")

def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    inicializar_tabla_bot()
    tab_general, tab_mensajes, tab_estados = st.tabs([
        "📊 1. General y Avance", 
        "💬 2. Opciones para Mensajes", 
        "📱 3. Opciones para Estados"
    ])

    # ==========================================================================
    # PESTAÑA 1: GENERAL, AVANCE DE DISPARO Y COBERTURA
    # ==========================================================================
    with tab_general:
        st.subheader("🤖 Centro de Mando General")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            
            # 1.1 Avance de disparo (Principal primero, Lentes segundo)
            env_principal = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND session_name = 'principal' AND fecha::date = CURRENT_DATE")).scalar() or 0
            env_lentes = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = 'default' AND fecha::date = CURRENT_DATE")).scalar() or 0

            # 1.2 Avance de cobertura general
            query_avance = text("""
                WITH enviados_recientes AS (
                    SELECT DISTINCT telefono 
                    FROM mensajes 
                    WHERE tipo = 'SALIENTE_BOT' AND fecha > (NOW() - INTERVAL '60 days')
                )
                SELECT 
                    SUM(CASE WHEN er.telefono IS NOT NULL THEN 1 ELSE 0 END) AS enviados_60d,
                    SUM(CASE WHEN er.telefono IS NULL THEN 1 ELSE 0 END) AS pendientes_60d
                FROM Clientes c
                JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                LEFT JOIN enviados_recientes er ON t.telefono = er.telefono
                WHERE c.activo = TRUE 
                  AND COALESCE(c.excluir_publicidad, FALSE) = FALSE
                  AND c.estado = 'Sin empezar'
                  AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6;
            """)
            row_avance = conn.execute(query_avance).fetchone()
        
        a_enviados = int(row_avance.enviados_60d) if row_avance and row_avance.enviados_60d else 0
        b_pendientes = int(row_avance.pendientes_60d) if row_avance and row_avance.pendientes_60d else 0
        total_habilitados = a_enviados + b_pendientes
        c_porcentaje = (a_enviados / total_habilitados * 100.0) if total_habilitados > 0 else 0.0

        # --- 1.1 MOSTRAR AVANCE DE DISPARO HOY ---
        col_estado, col_m1, col_m2 = st.columns([1.2, 1, 1])
        with col_estado:
            if config and config.bot_activo: st.success("🟢 **ESTADO: DISPARANDO**")
            else: st.error("🔴 **ESTADO: APAGADO**")
        with col_m1: st.metric("📨 Avance Principal", f"{env_principal} / {config.max_mensajes_dia if config else 10}")
        with col_m2: st.metric("📨 Avance Lentes", f"{env_lentes} / {config.max_mensajes_dia if config else 10}")

        st.divider()

        # --- 1.2 MOSTRAR AVANCE DE COBERTURA ---
        st.subheader("📈 Avance de Cobertura de Base (Clientes 'Sin empezar')")
        c_rep1, c_rep2, c_rep3 = st.columns(3)
        c_rep1.metric("a) Impactados (Últimos 60 días)", f"{a_enviados} clientes")
        c_rep2.metric("b) En Cola (Pendientes)", f"{b_pendientes} clientes")
        c_rep3.metric("c) Cobertura Total", f"{c_porcentaje:.1f}%")

        if total_habilitados > 0:
            st.progress(c_porcentaje / 100.0)
        else:
            st.info("No hay clientes en estado 'Sin empezar' disponibles en la base de datos.")
        
        st.divider()

        # --- 1.3 PARÁMETROS DE DISPARO ---
        st.subheader("⚙️ Parámetros de Disparo y Horarios")
        with st.form("form_config_bot"):
            nuevo_estado = st.toggle("Activar Francotirador Automático (Mensajes Directos)", value=config.bot_activo if config else False)
            st.write("")
            c_max, c_hor1, c_hor2 = st.columns(3)
            nuevo_max = c_max.number_input("📈 Límite diario (Por cada cuenta WSP)", min_value=1, max_value=200, value=config.max_mensajes_dia if config else 10)
            nuevo_inicio = c_hor1.time_input("⏰ Hora de Inicio", value=config.hora_inicio if config else datetime.time(9,0))
            nuevo_fin = c_hor2.time_input("⏰ Hora Límite", value=config.hora_fin if config else datetime.time(20,0))

            if st.form_submit_button("💾 Guardar Parámetros Generales", type="primary") and config:
                with engine.begin() as conn_w:
                    conn_w.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET bot_activo = :act, max_mensajes_dia = :maxm, hora_inicio = :hini, hora_fin = :hfin WHERE id = :id
                    """), {"act": nuevo_estado, "maxm": nuevo_max, "hini": nuevo_inicio, "hfin": nuevo_fin, "id": config.id})
                st.toast("✅ Parámetros guardados exitosamente.")
                st.rerun()

    # ==========================================================================
    # PESTAÑA 2: OPCIONES Y PROBABILIDADES PARA MENSAJES DIRECTOS
    # ==========================================================================
    with tab_mensajes:
        st.subheader("💬 Probabilidad de Envío por Cuenta (Mensajes Directos)")
        st.caption("Ajusta el porcentaje (0 a 100%) de cada producto. Si pones **0%**, ese número jamás enviará mensajes de esa categoría.")
        
        with engine.connect() as conn:
            df_prob_msg = pd.read_sql(text("""
                SELECT id, macro_categoria, subcategoria, prob_msg_principal, prob_msg_default 
                FROM Subcategorias_Sistema 
                ORDER BY macro_categoria, subcategoria
            """), conn)
            
        if not df_prob_msg.empty:
            df_edit_msg = st.data_editor(
                df_prob_msg,
                column_config={
                    "id": None,
                    "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True),
                    "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True),
                    "prob_msg_principal": st.column_config.NumberColumn("Principal %", min_value=0, max_value=100, step=5),
                    "prob_msg_default": st.column_config.NumberColumn("Lentes %", min_value=0, max_value=100, step=5)
                },
                hide_index=True,
                key="editor_prob_msg",
                use_container_width=True
            )
            
            # --- INDICADOR EN VIVO DE SUMA ---
            mostrar_indicador_suma(df_edit_msg, 'prob_msg_principal', 'prob_msg_default')
            
            if st.button("💾 Guardar Probabilidades de Mensajes", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_msg.iterrows():
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET prob_msg_principal = :p1, prob_msg_default = :p2 
                            WHERE id = :id
                        """), {"p1": row['prob_msg_principal'], "p2": row['prob_msg_default'], "id": row['id']})
                st.success("✅ Probabilidades de mensajes directos guardadas correctamente.")
        else:
            st.warning("No hay subcategorías registradas en el sistema.")

    # ==========================================================================
    # PESTAÑA 3: OPCIONES Y PROBABILIDADES PARA ESTADOS (Limpio)
    # ==========================================================================
    with tab_estados:
        st.subheader("📱 Probabilidades de Contenido para Estados")
        st.caption("Elige qué tipo de productos subirá cada número a sus historias. Si pones **0%** en todo, ese número no subirá estados.")
        
        with engine.connect() as conn:
            df_prob_est = pd.read_sql(text("""
                SELECT id, macro_categoria, subcategoria, prob_est_principal, prob_est_default 
                FROM Subcategorias_Sistema 
                ORDER BY macro_categoria, subcategoria
            """), conn)
            
        if not df_prob_est.empty:
            df_edit_est = st.data_editor(
                df_prob_est,
                column_config={
                    "id": None,
                    "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True),
                    "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True),
                    "prob_est_principal": st.column_config.NumberColumn("Principal %", min_value=0, max_value=100, step=5),
                    "prob_est_default": st.column_config.NumberColumn("Lentes %", min_value=0, max_value=100, step=5)
                },
                hide_index=True,
                key="editor_prob_est",
                use_container_width=True
            )
            
            # --- INDICADOR EN VIVO DE SUMA ---
            mostrar_indicador_suma(df_edit_est, 'prob_est_principal', 'prob_est_default')
            
            if st.button("💾 Guardar Probabilidades de Estados", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_est.iterrows():
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET prob_est_principal = :p1, prob_est_default = :p2 
                            WHERE id = :id
                        """), {"p1": row['prob_est_principal'], "p2": row['prob_est_default'], "id": row['id']})
                st.success("✅ Probabilidades de estados guardadas correctamente.")