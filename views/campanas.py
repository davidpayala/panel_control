import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime

def inicializar_tabla_bot():
    """Asegura que la tabla base y las columnas existan en BD"""
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
                    prob_sesion_principal INTEGER DEFAULT 50,
                    prompt_estado TEXT,
                    prompt_dm TEXT
                );
            """))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS excluir_publicidad BOOLEAN DEFAULT FALSE"))
            
            # --- COLUMNAS DE SUBCATEGORÍAS ---
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_msg_default INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_msg_principal INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_est_default INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS prob_est_principal INTEGER DEFAULT 100"))
            
            # --- NUEVA COLUMNA PARA LA IA ---
            conn.execute(text("ALTER TABLE Subcategorias_Sistema ADD COLUMN IF NOT EXISTS descripcion_ia TEXT"))
            
            # Limpieza de nulos por seguridad
            conn.execute(text("""
                UPDATE Subcategorias_Sistema 
                SET prob_msg_default = COALESCE(prob_msg_default, 100),
                    prob_msg_principal = COALESCE(prob_msg_principal, 100),
                    prob_est_default = COALESCE(prob_est_default, 100),
                    prob_est_principal = COALESCE(prob_est_principal, 100)
            """))
    except Exception as e:
        print(f"Error inicializando BD: {e}")

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
        "📊 1. General y Subcategorías", 
        "💬 2. Opciones para Mensajes", 
        "📱 3. Opciones para Estados"
    ])

    # ==========================================================================
    # PESTAÑA 1: GENERAL, AVANCE Y DESCRIPCIONES IA
    # ==========================================================================
    with tab_general:
        st.subheader("🤖 Centro de Mando General")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            
            env_principal = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND session_name = 'principal' AND fecha::date = CURRENT_DATE")).scalar() or 0
            env_lentes = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = 'default' AND fecha::date = CURRENT_DATE")).scalar() or 0

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

        col_estado, col_m1, col_m2 = st.columns([1.2, 1, 1])
        with col_estado:
            if config and config.bot_activo: st.success("🟢 **ESTADO: DISPARANDO**")
            else: st.error("🔴 **ESTADO: APAGADO**")
        with col_m1: st.metric("📨 Avance Principal", f"{env_principal} / {config.max_mensajes_dia if config else 10}")
        with col_m2: st.metric("📨 Avance Lentes", f"{env_lentes} / {config.max_mensajes_dia if config else 10}")

        st.divider()

        st.subheader("📈 Avance de Cobertura de Base (Clientes 'Sin empezar')")
        c_rep1, c_rep2, c_rep3 = st.columns(3)
        c_rep1.metric("a) Impactados (Últimos 60 días)", f"{a_enviados} clientes")
        c_rep2.metric("b) En Cola (Pendientes)", f"{b_pendientes} clientes")
        c_rep3.metric("c) Cobertura Total", f"{c_porcentaje:.1f}%")

        if total_habilitados > 0:
            st.progress(c_porcentaje / 100.0)
        else:
            st.info("No hay clientes en estado 'Sin empezar' disponibles.")
        
        st.divider()

        st.subheader("⚙️ Parámetros de Disparo y Horarios")
        with st.form("form_config_bot"):
            nuevo_estado = st.toggle("Activar Francotirador Automático", value=config.bot_activo if config else False)
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

        st.divider()

        # --- NUEVA SECCIÓN: DESCRIPCIONES PARA LA IA ---
        st.subheader("📝 Descripciones de Subcategorías para la IA")
        st.caption("Escribe los beneficios, el tono o el enfoque de ventas para cada subcategoría. La IA leerá esto al redactar.")
        
        with engine.connect() as conn:
            df_desc = pd.read_sql(text("""
                SELECT id, macro_categoria, subcategoria, descripcion_ia 
                FROM Subcategorias_Sistema 
                ORDER BY macro_categoria, subcategoria
            """), conn)
            
        if not df_desc.empty:
            df_edit_desc = st.data_editor(
                df_desc,
                column_config={
                    "id": None,
                    "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True),
                    "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True),
                    "descripcion_ia": st.column_config.TextColumn("Enfoque para la IA", help="Escribe los atractivos principales a destacar.")
                },
                hide_index=True,
                key="editor_desc_ia",
                use_container_width=True
            )
            
            if st.button("💾 Guardar Descripciones de IA", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_desc.iterrows():
                        desc_val = row['descripcion_ia'] if pd.notna(row['descripcion_ia']) else ""
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET descripcion_ia = :desc 
                            WHERE id = :id
                        """), {"desc": desc_val, "id": row['id']})
                st.success("✅ Descripciones de IA guardadas correctamente.")

    # ==========================================================================
    # PESTAÑA 2: OPCIONES, PROBABILIDADES Y PROMPT PARA MENSAJES DIRECTOS
    # ==========================================================================
    with tab_mensajes:
        st.subheader("💬 Probabilidad de Envío por Cuenta (Mensajes Directos)")
        st.caption("Ajusta el porcentaje (0 a 100%). Si pones **0%**, ese número jamás enviará mensajes de esa categoría.")
        
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
            mostrar_indicador_suma(df_edit_msg, 'prob_msg_principal', 'prob_msg_default')
            
            if st.button("💾 Guardar Probabilidades de Mensajes", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_msg.iterrows():
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET prob_msg_principal = :p1, prob_msg_default = :p2 
                            WHERE id = :id
                        """), {"p1": row['prob_msg_principal'], "p2": row['prob_msg_default'], "id": row['id']})
                st.success("✅ Probabilidades guardadas.")

        st.divider()

        # --- PROMPT MENSAJES DIRECTOS ---
        st.subheader("🧠 Personalidad de IA para Mensajes")
        val_dm = config.prompt_dm if config and hasattr(config, 'prompt_dm') and config.prompt_dm else "Eres un experto en cierres de ventas por WhatsApp.\nTienda Virtual que atiende a clientes de todo el Perú, con más de 10 años de experiencia."
        
        with st.form("form_prompt_dm"):
            p_dm = st.text_area("Prompt Base para Mensajes Directos (DM):", value=val_dm, height=120)
            if st.form_submit_button("💾 Guardar Personalidad DM", type="primary"):
                try:
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE Configuracion_Campanas SET prompt_dm = :pdm"), {"pdm": p_dm})
                    st.success("✅ ¡Personalidad de Mensajes actualizada!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")

    # ==========================================================================
    # PESTAÑA 3: OPCIONES, PROBABILIDADES Y PROMPT PARA ESTADOS
    # ==========================================================================
    with tab_estados:
        st.subheader("📱 Probabilidades de Contenido para Estados")
        st.caption("Elige qué tipo de productos subirá cada número a sus historias.")
        
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
            mostrar_indicador_suma(df_edit_est, 'prob_est_principal', 'prob_est_default')
            
            if st.button("💾 Guardar Probabilidades de Estados", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_est.iterrows():
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET prob_est_principal = :p1, prob_est_default = :p2 
                            WHERE id = :id
                        """), {"p1": row['prob_est_principal'], "p2": row['prob_est_default'], "id": row['id']})
                st.success("✅ Probabilidades de estados guardadas.")

        st.divider()

        # --- PROMPT ESTADOS ---
        st.subheader("🧠 Personalidad de IA para Estados")
        val_estado = config.prompt_estado if config and hasattr(config, 'prompt_estado') and config.prompt_estado else "Eres un copywriter experto en marketing digital.\nTienda que atiende a clientes de todo el Perú, contamos con más de 10 años de experiencia."
        
        with st.form("form_prompt_estado"):
            p_estado = st.text_area("Prompt Base para Estados/Facebook:", value=val_estado, height=120)
            if st.form_submit_button("💾 Guardar Personalidad Estados", type="primary"):
                try:
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE Configuracion_Campanas SET prompt_estado = :pe"), {"pe": p_estado})
                    st.success("✅ ¡Personalidad de Estados actualizada!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")