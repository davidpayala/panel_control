import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime

def mostrar_indicador_suma(df, col_pri, col_len):
    """Muestra un indicador visual en vivo de la suma de porcentajes para WhatsApp"""
    suma_pri = df[col_pri].sum()
    suma_len = df[col_len].sum()
    
    col1, col2 = st.columns(2)
    with col1:
        if suma_pri == 100: st.success(f"🟢 **Suma Principal: {suma_pri}%** (Perfecto)")
        else: st.warning(f"⚠️ **Suma Principal: {suma_pri}%** (Recomendado: 100%)")
            
    with col2:
        if suma_len == 100: st.success(f"🟢 **Suma Lentes: {suma_len}%** (Perfecto)")
        else: st.warning(f"⚠️ **Suma Lentes: {suma_len}%** (Recomendado: 100%)")

def mostrar_indicador_suma_fb(df, col_gen, col_pel, col_len):
    """Muestra un indicador visual en vivo de la suma de porcentajes para Facebook Multi-Página"""
    s_gen = df[col_gen].sum()
    s_pel = df[col_pel].sum()
    s_len = df[col_len].sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if s_gen == 100: st.success(f"🟢 **FB General: {s_gen}%** (Perfecto)")
        else: st.warning(f"⚠️ **FB General: {s_gen}%** (Rec: 100%)")
            
    with col2:
        if s_pel == 100: st.success(f"🟢 **FB Pelucas: {s_pel}%** (Perfecto)")
        else: st.warning(f"⚠️ **FB Pelucas: {s_pel}%** (Rec: 100%)")

    with col3:
        if s_len == 100: st.success(f"🟢 **FB Lentes: {s_len}%** (Perfecto)")
        else: st.warning(f"⚠️ **FB Lentes: {s_len}%** (Rec: 100%)")

def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    tab_general, tab_mensajes, tab_estados, tab_fb = st.tabs([
        "📊 1. General", 
        "💬 2. Mensajes", 
        "📱 3. Estados",
        "📘 4. Facebook"
    ])

    opciones_frecuencia = ["cada 30 minutos", "cada hora", "cada 6 horas"]

    # --- LECTURA GLOBAL DE DATOS ---
    with engine.connect() as conn:
        config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        # Consultas de Reportes Diarios
        env_principal = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND session_name = 'principal' AND fecha::date = CURRENT_DATE")).scalar() or 0
        env_lentes = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = 'default' AND fecha::date = CURRENT_DATE")).scalar() or 0

        # Conteo de Estados WSP publicados hoy
        query_est_hoy = text("""
            SELECT COUNT(*) 
            FROM Historial_Estados 
            WHERE COALESCE(fecha_publicacion, NOW())::date = CURRENT_DATE
        """)
        estados_hoy = conn.execute(query_est_hoy).scalar() or 0

        # Conteo de Publicaciones FB realizadas hoy
        query_fb_hoy = text("""
            SELECT COUNT(*) 
            FROM Historial_Facebook 
            WHERE fecha::date = CURRENT_DATE
        """)
        fb_posts_hoy = conn.execute(query_fb_hoy).scalar() or 0

        # Avance de Cobertura Base Clientes
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

    # ==========================================================================
    # PESTAÑA 1: GENERAL
    # ==========================================================================
    with tab_general:
        # --- 1.1 REPORTE ---
        st.subheader("📊 1.1 Reporte")
        
        col_estado, col_e_hoy, col_fb_hoy = st.columns([1.2, 1, 1])
        with col_estado:
            if config and config.bot_activo: st.success("🟢 **BOT ACTIVO Y DISPARANDO**")
            else: st.error("🔴 **BOT APAGADO**")
        with col_e_hoy:
            st.metric("📱 Estados WSP Hoy", f"{estados_hoy} historias")
        with col_fb_hoy:
            st.metric("📘 Posts Facebook Hoy", f"{fb_posts_hoy} publicaciones")

        c_m1, c_m2 = st.columns(2)
        with c_m1: st.metric("📨 Avance DMs (Principal)", f"{env_principal} / {config.max_mensajes_dia if config else 10}")
        with c_m2: st.metric("📨 Avance DMs (Lentes)", f"{env_lentes} / {config.max_mensajes_dia if config else 10}")

        st.caption("📈 Avance de Cobertura de Mensajes Directos (Clientes 'Sin empezar')")
        c_rep1, c_rep2, c_rep3 = st.columns(3)
        c_rep1.metric("Impactados (Últ. 60 días)", f"{a_enviados} clientes")
        c_rep2.metric("En Cola (Pendientes)", f"{b_pendientes} clientes")
        c_rep3.metric("Cobertura Total", f"{c_porcentaje:.1f}%")

        if total_habilitados > 0: st.progress(c_porcentaje / 100.0)
        else: st.info("No hay clientes en estado 'Sin empezar' disponibles.")
        
        st.divider()

        # --- 1.2 CONFIGURACIÓN GENERAL ---
        st.subheader("⚙️ 1.2 Configuración General")
        with st.form("form_config_general"):
            nuevo_estado = st.toggle("Activar Francotirador Automático Global", value=config.bot_activo if config else False)
            
            c_hor1, c_hor2 = st.columns(2)
            nuevo_inicio = c_hor1.time_input("⏰ Hora de Inicio", value=config.hora_inicio if config else datetime.time(9,0))
            nuevo_fin = c_hor2.time_input("⏰ Hora Límite", value=config.hora_fin if config else datetime.time(20,0))

            if st.form_submit_button("💾 Guardar Configuración General", type="primary") and config:
                with engine.begin() as conn_w:
                    conn_w.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET bot_activo = :act, hora_inicio = :hini, hora_fin = :hfin 
                        WHERE id = :id
                    """), {"act": nuevo_estado, "hini": nuevo_inicio, "hfin": nuevo_fin, "id": config.id})
                st.toast("✅ Configuración general actualizada.")
                st.rerun()

        st.write("")
        st.markdown("**🎉 Fechas Festivas y Eventos Especiales**")
        st.caption("Programa aniversarios, feriados o promociones locales para que la IA ambiente sus textos en esas fechas.")
        
        with engine.connect() as conn:
            df_fest = pd.read_sql(text("SELECT id, fecha, nombre_evento, descripcion, activo FROM Festividades ORDER BY fecha ASC"), conn)

        df_edit_fest = st.data_editor(
            df_fest,
            column_config={
                "id": None,
                "fecha": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD", required=True),
                "nombre_evento": st.column_config.TextColumn("Evento (ej: Aniversario Empresa)", required=True),
                "descripcion": st.column_config.TextColumn("Instrucción o Descuento especial"),
                "activo": st.column_config.CheckboxColumn("Activo")
            },
            num_rows="dynamic",
            hide_index=True,
            key="editor_festividades",
            use_container_width=True
        )

        if st.button("💾 Guardar Fechas Festivas", type="primary"):
            with engine.begin() as conn_f:
                conn_f.execute(text("DELETE FROM Festividades"))
                for idx, row in df_edit_fest.iterrows():
                    if pd.notna(row['fecha']) and pd.notna(row['nombre_evento']):
                        conn_f.execute(text("""
                            INSERT INTO Festividades (fecha, nombre_evento, descripcion, activo)
                            VALUES (:f, :nom, :desc, :act)
                        """), {
                            "f": row['fecha'],
                            "nom": row['nombre_evento'],
                            "desc": row['descripcion'] if pd.notna(row['descripcion']) else "",
                            "act": bool(row['activo'])
                        })
            st.success("✅ Calendario de eventos actualizado con éxito.")

        st.write("")
        st.markdown("**📝 Descripciones de Subcategorías (Lectura IA)**")
        with engine.connect() as conn:
            df_desc = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, descripcion_ia FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
            
        if not df_desc.empty:
            df_edit_desc = st.data_editor(
                df_desc,
                column_config={
                    "id": None,
                    "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True),
                    "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True),
                    "descripcion_ia": st.column_config.TextColumn("Enfoque/Beneficios para la IA")
                },
                hide_index=True, key="editor_desc_ia", use_container_width=True
            )
            
            if st.button("💾 Guardar Descripciones", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_desc.iterrows():
                        desc_val = row['descripcion_ia'] if pd.notna(row['descripcion_ia']) else ""
                        conn.execute(text("UPDATE Subcategorias_Sistema SET descripcion_ia = :desc WHERE id = :id"), {"desc": desc_val, "id": row['id']})
                st.success("✅ Descripciones de IA guardadas correctamente.")

    # ==========================================================================
    # PESTAÑA 2: MENSAJES DIRECTOS
    # ==========================================================================
    with tab_mensajes:
        st.subheader("💬 Configuración de Mensajes Directos")
        
        with st.form("form_config_mensajes"):
            col_max, col_freq = st.columns(2)
            nuevo_max = col_max.number_input("📈 Límite diario (Por cuenta)", min_value=1, max_value=200, value=config.max_mensajes_dia if config else 10)
            
            idx_msg = opciones_frecuencia.index(config.intervalo_mensajes) if config and hasattr(config, 'intervalo_mensajes') and config.intervalo_mensajes in opciones_frecuencia else 0
            nuevo_int_msg = col_freq.selectbox("⏱️ Control fino de tiempos:", opciones_frecuencia, index=idx_msg, help="Cada cuánto se envía un nuevo mensaje.")

            if st.form_submit_button("💾 Guardar Parámetros de Mensajes", type="primary") and config:
                with engine.begin() as conn_w:
                    conn_w.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET max_mensajes_dia = :maxm, intervalo_mensajes = :int_msg 
                        WHERE id = :id
                    """), {"maxm": nuevo_max, "int_msg": nuevo_int_msg, "id": config.id})
                st.toast("✅ Parámetros de mensajes actualizados.")
                st.rerun()

        st.divider()
        st.markdown("**🎯 Probabilidad de Envío por Subcategoría**")
        with engine.connect() as conn:
            df_prob_msg = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_msg_principal, prob_msg_default FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
            
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
                hide_index=True, key="editor_prob_msg", use_container_width=True
            )
            mostrar_indicador_suma(df_edit_msg, 'prob_msg_principal', 'prob_msg_default')
            
            if st.button("💾 Guardar Probabilidades (Mensajes)", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_msg.iterrows():
                        conn.execute(text("UPDATE Subcategorias_Sistema SET prob_msg_principal = :p1, prob_msg_default = :p2 WHERE id = :id"), {"p1": row['prob_msg_principal'], "p2": row['prob_msg_default'], "id": row['id']})
                st.success("✅ Probabilidades de mensajes guardadas.")

        st.divider()
        st.markdown("**🧠 Personalidad de IA para Mensajes**")
        val_dm = config.prompt_dm if config and hasattr(config, 'prompt_dm') and config.prompt_dm else "Eres un experto en cierres de ventas por WhatsApp."
        with st.form("form_prompt_dm"):
            p_dm = st.text_area("Instrucciones base para redactar DMs:", value=val_dm, height=120)
            if st.form_submit_button("💾 Guardar Personalidad DM", type="primary"):
                with engine.begin() as conn:
                    conn.execute(text("UPDATE Configuracion_Campanas SET prompt_dm = :pdm"), {"pdm": p_dm})
                st.success("✅ ¡Personalidad de Mensajes actualizada!")
                st.rerun()

    # ==========================================================================
    # PESTAÑA 3: ESTADOS (Stories)
    # ==========================================================================
    with tab_estados:
        st.subheader("📱 Configuración de Estados")
        
        with st.form("form_config_estados"):
            idx_est = opciones_frecuencia.index(config.intervalo_estados) if config and hasattr(config, 'intervalo_estados') and config.intervalo_estados in opciones_frecuencia else 1
            nuevo_int_est = st.selectbox("⏱️ Control fino de tiempos para Estados:", opciones_frecuencia, index=idx_est, help="Cada cuánto subirá una nueva historia.")

            if st.form_submit_button("💾 Guardar Tiempos de Estados", type="primary") and config:
                with engine.begin() as conn_w:
                    conn_w.execute(text("""
                        UPDATE Configuracion_Campanas SET intervalo_estados = :int_est WHERE id = :id
                    """), {"int_est": nuevo_int_est, "id": config.id})
                st.toast("✅ Frecuencia de estados actualizada.")
                st.rerun()

        st.divider()
        st.markdown("**🎯 Probabilidad de Envío por Subcategoría**")
        with engine.connect() as conn:
            df_prob_est = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_est_principal, prob_est_default FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
            
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
                hide_index=True, key="editor_prob_est", use_container_width=True
            )
            mostrar_indicador_suma(df_edit_est, 'prob_est_principal', 'prob_est_default')
            
            if st.button("💾 Guardar Probabilidades (Estados)", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_est.iterrows():
                        conn.execute(text("UPDATE Subcategorias_Sistema SET prob_est_principal = :p1, prob_est_default = :p2 WHERE id = :id"), {"p1": row['prob_est_principal'], "p2": row['prob_est_default'], "id": row['id']})
                st.success("✅ Probabilidades de estados guardadas.")

        st.divider()
        st.markdown("**🧠 Personalidad de IA para Estados**")
        val_estado = config.prompt_estado if config and hasattr(config, 'prompt_estado') and config.prompt_estado else "Eres un copywriter experto en marketing digital."
        with st.form("form_prompt_estado"):
            p_estado = st.text_area("Instrucciones base para historias cortas:", value=val_estado, height=120)
            if st.form_submit_button("💾 Guardar Personalidad Estados", type="primary"):
                with engine.begin() as conn:
                    conn.execute(text("UPDATE Configuracion_Campanas SET prompt_estado = :pe"), {"pe": p_estado})
                st.success("✅ ¡Personalidad de Estados actualizada!")
                st.rerun()

    # ==========================================================================
    # PESTAÑA 4: FACEBOOK (Multi-Página vía Make.com)
    # ==========================================================================
    with tab_fb:
        st.subheader("📘 Configuración Multi-Página de Facebook")
        
        with st.form("form_config_fb"):
            st.info("Configura los Webhooks de Make.com para cada una de tus páginas de Facebook.")
            c_activo, c_freq = st.columns([1, 2])
            
            val_activo = config.fb_activo if config and hasattr(config, 'fb_activo') else False
            nuevo_fb_activo = c_activo.toggle("Activar Auto-Publicación FB", value=bool(val_activo))
            
            idx_fb = opciones_frecuencia.index(config.intervalo_fb) if config and hasattr(config, 'intervalo_fb') and config.intervalo_fb in opciones_frecuencia else 2
            nuevo_int_fb = c_freq.selectbox("⏱️ Frecuencia de Posteo FB:", opciones_frecuencia, index=idx_fb)

            st.markdown("**🔗 Conexiones Webhook (Make.com)**")
            wh_gen = st.text_input("Webhook para FB General:", value=getattr(config, 'webhook_fb_general', ''))
            wh_pel = st.text_input("Webhook para FB Pelucas:", value=getattr(config, 'webhook_fb_pelucas', ''))
            wh_len = st.text_input("Webhook para FB Lentes:", value=getattr(config, 'webhook_fb_lentes', ''))

            if st.form_submit_button("💾 Guardar Configuración Webhooks", type="primary") and config:
                with engine.begin() as conn_w:
                    conn_w.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET fb_activo = :act, intervalo_fb = :int_fb, 
                            webhook_fb_general = :wg, webhook_fb_pelucas = :wp, webhook_fb_lentes = :wl
                        WHERE id = :id
                    """), {
                        "act": nuevo_fb_activo, "int_fb": nuevo_int_fb, 
                        "wg": wh_gen, "wp": wh_pel, "wl": wh_len, "id": config.id
                    })
                st.toast("✅ Configuración de Facebook actualizada.")
                st.rerun()

        st.divider()
        st.markdown("**🎯 Probabilidad de Envío por Página de Facebook**")
        st.caption("Ajusta qué subcategorías se publicarán en qué página. Pon 0% para bloquear una categoría en una página específica.")
        with engine.connect() as conn:
            df_prob_fb = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_fb_general, prob_fb_pelucas, prob_fb_lentes FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
            
        if not df_prob_fb.empty:
            df_edit_fb = st.data_editor(
                df_prob_fb,
                column_config={
                    "id": None,
                    "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True),
                    "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True),
                    "prob_fb_general": st.column_config.NumberColumn("FB General %", min_value=0, max_value=100, step=5),
                    "prob_fb_pelucas": st.column_config.NumberColumn("FB Pelucas %", min_value=0, max_value=100, step=5),
                    "prob_fb_lentes": st.column_config.NumberColumn("FB Lentes %", min_value=0, max_value=100, step=5)
                },
                hide_index=True, key="editor_prob_fb", use_container_width=True
            )
            
            # --- SUMA VISUAL DE PROBABILIDADES DE FACEBOOK ---
            mostrar_indicador_suma_fb(df_edit_fb, 'prob_fb_general', 'prob_fb_pelucas', 'prob_fb_lentes')
            
            if st.button("💾 Guardar Probabilidades (Facebook)", type="primary"):
                with engine.begin() as conn:
                    for idx, row in df_edit_fb.iterrows():
                        conn.execute(text("""
                            UPDATE Subcategorias_Sistema 
                            SET prob_fb_general = :p1, prob_fb_pelucas = :p2, prob_fb_lentes = :p3 
                            WHERE id = :id
                        """), {"p1": row['prob_fb_general'], "p2": row['prob_fb_pelucas'], "p3": row['prob_fb_lentes'], "id": row['id']})
                st.success("✅ Probabilidades de Facebook guardadas.")

        st.divider()
        # --- PERSONALIZACIÓN DE IA PARA FACEBOOK ---
        st.markdown("**🧠 Personalidad de IA para Facebook**")
        val_fb_prompt = config.prompt_fb if config and hasattr(config, 'prompt_fb') and config.prompt_fb else "Eres un creador de contenido experto en redes sociales. Redacta posts atractivos para Facebook con emojis, llamadas a la acción y hashtags relevantes."
        
        with st.form("form_prompt_fb"):
            p_fb = st.text_area("Instrucciones base para posts de Facebook:", value=val_fb_prompt, height=120)
            if st.form_submit_button("💾 Guardar Personalidad Facebook", type="primary"):
                with engine.begin() as conn:
                    conn.execute(text("UPDATE Configuracion_Campanas SET prompt_fb = :pfb"), {"pfb": p_fb})
                st.success("✅ ¡Personalidad de Facebook actualizada!")
                st.rerun()