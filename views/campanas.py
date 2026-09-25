import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime

# ==============================================================================
# 🧩 FUNCIONES AUXILIARES VISUALES
# ==============================================================================
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

# ==============================================================================
# 📊 PESTAÑA 1: GENERAL
# ==============================================================================
def render_tab_general(config):
    st.subheader("📊 1.1 Reporte de Operaciones Diarias")
    
    if config and config.bot_activo: 
        st.success("🟢 **ESTADO GLOBAL: BOT ACTIVO Y DISPARANDO**")
    else: 
        st.error("🔴 **ESTADO GLOBAL: BOT APAGADO**")

    # --- CONSULTAS DE MÉTRICAS ---
    with engine.connect() as conn:
        # Estados
        est_counts = {str(row[0]): int(row[1]) for row in conn.execute(text("""
            SELECT TRIM(LOWER(COALESCE(session_name, 'principal'))) as sesion, COUNT(*) as total
            FROM Historial_Estados 
            WHERE fecha_publicacion >= CURRENT_DATE
            GROUP BY 1
        """)).fetchall()}

        # Facebook
        fb_counts = {str(row[0]): int(row[1]) for row in conn.execute(text("""
            SELECT TRIM(LOWER(COALESCE(pagina, 'general'))) as pagina, COUNT(*) as total
            FROM Historial_Facebook 
            WHERE fecha >= CURRENT_DATE
            GROUP BY 1
        """)).fetchall()}

        # 🛠️ CORRECCIÓN 1: Mensajes Totales usando COUNT(DISTINCT) y TRIM() sin desfase horario
        env_principal = conn.execute(text("""
            SELECT COUNT(DISTINCT telefono) FROM mensajes 
            WHERE tipo = 'SALIENTE_BOT' 
              AND TRIM(COALESCE(session_name, 'principal')) = 'principal' 
              AND fecha >= CURRENT_DATE
        """)).scalar() or 0
        
        env_lentes = conn.execute(text("""
            SELECT COUNT(DISTINCT telefono) FROM mensajes 
            WHERE tipo = 'SALIENTE_BOT' 
              AND TRIM(COALESCE(session_name, 'default')) = 'default' 
              AND fecha >= CURRENT_DATE
        """)).scalar() or 0

        # 🛠️ CORRECCIÓN 2: Mensajes Nuevos/Fríos aplicando TRIM() en el agrupamiento
        query_nuevos_sesion = text("""
            SELECT 
                TRIM(COALESCE(m.session_name, 'default')) AS sesion,
                COUNT(DISTINCT m.telefono) AS total_nuevos
            FROM mensajes m
            WHERE m.tipo = 'SALIENTE_BOT' 
              AND m.fecha >= CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1 FROM mensajes me 
                  WHERE me.telefono = m.telefono AND me.tipo = 'ENTRANTE' AND me.fecha < m.fecha
              )
            GROUP BY 1
        """)
        nuevos_counts = {str(row[0]): int(row[1]) for row in conn.execute(query_nuevos_sesion).fetchall()}

        # Avance Cobertura
        row_avance = conn.execute(text("""
            WITH enviados_recientes AS (
                SELECT DISTINCT telefono FROM mensajes 
                WHERE tipo = 'SALIENTE_BOT' AND fecha >= (CURRENT_DATE - INTERVAL '60 days')
            )
            SELECT 
                SUM(CASE WHEN er.telefono IS NOT NULL THEN 1 ELSE 0 END), 
                SUM(CASE WHEN er.telefono IS NULL THEN 1 ELSE 0 END)
            FROM Clientes c 
            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente 
            LEFT JOIN enviados_recientes er ON t.telefono = er.telefono
            WHERE c.activo = TRUE 
              AND COALESCE(c.excluir_publicidad, FALSE) = FALSE 
              AND c.estado = 'Sin empezar' 
              AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6;
        """)).fetchone()

    a_enviados = int(row_avance[0]) if row_avance and row_avance[0] else 0
    b_pendientes = int(row_avance[1]) if row_avance and row_avance[1] else 0
    total_habilitados = a_enviados + b_pendientes
    c_porcentaje = (a_enviados / total_habilitados * 100.0) if total_habilitados > 0 else 0.0

# Extracción de valores de mensajes fríos independientes
    nuevos_principal = nuevos_counts.get('principal', 0)
    nuevos_lentes = nuevos_counts.get('default', 0)
    
    max_nuevos_pri = getattr(config, 'max_nuevos_principal', 10) if config else 10
    max_nuevos_len = getattr(config, 'max_nuevos_default', 10) if config else 10

    # 🛠️ NUEVO: Extracción de límites TOTALES independientes (heredando el general si falla)
    max_tot_pri = getattr(config, 'max_mensajes_principal', getattr(config, 'max_mensajes_dia', 10)) if config else 10
    max_tot_len = getattr(config, 'max_mensajes_default', getattr(config, 'max_mensajes_dia', 10)) if config else 10

    # --- UI MÉTRICAS ---
    st.write("")
    st.markdown("**🧊 Restricción Meta: Mensajes Fríos (Nuevos Contactos)**")
    c_frio1, c_frio2 = st.columns(2)
    
    with c_frio1:
        st.metric("Nuevos Contactos (Principal)", f"{nuevos_principal} / {max_nuevos_pri}")
        progreso_pri = min(nuevos_principal / max_nuevos_pri, 1.0) if max_nuevos_pri > 0 else 0.0
        st.progress(progreso_pri)

    with c_frio2:
        st.metric("Nuevos Contactos (Lentes)", f"{nuevos_lentes} / {max_nuevos_len}")
        progreso_len = min(nuevos_lentes / max_nuevos_len, 1.0) if max_nuevos_len > 0 else 0.0
        st.progress(progreso_len)

    st.write("")
    st.markdown("**📨 Mensajes Directos Totales (DMs) Enviados Hoy**")
    c_m1, c_m2 = st.columns(2)
    
    # 🛠️ CORRECCIÓN: Uso de variables separadas y barras de progreso para totales
    with c_m1:
        st.metric("Avance DMs (Principal)", f"{env_principal} / {max_tot_pri}")
        prog_tot_pri = min(env_principal / max_tot_pri, 1.0) if max_tot_pri > 0 else 0.0
        st.progress(prog_tot_pri)

    with c_m2:
        st.metric("Avance DMs (Lentes)", f"{env_lentes} / {max_tot_len}")
        prog_tot_len = min(env_lentes / max_tot_len, 1.0) if max_tot_len > 0 else 0.0
        st.progress(prog_tot_len)

    st.write("")
    st.markdown("**📱 Estados de WhatsApp & 📘 Facebook**")
    c_e1, c_e2, c_f4 = st.columns(3)
    c_e1.metric("Total Estados (WSP)", sum(est_counts.values()))
    c_e2.metric("Total Posts (FB)", sum(fb_counts.values()))

    st.write("")
    st.caption("📈 Cobertura de la Base de Datos (Clientes en estado 'Sin empezar')")
    c_rep1, c_rep2, c_rep3 = st.columns(3)
    c_rep1.metric("Impactados (Últ. 60 días)", f"{a_enviados} clientes")
    c_rep2.metric("En Cola (Pendientes)", f"{b_pendientes} clientes")
    c_rep3.metric("Cobertura Total", f"{c_porcentaje:.1f}%")
    if total_habilitados > 0: 
        st.progress(c_porcentaje / 100.0)
    else: 
        st.info("No hay clientes en estado 'Sin empezar' disponibles.")
    
    st.divider()

    # --- CONFIGURACIÓN GENERAL ---
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

    # --- TABLAS ADMINISTRATIVAS ---
    st.write("")
    st.markdown("**🎉 Fechas Festivas y Eventos Especiales**")
    with engine.connect() as conn:
        # Se extrae día, mes y anticipación
        df_fest = pd.read_sql(text("SELECT id, dia, mes, dias_anticipacion, nombre_evento, descripcion, activo FROM Festividades ORDER BY mes ASC, dia ASC"), conn)

    df_edit_fest = st.data_editor(
        df_fest,
        column_config={
            "id": None,
            "dia": st.column_config.NumberColumn("Día", min_value=1, max_value=31, required=True),
            "mes": st.column_config.NumberColumn("Mes", min_value=1, max_value=12, required=True),
            "dias_anticipacion": st.column_config.NumberColumn("Días Anticipación", min_value=0, required=True, help="Días antes del evento en que la IA empezará a usarlo"),
            "nombre_evento": st.column_config.TextColumn("Evento (ej: Halloween)", required=True),
            "descripcion": st.column_config.TextColumn("Instrucción IA"),
            "activo": st.column_config.CheckboxColumn("Activo")
        },
        num_rows="dynamic", hide_index=True, key="editor_fest", use_container_width=True
    )
    
    if st.button("💾 Guardar Fechas Festivas", type="primary"):
        with engine.begin() as conn_f:
            conn_f.execute(text("DELETE FROM Festividades"))
            for idx, row in df_edit_fest.iterrows():
                if pd.notna(row['dia']) and pd.notna(row['mes']) and pd.notna(row['nombre_evento']):
                    conn_f.execute(text("""
                        INSERT INTO Festividades (dia, mes, dias_anticipacion, nombre_evento, descripcion, activo) 
                        VALUES (:d, :m, :ant, :nom, :desc, :act)
                    """), {
                        "d": int(row['dia']), 
                        "m": int(row['mes']), 
                        "ant": int(row['dias_anticipacion']) if pd.notna(row['dias_anticipacion']) else 0,
                        "nom": row['nombre_evento'], 
                        "desc": row['descripcion'] if pd.notna(row['descripcion']) else "", 
                        "act": bool(row['activo'])
                    })
        st.success("✅ Calendario festivo actualizado.")

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
            hide_index=True, key="editor_desc", use_container_width=True
        )
        if st.button("💾 Guardar Descripciones", type="primary"):
            with engine.begin() as conn:
                for idx, row in df_edit_desc.iterrows():
                    conn.execute(text("UPDATE Subcategorias_Sistema SET descripcion_ia = :desc WHERE id = :id"), {
                        "desc": row['descripcion_ia'] if pd.notna(row['descripcion_ia']) else "", 
                        "id": row['id']
                    })
            st.success("✅ Descripciones guardadas.")
# ==============================================================================
# 💬 2PESTAÑA: CONFIGURACIÓN DE MENSAJES
# ==============================================================================
def render_tab_mensajes(config):
    st.subheader("💬 Configuración de Mensajes Directos")
    
    with st.form("form_config_mensajes"):
        st.markdown("**📱 Cuenta 1: KM (Principal)**")
        c_p1, c_p2, c_p3 = st.columns(3)
        
        # Leemos el nuevo valor, si no existe o es NULL, heredamos el antiguo valor general
        nuevo_max_pri = c_p1.number_input("Límite Total (Principal)", min_value=1, max_value=500, value=getattr(config, 'max_mensajes_principal', getattr(config, 'max_mensajes_dia', 10)))
        nuevo_max_frios_pri = c_p2.number_input("Límite Fríos (Principal)", min_value=1, max_value=300, value=getattr(config, 'max_nuevos_principal', 10))
        
        val_prob_pri = getattr(config, 'intervalo_mensajes_principal', config.intervalo_mensajes) if config else 100
        val_prob_pri = int(val_prob_pri) if str(val_prob_pri).isdigit() else 100
        nuevo_int_pri = c_p3.number_input("Probabilidad (Principal) %", min_value=0, max_value=100, value=val_prob_pri)

        st.divider()
        st.markdown("**👓 Cuenta 2: Lentes (Default)**")
        c_l1, c_l2, c_l3 = st.columns(3)
        
        nuevo_max_len = c_l1.number_input("Límite Total (Lentes)", min_value=1, max_value=500, value=getattr(config, 'max_mensajes_default', getattr(config, 'max_mensajes_dia', 10)))
        nuevo_max_frios_len = c_l2.number_input("Límite Fríos (Lentes)", min_value=1, max_value=300, value=getattr(config, 'max_nuevos_default', 10))
        
        val_prob_len = getattr(config, 'intervalo_mensajes_default', config.intervalo_mensajes) if config else 100
        val_prob_len = int(val_prob_len) if str(val_prob_len).isdigit() else 100
        nuevo_int_len = c_l3.number_input("Probabilidad (Lentes) %", min_value=0, max_value=100, value=val_prob_len)

        if st.form_submit_button("💾 Guardar Parámetros de Mensajes", type="primary") and config:
            with engine.begin() as conn_w:
                conn_w.execute(text("""
                    UPDATE Configuracion_Campanas 
                    SET max_mensajes_principal = :max_tot_pri, 
                        max_mensajes_default = :max_tot_len,
                        max_nuevos_principal = :max_pri, 
                        max_nuevos_default = :max_len, 
                        intervalo_mensajes_principal = :int_pri,
                        intervalo_mensajes_default = :int_len
                    WHERE id = :id
                """), {
                    "max_tot_pri": nuevo_max_pri,
                    "max_tot_len": nuevo_max_len,
                    "max_pri": nuevo_max_frios_pri, 
                    "max_len": nuevo_max_frios_len, 
                    "int_pri": str(nuevo_int_pri), 
                    "int_len": str(nuevo_int_len),
                    "id": config.id
                })
            st.toast("✅ Parámetros independientes actualizados.")
            st.rerun()

    st.divider()
    st.markdown("**🎯 Probabilidad de Envío por Subcategoría**")
    with engine.connect() as conn:
        df_prob_msg = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_msg_principal, prob_msg_default FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
        
    if not df_prob_msg.empty:
        df_edit_msg = st.data_editor(
            df_prob_msg,
            column_config={"id": None, "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True), "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True), "prob_msg_principal": st.column_config.NumberColumn("Principal %", min_value=0, max_value=100, step=5), "prob_msg_default": st.column_config.NumberColumn("Lentes %", min_value=0, max_value=100, step=5)},
            hide_index=True, key="editor_prob_msg", use_container_width=True
        )
        
        # Validar si existe la función mostrar_indicador_suma antes de llamarla para evitar errores visuales
        try:
            mostrar_indicador_suma(df_edit_msg, 'prob_msg_principal', 'prob_msg_default')
        except NameError:
            pass 
        
        if st.button("💾 Guardar Probabilidades (Mensajes)", type="primary"):
            with engine.begin() as conn:
                for idx, row in df_edit_msg.iterrows():
                    conn.execute(text("UPDATE Subcategorias_Sistema SET prob_msg_principal = :p1, prob_msg_default = :p2 WHERE id = :id"), {"p1": row['prob_msg_principal'], "p2": row['prob_msg_default'], "id": row['id']})
            st.success("✅ Probabilidades guardadas.")

    st.divider()
    st.markdown("**🧠 Personalidad de IA para Mensajes**")
    val_dm = getattr(config, 'prompt_dm', "Eres un experto en ventas.")
    with st.form("form_prompt_dm"):
        p_dm = st.text_area("Instrucciones base para redactar DMs:", value=val_dm, height=120)
        if st.form_submit_button("💾 Guardar Personalidad DM", type="primary"):
            with engine.begin() as conn:
                conn.execute(text("UPDATE Configuracion_Campanas SET prompt_dm = :pdm"), {"pdm": p_dm})
            st.toast("✅ ¡Personalidad actualizada!")
            st.rerun()
# ==============================================================================
# 📱 PESTAÑA 3: ESTADOS
# ==============================================================================
def render_tab_estados(config):
    st.subheader("📱 Configuración de Estados")
    
    with st.form("form_config_estados"):
        val_actual_est = int(config.intervalo_estados) if config and str(config.intervalo_estados).isdigit() else 100
        nuevo_int_est = st.number_input("🎲 Probabilidad de Subida (Cada 30 min):", min_value=0, max_value=100, value=val_actual_est)

        if st.form_submit_button("💾 Guardar Tiempos de Estados", type="primary") and config:
            with engine.begin() as conn_w:
                conn_w.execute(text("UPDATE Configuracion_Campanas SET intervalo_estados = :int_est WHERE id = :id"), {"int_est": str(nuevo_int_est), "id": config.id})
            st.toast("✅ Frecuencia actualizada.")
            st.rerun()

    st.divider()
    st.markdown("**🎯 Probabilidad de Envío por Subcategoría**")
    with engine.connect() as conn:
        df_prob_est = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_est_principal, prob_est_default FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
        
    if not df_prob_est.empty:
        df_edit_est = st.data_editor(
            df_prob_est,
            column_config={"id": None, "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True), "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True), "prob_est_principal": st.column_config.NumberColumn("Principal %", min_value=0, max_value=100, step=5), "prob_est_default": st.column_config.NumberColumn("Lentes %", min_value=0, max_value=100, step=5)},
            hide_index=True, key="editor_prob_est", use_container_width=True
        )
        mostrar_indicador_suma(df_edit_est, 'prob_est_principal', 'prob_est_default')
        
        if st.button("💾 Guardar Probabilidades (Estados)", type="primary"):
            with engine.begin() as conn:
                for idx, row in df_edit_est.iterrows():
                    conn.execute(text("UPDATE Subcategorias_Sistema SET prob_est_principal = :p1, prob_est_default = :p2 WHERE id = :id"), {"p1": row['prob_est_principal'], "p2": row['prob_est_default'], "id": row['id']})
            st.success("✅ Probabilidades guardadas.")

    st.divider()
    st.markdown("**🧠 Personalidad de IA para Estados**")
    val_estado = getattr(config, 'prompt_estado', "Eres un experto copywriter.")
    with st.form("form_prompt_estado"):
        p_estado = st.text_area("Instrucciones base para historias cortas:", value=val_estado, height=120)
        if st.form_submit_button("💾 Guardar Personalidad Estados", type="primary"):
            with engine.begin() as conn:
                conn.execute(text("UPDATE Configuracion_Campanas SET prompt_estado = :pe"), {"pe": p_estado})
            st.toast("✅ ¡Personalidad actualizada!")
            st.rerun()

# ==============================================================================
# 📘 PESTAÑA 4: FACEBOOK
# ==============================================================================
def render_tab_facebook(config):
    st.subheader("📘 Configuración Multi-Página de Facebook")
    with st.form("form_config_fb"):
        c_activo, c_freq = st.columns([1, 2])
        nuevo_fb_activo = c_activo.toggle("Activar Auto-Publicación FB", value=bool(getattr(config, 'fb_activo', False)))
        val_actual_fb = int(getattr(config, 'intervalo_fb', '100')) if str(getattr(config, 'intervalo_fb', '100')).isdigit() else 100
        nuevo_int_fb = c_freq.number_input("🎲 Probabilidad de Posteo FB (Cada 30 min):", min_value=0, max_value=100, value=val_actual_fb)
        
        st.markdown("**🔗 Conexiones Webhook (Make.com)**")
        wh_gen = st.text_input("Webhook para FB General:", value=getattr(config, 'webhook_fb_general', ''))
        wh_pel = st.text_input("Webhook para FB Pelucas:", value=getattr(config, 'webhook_fb_pelucas', ''))
        wh_len = st.text_input("Webhook para FB Lentes:", value=getattr(config, 'webhook_fb_lentes', ''))

        if st.form_submit_button("💾 Guardar Configuración", type="primary") and config:
            with engine.begin() as conn_w:
                conn_w.execute(text("""
                    UPDATE Configuracion_Campanas 
                    SET fb_activo = :act, intervalo_fb = :int_fb, webhook_fb_general = :wg, webhook_fb_pelucas = :wp, webhook_fb_lentes = :wl
                    WHERE id = :id
                """), {"act": nuevo_fb_activo, "int_fb": str(nuevo_int_fb), "wg": wh_gen, "wp": wh_pel, "wl": wh_len, "id": config.id})
            st.toast("✅ Configuración FB actualizada.")
            st.rerun()

    st.divider()
    st.markdown("**🎯 Probabilidad de Envío por Página de Facebook**")
    with engine.connect() as conn:
        df_prob_fb = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_fb_general, prob_fb_pelucas, prob_fb_lentes FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
        
    if not df_prob_fb.empty:
        df_edit_fb = st.data_editor(
            df_prob_fb,
            column_config={"id": None, "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True), "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True), "prob_fb_general": st.column_config.NumberColumn("FB General %", min_value=0, max_value=100, step=5), "prob_fb_pelucas": st.column_config.NumberColumn("FB Pelucas %", min_value=0, max_value=100, step=5), "prob_fb_lentes": st.column_config.NumberColumn("FB Lentes %", min_value=0, max_value=100, step=5)},
            hide_index=True, key="editor_prob_fb", use_container_width=True
        )
        mostrar_indicador_suma_fb(df_edit_fb, 'prob_fb_general', 'prob_fb_pelucas', 'prob_fb_lentes')
        
        if st.button("💾 Guardar Probabilidades (Facebook)", type="primary"):
            with engine.begin() as conn:
                for idx, row in df_edit_fb.iterrows():
                    conn.execute(text("UPDATE Subcategorias_Sistema SET prob_fb_general = :p1, prob_fb_pelucas = :p2, prob_fb_lentes = :p3 WHERE id = :id"), {"p1": row['prob_fb_general'], "p2": row['prob_fb_pelucas'], "p3": row['prob_fb_lentes'], "id": row['id']})
            st.success("✅ Probabilidades de FB guardadas.")

    st.divider()
    st.markdown("**🧠 Personalidad de IA para Facebook**")
    val_fb_prompt = getattr(config, 'prompt_fb', "Eres un experto en redes sociales.")
    with st.form("form_prompt_fb"):
        p_fb = st.text_area("Instrucciones base para posts de Facebook:", value=val_fb_prompt, height=120)
        if st.form_submit_button("💾 Guardar Personalidad Facebook", type="primary"):
            with engine.begin() as conn:
                conn.execute(text("UPDATE Configuracion_Campanas SET prompt_fb = :pfb"), {"pfb": p_fb})
            st.toast("✅ ¡Personalidad FB actualizada!")
            st.rerun()

# ==============================================================================
# 🚀 ORQUESTADOR PRINCIPAL DE LA VISTA
# ==============================================================================
def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    # 1. AUTO-SANACIÓN DE BASE DE DATOS
    try:
        with engine.begin() as conn_heal:
            conn_heal.execute(text("ALTER TABLE Historial_Estados ADD COLUMN IF NOT EXISTS session_name VARCHAR(50) DEFAULT 'principal'"))
            conn_heal.execute(text("ALTER TABLE Historial_Facebook ADD COLUMN IF NOT EXISTS pagina VARCHAR(50) DEFAULT 'General'"))
            # NUEVA COLUMNA PARA LÍMITE DE MENSAJES FRÍOS
            conn_heal.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS max_mensajes_nuevos_dia INTEGER DEFAULT 10"))
    except:
        pass

    # 2. LECTURA ÚNICA GLOBAL DE LA CONFIGURACIÓN BASE
    with engine.connect() as conn:
        config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()

    # 3. RENDERIZADO MODULAR DE PESTAÑAS
    tab_general, tab_mensajes, tab_estados, tab_fb = st.tabs([
        "📊 1. General", 
        "💬 2. Mensajes", 
        "📱 3. Estados",
        "📘 4. Facebook"
    ])

    with tab_general:
        render_tab_general(config)

    with tab_mensajes:
        render_tab_mensajes(config)

    with tab_estados:
        render_tab_estados(config)

    with tab_fb:
        render_tab_facebook(config) 