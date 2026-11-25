import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime
from marketing_grupos import asegurar_esquema, leer_limites, NOMBRE_GRUPO, SESIONES, ZOMBIE_VENTANAS, ZOMBIE_DIA_SALIDA

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
# 📨 REPORTE: ENVÍOS POR GRUPO (mensajes directos) Y POR TAREA ZOMBIE
# ==============================================================================
def render_reporte_envios_por_grupo():
    st.markdown("**👥 Envíos del bot por grupo y Zombies**")
    periodo = st.radio("Periodo", ["Hoy", "Últimos 7 días", "Últimos 30 días"], horizontal=True,
                       key="periodo_envios_grupo", label_visibility="collapsed")
    dias = {"Hoy": 0, "Últimos 7 días": 6, "Últimos 30 días": 29}[periodo]

    with engine.connect() as conn:
        # Un «envío» = un cliente contactado en un día (evita contar doble el eco del webhook)
        filas = conn.execute(text("""
            SELECT TRIM(COALESCE(session_name, 'default')) AS sesion,
                   COALESCE(origen_bot, 'SIN') AS origen,
                   COUNT(DISTINCT (telefono, fecha::date)) AS envios
            FROM mensajes
            WHERE tipo = 'SALIENTE_BOT' AND fecha >= CURRENT_DATE - (:d * INTERVAL '1 day')
            GROUP BY 1, 2
        """), {"d": dias}).fetchall()
        limites = leer_limites(conn)

    conteo = {}
    for sesion, origen, n in filas:
        conteo[(sesion, origen)] = conteo.get((sesion, origen), 0) + int(n)

    filas_directos, filas_zombie = [], []
    for g, nombre in NOMBRE_GRUPO.items():
        lim = limites.get(g, {})
        fila = {"Grupo": f"G{g} · {lim.get('nombre', nombre)}" + ("" if lim.get("activo", True) else " (apagado)")}
        for ses, vis in SESIONES.items():
            n = conteo.get((ses, f"G{g}"), 0)
            fila[vis] = f"{n} / {lim.get(ses, 0)}" if dias == 0 else str(n)
        fila["Total"] = sum(conteo.get((ses, f"G{g}"), 0) for ses in SESIONES)
        filas_directos.append(fila)
    for nivel in (1, 2, 3):
        fila = {"Nivel": f"🧟 Zombie Lvl {nivel}"}
        for ses, vis in SESIONES.items():
            fila[vis] = conteo.get((ses, f"Z{nivel}"), 0)
        fila["Total"] = sum(conteo.get((ses, f"Z{nivel}"), 0) for ses in SESIONES)
        filas_zombie.append(fila)

    tot_dir = sum(f["Total"] for f in filas_directos)
    tot_zom = sum(f["Total"] for f in filas_zombie)
    tot_sin = sum(conteo.get((ses, "SIN"), 0) for ses in SESIONES)

    k1, k2, k3 = st.columns(3)
    k1.metric("📨 Mensajes directos (grupos)", tot_dir)
    k2.metric("🧟 Reactivación Zombie", tot_zom)
    k3.metric("✍️ Otros salientes sin etiqueta", tot_sin,
              help="Mensajes salientes que NO envió el bot: escritos desde el teléfono, o enviados antes de "
                   "que el bot empezara a etiquetar cada mensaje con su grupo.")

    c_dir, c_zom = st.columns([3, 2])
    with c_dir:
        st.caption("Mensajes directos por grupo" + (" (enviados / límite diario)" if dias == 0 else ""))
        st.dataframe(pd.DataFrame(filas_directos), hide_index=True, use_container_width=True)
    with c_zom:
        st.caption("Tarea Zombie (nivel del cliente al enviar)")
        st.dataframe(pd.DataFrame(filas_zombie), hide_index=True, use_container_width=True)


# ==============================================================================
# 📈 REPORTE: AVANCE DE CADA GRUPO (cuántos ya recibieron mensaje dentro de su espera)
# ==============================================================================
SQL_AVANCE_GRUPOS = """
    WITH base AS (
        -- mismos filtros de cliente que usa el bot (recurrente/bot_marketing.py → SQL_PROSPECTOS)
        SELECT bg.grupo, bg.espera_dias, c.id_cliente,
               COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid) AS destino
        FROM bot_grupos bg
        JOIN clientes c ON c.id_cliente = bg.id_cliente
        JOIN telefonoscliente t ON t.id_cliente = c.id_cliente AND t.activo = TRUE AND t.es_principal = TRUE
        WHERE c.activo = TRUE AND c.estado = 'Sin empezar'
          AND COALESCE(c.excluir_publicidad, FALSE) = FALSE
          AND COALESCE(c.nivel_zombie, 0) = 0
          AND length(COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid)) > 6
    ),
    det AS (
        SELECT b.grupo, b.espera_dias, b.id_cliente,
               ub.ult_bot,
               EXISTS (SELECT 1 FROM mensajes m WHERE m.telefono = b.destino
                        AND m.fecha > NOW() - INTERVAL '14 days') AS conversa_14d,
               EXISTS (SELECT 1 FROM ventas v WHERE v.id_cliente = b.id_cliente
                        AND COALESCE(v.anulado, FALSE) = FALSE
                        AND v.fecha_venta > NOW() - INTERVAL '60 days') AS venta_60d
        FROM base b
        LEFT JOIN LATERAL (SELECT MAX(m.fecha) AS ult_bot FROM mensajes m
                           WHERE m.telefono = b.destino AND m.tipo = 'SALIENTE_BOT') ub ON TRUE
    )
    SELECT grupo, MAX(espera_dias) AS espera,
           COUNT(DISTINCT id_cliente) AS total,
           COUNT(DISTINCT id_cliente) FILTER (WHERE ult_bot > NOW() - espera_dias * INTERVAL '1 day') AS contactados,
           COUNT(DISTINCT id_cliente) FILTER (WHERE (ult_bot IS NULL OR ult_bot <= NOW() - espera_dias * INTERVAL '1 day')
                                                AND NOT conversa_14d AND NOT venta_60d) AS disponibles,
           COUNT(DISTINCT id_cliente) FILTER (WHERE (ult_bot IS NULL OR ult_bot <= NOW() - espera_dias * INTERVAL '1 day')
                                                AND (conversa_14d OR venta_60d)) AS en_pausa,
           COUNT(DISTINCT id_cliente) FILTER (WHERE ult_bot > NOW() - espera_dias * INTERVAL '1 day'
                                                AND ult_bot + espera_dias * INTERVAL '1 day' <= NOW() + INTERVAL '7 days') AS libres_7d,
           MIN(ult_bot + espera_dias * INTERVAL '1 day') FILTER (WHERE ult_bot > NOW() - espera_dias * INTERVAL '1 day') AS proxima
    FROM det GROUP BY grupo ORDER BY grupo
"""


def leer_avance_grupos(conn):
    try:
        return pd.read_sql(text(SQL_AVANCE_GRUPOS), conn)
    except Exception as e:
        st.warning(f"No se pudo calcular el avance por grupo: {e}")
        return pd.DataFrame()


def render_avance_grupos(df):
    st.markdown("**📈 Avance de cada grupo** · clientes que el bot puede contactar ('Sin empezar', con publicidad)")
    if df.empty:
        st.info("Aún no hay grupos calculados (los arma el reporte semanal cada domingo).")
        return

    tot = int(df["total"].sum())
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Clientes en grupos", tot)
    k2.metric("Ya contactados", int(df["contactados"].sum()),
              help="Recibieron mensaje del bot y todavía no se cumple la espera de su grupo.")
    k3.metric("Disponibles ahora", int(df["disponibles"].sum()),
              help="El bot les puede escribir hoy (si el grupo tiene cupo).")
    k4.metric("Avance total", f"{(df['contactados'].sum() / tot * 100) if tot else 0:.1f}%")

    hoy = datetime.date.today()
    filas = []
    for r in df.itertuples(index=False):
        g, total = int(r.grupo), int(r.total)
        if int(r.disponibles) > 0:
            prox = "Ahora"
        elif pd.notna(r.proxima):
            f = pd.Timestamp(r.proxima).date()
            prox = f"{f:%d/%m/%Y} ({(f - hoy).days} días)" if f > hoy else "Hoy"
        else:
            prox = "—"
        filas.append({
            "Grupo": f"G{g} · {NOMBRE_GRUPO.get(g, '')}",
            "Espera": f"{int(r.espera)} días",
            "Clientes": total,
            "Ya contactados": int(r.contactados),
            "Disponibles ahora": int(r.disponibles),
            "En pausa": int(r.en_pausa),
            "Se liberan en 7 días": int(r.libres_7d),
            "Avance": (int(r.contactados) / total) if total else 0.0,
            "Próximo disponible": prox,
        })
    st.dataframe(pd.DataFrame(filas), hide_index=True, width="stretch", column_config={
        "Avance": st.column_config.ProgressColumn("Avance", min_value=0, max_value=1, format="percent"),
        "Ya contactados": st.column_config.NumberColumn(help="Recibieron mensaje del bot dentro de la espera de su grupo."),
        "En pausa": st.column_config.NumberColumn(help="Ya cumplieron la espera, pero conversaron en los últimos 14 días o compraron en los últimos 60: el bot no les escribe todavía."),
        "Próximo disponible": st.column_config.TextColumn(help="Si no queda nadie disponible, cuándo se libera el primero."),
    })
    st.caption("Un grupo al 100% no es un error: ya se le escribió a todos y el bot espera a que se cumpla su plazo. "
               "Los grupos se recalculan cada domingo (reporte semanal).")


# ==============================================================================
# 📊 PESTAÑA 1: GENERAL
# ==============================================================================
def render_tab_general(config):
    if config and config.bot_activo: 
        st.success("🟢 **ESTADO GLOBAL: BOT ACTIVO Y DISPARANDO**")
    else: 
        st.error("🔴 **ESTADO GLOBAL: BOT APAGADO**")

    # --- CONSULTAS DE MÉTRICAS ---
    with engine.connect() as conn:
        est_counts = {str(row[0]): int(row[1]) for row in conn.execute(text("""
            SELECT TRIM(LOWER(COALESCE(session_name, 'principal'))) as sesion, COUNT(*) as total
            FROM Historial_Estados 
            WHERE fecha_publicacion >= CURRENT_DATE
            GROUP BY 1
        """)).fetchall()}

        fb_counts = {str(row[0]): int(row[1]) for row in conn.execute(text("""
            SELECT TRIM(LOWER(COALESCE(pagina, 'general'))) as pagina, COUNT(*) as total
            FROM Historial_Facebook 
            WHERE fecha >= CURRENT_DATE
            GROUP BY 1
        """)).fetchall()}

        avance_grupos = leer_avance_grupos(conn)

    # ==================================================================
    # 📊 1.1 REPORTE DE OPERACIONES DIARIAS
    # ==================================================================
    with st.container(border=True):
        st.subheader("📊 1.1 Reporte de Operaciones Diarias")
        render_reporte_envios_por_grupo()

        st.write("")
        render_avance_grupos(avance_grupos)

    # ==================================================================
    # 📱 1.2 ESTADOS DE WHATSAPP Y FACEBOOK
    # ==================================================================
    with st.container(border=True):
        st.subheader("📱 1.2 Estados de WhatsApp y 📘 Facebook (hoy)")
        c_wsp, c_fb = st.columns(2)
        with c_wsp:
            st.markdown("**📱 Estados de WhatsApp**")
            e1, e2, e3 = st.columns(3)
            e1.metric("Principal", est_counts.get("principal", 0))
            e2.metric("Lentes", est_counts.get("default", 0))
            e3.metric("Total", sum(est_counts.values()))
        with c_fb:
            st.markdown("**📘 Posts de Facebook / Instagram**")
            f1, f2, f3 = st.columns(3)
            f1.metric("Pelucas", fb_counts.get("pelucas", 0))
            f2.metric("Lentes", fb_counts.get("lentes", 0))
            f3.metric("Total", sum(fb_counts.values()))

    st.write("")

    # ==================================================================
    # ⚙️ 2. CONFIGURACIÓN GENERAL
    # ==================================================================
    st.subheader("⚙️ 2. Configuración General")
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
# 👥 LÍMITES DIARIOS POR GRUPO (mensajes directos)
# ==============================================================================
def render_limites_por_grupo():
    st.markdown("**👥 Límites diarios por grupo**")
    st.caption(
        "Cuántos clientes de cada grupo puede contactar cada cuenta por día (el total diario de la cuenta es la suma "
        "de sus grupos activos). El bot reparte el día dando prioridad al grupo con menos avance respecto a su límite. "
        "G3 y G4 son los contactos fríos (nunca escribieron): mantenlos bajos para cuidar la cuenta ante Meta. "
        "Los grupos los recalcula el agente semanal cada domingo."
    )
    with engine.connect() as conn:
        df_lim = pd.read_sql(text("""
            SELECT l.grupo, l.nombre, l.activo, l.limite_principal, l.limite_default,
                   (SELECT COUNT(*) FROM bot_grupos bg WHERE bg.grupo = l.grupo) AS clientes
            FROM bot_grupos_limites l ORDER BY l.grupo
        """), conn)

    df_edit_lim = st.data_editor(
        df_lim,
        column_config={
            "grupo": st.column_config.NumberColumn("Grupo", disabled=True),
            "nombre": st.column_config.TextColumn("Nombre", disabled=True),
            "activo": st.column_config.CheckboxColumn("Activo"),
            "limite_principal": st.column_config.NumberColumn("Límite/día Principal", min_value=0, max_value=500, step=1),
            "limite_default": st.column_config.NumberColumn("Límite/día Lentes", min_value=0, max_value=500, step=1),
            "clientes": st.column_config.NumberColumn("Clientes en el grupo", disabled=True),
        },
        hide_index=True, key="editor_limites_grupo", use_container_width=True
    )
    if st.button("💾 Guardar Límites por Grupo", type="primary"):
        with engine.begin() as conn:
            for _, row in df_edit_lim.iterrows():
                conn.execute(text("""
                    UPDATE bot_grupos_limites
                    SET activo = :a, limite_principal = :lp, limite_default = :ld
                    WHERE grupo = :g
                """), {
                    "a": bool(row["activo"]),
                    "lp": int(row["limite_principal"]) if pd.notna(row["limite_principal"]) else 0,
                    "ld": int(row["limite_default"]) if pd.notna(row["limite_default"]) else 0,
                    "g": int(row["grupo"]),
                })
        st.success("✅ Límites por grupo guardados.")


# ==============================================================================
# 💬 2PESTAÑA: CONFIGURACIÓN DE MENSAJES
# ==============================================================================
def render_tab_mensajes(config):
    st.subheader("💬 Configuración de Mensajes Directos")
    
    with st.form("form_config_mensajes"):
        st.markdown("**🎲 Probabilidad de disparo por ejecución**")
        st.caption("El bot se ejecuta cada 30 min; en cada ejecución cada cuenta tira un dado y solo envía si cae dentro de este %.")
        c_p, c_l = st.columns(2)

        val_prob_pri = getattr(config, 'intervalo_mensajes_principal', config.intervalo_mensajes) if config else 100
        val_prob_pri = int(val_prob_pri) if str(val_prob_pri).isdigit() else 100
        nuevo_int_pri = c_p.number_input("📱 Principal (KM) %", min_value=0, max_value=100, value=val_prob_pri)

        val_prob_len = getattr(config, 'intervalo_mensajes_default', config.intervalo_mensajes) if config else 100
        val_prob_len = int(val_prob_len) if str(val_prob_len).isdigit() else 100
        nuevo_int_len = c_l.number_input("👓 Lentes %", min_value=0, max_value=100, value=val_prob_len)

        if st.form_submit_button("💾 Guardar Probabilidades por Cuenta", type="primary") and config:
            with engine.begin() as conn_w:
                conn_w.execute(text("""
                    UPDATE Configuracion_Campanas 
                    SET intervalo_mensajes_principal = :int_pri,
                        intervalo_mensajes_default = :int_len
                    WHERE id = :id
                """), {"int_pri": str(nuevo_int_pri), "int_len": str(nuevo_int_len), "id": config.id})
            st.toast("✅ Probabilidades por cuenta actualizadas.")
            st.rerun()

    st.divider()
    render_limites_por_grupo()

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
        
        # Se han retirado los campos de Webhook ya que ahora operan globalmente desde el .env

        if st.form_submit_button("💾 Guardar Configuración", type="primary") and config:
            with engine.begin() as conn_w:
                # 🛠️ CORRECCIÓN: Se retiraron las columnas de webhook_fb_... del UPDATE
                conn_w.execute(text("""
                    UPDATE Configuracion_Campanas 
                    SET fb_activo = :act, intervalo_fb = :int_fb
                    WHERE id = :id
                """), {"act": nuevo_fb_activo, "int_fb": str(nuevo_int_fb), "id": config.id})
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
        
        try:
            mostrar_indicador_suma_fb(df_edit_fb, 'prob_fb_general', 'prob_fb_pelucas', 'prob_fb_lentes')
        except NameError:
            pass
        
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
# 🧟 PESTAÑA 5: ZOMBIES
# ==============================================================================
def render_tab_zombies(config):
    st.subheader("🧟 Configuración de Reactivación Zombie")
    
    # --- 1. CONFIGURACIÓN DE NIVELES ZOMBIE ---
    with st.form("form_config_zombies"):
        st.markdown("**⚙️ Límites y Probabilidades por Nivel**")
        st.caption(
            "Un cliente queda como Zombie cuando su primer mensaje es una frase clave (tabla de abajo). El nivel lo "
            "decide la antigüedad en días calendario y las ventanas no se cruzan; si vuelve a enviar la frase, reinicia "
            f"en Lvl 1, y si escribe texto propio o multimedia deja de ser zombie. Desde el día {ZOMBIE_DIA_SALIDA} sale solo. "
            "«Límite diario» = máximo de mensajes por cliente al día en ese nivel; «Probabilidad» = dado en cada ejecución (cada 30 min)."
        )
        
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.markdown(f"**Lvl 1 · {ZOMBIE_VENTANAS[1]}**")
            m_z1 = st.number_input("Límite Diario Lvl 1", min_value=0, value=getattr(config, 'max_zombies_lvl1', 10))
            p_z1 = st.number_input("Probabilidad Lvl 1 (%)", min_value=0, max_value=100, value=getattr(config, 'prob_zombie_lvl1', 10))
            
        with c2:
            st.markdown(f"**Lvl 2 · {ZOMBIE_VENTANAS[2]}**")
            m_z2 = st.number_input("Límite Diario Lvl 2", min_value=0, value=getattr(config, 'max_zombies_lvl2', 10))
            p_z2 = st.number_input("Probabilidad Lvl 2 (%)", min_value=0, max_value=100, value=getattr(config, 'prob_zombie_lvl2', 20))
            
        with c3:
            st.markdown(f"**Lvl 3 · {ZOMBIE_VENTANAS[3]}**")
            m_z3 = st.number_input("Límite Diario Lvl 3", min_value=0, value=getattr(config, 'max_zombies_lvl3', 10))
            p_z3 = st.number_input("Probabilidad Lvl 3 (%)", min_value=0, max_value=100, value=getattr(config, 'prob_zombie_lvl3', 30))
            
        if st.form_submit_button("💾 Guardar Parámetros Zombie", type="primary") and config:
            with engine.begin() as conn_w:
                conn_w.execute(text("""
                    UPDATE Configuracion_Campanas 
                    SET max_zombies_lvl1 = :m1, prob_zombie_lvl1 = :p1,
                        max_zombies_lvl2 = :m2, prob_zombie_lvl2 = :p2,
                        max_zombies_lvl3 = :m3, prob_zombie_lvl3 = :p3
                    WHERE id = :id
                """), {"m1": m_z1, "p1": p_z1, "m2": m_z2, "p2": p_z2, "m3": m_z3, "p3": p_z3, "id": config.id})
            st.toast("✅ Parámetros Zombie actualizados.")
            st.rerun()

    st.divider()

    # --- 2. PROBABILIDADES POR SUBCATEGORÍA ---
    st.markdown("**🎯 Probabilidad de Envío por Subcategoría (Zombies)**")
    with engine.connect() as conn:
        df_prob_zom = pd.read_sql(text("SELECT id, macro_categoria, subcategoria, prob_zombie_principal, prob_zombie_default FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
    
    if not df_prob_zom.empty:
        df_edit_zom = st.data_editor(
            df_prob_zom,
            column_config={
                "id": None, 
                "macro_categoria": st.column_config.TextColumn("Línea Mayor", disabled=True), 
                "subcategoria": st.column_config.TextColumn("Subcategoría", disabled=True), 
                "prob_zombie_principal": st.column_config.NumberColumn("Principal %", min_value=0, max_value=100, step=5), 
                "prob_zombie_default": st.column_config.NumberColumn("Lentes %", min_value=0, max_value=100, step=5)
            },
            hide_index=True, key="editor_prob_zom", use_container_width=True
        )
        
        try:
            mostrar_indicador_suma(df_edit_zom, 'prob_zombie_principal', 'prob_zombie_default')
        except NameError:
            pass 
        
        if st.button("💾 Guardar Probabilidades (Zombies)", type="primary"):
            with engine.begin() as conn:
                for idx, row in df_edit_zom.iterrows():
                    conn.execute(text("""
                        UPDATE Subcategorias_Sistema 
                        SET prob_zombie_principal = :p1, prob_zombie_default = :p2 
                        WHERE id = :id
                    """), {"p1": row['prob_zombie_principal'], "p2": row['prob_zombie_default'], "id": row['id']})
            st.success("✅ Probabilidades Zombie guardadas.")

    st.divider()

    # --- 3. PERSONALIDAD DE IA ZOMBIE ---
    st.markdown("**🧠 Personalidad de IA para Reactivación Zombie**")
    val_zom = getattr(config, 'prompt_zombie', "Eres un asesor empático intentando reactivar a un cliente que dejó de responder. Sé cálido y no seas insistente.")
    with st.form("form_prompt_zombie"):
        p_zom = st.text_area("Instrucciones base para redactar mensajes a Zombies:", value=val_zom, height=120)
        if st.form_submit_button("💾 Guardar Personalidad Zombie", type="primary"):
            with engine.begin() as conn:
                conn.execute(text("UPDATE Configuracion_Campanas SET prompt_zombie = :pz"), {"pz": p_zom})
            st.toast("✅ ¡Personalidad Zombie actualizada!")
            st.rerun()
            
    st.divider()

    # --- 4. RESPUESTAS AUTOMÁTICAS (FRASES CLAVE) ---
    st.markdown("**📝 Frases Clave y Respuestas Automáticas (Webhooks)**")
    st.caption("Añade, edita o elimina las frases que catalogarán a un cliente como Zombie. El webhook leerá estas reglas en vivo.")
    
    with engine.connect() as conn:
        df_resp = pd.read_sql(text("SELECT id, frase_clave, respuesta_nivel_1, respuesta_nivel_2 FROM respuestas_automaticas ORDER BY id"), conn)
    
    df_edit_resp = st.data_editor(
        df_resp,
        column_config={
            "id": None,
            "frase_clave": st.column_config.TextColumn("Frase Clave (Activador)", required=True),
            "respuesta_nivel_1": st.column_config.TextColumn("Respuesta Rápida 30s (Obligatorio)"),
            "respuesta_nivel_2": st.column_config.TextColumn("Respuesta Extra (Opcional)")
        },
        num_rows="dynamic", hide_index=True, key="editor_resp_auto", use_container_width=True
    )
    
    if st.button("💾 Guardar Respuestas Automáticas", type="primary"):
        with engine.begin() as conn_r:
            # Borramos y re-insertamos para soportar la creación/eliminación dinámica desde Streamlit
            conn_r.execute(text("DELETE FROM respuestas_automaticas"))
            for idx, row in df_edit_resp.iterrows():
                if pd.notna(row['frase_clave']) and str(row['frase_clave']).strip() != "":
                    conn_r.execute(text("""
                        INSERT INTO respuestas_automaticas (frase_clave, respuesta_nivel_1, respuesta_nivel_2)
                        VALUES (:f, :r1, :r2)
                    """), {
                        "f": str(row['frase_clave']).strip().lower(), # Forzamos minúsculas para el Webhook
                        "r1": str(row['respuesta_nivel_1']) if pd.notna(row['respuesta_nivel_1']) else "",
                        "r2": str(row['respuesta_nivel_2']) if pd.notna(row['respuesta_nivel_2']) else ""
                    })
        st.success("✅ Frases y respuestas actualizadas correctamente.")

# ==============================================================================
# 🚀 ORQUESTADOR PRINCIPAL DE LA VISTA
# ==============================================================================
def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    # 1. LECTURA ÚNICA GLOBAL DE LA CONFIGURACIÓN BASE
    with engine.begin() as conn:
        asegurar_esquema(conn)  # columna mensajes.origen_bot y tabla bot_grupos_limites
    with engine.connect() as conn:
        config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()

    # 2. RENDERIZADO MODULAR DE PESTAÑAS
    tab_general, tab_mensajes, tab_estados, tab_fb, tab_zombies = st.tabs([
        "📊 1. General", 
        "💬 2. Mensajes", 
        "📱 3. Estados",
        "📘 4. Facebook",
        "🧟 5. Zombies"
    ])

    with tab_general:
        render_tab_general(config)

    with tab_mensajes:
        render_tab_mensajes(config)

    with tab_estados:
        render_tab_estados(config)

    with tab_fb:
        render_tab_facebook(config)
        
    with tab_zombies:
        render_tab_zombies(config)