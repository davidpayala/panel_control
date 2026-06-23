import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime
import utils 

def inicializar_tabla_bot():
    """Asegura que la tabla base y las nuevas columnas existan"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS Configuracion_Campanas (
                    id SERIAL PRIMARY KEY,
                    bot_activo BOOLEAN DEFAULT FALSE,
                    tipo_objetivo VARCHAR(50) DEFAULT 'Todos',
                    max_mensajes_dia INTEGER DEFAULT 10,
                    hora_inicio TIME DEFAULT '09:00:00',
                    hora_fin TIME DEFAULT '20:00:00'
                );
            """))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS excluir_publicidad BOOLEAN DEFAULT FALSE"))
    except Exception:
        pass

def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    inicializar_tabla_bot()
    tab_francotirador, tab_estados = st.tabs(["🤖 Bot Francotirador WSP", "📱 Estados de WhatsApp"])

    # --------------------------------------------------------------------------
    # PESTAÑA 1: BOT FRANCOTIRADOR (MENSAJES DIRECTOS & REPORTE DE AVANCE)
    # --------------------------------------------------------------------------
    with tab_francotirador:
        st.subheader("Centro de Mando: Bot Francotirador (Venta Directa)")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            
            env_lentes = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = 'default' AND fecha::date = CURRENT_DATE")).scalar()
            env_principal = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND session_name = 'principal' AND fecha::date = CURRENT_DATE")).scalar()

            # ==================================================================
            # 📊 NUEVO: CÁLCULO DE REPORTE DE AVANCE (Últimos 60 Días)
            # ==================================================================
            query_avance = text("""
                SELECT 
                    SUM(CASE WHEN t.telefono IN (
                        SELECT telefono FROM mensajes 
                        WHERE tipo = 'SALIENTE_BOT' AND fecha > (NOW() - INTERVAL '60 days')
                    ) THEN 1 ELSE 0 END) AS enviados_60d,
                    
                    SUM(CASE WHEN t.telefono NOT IN (
                        SELECT telefono FROM mensajes 
                        WHERE tipo = 'SALIENTE_BOT' AND fecha > (NOW() - INTERVAL '60 days')
                    ) THEN 1 ELSE 0 END) AS pendientes_60d

                FROM Clientes c
                JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
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

        # --- TARJETAS SUPERIORES DE DISPARO HOY ---
        col_estado, col_m1, col_m2 = st.columns([1.2, 1, 1])
        with col_estado:
            if config.bot_activo: st.success("🟢 **ESTADO: DISPARANDO**")
            else: st.error("🔴 **ESTADO: APAGADO**")
        with col_m1: st.metric("📨 Avance Lentes (default)", f"{env_lentes} / {config.max_mensajes_dia}")
        with col_m2: st.metric("📨 Avance Master (principal)", f"{env_principal} / {config.max_mensajes_dia}")

        st.divider()

        # --- NUEVA SECCIÓN VISUAL DE COBERTURA ---
        st.subheader("📊 Reporte de Avance de Cobertura (Campañas en curso)")
        st.caption("Mide la saturación de tu público objetivo habilitado (clientes en estado 'Sin empezar' y no restringidos).")

        c_rep1, c_rep2, c_rep3 = st.columns(3)
        c_rep1.metric("a) Contactados (Últimos 60 días)", f"{a_enviados} clientes", help="Clientes a los que el bot ya impactó recientemente.")
        c_rep2.metric("b) En cola (No enviados aún)", f"{b_pendientes} clientes", help="Clientes elegibles pendientes que el bot irá contactando cada hora.")
        c_rep3.metric("c) Cobertura de Base", f"{c_porcentaje:.1f}%", f"{a_enviados}/{total_habilitados} Total", delta_color="normal")

        if total_habilitados > 0:
            st.progress(c_porcentaje / 100.0)
        else:
            st.info("No hay clientes habilitados en estado 'Sin empezar' en este momento.")

        st.divider()

        # --- FORMULARIO DE ESTRATEGIA ---
        st.subheader("⚙️ Parámetros de Disparo")
        with st.form("form_config_bot"):
            nuevo_estado = st.toggle("Activar Francotirador Automático", value=config.bot_activo)
            st.write("") 
            
            c_max, c_hor1, c_hor2 = st.columns(3)
            nuevo_max = c_max.number_input("📈 Límite diario (Por cada WSP)", min_value=1, max_value=200, value=config.max_mensajes_dia)
            nuevo_inicio = c_hor1.time_input("⏰ Hora de Inicio", value=config.hora_inicio)
            nuevo_fin = c_hor2.time_input("⏰ Hora Límite", value=config.hora_fin)

            submit = st.form_submit_button("💾 Guardar Nueva Estrategia", type="primary")
            if submit:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET bot_activo = :act, max_mensajes_dia = :maxm, hora_inicio = :hini, hora_fin = :hfin
                        WHERE id = :id
                    """), {"act": nuevo_estado, "maxm": nuevo_max, "hini": nuevo_inicio, "hfin": nuevo_fin, "id": config.id})
                st.toast("✅ Órdenes actualizadas.")
                st.rerun()

    # --------------------------------------------------------------------------
    # PESTAÑA 2: ESTADOS DE WHATSAPP (Tu código original intacto)
    # --------------------------------------------------------------------------
    with tab_estados:
        st.subheader("⚙️ Automatización Inteligente de Estados")
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()

        with st.form("form_probabilidades"):
            activo = st.toggle("🤖 Activar Publicación Automática de Estados", value=getattr(config, 'estados_activo', False))
            
            st.write("#### Pesos de Probabilidad por Categoría")
            col1, col2, col3 = st.columns(3)
            with col1: p_nat = st.slider("🌿 Estilo Natural", 0, 100, getattr(config, 'prob_natural', 34))
            with col2: p_fan = st.slider("✨ Estilo Fantasía", 0, 100, getattr(config, 'prob_fantasia', 33))
            with col3: p_acc = st.slider("💍 Accesorio", 0, 100, getattr(config, 'prob_accesorios', 33))

            st.write("#### 📲 Probabilidad de Publicación por Cuenta")
            col_len, col_pri = st.columns(2)
            with col_len: p_sesion_len = st.slider("👓 Cuenta Lentes (default)", 0, 100, getattr(config, 'prob_sesion_lentes', 100), format="%d%%")
            with col_pri: p_sesion_pri = st.slider("⭐ Cuenta Principal (principal)", 0, 100, getattr(config, 'prob_sesion_principal', 50), format="%d%%")

            submit_estados = st.form_submit_button("💾 Guardar Configuración", type="primary")

            if submit_estados:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET estados_activo = :act, prob_natural = :nat, prob_fantasia = :fan, prob_accesorios = :acc,
                            prob_sesion_lentes = :ps_len, prob_sesion_principal = :ps_pri
                        WHERE id = :id
                    """), {"act": activo, "nat": p_nat, "fan": p_fan, "acc": p_acc, "ps_len": p_sesion_len, "ps_pri": p_sesion_pri, "id": config.id})
                st.toast("✅ Configuración de estados actualizada.")
                st.rerun()