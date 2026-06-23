import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime
import utils 

# ==============================================================================
# 🧠 INICIALIZACIÓN DE LA BASE DE DATOS DEL BOT (Con Filtros de Categoría)
# ==============================================================================
def inicializar_tabla_bot():
    """Asegura que la tabla exista separando las transacciones para evitar bloqueos"""
    
    # 1. Crear la tabla base
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
    except Exception as e:
        st.error(f"Error creando tabla bot: {e}")

    # 2. Inyectar nuevas columnas de control (Probabilidades y Toggles de Categoría)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS prob_sesion_lentes INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS prob_sesion_principal INTEGER DEFAULT 50"))
            
            # --- NUEVOS TOGGLES DE SUBCATEGORÍA ---
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_len_nat BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_len_fan BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_len_acc BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_pel_nat BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_pel_fan BOOLEAN DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS cat_pel_acc BOOLEAN DEFAULT TRUE"))
    except Exception:
        pass

    # 3. Leer e insertar datos por defecto si la tabla está vacía
    try:
        with engine.begin() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM Configuracion_Campanas")).scalar()
            if count == 0:
                conn.execute(text("""
                    INSERT INTO Configuracion_Campanas 
                    (bot_activo, tipo_objetivo, prob_sesion_lentes, prob_sesion_principal) 
                    VALUES (FALSE, 'Todos', 100, 50)
                """))
    except Exception as e:
        st.error(f"Error insertando configuración del bot: {e}")

# ==============================================================================
# UI - PANEL DE CONTROL PRINCIPAL DE CAMPAÑAS (Presupuestos Independientes)
# ==============================================================================
def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    inicializar_tabla_bot()
    tab_francotirador, tab_estados = st.tabs(["🤖 Bot Francotirador WSP", "📱 Estados de WhatsApp"])

    # --------------------------------------------------------------------------
    # PESTAÑA 1: BOT FRANCOTIRADOR (MENSAJES DIRECTOS)
    # --------------------------------------------------------------------------
    with tab_francotirador:
        st.subheader("Centro de Mando: Bot Francotirador (Venta Directa)")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            
            # --- CONTEOS INDEPENDIENTES POR SESIÓN ---
            env_lentes = conn.execute(text("""
                SELECT COUNT(*) FROM mensajes 
                WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = 'default' AND fecha::date = CURRENT_DATE
            """)).scalar()

            env_principal = conn.execute(text("""
                SELECT COUNT(*) FROM mensajes 
                WHERE tipo = 'SALIENTE_BOT' AND session_name = 'principal' AND fecha::date = CURRENT_DATE
            """)).scalar()

        st.markdown("---")

        # Layout de 3 columnas para ver ambos motores en paralelo
        col_estado, col_m1, col_m2 = st.columns([1.2, 1, 1])
        with col_estado:
            if config.bot_activo: st.success("🟢 **ESTADO: DISPARANDO**")
            else: st.error("🔴 **ESTADO: APAGADO**")
                
        with col_m1:
            st.metric("📨 Avance Lentes (default)", f"{env_lentes} / {config.max_mensajes_dia}")
            
        with col_m2:
            st.metric("📨 Avance Master (principal)", f"{env_principal} / {config.max_mensajes_dia}")

        st.markdown("---")
        st.subheader("⚙️ Parámetros de Disparo (Aplicable a cada cuenta)")
        
        with st.form("form_config_bot"):
            nuevo_estado = st.toggle("Activar Francotirador Automático", value=config.bot_activo)
            st.write("") 
            
            c_max, c_hor1, c_hor2 = st.columns(3)
            nuevo_max = c_max.number_input("📈 Límite diario (Por cada WSP)", min_value=1, max_value=200, value=config.max_mensajes_dia, help="Si pones 16, Lentes enviará 16 y Principal enviará 16.")
            nuevo_inicio = c_hor1.time_input("⏰ Hora de Inicio", value=config.hora_inicio)
            nuevo_fin = c_hor2.time_input("⏰ Hora Límite", value=config.hora_fin)

            st.divider()
            st.write("#### 📂 Carpetas autorizadas para enviar mensajes a clientes")
            st.caption("Quita el check de las mercaderías que NO quieres que el bot ofrezca en los chats.")

            c_sub_len, c_sub_pel = st.columns(2)
            with c_sub_len:
                st.markdown("##### 👓 Catálogo Lentes (kmlentes.pe)")
                len_nat = st.checkbox("🌿 Estilo Natural", value=getattr(config, 'cat_len_nat', True))
                len_fan = st.checkbox("✨ Estilo Fantasía", value=getattr(config, 'cat_len_fan', True))
                len_acc = st.checkbox("💍 Accesorios Lentes", value=getattr(config, 'cat_len_acc', True))

            with c_sub_pel:
                st.markdown("##### ⭐ Catálogo Pelucas (pelucat.pe)")
                pel_nat = st.checkbox("💇‍♀️ Peluca Natural", value=getattr(config, 'cat_pel_nat', True))
                pel_fan = st.checkbox("🦄 Peluca Fantasía", value=getattr(config, 'cat_pel_fan', True))
                pel_acc = st.checkbox("🎀 Accesorios Pelucas", value=getattr(config, 'cat_pel_acc', True))

            st.write("")
            submit = st.form_submit_button("💾 Guardar Nueva Estrategia", type="primary")
            
            if submit:
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        conn.execute(text("""
                            UPDATE Configuracion_Campanas 
                            SET bot_activo = :act, max_mensajes_dia = :maxm, 
                                hora_inicio = :hini, hora_fin = :hfin,
                                cat_len_nat = :ln, cat_len_fan = :lf, cat_len_acc = :la,
                                cat_pel_nat = :pn, cat_pel_fan = :pf, cat_pel_acc = :pa
                            WHERE id = :id
                        """), {
                            "act": nuevo_estado, "maxm": nuevo_max, "hini": nuevo_inicio, "hfin": nuevo_fin,
                            "ln": len_nat, "lf": len_fan, "la": len_acc,
                            "pn": pel_nat, "pf": pel_fan, "pa": pel_acc, "id": config.id
                        })
                        trans.commit()
                        st.toast("✅ Nueva estrategia guardada con éxito.")
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error al guardar: {e}")

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