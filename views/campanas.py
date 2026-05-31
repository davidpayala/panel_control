import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime
import utils 

# ==============================================================================
# 🧠 INICIALIZACIÓN DE LA BASE DE DATOS DEL BOT
# ==============================================================================
def inicializar_tabla_bot():
    """Asegura que la tabla exista separando las transacciones para evitar bloqueos"""
    
    # 1. Crear la tabla (Transacción independiente)
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

    # 2. Intentar aplicar columnas nuevas (Transacción independiente)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS prob_sesion_lentes INTEGER DEFAULT 100"))
            conn.execute(text("ALTER TABLE Configuracion_Campanas ADD COLUMN IF NOT EXISTS prob_sesion_principal INTEGER DEFAULT 50"))
    except Exception:
        # Falla silenciosamente si no hay permisos, ya lo creaste manualmente
        pass

    # 3. Leer e insertar datos por defecto (Transacción independiente)
    try:
        with engine.begin() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM Configuracion_Campanas")).scalar()
            if count == 0:
                conn.execute(text("INSERT INTO Configuracion_Campanas (bot_activo, tipo_objetivo, prob_sesion_lentes, prob_sesion_principal) VALUES (FALSE, 'Todos', 100, 50)"))
    except Exception as e:
        st.error(f"Error insertando configuración del bot: {e}")

# ==============================================================================
# UI - PANEL DE CONTROL PRINCIPAL DE CAMPAÑAS
# ==============================================================================
def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    inicializar_tabla_bot()
    tab_francotirador, tab_estados = st.tabs(["🤖 Bot Francotirador WSP", "📱 Estados de WhatsApp"])

    # --------------------------------------------------------------------------
    # PESTAÑA 1: BOT FRANCOTIRADOR
    # --------------------------------------------------------------------------
    with tab_francotirador:
        st.subheader("Centro de Mando: Bot Francotirador")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            enviados_hoy = conn.execute(text("""
                SELECT COUNT(*) FROM mensajes 
                WHERE tipo = 'SALIENTE_BOT' AND fecha::date = CURRENT_DATE
            """)).scalar()

        st.markdown("---")

        col_estado, col_metric = st.columns([1, 1])
        with col_estado:
            if config.bot_activo:
                st.success("🟢 **ESTADO: EL BOT ESTÁ ACTIVO Y VIGILANDO**")
            else:
                st.error("🔴 **ESTADO: EL BOT ESTÁ APAGADO**")
                
        with col_metric:
            st.metric("📨 Mensajes enviados hoy (Total Salientes)", f"{enviados_hoy} / {config.max_mensajes_dia}")

        st.markdown("---")
        st.subheader("⚙️ Parámetros de la Campaña Actual")
        
        with st.form("form_config_bot"):
            nuevo_estado = st.toggle("Activar Francotirador Automático", value=config.bot_activo)
            st.write("") 
            
            col1, col2 = st.columns(2)
            with col1:
                opciones_tipo = ["Todos", "Natural", "Fantasía", "Accesorios"]
                idx_tipo = opciones_tipo.index(config.tipo_objetivo) if config.tipo_objetivo in opciones_tipo else 0
                nuevo_tipo = st.selectbox("🎯 Tipo de lente a promocionar", opciones_tipo, index=idx_tipo)
                nuevo_max = st.number_input("📈 Máximo de mensajes diarios", min_value=1, max_value=200, value=config.max_mensajes_dia)
                
            with col2:
                st.caption("⏰ Rango de horario permitido:")
                nuevo_inicio = st.time_input("Hora de Inicio", value=config.hora_inicio)
                nuevo_fin = st.time_input("Hora Límite", value=config.hora_fin)

            st.write("")
            submit = st.form_submit_button("💾 Guardar Órdenes", type="primary")
            
            if submit:
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        conn.execute(text("""
                            UPDATE Configuracion_Campanas 
                            SET bot_activo = :act, tipo_objetivo = :tip, max_mensajes_dia = :maxm, hora_inicio = :hini, hora_fin = :hfin
                            WHERE id = :id
                        """), {"act": nuevo_estado, "tip": nuevo_tipo, "maxm": nuevo_max, "hini": nuevo_inicio, "hfin": nuevo_fin, "id": config.id})
                        trans.commit()
                        st.toast("✅ Órdenes actualizadas con éxito.")
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error al guardar: {e}")

    # --------------------------------------------------------------------------
    # PESTAÑA 2: AUTOMATIZACIÓN E INTERFAZ MANUAL DE ESTADOS
    # --------------------------------------------------------------------------
    with tab_estados:
        st.subheader("⚙️ Automatización Inteligente de Estados")
        
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()

        with st.form("form_probabilidades"):
            activo = st.toggle("🤖 Activar Publicación Automática de Estados", value=getattr(config, 'estados_activo', False))
            
            st.write("#### Pesos de Probabilidad por Categoría")
            col1, col2, col3 = st.columns(3)
            with col1:
                p_nat = st.slider("🌿 Estilo Natural", 0, 100, getattr(config, 'prob_natural', 34))
            with col2:
                p_fan = st.slider("✨ Estilo Fantasía", 0, 100, getattr(config, 'prob_fantasia', 33))
            with col3:
                p_acc = st.slider("💍 Accesorio", 0, 100, getattr(config, 'prob_accesorios', 33))

            # --- NUEVA SECCIÓN DE PROBABILIDADES POR LÍNEA DE TELÉFONO ---
            st.write("#### 📲 Probabilidad de Publicación por Cuenta")
            st.caption("Determina qué tan seguido publicará cada cuenta de WhatsApp cuando se dispare el script.")
            
            col_len, col_pri = st.columns(2)
            with col_len:
                p_sesion_len = st.slider("👓 Cuenta Lentes (default)", 0, 100, getattr(config, 'prob_sesion_lentes', 100), format="%d%%")
            with col_pri:
                p_sesion_pri = st.slider("⭐ Cuenta Principal (principal)", 0, 100, getattr(config, 'prob_sesion_principal', 50), format="%d%%")
            # -------------------------------------------------------------

            submit_estados = st.form_submit_button("💾 Guardar Configuración", type="primary")

            if submit_estados:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET estados_activo = :act, prob_natural = :nat, prob_fantasia = :fan, prob_accesorios = :acc,
                            prob_sesion_lentes = :ps_len, prob_sesion_principal = :ps_pri
                        WHERE id = :id
                    """), {
                        "act": activo, "nat": p_nat, "fan": p_fan, "acc": p_acc, 
                        "ps_len": p_sesion_len, "ps_pri": p_sesion_pri, "id": config.id
                    })
                st.toast("✅ Configuración de estados actualizada con éxito.")
                st.rerun()

        st.markdown("---")
        render_prueba_estados()

def render_prueba_estados():
    st.subheader("📱 Prueba Manual: Subir Estado a WhatsApp")
    
    col1, col2 = st.columns(2)
    with col1:
        sesion_waha = st.selectbox("Seleccionar Sesión para Prueba", ["default", "principal"])
    
    texto_estado = st.text_area("Texto del Estado (o Pie de foto)", placeholder="¡Nuevos modelos disponibles! ✨", key="txt_estado_wsp")
    url_imagen = st.text_input("URL de la Imagen (Opcional)", placeholder="https://tutienda.com/.../imagen.jpg", key="img_estado_wsp")
    
    if url_imagen:
        try: st.image(url_imagen, width=200, caption="Vista previa")
        except: st.warning("No se pudo cargar la vista previa de la imagen.")

    if st.button("🚀 Subir Estado a WhatsApp", type="primary", use_container_width=True, key="btn_subir_estado"):
        if not texto_estado and not url_imagen:
            st.warning("⚠️ Debes proporcionar al menos un texto o una imagen.")
        else:
            with st.spinner("Enviando orden a WAHA..."):
                exito, mensaje_respuesta = utils.subir_estado_whatsapp(
                    session_name=sesion_waha,
                    texto=texto_estado,
                    media_url=url_imagen if url_imagen else None
                )
                if exito:
                    st.success(f"¡Éxito! {mensaje_respuesta}")
                    st.balloons()
                else:
                    st.error(f"Error al intentar subir: {mensaje_respuesta}")