import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime

# ==============================================================================
# 🧠 INICIALIZACIÓN DE LA BASE DE DATOS DEL BOT
# ==============================================================================
def inicializar_tabla_bot():
    """Asegura que la tabla de configuración del bot exista y tenga 1 fila de control"""
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
            # Verificar si está vacía
            count = conn.execute(text("SELECT COUNT(*) FROM Configuracion_Campanas")).scalar()
            if count == 0:
                conn.execute(text("INSERT INTO Configuracion_Campanas (bot_activo, tipo_objetivo) VALUES (FALSE, 'Todos')"))
    except Exception as e:
        st.error(f"Error al inicializar la base de datos del bot: {e}")

# ==============================================================================
# UI - PANEL DE CONTROL DEL FRANCOTIRADOR
# ==============================================================================
def render_campanas():
    st.title("🤖 Centro de Mando: Bot Francotirador WSP")
    
    inicializar_tabla_bot()
    
    # 1. LEER CONFIGURACIÓN ACTUAL
    with engine.connect() as conn:
        config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        # Leer cuántos mensajes se enviaron hoy para las métricas
        enviados_hoy = conn.execute(text("""
            SELECT COUNT(*) FROM mensajes 
            WHERE tipo = 'SALIENTE' AND fecha::date = CURRENT_DATE
        """)).scalar()

    st.markdown("---")

    # 2. PANEL DE MÉTRICAS Y ESTADO VISUAL
    col_estado, col_metric = st.columns([1, 1])
    
    with col_estado:
        if config.bot_activo:
            st.success("🟢 **ESTADO: EL BOT ESTÁ ACTIVO Y VIGILANDO**")
            st.caption("El servidor enviará mensajes automáticamente según el horario establecido.")
        else:
            st.error("🔴 **ESTADO: EL BOT ESTÁ APAGADO**")
            st.caption("No se enviará ningún mensaje automático hasta que lo enciendas.")
            
    with col_metric:
        st.metric("📨 Mensajes enviados hoy (Total Salientes)", f"{enviados_hoy} / {config.max_mensajes_dia}")

    st.markdown("---")

    # 3. ZONA DE CONFIGURACIÓN (INTERFAZ)
    st.subheader("⚙️ Parámetros de la Campaña Actual")
    st.info("💡 Los cambios que hagas aquí serán leídos por el servidor en la próxima hora.")
    
    with st.form("form_config_bot"):
        # Interruptor maestro
        nuevo_estado = st.toggle("Activar Francotirador Automático", value=config.bot_activo)
        
        st.write("") # Espacio
        
        col1, col2 = st.columns(2)
        with col1:
            opciones_tipo = ["Todos", "Natural", "Fantasía", "Accesorios"]
            idx_tipo = opciones_tipo.index(config.tipo_objetivo) if config.tipo_objetivo in opciones_tipo else 0
            nuevo_tipo = st.selectbox("🎯 Tipo de lente a promocionar", opciones_tipo, index=idx_tipo)
            
            nuevo_max = st.number_input("📈 Máximo de mensajes diarios", min_value=1, max_value=200, value=config.max_mensajes_dia)
            
        with col2:
            st.caption("⏰ Rango de horario permitido para no molestar:")
            nuevo_inicio = st.time_input("Hora de Inicio", value=config.hora_inicio)
            nuevo_fin = st.time_input("Hora Límite", value=config.hora_fin)

        st.write("")
        submit = st.form_submit_button("💾 Guardar Órdenes", type="primary")
        
        # 4. GUARDAR CAMBIOS EN LA BASE DE DATOS
        if submit:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET bot_activo = :act, 
                            tipo_objetivo = :tip, 
                            max_mensajes_dia = :maxm, 
                            hora_inicio = :hini, 
                            hora_fin = :hfin
                        WHERE id = :id
                    """), {
                        "act": nuevo_estado, 
                        "tip": nuevo_tipo, 
                        "maxm": nuevo_max, 
                        "hini": nuevo_inicio, 
                        "hfin": nuevo_fin, 
                        "id": config.id
                    })
                    trans.commit()
                    st.toast("✅ Órdenes actualizadas con éxito.")
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al guardar: {e}")