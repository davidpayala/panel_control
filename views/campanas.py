import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
import datetime
import utils # Asegúrate de tener importado tu módulo utils

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
# UI - PANEL DE CONTROL PRINCIPAL DE CAMPAÑAS
# ==============================================================================
def render_campanas():
    st.title("🎯 Gestión de Campañas y Automatizaciones")
    
    # Inicializamos la tabla por si acaso
    inicializar_tabla_bot()
    
    # Creamos las dos pestañas para separar las herramientas
    tab_francotirador, tab_estados = st.tabs(["🤖 Bot Francotirador WSP", "📱 Estados de WhatsApp"])

    # --------------------------------------------------------------------------
    # PESTAÑA 1: BOT FRANCOTERADOR (Tu código original)
    # --------------------------------------------------------------------------
    with tab_francotirador:
        st.subheader("Centro de Mando: Bot Francotirador")
        
        # 1. LEER CONFIGURACIÓN ACTUAL
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            
            # Leer cuántos mensajes se enviaron hoy para las métricas
            enviados_hoy = conn.execute(text("""
                SELECT COUNT(*) FROM mensajes 
                WHERE tipo = 'SALIENTE_BOT' AND fecha::date = CURRENT_DATE
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
# --------------------------------------------------------------------------
    # PESTAÑA 2: AUTOMATIZACIÓN E INTERFAZ MANUAL DE ESTADOS
    # --------------------------------------------------------------------------
    with tab_estados:
        st.subheader("⚙️ Automatización Inteligente de Estados")
        st.write("Configura las probabilidades de aparición por categoría. El algoritmo priorizará los productos con más stock y evitará repetir los de los últimos 14 días.")

        # 2. Leer configuración actual (AQUÍ YA QUITAMOS EL BLOQUE PROBLEMÁTICO)
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()

        # 3. Interfaz de controles
        with st.form("form_probabilidades"):
            # Interruptor maestro
            activo = st.toggle("🤖 Activar Publicación Automática de Estados", value=getattr(config, 'estados_activo', False))
            
            st.write("#### Pesos de Probabilidad")
            st.caption("Ajusta los valores. El sistema calculará la proporción real automáticamente (no necesitan sumar 100).")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                p_nat = st.slider("🌿 Estilo Natural", 0, 100, getattr(config, 'prob_natural', 34))
            with col2:
                p_fan = st.slider("✨ Estilo Fantasía", 0, 100, getattr(config, 'prob_fantasia', 33))
            with col3:
                p_acc = st.slider("💍 Accesorio", 0, 100, getattr(config, 'prob_accesorios', 33))

            # Mostrar la probabilidad real calculada
            total = p_nat + p_fan + p_acc
            if total > 0:
                st.info(f"📊 **Probabilidad real:** Estilo Natural ({p_nat/total*100:.1f}%) | Estilo Fantasía ({p_fan/total*100:.1f}%) | Accesorio ({p_acc/total*100:.1f}%)")
            else:
                st.warning("⚠️ Debes asignar al menos un valor mayor a 0 para que el algoritmo funcione.")

            submit_estados = st.form_submit_button("💾 Guardar Configuración", type="primary")

            # 4. Guardar cambios
            if submit_estados:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE Configuracion_Campanas 
                        SET estados_activo = :act, prob_natural = :nat, prob_fantasia = :fan, prob_accesorios = :acc
                        WHERE id = :id
                    """), {"act": activo, "nat": p_nat, "fan": p_fan, "acc": p_acc, "id": config.id})
                st.toast("✅ Configuración de estados actualizada con éxito.")
                st.rerun()

        st.markdown("---")
        
        # Herramienta manual de pruebas
        render_prueba_estados()


def render_prueba_estados():
    st.subheader("📱 Prueba Manual: Subir Estado a WhatsApp")
    st.write("Usa esta herramienta para probar la conexión con WAHA enviando tu primer estado.")
    
    # 1. Parámetros básicos
    col1, col2 = st.columns(2)
    with col1:
        sesion_waha = st.text_input("Sesión WAHA", value="default", help="El nombre de la sesión en tu servidor WAHA.")
    
    # 2. Entradas para el Estado
    texto_estado = st.text_area("Texto del Estado (o Pie de foto)", placeholder="¡Nuevos modelos de la colección disponibles! ✨", key="txt_estado_wsp")
    
    url_imagen = st.text_input("URL de la Imagen (Opcional)", placeholder="https://tutienda.com/wp-content/uploads/.../imagen.jpg", key="img_estado_wsp")
    
    # Mostrar vista previa de la imagen si se pegó un link
    if url_imagen:
        try:
            st.image(url_imagen, width=200, caption="Vista previa de la imagen a subir")
        except:
            st.warning("No se pudo cargar la vista previa de la imagen. Verifica que la URL sea pública y directa.")

    # 3. Botón de Ejecución
    if st.button("🚀 Subir Estado a WhatsApp", type="primary", use_container_width=True, key="btn_subir_estado"):
        if not texto_estado and not url_imagen:
            st.warning("⚠️ Debes proporcionar al menos un texto o una imagen.")
        else:
            with st.spinner("Enviando orden a WAHA..."):
                exito, mensaje_respuesta = utils.subir_estado_whatsapp(
                    session_name=sesion_waha,
                    texto=texto_estado,
                    image_url=url_imagen if url_imagen else None
                )
                
                # Evaluamos el resultado
                if exito:
                    st.success(f"¡Éxito! {mensaje_respuesta}")
                    st.balloons()
                else:
                    st.error(f"Error al intentar subir: {mensaje_respuesta}")