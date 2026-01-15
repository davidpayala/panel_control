import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from streamlit_autorefresh import st_autorefresh 

# Importamos lo compartido
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp

# ==============================================================================
# FUNCIÓN PRINCIPAL (Llamada desde app.py)
# ==============================================================================
def render_chat():   # <--- AQUÍ ESTABA EL CAMBIO DE NOMBRE
    # 1. AUTO-REFRESH: Recarga cada 5 segundos para ver mensajes nuevos
    st_autorefresh(interval=5000, key="chat_autorefresh")
    
    st.title("💬 Chat Center (WAHA)")

    # Inicializar estado si no existe
    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS para botones
    st.markdown("""
    <style>
    div.stButton > button:first-child {
        text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

    # Layout: Columna 1 (Lista) - Columna 2 (Chat y Datos)
    col_lista, col_chat = st.columns([1, 2.5])

    # ==========================================================================
    # 1. COLUMNA IZQUIERDA: BANDEJA DE ENTRADA
    # ==========================================================================
    with col_lista:
        st.subheader("📩 Bandeja")
        
        # Consulta INTELIGENTE: 
        # - Agrupa por teléfono
        # - Trae el nombre del cliente si existe
        # - Cuenta mensajes no leídos
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
                -- Prioridad de nombre: Nombre Corto > Nombre+Apellido > Teléfono
                COALESCE(MAX(c.nombre), m.telefono) as nombre_pila,
                MAX(c.apellido) as apellido,
                SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
            FROM mensajes m
            LEFT JOIN Clientes c ON m.telefono = c.telefono
            GROUP BY m.telefono 
            ORDER BY ultima_fecha DESC
        """

        with engine.connect() as conn:
            lista_chats = conn.execute(text(query_lista)).fetchall()

        if not lista_chats: 
            st.info("📭 No hay mensajes.")

        for chat in lista_chats:
            tel = chat.telefono
            
            # Formatear nombre para mostrar
            nombre_mostrar = chat.nombre_pila
            if chat.apellido: nombre_mostrar += f" {chat.apellido}"
            
            # Formatear hora
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            
            # Notificación roja si hay no leídos
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            
            # Botón visual (Primary si está seleccionado)
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            label = f"{notif} {nombre_mostrar}\n🕑 {hora}"

            if st.button(label, key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # ==========================================================================
    # 2. COLUMNA DERECHA: CONVERSACIÓN ACTIVA
    # ==========================================================================
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            # --- HEADER DEL CHAT ---
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f"### 💬 Chat con: **{telefono_activo}**")
            with c2:
                # Toggle para mostrar/ocultar ficha
                ver_ficha = st.toggle("📋 Ficha Cliente", value=True)
            
            st.divider()

            # --- A. FICHA DEL CLIENTE (DIRECCIÓN Y DATOS) ---
            if ver_ficha:
                with st.container():
                    st.info("📂 **Datos del Cliente y Envío**")
                    cargar_info_cliente(telefono_activo)
                    st.divider()

            # --- B. CARGAR MENSAJES ---
            contenedor_mensajes = st.container(height=450)
            
            # 1. Marcar como leídos
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                
                # 2. Traer historial
                historial = pd.read_sql(text("""
                    SELECT tipo, contenido, fecha, archivo_data 
                    FROM mensajes 
                    WHERE telefono = :t 
                    ORDER BY fecha ASC
                """), conn, params={"t": telefono_activo})

            # 3. Renderizar Burbujas
            with contenedor_mensajes:
                if historial.empty: st.write("Inicia la conversación...")
                
                for _, row in historial.iterrows():
                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role = "user" if es_cliente else "assistant"
                    avatar = "👤" if es_cliente else "🛍️"
                    
                    with st.chat_message(role, avatar=avatar):
                        contenido = row['contenido']
                        data_binaria = row['archivo_data'] 

                        # Si es imagen
                        if data_binaria is not None:
                            st.markdown(f"**{contenido}**")
                            try:
                                st.image(io.BytesIO(data_binaria), width=250)
                            except:
                                st.error("Error visualizando imagen")
                        else:
                            # Limpieza visual de IDs técnicos si aparecen
                            if "|ID:" in contenido:
                                st.markdown(contenido.split("|ID:")[0].strip())
                            else:
                                st.markdown(contenido)
                        
                        hora_msg = row['fecha'].strftime('%H:%M') if row['fecha'] else ""
                        st.caption(f"{hora_msg}")

            # --- C. ÁREA DE ENVÍO ---
            prompt = st.chat_input("Escribe tu respuesta...")
            
            # Adjuntar archivos
            with st.expander("📎 Adjuntar Imagen", expanded=False):
                archivo_upload = st.file_uploader("Subir imagen", type=["png", "jpg", "jpeg", "pdf"], key="uploader_chat")
                if archivo_upload and st.button("📤 Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo_upload)

            # Enviar texto
            if prompt:
                enviar_texto_chat(telefono_activo, prompt)

        else:
            # Pantalla vacía inicial
            st.markdown("<div style='text-align: center; margin-top: 50px; color: gray;'><h3>👈 Selecciona un chat de la izquierda</h3></div>", unsafe_allow_html=True)

# ==============================================================================
# FUNCIONES AUXILIARES (LÓGICA INTERNA)
# ==============================================================================

def cargar_info_cliente(telefono):
    """Muestra y permite editar nombre, dirección y notas del cliente"""
    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM Clientes WHERE telefono = :t"), {"t": telefono}).fetchone()
        
        if res:
            cliente = res._mapping
            c1, c2 = st.columns(2)
            with c1:
                nuevo_nombre = st.text_input("Nombre", value=cliente['nombre'] or "")
                nuevo_apellido = st.text_input("Apellido", value=cliente['apellido'] or "")
            with c2:
                nueva_direccion = st.text_area("🏠 Dirección de Envío", value=cliente['direccion'] or "", height=100)
                notas = st.text_area("📝 Notas Internas", value=cliente.get('notas', ''), height=100)

            if st.button("💾 Guardar Datos Ficha"):
                conn.execute(text("""
                    UPDATE Clientes 
                    SET nombre=:n, apellido=:a, direccion=:d, notas=:nt 
                    WHERE telefono=:t
                """), {
                    "n": nuevo_nombre, "a": nuevo_apellido, 
                    "d": nueva_direccion, "nt": notas, "t": telefono
                })
                conn.commit()
                st.success("¡Ficha actualizada!")
                st.rerun()
        else:
            st.warning("Número nuevo. Crea la ficha para guardar datos.")
            if st.button("➕ Crear Ficha de Cliente"):
                conn.execute(text("INSERT INTO Clientes (telefono, activo) VALUES (:t, TRUE)"), {"t": telefono})
                conn.commit()
                st.rerun()

def enviar_texto_chat(telefono, texto):
    exito, resp = enviar_mensaje_whatsapp(telefono, texto)
    if exito:
        guardar_mensaje_saliente(telefono, texto, None)
        st.rerun()
    else:
        st.error(f"Error al enviar: {resp}")

def enviar_archivo_chat(telefono, archivo):
    with st.spinner("Enviando..."):
        bytes_data = archivo.getvalue()
        mime_type = archivo.type
        nombre = archivo.name
        
        # 1. Subir/Preparar (WAHA maneja base64 directo en utils)
        media_uri, err = subir_archivo_meta(bytes_data, mime_type)
        if err:
            st.error(err)
            return

        # 2. Enviar
        exito, resp = enviar_mensaje_media(telefono, media_uri, mime_type, "", nombre)
        if exito:
            txt_db = f"📷 {nombre}" if "image" in mime_type else f"📎 {nombre}"
            guardar_mensaje_saliente(telefono, txt_db, bytes_data)
            st.success("Enviado")
            st.rerun()
        else:
            st.error(f"Fallo envío: {resp}")

def guardar_mensaje_saliente(telefono, texto, archivo_data):
    tel_clean = telefono.replace("+", "").strip()
    if len(tel_clean) == 9: tel_clean = f"51{tel_clean}"
    
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) 
            VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :data)
        """), {"tel": tel_clean, "txt": texto, "data": archivo_data})
        conn.commit()