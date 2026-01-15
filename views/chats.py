import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import requests

# Importamos lo compartido
from database import engine 
# AQUI EL CAMBIO IMPORTANTE: Importamos también enviar_mensaje_whatsapp
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp

def render_chat():
    # --- TODO EL CÓDIGO DEBE ESTAR INDENTADO AQUÍ DENTRO ---
    st.subheader("💬 Chat Center (WAHA)")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    st.markdown("""
    <style>
    div.stButton > button:first-child {
        text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2])

    # --- 1. IZQUIERDA: LISTA DE CONTACTOS ---
    with col_lista:
        st.markdown("#### 📩 Bandeja")
        with engine.connect() as conn:
            lista_chats = conn.execute(text("""
                SELECT m.telefono, MAX(m.fecha) as ultima_fecha,
                COALESCE(MAX(c.nombre_corto), MAX(c.nombre) || ' ' || MAX(c.apellido), m.telefono) as nombre_mostrar,
                SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
                FROM mensajes m
                LEFT JOIN Clientes c ON m.telefono = c.telefono
                GROUP BY m.telefono ORDER BY ultima_fecha DESC
            """)).fetchall()

        if not lista_chats: st.info("📭 No hay mensajes.")

        for chat in lista_chats:
            tel, nombre = chat.telefono, chat.nombre_mostrar
            # Manejo seguro de fechas
            hora = chat.ultima_fecha.strftime('%d/%m %H:%M') if chat.ultima_fecha else ""
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"

            if st.button(f"👤 {nombre}\n⏱ {hora} {notif}", key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- 2. DERECHA: VENTANA DE CHAT ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            st.markdown(f"### 💬 Chat con: **{telefono_activo}**")
            st.divider()
            
            contenedor_mensajes = st.container(height=450)
            
            # A. Leer DB 
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                
                historial = pd.read_sql(text("""
                    SELECT tipo, contenido, fecha, archivo_data 
                    FROM mensajes 
                    WHERE telefono = :t 
                    ORDER BY fecha ASC
                """), conn, params={"t": telefono_activo})

            # B. Renderizar Mensajes
            with contenedor_mensajes:
                if historial.empty: st.write("Inicia la conversación...")
                
                for _, row in historial.iterrows():
                    es_usuario = (row['tipo'] == 'ENTRANTE')
                    role = "user" if es_usuario else "assistant"
                    avatar = "👤" if es_usuario else "🛍️"
                    
                    with st.chat_message(role, avatar=avatar):
                        contenido = row['contenido']
                        data_binaria = row['archivo_data'] 

                        # 1. SI HAY IMAGEN GUARDADA (Mensajes nuevos)
                        if data_binaria is not None:
                            st.markdown(f"**{contenido}**")
                            try:
                                imagen_stream = io.BytesIO(data_binaria)
                                st.image(imagen_stream, width=250)
                            except Exception as e:
                                st.error(f"Error visualizando imagen: {e}")

                        # 2. SI ES MENSAJE DE TEXTO
                        else:
                            # Limpieza visual de IDs antiguos si existen
                            if "|ID:" in contenido:
                                partes = contenido.split("|ID:")
                                st.markdown(partes[0].strip())
                            else:
                                st.markdown(contenido)
                        
                        # Hora del mensaje
                        hora_msg = row['fecha'].strftime('%H:%M') if row['fecha'] else ""
                        st.caption(f"{hora_msg} - {row['tipo']}")

            # ============================================================
            # C. ÁREA DE ENVÍO
            # ============================================================
            prompt = st.chat_input("Escribe tu respuesta...")
            
            with st.expander("📎 Adjuntar Imagen o Documento", expanded=False):
                archivo_upload = st.file_uploader("Selecciona archivo", type=["png", "jpg", "jpeg", "pdf"], key="uploader_chat")
                
                if archivo_upload is not None:
                    if st.button("📤 Enviar Archivo", key="btn_send_media"):
                        with st.spinner("Enviando a través de WAHA..."):
                            
                            bytes_data = archivo_upload.getvalue() 
                            mime_type = archivo_upload.type
                            nombre_archivo = archivo_upload.name
                            
                            # 1. Preparar archivo (Base64) usando utils
                            media_uri, error_upload = subir_archivo_meta(bytes_data, mime_type)
                            
                            if error_upload:
                                st.error(error_upload)
                            else:
                                # 2. Enviar mensaje multimedia usando utils
                                exito, resp = enviar_mensaje_media(
                                    telefono=telefono_activo,
                                    media_id=media_uri, # Ahora pasamos el URI base64
                                    tipo_archivo=mime_type,
                                    caption="",
                                    filename=nombre_archivo
                                )
                                
                                if exito:
                                    # 3. Guardar en DB
                                    tel_guardar = telefono_activo.replace("+", "").strip()
                                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                                    
                                    etiqueta_db = f"📷 [Imagen] {nombre_archivo}" if "image" in mime_type else f"📎 [Archivo] {nombre_archivo}"
                                    
                                    with engine.connect() as conn:
                                        conn.execute(text("""
                                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                                            VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :data)
                                        """), {
                                            "tel": tel_guardar, 
                                            "txt": etiqueta_db, 
                                            "data": bytes_data 
                                        })
                                        conn.commit()
                                    
                                    st.success("¡Enviado!")
                                    st.rerun()
                                else:
                                    st.error(f"Error al enviar: {resp}")

            # --- ENVÍO DE TEXTO NORMAL (CORREGIDO) ---
            if prompt:
                # Usamos la función de utils en lugar de requests directo
                exito, resp = enviar_mensaje_whatsapp(telefono_activo, prompt)
                
                if exito:
                    # Guardar en DB
                    tel_guardar = telefono_activo.replace("+", "").strip()
                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                    
                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                            VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, NULL)
                        """), {"tel": tel_guardar, "txt": prompt})
                        conn.commit()
                    st.rerun()
                else:
                    st.error(f"Error enviando mensaje: {resp}")

        else:
            st.markdown("<div style='text-align: center; margin-top: 50px; color: gray;'><h3>👈 Selecciona un cliente de la lista</h3></div>", unsafe_allow_html=True)