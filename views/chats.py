import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import requests

# Importamos lo compartido
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media

def render_chat():
    # --- TODO EL CÓDIGO DEBE ESTAR INDENTADO AQUÍ DENTRO ---
    st.subheader("💬 Chat Center")

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
            hora = chat.ultima_fecha.strftime('%d/%m %H:%M')
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
            
            # A. Leer DB (Incluyendo la nueva columna archivo_data)
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                # ¡OJO! Aquí agregamos archivo_data a la consulta SELECT
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
                        data_binaria = row['archivo_data'] # Los bytes de la imagen

                        # 1. SI HAY IMAGEN GUARDADA (Mensajes salientes nuevos)
                        if data_binaria is not None:
                            # Mostramos la etiqueta (nombre del archivo)
                            st.markdown(f"**{contenido}**")
                            try:
                                # Convertimos los datos crudos en un stream de archivo
                                imagen_stream = io.BytesIO(data_binaria)
                                st.image(imagen_stream, width=250)
                            except Exception as e:
                                st.error(f"Error técnico mostrando imagen: {e}")

                        # 2. SI ES MENSAJE DE TEXTO O MULTIMEDIA ENTRANTE (Lógica anterior)
                        elif "|ID:" in contenido:
                            partes = contenido.split("|ID:")
                            texto_vis = partes[0].strip()
                            st.markdown(f"**{texto_vis}**")
                        else:
                            st.markdown(contenido)
                        
                        st.caption(f"{row['fecha'].strftime('%H:%M')} - {row['tipo']}")

            # ============================================================
            # C. ÁREA DE ENVÍO
            # ============================================================
            prompt = st.chat_input("Escribe tu respuesta...")
            
            with st.expander("📎 Adjuntar Imagen o Documento", expanded=False):
                archivo_upload = st.file_uploader("Selecciona archivo", type=["png", "jpg", "jpeg", "pdf"], key="uploader_chat")
                
                if archivo_upload is not None:
                    if st.button("📤 Enviar Archivo", key="btn_send_media"):
                        with st.spinner("Subiendo a WhatsApp..."):
                            
                            bytes_data = archivo_upload.getvalue() # Leemos los bytes
                            mime_type = archivo_upload.type
                            nombre_archivo = archivo_upload.name
                            
                            # 1. Subir a Meta
                            media_id, error_upload = subir_archivo_meta(bytes_data, mime_type)
                            
                            if error_upload:
                                st.error(error_upload)
                            else:
                                # 2. Enviar mensaje en WhatsApp
                                exito, resp = enviar_mensaje_media(
                                    telefono=telefono_activo,
                                    media_id=media_id,
                                    tipo_archivo=mime_type,
                                    caption="",
                                    filename=nombre_archivo
                                )
                                
                                if exito:
                                    # 3. Guardar en DB (AHORA INCLUYENDO LOS BYTES)
                                    tel_guardar = telefono_activo.replace("+", "").strip()
                                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                                    
                                    etiqueta_db = f"📷 [Imagen enviada] {nombre_archivo}" if "image" in mime_type else f"📎 [Archivo enviado] {nombre_archivo}"
                                    
                                    with engine.connect() as conn:
                                        # Usamos :data para pasar los bytes de forma segura
                                        conn.execute(text("""
                                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                                            VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :data)
                                        """), {
                                            "tel": tel_guardar, 
                                            "txt": etiqueta_db, 
                                            "data": bytes_data # <--- ¡Aquí guardamos la foto!
                                        })
                                        conn.commit()
                                    
                                    st.success("¡Enviado correctamente!")
                                    st.rerun()
                                else:
                                    st.error(f"Error al enviar mensaje: {resp}")

            if prompt:
                # Lógica de texto normal
                try:
                    tk = os.getenv("WHATSAPP_TOKEN")
                    pid = os.getenv("WHATSAPP_PHONE_ID")
                    headers = {"Authorization": f"Bearer {tk}", "Content-Type": "application/json"}
                    data_txt = {
                        "messaging_product": "whatsapp", "to": telefono_activo, "type": "text",
                        "text": {"body": prompt}
                    }
                    r_txt = requests.post(f"https://graph.facebook.com/v17.0/{pid}/messages", headers=headers, json=data_txt)
                    
                    if r_txt.status_code == 200:
                        tel_guardar = telefono_activo.replace("+", "").strip()
                        if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                        with engine.connect() as conn:
                            # Texto normal no lleva archivo_data (se pasa None)
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                                VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, NULL)
                            """), {"tel": tel_guardar, "txt": prompt})
                            conn.commit()
                        st.rerun()
                    else:
                        st.error(f"Error enviando texto: {r_txt.text}")
                except Exception as e:
                    st.error(f"Error de conexión: {e}")

        else:
            st.markdown("<div style='text-align: center; margin-top: 50px; color: gray;'><h3>👈 Selecciona un cliente de la lista</h3></div>", unsafe_allow_html=True)
            