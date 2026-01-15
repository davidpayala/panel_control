import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import requests
from streamlit_autorefresh import st_autorefresh  # <--- IMPORTACIÓN NUEVA

# Importamos lo compartido
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp

def mostrar_vista_chats():
    st.title("💬 Chat Center")

    # Layout: Columna izquierda (Lista) - Columna derecha (Chat y Datos)
    col_lista, col_chat = st.columns([1, 2.5])

    # ==========================================================================
    # 1. COLUMNA IZQUIERDA: LISTA DE CONTACTOS CON NOMBRES
    # ==========================================================================
    with col_lista:
        st.subheader("Conversaciones")
        
        # Consulta INTELIGENTE: Trae el último mensaje Y el nombre del cliente
        query_chats = """
            SELECT DISTINCT ON (m.telefono) 
                m.telefono, 
                m.contenido, 
                m.fecha,
                m.leido,
                c.nombre,
                c.apellido
            FROM mensajes m
            LEFT JOIN Clientes c ON m.telefono = c.telefono
            ORDER BY m.telefono, m.fecha DESC
        """
        
        with engine.connect() as conn:
            df_chats = pd.read_sql(text(query_chats), conn)
            # Reordenamos por fecha real (el DISTINCT ON desordena un poco)
            df_chats = df_chats.sort_values(by="fecha", ascending=False)

        for i, row in df_chats.iterrows():
            tel = row['telefono']
            
            # Lógica del Nombre: Si tiene Nombre en DB úsalo, si no, usa el Teléfono
            nombre_mostrar = row['nombre'] if row['nombre'] else tel
            if row['apellido']:
                nombre_mostrar += f" {row['apellido']}"
            
            # Estilo del botón (Negrita si no leyó)
            icono = "🟢" if not row['leido'] else "👤"
            label_boton = f"{icono} {nombre_mostrar}\nMessage: {row['contenido'][:20]}..."
            
            if st.button(label_boton, key=f"chat_{tel}", use_container_width=True):
                st.session_state.chat_actual = tel
                st.session_state.nombre_actual = nombre_mostrar # Guardamos el nombre para el header
                st.rerun()

    # ==========================================================================
    # 2. COLUMNA DERECHA: CONVERSACIÓN ACTIVA
    # ==========================================================================
    with col_chat:
        if 'chat_actual' in st.session_state:
            telefono_activo = st.session_state.chat_actual
            nombre_activo = st.session_state.get('nombre_actual', telefono_activo)
            
            # --- HEADER DEL CHAT ---
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f"### 💬 Chat con: **{nombre_activo}**")
                st.caption(f"Tel: {telefono_activo}")
            with c2:
                # Botón para ver info detallada
                ver_info = st.toggle("Ver Ficha Cliente", value=True)

            st.divider()

            # --- PANEL DE INFORMACIÓN DEL CLIENTE (Lado Derecho o Expander) ---
            if ver_info:
                with st.expander("📂 Información del Cliente y Envíos", expanded=True):
                    cargar_info_cliente(telefono_activo)

            # --- MOSTRAR MENSAJES (Tu lógica actual de chat) ---
            cargar_mensajes_chat(telefono_activo)
            
            # --- INPUT DE RESPUESTA ---
            # (Aquí va tu input de texto normal)

# ==============================================================================
# FUNCIÓN AUXILIAR: CARGAR INFO DEL CLIENTE
# ==============================================================================
def cargar_info_cliente(telefono):
    with engine.connect() as conn:
        # Traemos todo lo que sepamos del cliente
        res = conn.execute(text("SELECT * FROM Clientes WHERE telefono = :t"), {"t": telefono}).fetchone()
        
        if res:
            # Convertimos a diccionario para fácil acceso
            cliente = res._mapping
            
            c1, c2 = st.columns(2)
            with c1:
                nuevo_nombre = st.text_input("Nombre", value=cliente['nombre'] or "")
                nuevo_apellido = st.text_input("Apellido", value=cliente['apellido'] or "")
            with c2:
                nueva_direccion = st.text_area("🏠 Dirección de Envío", value=cliente['direccion'] or "")
                # Asumo que tienes un campo email o notas, si no, bórralo
                notas = st.text_area("📝 Notas / Referencia", value=cliente.get('notas', ''))

            if st.button("💾 Guardar Datos del Cliente"):
                conn.execute(text("""
                    UPDATE Clientes 
                    SET nombre=:n, apellido=:a, direccion=:d, notas=:nt 
                    WHERE telefono=:t
                """), {
                    "n": nuevo_nombre, "a": nuevo_apellido, 
                    "d": nueva_direccion, "nt": notas, "t": telefono
                })
                conn.commit()
                st.success("Datos actualizados!")
                st.rerun()
        else:
            st.warning("Este cliente aún no está registrado en la tabla Clientes.")
            if st.button("Crear Ficha Ahora"):
                conn.execute(text("INSERT INTO Clientes (telefono, activo) VALUES (:t, TRUE)"), {"t": telefono})
                conn.commit()
                st.rerun()

# ==============================================================================
# FUNCIÓN AUXILIAR: CARGAR MENSAJES (Simplificada)
# ==============================================================================
def cargar_mensajes_chat(telefono):
    # Aquí pones tu lógica actual de cargar burbujas de chat
    # Solo asegúrate de marcar como LEÍDO al abrir
    with engine.connect() as conn:
        conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t"), {"t": telefono})
        conn.commit()
        
        msgs = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono = :t ORDER BY fecha ASC"), conn, params={"t": telefono})
    
    # Renderizar burbujas... (Tu código actual)
    chat_container = st.container(height=400)
    with chat_container:
        for i, m in msgs.iterrows():
            es_mio = (m['tipo'] == 'SALIENTE')
            alignment = "flex-end" if es_mio else "flex-start"
            color = "#dcf8c6" if es_mio else "#ffffff"
            
            st.markdown(f"""
            <div style='display: flex; justify-content: {alignment}; margin-bottom: 5px;'>
                <div style='background-color: {color}; padding: 10px; border-radius: 10px; max-width: 70%; color: black;'>
                    {m['contenido']}
                </div>
            </div>
            """, unsafe_allow_html=True)
def render_chat():
    # --- LÍNEA MÁGICA: Recarga la página cada 4000ms (4 segundos) ---
    st_autorefresh(interval=4000, key="chat_autorefresh")

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

                        if data_binaria is not None:
                            st.markdown(f"**{contenido}**")
                            try:
                                imagen_stream = io.BytesIO(data_binaria)
                                st.image(imagen_stream, width=250)
                            except Exception as e:
                                st.error(f"Error visualizando imagen: {e}")
                        else:
                            if "|ID:" in contenido:
                                partes = contenido.split("|ID:")
                                st.markdown(partes[0].strip())
                            else:
                                st.markdown(contenido)
                        
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
                            
                            media_uri, error_upload = subir_archivo_meta(bytes_data, mime_type)
                            
                            if error_upload:
                                st.error(error_upload)
                            else:
                                exito, resp = enviar_mensaje_media(telefono_activo, media_uri, mime_type, "", nombre_archivo)
                                if exito:
                                    tel_guardar = telefono_activo.replace("+", "").strip()
                                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                                    etiqueta_db = f"📷 [Imagen] {nombre_archivo}" if "image" in mime_type else f"📎 [Archivo] {nombre_archivo}"
                                    with engine.connect() as conn:
                                        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :data)"), {"tel": tel_guardar, "txt": etiqueta_db, "data": bytes_data})
                                        conn.commit()
                                    st.success("¡Enviado!")
                                    st.rerun()
                                else:
                                    st.error(f"Error al enviar: {resp}")

            if prompt:
                exito, resp = enviar_mensaje_whatsapp(telefono_activo, prompt)
                if exito:
                    tel_guardar = telefono_activo.replace("+", "").strip()
                    if len(tel_guardar) == 9: tel_guardar = f"51{tel_guardar}"
                    with engine.connect() as conn:
                        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:tel, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, NULL)"), {"tel": tel_guardar, "txt": prompt})
                        conn.commit()
                    st.rerun()
                else:
                    st.error(f"Error enviando mensaje: {resp}")

        else:
            st.markdown("<div style='text-align: center; margin-top: 50px; color: gray;'><h3>👈 Selecciona un cliente de la lista</h3></div>", unsafe_allow_html=True)