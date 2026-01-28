import streamlit as st
import pandas as pd
from sqlalchemy import text
import json
import io
import os
import time
import streamlit.components.v1 as components 
import requests  # <--- AGREGAR ESTO
from datetime import datetime
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import (
    enviar_mensaje_media, enviar_mensaje_whatsapp, 
    normalizar_telefono_maestro, buscar_contacto_google, 
    crear_en_google, sincronizar_historial
)

# Copiamos las mismas opciones para mantener consistencia
OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente", 
    "✅ Compró", "👀 Prospecto", "❓ Preguntón", 
    "📉 Pide Rebaja", "📦 Mayorista"
]

def render_chat():
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS (Botones compactos + Badges de colores)
    st.markdown("""
    <style>
    div.stButton > button:first-child { 
        text-align: left; 
        width: 100%; 
        padding: 10px 15px;
        border-radius: 8px; 
        margin-bottom: 2px;
        white-space: nowrap; 
        overflow: hidden; 
        text-overflow: ellipsis; 
    }
    .date-separator { text-align: center; margin: 15px 0; position: relative; }
    .date-separator span { background-color: #f0f2f6; padding: 4px 12px; border-radius: 12px; font-size: 0.75em; color: #555; font-weight: bold;}
    /* Estilos para etiquetas en el header */
    .tag-badge { padding: 2px 8px; border-radius: 10px; font-size: 0.75em; font-weight: bold; margin-right: 5px; color: black; display: inline-block;}
    .tag-spam { background-color: #ffcccc; border: 1px solid red; }
    .tag-vip { background-color: #d4edda; border: 1px solid green; }
    .tag-warn { background-color: #fff3cd; border: 1px solid orange; }
    .tag-default { background-color: #e2e3e5; border: 1px solid #ccc; }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- COLUMNA IZQUIERDA ---
    with col_lista:
        st.subheader("📩 Chats")
        
        # 1. Iniciar Nuevo Chat
        with st.expander("➕ Iniciar Nuevo Chat", expanded=False):
            nuevo_num = st.text_input("Escribe el número:", placeholder="Ej: 999888777")
            if st.button("Ir al Chat", use_container_width=True):
                norm = normalizar_telefono_maestro(nuevo_num)
                if norm:
                    st.session_state['chat_actual_telefono'] = norm['db']
                    st.rerun()
                else:
                    st.error("Número inválido")

        # 2. Lista de Chats (AHORA CON ETIQUETAS)
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
                COALESCE(NULLIF(MAX(c.nombre_corto), ''), NULLIF(MAX(c.nombre_corto), 'Cliente WhatsApp'), MAX(m.telefono)) as display_name,
                MAX(c.etiquetas) as etiquetas,  -- <--- TRAEMOS ETIQUETAS
                SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
            FROM mensajes m
            LEFT JOIN Clientes c ON m.telefono = c.telefono
            GROUP BY m.telefono 
            ORDER BY ultima_fecha DESC
        """
        with engine.connect() as conn:
            lista_chats = conn.execute(text(query_lista)).fetchall()

        if not lista_chats: 
            st.info("Sin historial.")

        st.divider()

        for chat in lista_chats:
            tel = chat.telefono
            notif = "🔴" if chat.no_leidos > 0 else "👤"
            
            # Icono extra si es especial
            icono_tag = ""
            tags_str = chat.etiquetas or ""
            if "SPAM" in tags_str: icono_tag = "🚫"
            elif "Problemático" in tags_str: icono_tag = "⚠️"
            elif "VIP" in tags_str: icono_tag = "💎"
            
            label = f"{notif} {icono_tag} {chat.display_name}"
            
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            if st.button(label, key=f"btn_{tel}", type=tipo_btn, use_container_width=True):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- COLUMNA DERECHA ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            norm_activo = normalizar_telefono_maestro(telefono_activo)
            titulo_tel = norm_activo['corto'] if norm_activo else telefono_activo
            
            # Obtener datos frescos del cliente (incluyendo etiquetas)
            with engine.connect() as conn:
                cli_data = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono_activo}).fetchone()
            
            # HEADER DEL CHAT (FUSIONADO: TÍTULO + SYNC + FICHA)
            # Dividimos en 3 columnas: Título (Grande), Sync (Pequeño), Toggle (Pequeño)
            c1, c2, c3 = st.columns([3, 0.7, 0.8])
            
            # COLUMNA 1: TÍTULO Y ETIQUETAS (Lo que ya tenías)
            with c1: 
                st.markdown(f"### 💬 {titulo_tel}")
                if cli_data and cli_data.etiquetas:
                    html_tags = ""
                    for tag in cli_data.etiquetas.split(','):
                        css_class = "tag-default"
                        if "SPAM" in tag: css_class = "tag-spam"
                        elif "VIP" in tag: css_class = "tag-vip"
                        elif "Problemático" in tag: css_class = "tag-warn"
                        html_tags += f'<span class="tag-badge {css_class}">{tag}</span>'
                    st.markdown(html_tags, unsafe_allow_html=True)

            # COLUMNA 2: BOTÓN SYNC (Lo nuevo)
            with c2:
                st.write("") # Espaciador para alinear verticalmente
                if st.button("🔄 Sync", help="Descargar historial faltante"):
                    with st.spinner("Sincronizando..."):
                        # Llamamos a la función que definimos arriba
                        ok, msg = sincronizar_historial(telefono_activo) 
                        if ok:
                            st.toast(msg, icon="✅")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

            # COLUMNA 3: TOGGLE FICHA (Lo que ya tenías)
            with c3: 
                st.write("") # Espaciador
                ver_info = st.toggle("Ver Ficha", value=False)
            
            st.divider()

            if ver_info:
                mostrar_info_avanzada(telefono_activo)
                st.divider()

            # Historial de Mensajes
            contenedor = st.container(height=450)
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                historial = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_activo})

            with contenedor:
                if historial.empty: st.info("👋 Conversación nueva.")
                last_date = None
                for _, row in historial.iterrows():
                    msg_date = row['fecha'].date()
                    if last_date != msg_date:
                        st.markdown(f"""<div class="date-separator"><span>📅 {msg_date.strftime('%d/%m/%Y')}</span></div>""", unsafe_allow_html=True)
                        last_date = msg_date

                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role, avatar = ("user", "👤") if es_cliente else ("assistant", "🛍️")
                    with st.chat_message(role, avatar=avatar):
                        if row['archivo_data']:
                            try: st.image(io.BytesIO(row['archivo_data']), width=250)
                            except: st.error("Error imagen")
                        txt = row['contenido'] or ""
                        if txt: st.markdown(txt)
                        st.caption(row['fecha'].strftime('%H:%M'))
            
            # Auto-Scroll JS
            components.html("""<script>const elements = window.parent.document.querySelectorAll('.stChatMessage'); if (elements.length > 0) { elements[elements.length - 1].scrollIntoView({behavior: "smooth"}); }</script>""", height=0, width=0)

            # Inputs
            prompt = st.chat_input("Escribe un mensaje...")
            with st.expander("📎 Adjuntar Archivo", expanded=False):
                archivo = st.file_uploader("Subir", key="up_file")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            if prompt: enviar_texto_chat(telefono_activo, prompt)
        else:
            st.markdown("### 👈 Selecciona un chat")

def mostrar_info_avanzada(telefono):
    """Ficha de cliente integrada en el chat"""
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.warning("⚠️ No registrado.")
            if st.button("Crear Ficha Rápida"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo Cliente')"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        
        dirs = pd.DataFrame()
        if id_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- EDICIÓN PRINCIPAL (INCLUYE ETIQUETAS) ---
    with st.container():
        c1, c2 = st.columns(2)
        new_corto = c1.text_input("Alias", value=cl.get('nombre_corto') or "", key=f"in_corto_{telefono}")
        
        # Recuperar etiquetas actuales
        tags_actuales_db = cl.get('etiquetas', '') or ""
        lista_tags = [t for t in tags_actuales_db.split(',') if t] # Limpiar vacíos
        
        # Selector Múltiple
        new_tags = c2.multiselect("Etiquetas", OPCIONES_TAGS, default=[t for t in lista_tags if t in OPCIONES_TAGS], key=f"tag_chat_{telefono}")

    # --- GOOGLE ---
    st.markdown("#### 👤 Datos")
    col_nom, col_ape, col_btns = st.columns([1.5, 1.5, 1.5])
    
    new_nombre = col_nom.text_input("Nombre", value=cl.get('nombre') or "", key=f"in_nom_{telefono}")
    new_apellido = col_ape.text_input("Apellido", value=cl.get('apellido') or "", key=f"in_ape_{telefono}")

    with col_btns:
            st.write("") 
            # Cambiamos el texto del botón para reflejar que también crea
            if st.button("📥 Google (Buscar/Crear)", key=f"btn_search_{telefono}", use_container_width=True):
                with st.spinner("Conectando con Google..."):
                    norm = normalizar_telefono_maestro(telefono)
                    tel_format = norm['db']
                    
                    # 1. Intentamos BUSCAR primero
                    datos = buscar_contacto_google(tel_format) 
                    
                    if datos and datos['encontrado']:
                        # CASO A: ENCONTRADO -> Actualizamos local
                        with engine.connect() as conn:
                            conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                        {"n": datos['nombre'], "a": datos['apellido'], "gid": datos['google_id'], "nc": datos['nombre_completo'], "t": telefono})
                            conn.commit()
                        st.toast("✅ Sincronizado desde Google")
                        time.sleep(1)
                        st.rerun()
                    
                    else:
                        # CASO B: NO ENCONTRADO -> CREAMOS EN GOOGLE
                        # Verificamos si el usuario escribió un nombre en el input
                        if new_nombre:
                            gid_nuevo = crear_en_google(new_nombre, new_apellido, tel_format)
                            
                            if gid_nuevo:
                                # Guardamos el nuevo ID de Google en nuestra BD local
                                nombre_completo = f"{new_nombre} {new_apellido}".strip()
                                with engine.connect() as conn:
                                    conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                                {"n": new_nombre, "a": new_apellido, "gid": gid_nuevo, "nc": nombre_completo, "t": telefono})
                                    conn.commit()
                                
                                st.success(f"✅ Contacto creado en Google: {nombre_completo}")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Error al intentar crear en Google Contacts.")
                        else:
                            st.warning("⚠️ Para crear el contacto, escribe primero el NOMBRE en la casilla.")

    # BOTÓN GUARDAR GENERAL (Guarda Alias, Etiquetas y Nombres)
    if st.button("💾 GUARDAR CAMBIOS", key=f"btn_save_loc_{telefono}", type="primary", use_container_width=True):
        tags_str = ",".join(new_tags)
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE Clientes SET nombre_corto=:nc, etiquetas=:tag, nombre=:n, apellido=:a WHERE telefono=:t
            """), {"nc": new_corto, "tag": tags_str, "n": new_nombre, "a": new_apellido, "t": telefono})
            conn.commit()
        st.toast("✅ Datos guardados")
        time.sleep(0.5)
        st.rerun()

    # DIRECCIONES
    st.markdown("---")
    if dirs.empty:
        st.caption("Sin direcciones.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            txt = row.get('direccion_texto') or ""
            dist = row.get('distrito') or ""
            st.markdown(f"📍 **{tipo}:** {txt} ({dist})")

def enviar_texto_chat(telefono, texto):
    exito, resp = enviar_mensaje_whatsapp(telefono, texto)
    if exito:
        guardar_mensaje_saliente(telefono, texto, None)
        st.rerun()
    else: st.error(f"Error: {resp}")

def enviar_archivo_chat(telefono, archivo):
    with st.spinner("Enviando..."):
        # YA NO LLAMAMOS A subir_archivo_meta
        # Pasamos los bytes directos (archivo.getvalue()) a la función de envío
        
        exito, resp = enviar_mensaje_media(
            telefono, 
            archivo.getvalue(), # <--- AQUÍ ESTÁ EL CAMBIO: Pasamos el archivo crudo
            archivo.type, 
            "", 
            archivo.name
        )
        
        if exito:
            guardar_mensaje_saliente(telefono, f"📎 {archivo.name}", archivo.getvalue())
            st.rerun()
        else: 
            st.error(f"Error: {resp}")

def guardar_mensaje_saliente(telefono, texto, data):
    norm = normalizar_telefono_maestro(telefono)
    tel_db = norm['db'] if norm else telefono
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo Chat') ON CONFLICT (telefono) DO NOTHING"), {"t": tel_db})
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), {"t": tel_db, "txt": texto, "d": data})
        conn.commit()