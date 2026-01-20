import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import streamlit.components.v1 as components 
from datetime import datetime
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import (
    subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp, 
    normalizar_telefono_maestro, buscar_contacto_google, 
    crear_en_google, actualizar_en_google
)

def render_chat():
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS para botones compactos
    st.markdown("""
    <style>
    div.stButton > button:first-child { 
        text-align: left; 
        width: 100%; 
        padding: 10px 15px; /* Menos padding */
        border-radius: 8px; 
        margin-bottom: 2px; /* Menos margen entre botones */
        white-space: nowrap; /* Evita saltos de línea */
        overflow: hidden; 
        text-overflow: ellipsis; /* Pone '...' si es muy largo */
    }
    .date-separator { 
        text-align: center; margin: 15px 0; position: relative; 
    }
    .date-separator span { 
        background-color: #f0f2f6; padding: 4px 12px; border-radius: 12px; 
        font-size: 0.75em; color: #555; font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- COLUMNA IZQUIERDA: LISTA Y BUSCADOR ---
    with col_lista:
        st.subheader("📩 Chats")
        
        # 1. FUNCIÓN: INICIAR NUEVO CHAT
        with st.expander("➕ Iniciar Nuevo Chat", expanded=False):
            nuevo_num = st.text_input("Escribe el número:", placeholder="Ej: 999888777")
            if st.button("Ir al Chat", use_container_width=True):
                norm = normalizar_telefono_maestro(nuevo_num)
                if norm:
                    st.session_state['chat_actual_telefono'] = norm['db']
                    st.rerun()
                else:
                    st.error("Número inválido")

        # 2. LISTA DE CHATS EXISTENTES
        # Consulta optimizada para mostrar SOLO nombre
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
                COALESCE(
                    NULLIF(MAX(c.nombre_corto), ''),
                    NULLIF(MAX(c.nombre_corto), 'Cliente WhatsApp'),
                    MAX(m.telefono) 
                ) as display_name,
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

        st.divider() # Separador visual

        for chat in lista_chats:
            tel = chat.telefono
            # Logica de visualización COMPACTA
            notif = "🔴" if chat.no_leidos > 0 else "👤"
            nombre_mostrar = chat.display_name
            
            # Si es el chat activo, lo resaltamos con primary
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            
            # Botón Simple: "🔴 Juan Perez"
            if st.button(f"{notif} {nombre_mostrar}", key=f"btn_{tel}", type=tipo_btn, use_container_width=True):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- COLUMNA DERECHA: CONVERSACIÓN ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            norm_activo = normalizar_telefono_maestro(telefono_activo)
            titulo_tel = norm_activo['corto'] if norm_activo else telefono_activo
            
            # Header del Chat
            c1, c2 = st.columns([3, 1])
            with c1: st.markdown(f"### 💬 {titulo_tel}")
            with c2: ver_info = st.toggle("Ver Ficha", value=False)
            st.divider()

            if ver_info:
                mostrar_info_avanzada(telefono_activo)
                st.divider()

            # Área de Mensajes
            contenedor = st.container(height=450)
            
            with engine.connect() as conn:
                # Marcar leido
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                # Traer historial
                historial = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_activo})

            with contenedor:
                if historial.empty: 
                    st.info("👋 Conversación nueva. Escribe el primer mensaje.")
                
                last_date = None
                for _, row in historial.iterrows():
                    # Separador de Fechas
                    msg_date = row['fecha'].date()
                    if last_date != msg_date:
                        st.markdown(f"""<div class="date-separator"><span>📅 {msg_date.strftime('%d/%m/%Y')}</span></div>""", unsafe_allow_html=True)
                        last_date = msg_date

                    # Mensaje
                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role, avatar = ("user", "👤") if es_cliente else ("assistant", "🛍️")
                    
                    with st.chat_message(role, avatar=avatar):
                        if row['archivo_data']:
                            try: st.image(io.BytesIO(row['archivo_data']), width=250)
                            except: st.error("Error imagen")
                        txt = row['contenido'] or ""
                        if txt: st.markdown(txt)
                        st.caption(row['fecha'].strftime('%H:%M'))
            
            # Auto-Scroll
            components.html("""
            <script>
                const elements = window.parent.document.querySelectorAll('.stChatMessage');
                if (elements.length > 0) {
                    elements[elements.length - 1].scrollIntoView({behavior: "smooth"});
                }
            </script>
            """, height=0, width=0)

            # Input Texto
            prompt = st.chat_input("Escribe un mensaje...")
            
            # Input Archivo
            with st.expander("📎 Adjuntar Archivo", expanded=False):
                archivo = st.file_uploader("Subir", key="up_file")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            
            if prompt: enviar_texto_chat(telefono_activo, prompt)
        else:
            st.markdown("### 👈 Selecciona un chat o inicia uno nuevo")
            st.image("https://cdn-icons-png.flaticon.com/512/1041/1041916.png", width=150)


def mostrar_info_avanzada(telefono):
    """
    Ficha de cliente (Igual que antes pero encapsulada)
    """
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        # Si no existe, damos opción de crear ficha rápida para que no falle la vista
        if not res_cliente:
            st.warning("⚠️ Este número no está registrado como cliente.")
            if st.button("Crear Ficha Rápida"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo Cliente')"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        google_id_actual = cl.get('google_id')
        
        dirs = pd.DataFrame()
        if id_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- EDITAR DATOS ---
    with st.container():
        c1, c2 = st.columns(2)
        new_corto = c1.text_input("Alias", value=cl.get('nombre_corto') or "", key=f"in_corto_{telefono}")
        
        estados = ["Sin empezar", "Interesado", "En proceso", "Venta cerrada", "Post-venta"]
        estado_act = cl.get('estado')
        idx = estados.index(estado_act) if estado_act in estados else 0
        new_estado = c2.selectbox("Estado", estados, index=idx, key=f"in_estado_{telefono}")

    st.markdown("#### 👤 Google Contacts")
    col_nom, col_ape, col_btns = st.columns([1.5, 1.5, 1.5])
    
    val_nombre = cl.get('nombre') or ""
    val_apellido = cl.get('apellido') or ""
    
    new_nombre = col_nom.text_input("Nombre", value=val_nombre, key=f"in_nom_{telefono}")
    new_apellido = col_ape.text_input("Apellido", value=val_apellido, key=f"in_ape_{telefono}")

    with col_btns:
        st.write("") 
        
        # BOTÓN BUSCAR
        if st.button("📥 Buscar", key=f"btn_search_{telefono}", use_container_width=True):
            with st.spinner("..."):
                norm = normalizar_telefono_maestro(telefono)
                datos = buscar_contacto_google(norm['db']) 
                if datos and datos['encontrado']:
                    with engine.connect() as conn:
                        conn.execute(text("""
                            UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc 
                            WHERE telefono=:t
                        """), {
                            "n": datos['nombre'], "a": datos['apellido'], 
                            "gid": datos['google_id'], "nc": datos['nombre_completo'], "t": telefono
                        })
                        conn.commit()
                    st.success("Encontrado")
                    st.rerun()
                else:
                    st.warning("No encontrado")

        # BOTÓN GUARDAR GOOGLE
        label_google = "🔄 Actualizar" if google_id_actual else "☁️ Crear"
        if st.button(label_google, key=f"btn_push_{telefono}", use_container_width=True):
            with st.spinner("..."):
                nuevo_gid = None
                if google_id_actual:
                    ok = actualizar_en_google(google_id_actual, new_nombre, new_apellido, telefono)
                    if ok: st.success("OK")
                else:
                    nuevo_gid = crear_en_google(new_nombre, new_apellido, telefono)
                    if nuevo_gid: 
                        with engine.connect() as conn:
                            conn.execute(text("UPDATE Clientes SET google_id=:g WHERE telefono=:t"), {"g": nuevo_gid, "t": telefono})
                            conn.commit()
                        st.success("Creado")
                        st.rerun()

    if st.button("💾 GUARDAR LOCAL", key=f"btn_save_loc_{telefono}", type="primary", use_container_width=True):
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE Clientes SET nombre_corto=:nc, estado=:est, nombre=:n, apellido=:a WHERE telefono=:t
            """), {"nc": new_corto, "est": new_estado, "n": new_nombre, "a": new_apellido, "t": telefono})
            conn.commit()
        st.toast("Guardado")
        st.rerun()

    st.markdown("---")
    if dirs.empty:
        st.caption("Sin direcciones registradas.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            txt = row.get('direccion_texto') or ""
            st.markdown(f"📍 **{tipo}:** {txt}")

def enviar_texto_chat(telefono, texto):
    exito, resp = enviar_mensaje_whatsapp(telefono, texto)
    if exito:
        guardar_mensaje_saliente(telefono, texto, None)
        st.rerun()
    else: st.error(f"Error: {resp}")

def enviar_archivo_chat(telefono, archivo):
    with st.spinner("Enviando..."):
        uri, err = subir_archivo_meta(archivo.getvalue(), archivo.type)
        if err: return st.error(err)
        exito, resp = enviar_mensaje_media(telefono, uri, archivo.type, "", archivo.name)
        if exito:
            guardar_mensaje_saliente(telefono, f"📎 {archivo.name}", archivo.getvalue())
            st.rerun()
        else: st.error(f"Error: {resp}")

def guardar_mensaje_saliente(telefono, texto, data):
    norm = normalizar_telefono_maestro(telefono)
    tel_db = norm['db'] if norm else telefono
    with engine.connect() as conn:
        # Aseguramos que el cliente exista antes de guardar el mensaje
        conn.execute(text("""
            INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) 
            VALUES (:t, TRUE, NOW(), 'Nuevo Chat') 
            ON CONFLICT (telefono) DO NOTHING
        """), {"t": tel_db})
        
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), {"t": tel_db, "txt": texto, "d": data})
        conn.commit()