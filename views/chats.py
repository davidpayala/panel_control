import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp, normalizar_telefono_maestro

def render_chat():
    # Refresco automático cada 5 segundos para ver mensajes nuevos
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS para las tarjetas
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px; }
    .badge-moto { background-color: #ffebd3; color: #ff8c00; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    .badge-agencia { background-color: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- COLUMNA IZQUIERDA: LISTA DE CHATS ---
    with col_lista:
        st.subheader("📩 Bandeja")
        
        # Consulta optimizada: Prioriza Nombre Corto > Nombre Google > Teléfono
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
                COALESCE(NULLIF(MAX(c.nombre_corto), 'Cliente WhatsApp'), MAX(c.nombre), MAX(c.telefono)) as display_name,
                SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
            FROM mensajes m
            LEFT JOIN Clientes c ON m.telefono = c.telefono
            GROUP BY m.telefono 
            ORDER BY ultima_fecha DESC
        """
        with engine.connect() as conn:
            lista_chats = conn.execute(text(query_lista)).fetchall()

        if not lista_chats: 
            st.info("Sin mensajes.")

        for chat in lista_chats:
            tel = chat.telefono
            # Normalizamos para mostrar bonito en el botón (986...)
            norm = normalizar_telefono_maestro(tel)
            tel_visual = norm['corto'] if norm else tel
            
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            
            # Botón de selección de chat
            label_btn = f"{notif} {chat.display_name}\n📱 {tel_visual} | 🕑 {hora}"
            if st.button(label_btn, key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- COLUMNA DERECHA: CONVERSACIÓN ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            # Encabezado del chat
            c1, c2 = st.columns([3, 1])
            norm_activo = normalizar_telefono_maestro(telefono_activo)
            titulo_tel = norm_activo['corto'] if norm_activo else telefono_activo
            
            with c1: st.markdown(f"### 💬 Chat con {titulo_tel}")
            with c2: ver_info = st.toggle("Ver Info Cliente", value=True)
            st.divider()

            # Panel de Información (toggleable)
            if ver_info:
                mostrar_info_avanzada(telefono_activo)
                st.divider()

            # Área de Mensajes (Scrollable)
            contenedor = st.container(height=450)
            
            # Marcar como leídos y cargar historial
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                historial = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_activo})

            with contenedor:
                if historial.empty: st.write("Conversación nueva.")
                for _, row in historial.iterrows():
                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role, avatar = ("user", "👤") if es_cliente else ("assistant", "🛍️")
                    
                    with st.chat_message(role, avatar=avatar):
                        # Mostrar imagen si existe
                        if row['archivo_data']:
                            try: st.image(io.BytesIO(row['archivo_data']), width=250)
                            except: st.error("Error visualizando imagen")
                        
                        # Mostrar texto
                        txt = row['contenido'] or ""
                        if txt: st.markdown(txt)
                            
                        # Hora pequeña
                        st.caption(row['fecha'].strftime('%d/%m %H:%M'))

            # Área de Envío
            prompt = st.chat_input("Escribe un mensaje...")
            
            with st.expander("📎 Adjuntar Imagen/Archivo", expanded=False):
                archivo = st.file_uploader("Subir archivo", key="up_file")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            
            if prompt: 
                enviar_texto_chat(telefono_activo, prompt)


def mostrar_info_avanzada(telefono):
    """Muestra la ficha del cliente y sus direcciones de forma segura"""
    with engine.connect() as conn:
        # 1. Obtener datos del Cliente
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.warning("⚠️ Cliente no registrado en tabla maestra (pero tiene mensajes).")
            if st.button("🛠️ Crear Ficha Básica"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro) VALUES (:t, TRUE, NOW())"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')

        # 2. Obtener Direcciones (CORRECCIÓN: id_cliente)
        dirs = pd.DataFrame()
        if id_cliente:
            # Aquí estaba el error antes. Ahora buscamos por ID.
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- RENDERIZADO DE LA FICHA ---
    
    # Fila 1: Datos Principales
    c1, c2, c3 = st.columns(3)
    with c1: st.text_input("Nombre", value=f"{cl.get('nombre') or ''} {cl.get('apellido') or ''}", disabled=True)
    with c2: st.text_input("Estado", value=cl.get('estado') or "Sin empezar", disabled=True)
    with c3: st.text_input("Fecha Registro", value=str(cl.get('fecha_registro') or "-"), disabled=True)

    # Fila 2: Google ID (Para verificar sync)
    col_g, _ = st.columns([2, 2])
    with col_g:
        val_google = cl.get('google_id')
        st.caption(f"🔗 Google ID: {val_google if val_google else '❌ No sincronizado'}")

    # Fila 3: Direcciones (CORRECCIÓN: direccion_texto)
    st.markdown("#### 📍 Direcciones")
    
    if dirs.empty:
        st.info("No hay direcciones registradas.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            # AQUÍ ESTÁ LA CORRECCIÓN CLAVE: 'direccion_texto' en vez de 'direccion'
            dir_txt = row.get('direccion_texto') or row.get('direccion') or "Sin detalle"
            
            badge = ""
            if tipo == 'AGENCIA': badge = '<span class="badge-agencia">🏢 AGENCIA</span>'
            elif tipo == 'MOTO': badge = '<span class="badge-moto">🏍️ MOTO</span>'
            
            st.markdown(f"{badge} {dir_txt}", unsafe_allow_html=True)


def enviar_texto_chat(telefono, texto):
    exito, resp = enviar_mensaje_whatsapp(telefono, texto)
    if exito:
        guardar_mensaje_saliente(telefono, texto, None)
        st.rerun()
    else: st.error(f"Error enviando: {resp}")

def enviar_archivo_chat(telefono, archivo):
    with st.spinner("Enviando..."):
        uri, err = subir_archivo_meta(archivo.getvalue(), archivo.type)
        if err: return st.error(err)
        exito, resp = enviar_mensaje_media(telefono, uri, archivo.type, "", archivo.name)
        if exito:
            guardar_mensaje_saliente(telefono, f"📎 {archivo.name}", archivo.getvalue())
            st.rerun()
        else: st.error(f"Fallo al enviar: {resp}")

def guardar_mensaje_saliente(telefono, texto, data):
    # Asegurar formato DB (51...)
    norm = normalizar_telefono_maestro(telefono)
    tel_db = norm['db'] if norm else telefono
    
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) 
            VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)
        """), {"t": tel_db, "txt": texto, "d": data})
        conn.commit()