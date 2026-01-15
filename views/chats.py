import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from streamlit_autorefresh import st_autorefresh 

# Importamos DB y UTILS (Incluyendo las de Google)
from database import engine 
from utils import (
    subir_archivo_meta, 
    enviar_mensaje_media, 
    enviar_mensaje_whatsapp,
    crear_en_google,         # <--- NUEVO
    actualizar_en_google     # <--- NUEVO
)

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================
def render_chat():
    # Auto-refresh cada 5s
    st_autorefresh(interval=5000, key="chat_autorefresh")
    
    st.title("💬 Chat Center (WAHA)")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS
    st.markdown("""
    <style>
    div.stButton > button:first-child {
        text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- 1. BANDEJA DE ENTRADA (IZQUIERDA) ---
    with col_lista:
        st.subheader("📩 Bandeja")
        
        # Consulta para traer chats + nombres
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
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

        if not lista_chats: st.info("📭 Vacío.")

        for chat in lista_chats:
            tel = chat.telefono
            nombre_mostrar = chat.nombre_pila
            if chat.apellido: nombre_mostrar += f" {chat.apellido}"
            
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            
            if st.button(f"{notif} {nombre_mostrar}\n🕑 {hora}", key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- 2. CHAT ACTIVO (DERECHA) ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            # Header
            c1, c2 = st.columns([3, 1])
            with c1: st.markdown(f"### 💬 Chat: **{telefono_activo}**")
            with c2: ver_ficha = st.toggle("📋 Ficha", value=True)
            st.divider()

            # A. FICHA CLIENTE (CON SYNC GOOGLE)
            if ver_ficha:
                with st.container():
                    st.info("📂 **Datos y Google Contacts**")
                    cargar_info_cliente_con_google(telefono_activo) # <--- FUNCIÓN ACTUALIZADA
                    st.divider()

            # B. MENSAJES
            contenedor_mensajes = st.container(height=450)
            
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                historial = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono = :t ORDER BY fecha ASC"), conn, params={"t": telefono_activo})

            with contenedor_mensajes:
                if historial.empty: st.write("Conversación nueva...")
                for _, row in historial.iterrows():
                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role, avatar = ("user", "👤") if es_cliente else ("assistant", "🛍️")
                    
                    with st.chat_message(role, avatar=avatar):
                        contenido = row['contenido']
                        data_b = row['archivo_data']
                        
                        if data_b:
                            try: st.image(io.BytesIO(data_b), width=250)
                            except: st.error("Error imagen")
                            if contenido: st.caption(contenido)
                        else:
                            if "|ID:" in contenido: st.markdown(contenido.split("|ID:")[0].strip())
                            else: st.markdown(contenido)
                        st.caption(row['fecha'].strftime('%H:%M'))

            # C. ENVÍO
            prompt = st.chat_input("Escribe...")
            
            with st.expander("📎 Adjuntar", expanded=False):
                archivo = st.file_uploader("Archivo", key="up_chat")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)

            if prompt: enviar_texto_chat(telefono_activo, prompt)

        else:
            st.markdown("<br><br><h3 style='text-align: center; color: gray;'>👈 Selecciona un chat</h3>", unsafe_allow_html=True)

# ==============================================================================
# LÓGICA DE CLIENTE + GOOGLE
# ==============================================================================
def cargar_info_cliente_con_google(telefono):
    """Muestra datos, edita DB Local y Sincroniza con Google Contacts"""
    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM Clientes WHERE telefono = :t"), {"t": telefono}).fetchone()
        
        if res:
            cliente = res._mapping
            c1, c2 = st.columns(2)
            
            # Formulario
            with c1:
                nuevo_nombre = st.text_input("Nombre", value=cliente['nombre'] or "")
                nuevo_apellido = st.text_input("Apellido", value=cliente['apellido'] or "")
            with c2:
                # Usamos .get por si la columna aun no existe (evita crash)
                dir_actual = cliente.get('direccion') or ""
                notas_actual = cliente.get('notas') or ""
                nueva_direccion = st.text_area("🏠 Dirección", value=dir_actual, height=100)
                notas = st.text_area("📝 Notas", value=notas_actual, height=100)
                
            # Recuperar Google ID si existe
            google_id_actual = cliente.get('google_id')

            if st.button("💾 Guardar y Sincronizar Google"):
                with st.spinner("Guardando en DB y Google..."):
                    # 1. Google Sync Logic
                    nuevo_gid = google_id_actual
                    
                    if google_id_actual:
                        # Si ya tiene ID, actualizamos
                        ok = actualizar_en_google(google_id_actual, nuevo_nombre, nuevo_apellido, telefono)
                        if ok: st.toast("✅ Actualizado en Google Contacts")
                        else: st.toast("⚠️ Error actualizando Google (Token expirado?)")
                    else:
                        # Si no tiene ID, creamos
                        nuevo_gid = crear_en_google(nuevo_nombre, nuevo_apellido, telefono)
                        if nuevo_gid: st.toast("✅ Creado nuevo en Google Contacts")
                        else: st.toast("⚠️ No se pudo crear en Google (Revisar Token)")

                    # 2. Update Local DB
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET nombre=:n, apellido=:a, direccion=:d, notas=:nt, google_id=:gid
                        WHERE telefono=:t
                    """), {
                        "n": nuevo_nombre, "a": nuevo_apellido, 
                        "d": nueva_direccion, "nt": notas, "gid": nuevo_gid, "t": telefono
                    })
                    conn.commit()
                    st.success("¡Datos guardados!")
                    st.rerun()
        else:
            st.warning("Cliente nuevo (Sin ficha).")
            if st.button("➕ Crear Ficha Base"):
                conn.execute(text("INSERT INTO Clientes (telefono, activo) VALUES (:t, TRUE)"), {"t": telefono})
                conn.commit()
                st.rerun()

# ==============================================================================
# FUNCIONES ENVÍO
# ==============================================================================
def enviar_texto_chat(telefono, texto):
    exito, resp = enviar_mensaje_whatsapp(telefono, texto)
    if exito:
        guardar_mensaje_saliente(telefono, texto, None)
        st.rerun()
    else: st.error(f"Error: {resp}")

def enviar_archivo_chat(telefono, archivo):
    with st.spinner("Enviando..."):
        bytes_data = archivo.getvalue()
        mime = archivo.type
        name = archivo.name
        
        uri, err = subir_archivo_meta(bytes_data, mime)
        if err: 
            st.error(err)
            return

        exito, resp = enviar_mensaje_media(telefono, uri, mime, "", name)
        if exito:
            txt_db = f"📷 {name}" if "image" in mime else f"📎 {name}"
            guardar_mensaje_saliente(telefono, txt_db, bytes_data)
            st.success("Enviado")
            st.rerun()
        else: st.error(f"Fallo: {resp}")

def guardar_mensaje_saliente(telefono, texto, data):
    tel_clean = telefono.replace("+", "").strip()
    if len(tel_clean) == 9: tel_clean = f"51{tel_clean}"
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), {"t": tel_clean, "txt": texto, "d": data})
        conn.commit()