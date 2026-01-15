import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp, crear_en_google, actualizar_en_google, normalizar_telefono_maestro

def render_chat():
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # CSS
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px; }
    .badge-moto { background-color: #ffebd3; color: #ff8c00; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    .badge-agencia { background-color: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- BANDEJA ---
    with col_lista:
        st.subheader("📩 Bandeja")
        query_lista = """
            SELECT m.telefono, MAX(m.fecha) as ultima_fecha,
            COALESCE(MAX(c.nombre_corto), MAX(c.nombre), m.telefono) as display_name,
            SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END) as no_leidos
            FROM mensajes m
            LEFT JOIN Clientes c ON m.telefono = c.telefono
            GROUP BY m.telefono 
            ORDER BY ultima_fecha DESC
        """
        with engine.connect() as conn:
            lista_chats = conn.execute(text(query_lista)).fetchall()

        if not lista_chats: st.info("Sin mensajes.")

        for chat in lista_chats:
            tel = chat.telefono
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            
            if st.button(f"{notif} {chat.display_name}\n🕑 {hora}", key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- CHAT ACTIVO ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            c1, c2 = st.columns([3, 1])
            with c1: st.markdown(f"### 💬 {telefono_activo}")
            with c2: ver_info = st.toggle("Ver Info", value=True)
            st.divider()

            if ver_info:
                mostrar_info_avanzada(telefono_activo)
                st.divider()

            # Mensajes
            contenedor = st.container(height=450)
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
                        if row['archivo_data']:
                            try: st.image(io.BytesIO(row['archivo_data']), width=250)
                            except: st.error("Error imagen")
                        else:
                            txt = row['contenido'] or ""
                            if "|ID:" in txt: txt = txt.split("|ID:")[0]
                            st.markdown(txt)
                        st.caption(row['fecha'].strftime('%H:%M'))

            # Envío
            prompt = st.chat_input("Escribe...")
            with st.expander("📎 Adjuntar", expanded=False):
                archivo = st.file_uploader("Subir", key="up")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            
            if prompt: enviar_texto_chat(telefono_activo, prompt)

# --- INFO AVANZADA Y EDICIÓN ---
def mostrar_info_avanzada(telefono):
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.error("Cliente no registrado.")
            if st.button("Crear Ficha"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro) VALUES (:t, TRUE, NOW())"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_del_cliente = cl.get('id_cliente') or cl.get('id')

        # --- FORMULARIO DE EDICIÓN DATOS BÁSICOS ---
        c1, c2, c3 = st.columns(3)
        with c1: 
            nuevo_nombre = st.text_input("Nombre Corto", value=cl.get('nombre_corto') or "")
        with c2: 
            nuevo_estado = st.selectbox("Estado", ["Sin empezar", "En proceso", "Cerrado"], index=0 if not cl.get('estado') else ["Sin empezar", "En proceso", "Cerrado"].index(cl.get('estado')))
        with c3: 
            st.text_input("Fecha Reg.", value=str(cl.get('fecha_registro') or ""), disabled=True)

        if st.button("💾 Guardar Datos Básicos"):
            with engine.connect() as conn:
                conn.execute(text("UPDATE Clientes SET nombre_corto=:n, estado=:e WHERE id_cliente=:id"),
                             {"n": nuevo_nombre, "e": nuevo_estado, "id": id_del_cliente})
                conn.commit()
            st.success("Guardado")
            st.rerun()

        # --- GOOGLE SYNC ---
        col_g1, col_g2 = st.columns([3, 1])
        with col_g1:
            st.caption(f"Google ID: {cl.get('google_id') or 'Sin sincronizar'}")
        with col_g2:
            if st.button("🔄 Sync"):
                # Aquí iría la lógica de sync si quieres activarla
                st.toast("Función Sync invocada")

        # --- DIRECCIONES ---
        st.markdown("#### 📍 Direcciones Registradas")
        
        # 1. Listar existentes
        if id_del_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id AND activo=TRUE"), conn, params={"id": id_del_cliente})
            
            if dirs.empty:
                st.warning("⚠️ No tiene direcciones.")
            else:
                for _, row in dirs.iterrows():
                    tipo = row.get('tipo_envio', 'GENERAL')
                    badge = "🏢" if tipo == 'AGENCIA' else "🏍️"
                    st.markdown(f"**{badge} {tipo}**: {row['direccion']} ({row.get('distrito') or '-'})")

        # 2. Agregar Nueva Dirección
        with st.expander("➕ Agregar Nueva Dirección"):
            with st.form("form_direccion"):
                d_tipo = st.selectbox("Tipo Envío", ["AGENCIA", "MOTO"])
                d_texto = st.text_input("Dirección Exacta")
                d_distrito = st.text_input("Distrito / Ciudad")
                d_ref = st.text_input("Referencia")
                
                if st.form_submit_button("Guardar Dirección"):
                    if d_texto:
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO Direcciones (id_cliente, direccion, tipo_envio, distrito, referencia, activo)
                                VALUES (:id, :dir, :tipo, :dis, :ref, TRUE)
                            """), {"id": id_del_cliente, "dir": d_texto, "tipo": d_tipo, "dis": d_distrito, "ref": d_ref})
                            conn.commit()
                        st.success("Dirección agregada")
                        st.rerun()
                    else:
                        st.error("Escribe una dirección")

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
        else: st.error(f"Fallo: {resp}")

def guardar_mensaje_saliente(telefono, texto, data):
    tel_clean = telefono.replace("+", "").strip()
    if len(tel_clean) == 9: tel_clean = f"51{tel_clean}"
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), {"t": tel_clean, "txt": texto, "d": data})
        conn.commit()