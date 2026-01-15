import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp, crear_en_google, actualizar_en_google

def render_chat():
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # CSS para las tarjetas de mensajes
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
        
        # Mostramos nombre_corto si existe, sino el nombre completo
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
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


# BUSCAR ESTA FUNCIÓN Y REEMPLAZARLA COMPLETA
def mostrar_info_avanzada(telefono):
    with engine.connect() as conn:
        # 1. BUSCAR CLIENTE POR TELÉFONO
        # Obtenemos su ID único (id_cliente o id)
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.error("Cliente no registrado en tabla maestra.")
            if st.button("Crear Ficha"):
                 with engine.connect() as conn:
                    # Creamos cliente básico
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_creacion) VALUES (:t, TRUE, NOW())"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        # Convertimos cliente a diccionario
        cl = res_cliente._mapping
        id_del_cliente = cl.get('id_cliente') or cl.get('id') # Soporte para ambos nombres de ID

        # 2. BUSCAR DIRECCIONES POR ID (NO POR TELÉFONO)
        # Aquí es donde fallaba antes. Ahora usamos el ID.
        if id_del_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_del_cliente})
        else:
            dirs = pd.DataFrame() # Vacío si no hay ID

    # --- MOSTRAR DATOS ---
    
    # Fila 1: Datos Básicos
    c1, c2, c3 = st.columns(3)
    with c1: st.text_input("Nombre Corto", value=cl.get('nombre_corto') or "", disabled=True)
    with c2: st.text_input("Estado", value=cl.get('estado') or "", disabled=True)
    with c3: st.text_input("Fecha Seg.", value=str(cl.get('fecha_seguimiento') or ""), disabled=True)

    # Fila 2: Sincronización Google
    col_g1, col_g2 = st.columns([3, 1])
    with col_g1:
        st.caption(f"Google ID: {cl.get('google_id') or 'Sin sincronizar'}")
        st.caption(f"Nombre: {cl.get('nombre') or '-'} {cl.get('apellido') or '-'}")
    with col_g2:
        st.button("🔄 Sync", disabled=True) 

    # Fila 3: DIRECCIONES (Lectura)
    st.markdown("#### 📍 Direcciones Registradas")
    
    if dirs.empty:
        st.warning("⚠️ No tiene direcciones registradas.")
    else:
        tiene_agencia = False
        tiene_moto = False
        
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')  # AGENCIA o MOTO
            direccion = row.get('direccion', '')
            
            if tipo == 'AGENCIA': 
                tiene_agencia = True
                badge = '<span class="badge-agencia">🏢 AGENCIA</span>'
            elif tipo == 'MOTO': 
                tiene_moto = True
                badge = '<span class="badge-moto">🏍️ MOTO</span>'
            else:
                badge = f"<span style='background:#eee; padding:2px 5px; border-radius:4px'>{tipo}</span>"
            
            st.markdown(f"{badge} **{direccion}**", unsafe_allow_html=True)
        
        # Resumen Visual
        st.markdown("---")
        if tiene_agencia and tiene_moto:
            st.success("✅ Cliente HÍBRIDO (Usa Agencia y Moto)")
        elif tiene_agencia:
            st.info("📦 Cliente SOLO AGENCIA")
        elif tiene_moto:
            st.warning("🛵 Cliente SOLO MOTO")

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