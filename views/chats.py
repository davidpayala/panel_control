import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh 
from database import engine 
# Importamos la función de búsqueda para el botón Sync
from utils import subir_archivo_meta, enviar_mensaje_media, enviar_mensaje_whatsapp, normalizar_telefono_maestro, buscar_contacto_google

def render_chat():
    # Refresco automático cada 5 segundos
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # Estilos CSS
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px; }
    .badge-moto { background-color: #ffebd3; color: #ff8c00; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    .badge-agencia { background-color: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- BANDEJA DE ENTRADA ---
    with col_lista:
        st.subheader("📩 Bandeja")
        
        # Consulta inteligente
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

        if not lista_chats: st.info("Sin mensajes.")

        for chat in lista_chats:
            tel = chat.telefono
            norm = normalizar_telefono_maestro(tel)
            tel_visual = norm['corto'] if norm else tel
            
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            
            if st.button(f"{notif} {chat.display_name}\n📱 {tel_visual} | 🕑 {hora}", key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- ÁREA DE CHAT ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            norm_activo = normalizar_telefono_maestro(telefono_activo)
            titulo_tel = norm_activo['corto'] if norm_activo else telefono_activo
            
            # Header
            c1, c2 = st.columns([3, 1])
            with c1: st.markdown(f"### 💬 Chat con {titulo_tel}")
            with c2: ver_info = st.toggle("Ver Ficha Cliente", value=True)
            st.divider()

            # FICHA DE CLIENTE (Editable)
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
                        
                        txt = row['contenido'] or ""
                        if txt: st.markdown(txt)
                        st.caption(row['fecha'].strftime('%d/%m %H:%M'))

            # Input Texto
            prompt = st.chat_input("Escribe un mensaje...")
            
            # Input Archivo
            with st.expander("📎 Adjuntar Archivo", expanded=False):
                archivo = st.file_uploader("Subir", key="up_file")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            
            if prompt: enviar_texto_chat(telefono_activo, prompt)


def mostrar_info_avanzada(telefono):
    """Muestra y permite EDITAR la ficha del cliente"""
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.warning("⚠️ Cliente no registrado.")
            if st.button("Crear Ficha"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro) VALUES (:t, TRUE, NOW())"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        
        # Direcciones
        dirs = pd.DataFrame()
        if id_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- FORMULARIO DE EDICIÓN ---
    with st.container():
        st.caption("📝 Datos Generales")
        c1, c2, c3 = st.columns([1.5, 1.5, 1])
        
        # 1. Nombre Corto
        new_corto = c1.text_input("Nombre Corto (Alias)", value=cl.get('nombre_corto') or "", key="in_corto")
        
        # 2. Estado
        estados_posibles = ["Sin empezar", "Interesado", "En proceso", "Venta cerrada", "Post-venta"]
        estado_actual = cl.get('estado')
        idx_estado = 0
        if estado_actual in estados_posibles:
            idx_estado = estados_posibles.index(estado_actual)
        new_estado = c2.selectbox("Estado", estados_posibles, index=idx_estado, key="in_estado")

    # --- DATOS DE CONTACTO (GOOGLE / MANUAL) ---
    st.caption("👤 Datos Personales (Google / Manual)")
    cg1, cg2, cg3 = st.columns([1.5, 1.5, 1])
    
    # AHORA SON EDITABLES (Quitamos disabled=True)
    new_nombre = cg1.text_input("Nombre", value=cl.get('nombre') or "", key="in_nom")
    new_apellido = cg2.text_input("Apellido", value=cl.get('apellido') or "", key="in_ape")
    
    # --- LÓGICA DEL BOTÓN RE-SYNC ---
    with cg3: 
        st.write("") # Espaciador
        if st.button("🔄 Re-Sync Google"):
            with st.spinner("Buscando en Google..."):
                norm = normalizar_telefono_maestro(telefono)
                # Buscamos usando el número corto (986...)
                datos_google = buscar_contacto_google(norm['corto'])
                
                if datos_google and datos_google['encontrado']:
                    with engine.connect() as conn:
                        conn.execute(text("""
                            UPDATE Clientes 
                            SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc
                            WHERE telefono=:t
                        """), {
                            "n": datos_google['nombre'],
                            "a": datos_google['apellido'],
                            "gid": datos_google['google_id'],
                            "nc": datos_google['nombre_completo'], # Actualizamos también el corto
                            "t": telefono
                        })
                        conn.commit()
                    st.success("¡Sincronizado!")
                    st.rerun()
                else:
                    st.error("No encontrado en Google Contacts.")

    # --- BOTÓN DE GUARDADO GENERAL ---
    # Lo ponemos abajo para que guarde TODO (lo de arriba y lo de abajo)
    if st.button("💾 GUARDAR TODOS LOS CAMBIOS", type="primary", use_container_width=True):
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE Clientes 
                SET nombre_corto=:nc, estado=:est, nombre=:nom, apellido=:ape
                WHERE telefono=:tel
            """), {
                "nc": new_corto, 
                "est": new_estado, 
                "nom": new_nombre,     # Guarda lo que escribiste en Nombre
                "ape": new_apellido,   # Guarda lo que escribiste en Apellido
                "tel": telefono
            })
            conn.commit()
        st.success("¡Datos guardados!")
        st.rerun()

    # --- DIRECCIONES ---
    st.markdown("---")
    st.markdown("#### 📍 Direcciones Registradas")
    if dirs.empty:
        st.info("No hay direcciones registradas.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            txt_dir = row.get('direccion_texto') or row.get('direccion') or "Sin detalle"
            badge = "🏢" if tipo == 'AGENCIA' else "🏍️" if tipo == 'MOTO' else "📍"
            st.markdown(f"**{badge} {tipo}:** {txt_dir}")


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
        conn.execute(text("""
            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) 
            VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)
        """), {"t": tel_db, "txt": texto, "d": data})
        conn.commit()