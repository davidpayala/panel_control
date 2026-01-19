import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import streamlit.components.v1 as components # Necesario para el auto-scroll
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

    # Estilos CSS
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; padding: 15px; border-radius: 10px; margin-bottom: 5px; }
    .badge-moto { background-color: #ffebd3; color: #ff8c00; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    .badge-agencia { background-color: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 5px; font-size: 0.8em; font-weight: bold; }
    .date-separator { 
        text-align: center; 
        margin: 15px 0; 
        position: relative; 
    }
    .date-separator span { 
        background-color: #f0f2f6; 
        padding: 4px 12px; 
        border-radius: 12px; 
        font-size: 0.75em; 
        color: #555; 
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- BANDEJA (Izquierda) ---
    with col_lista:
        st.subheader("📩 Bandeja")
        
        # 1. CAMBIO AQUÍ: Lógica estricta para el nombre
        # Solo Nombre Corto O Teléfono. Nada más.
        query_lista = """
            SELECT 
                m.telefono, 
                MAX(m.fecha) as ultima_fecha,
                COALESCE(
                    NULLIF(MAX(c.nombre_corto), ''),              -- 1. Nombre Corto (Si existe y no es vacío)
                    NULLIF(MAX(c.nombre_corto), 'Cliente WhatsApp'), -- (Ignorar default viejo)
                    MAX(m.telefono)                               -- 2. Si no, Teléfono
                ) as display_name,
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
            # Para el subtítulo del botón usamos formato limpio (986...)
            norm = normalizar_telefono_maestro(tel)
            tel_visual = norm['corto'] if norm else tel
            
            notif = f"🔴 {chat.no_leidos}" if chat.no_leidos > 0 else ""
            tipo_btn = "primary" if st.session_state['chat_actual_telefono'] == tel else "secondary"
            hora = chat.ultima_fecha.strftime('%H:%M') if chat.ultima_fecha else ""
            
            # Botón del Chat
            if st.button(f"{notif} {chat.display_name}\n📱 {tel_visual} | 🕑 {hora}", key=f"btn_{tel}", type=tipo_btn):
                st.session_state['chat_actual_telefono'] = tel
                st.rerun()

    # --- CHAT (Derecha) ---
    with col_chat:
        telefono_activo = st.session_state['chat_actual_telefono']

        if telefono_activo:
            norm_activo = normalizar_telefono_maestro(telefono_activo)
            titulo_tel = norm_activo['corto'] if norm_activo else telefono_activo
            
            # Header
            c1, c2 = st.columns([3, 1])
            with c1: st.markdown(f"### 💬 Chat con {titulo_tel}")
            
            # 2. CAMBIO AQUÍ: value=False para que inicie cerrado
            with c2: ver_info = st.toggle("Ver Ficha", value=False)
            st.divider()

            if ver_info:
                mostrar_info_avanzada(telefono_activo)
                st.divider()

            # Área de Mensajes
            contenedor = st.container(height=450)
            
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_activo})
                conn.commit()
                historial = pd.read_sql(text("SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha ASC"), conn, params={"t": telefono_activo})

            with contenedor:
                if historial.empty: st.write("Conversación nueva.")
                
                # 3. CAMBIO AQUÍ: Separadores de Fecha
                last_date = None
                
                for _, row in historial.iterrows():
                    # Calcular fecha actual del mensaje
                    msg_date = row['fecha'].date()
                    
                    # Si la fecha cambió respecto al mensaje anterior, poner separador
                    if last_date != msg_date:
                        st.markdown(f"""
                            <div class="date-separator">
                                <span>📅 {msg_date.strftime('%d/%m/%Y')}</span>
                            </div>
                        """, unsafe_allow_html=True)
                        last_date = msg_date

                    # Renderizar mensaje normal
                    es_cliente = (row['tipo'] == 'ENTRANTE')
                    role, avatar = ("user", "👤") if es_cliente else ("assistant", "🛍️")
                    
                    with st.chat_message(role, avatar=avatar):
                        if row['archivo_data']:
                            try: st.image(io.BytesIO(row['archivo_data']), width=250)
                            except: st.error("Error imagen")
                        txt = row['contenido'] or ""
                        if txt: st.markdown(txt)
                        st.caption(row['fecha'].strftime('%H:%M'))
            
            # 4. CAMBIO AQUÍ: Auto-Scroll Javascript
            # Este script busca el último mensaje y hace scroll hacia él
            components.html("""
            <script>
                const elements = window.parent.document.querySelectorAll('.stChatMessage');
                if (elements.length > 0) {
                    elements[elements.length - 1].scrollIntoView({behavior: "smooth"});
                }
            </script>
            """, height=0, width=0)

            # Inputs
            prompt = st.chat_input("Escribe...")
            with st.expander("📎 Adjuntar", expanded=False):
                archivo = st.file_uploader("Subir", key="up_file")
                if archivo and st.button("Enviar Archivo"):
                    enviar_archivo_chat(telefono_activo, archivo)
            
            if prompt: enviar_texto_chat(telefono_activo, prompt)


def mostrar_info_avanzada(telefono):
    """
    Ficha de cliente con KEYS DINÁMICAS
    """
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            st.warning("⚠️ Cliente no registrado.")
            if st.button("Crear Ficha Inicial"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro) VALUES (:t, TRUE, NOW())"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        google_id_actual = cl.get('google_id')
        
        dirs = pd.DataFrame()
        if id_cliente:
            dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    # --- 1. DATOS INTERNOS ---
    with st.container():
        c1, c2 = st.columns(2)
        new_corto = c1.text_input("📝 Alias (Nombre Corto)", value=cl.get('nombre_corto') or "", key=f"in_corto_{telefono}")
        
        estados = ["Sin empezar", "Interesado", "En proceso", "Venta cerrada", "Post-venta"]
        estado_act = cl.get('estado')
        idx = estados.index(estado_act) if estado_act in estados else 0
        new_estado = c2.selectbox("Estado", estados, index=idx, key=f"in_estado_{telefono}")

    # --- 2. DATOS PERSONALES ---
    st.markdown("#### 👤 Sincronización con Google")
    col_nom, col_ape, col_btns = st.columns([1.5, 1.5, 1.5])
    
    val_nombre = cl.get('nombre') or ""
    val_apellido = cl.get('apellido') or ""
    
    new_nombre = col_nom.text_input("Nombre", value=val_nombre, key=f"in_nom_{telefono}")
    new_apellido = col_ape.text_input("Apellido", value=val_apellido, key=f"in_ape_{telefono}")

    with col_btns:
        st.write("") 
        
        if st.button("📥 Buscar en Google", key=f"btn_search_{telefono}", use_container_width=True):
            with st.spinner("Buscando..."):
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
                    st.success("✅ Datos de Google")
                    st.rerun()
                else:
                    st.warning("No encontrado.")

        label_google = "🔄 Actualizar Google" if google_id_actual else "☁️ Crear en Google"
        tipo_btn_google = "primary" if not google_id_actual else "secondary"
        
        if st.button(label_google, key=f"btn_push_{telefono}", type=tipo_btn_google, use_container_width=True):
            if not new_nombre:
                st.error("Falta nombre")
            else:
                with st.spinner("Conectando..."):
                    nuevo_gid = None
                    if google_id_actual:
                        ok = actualizar_en_google(google_id_actual, new_nombre, new_apellido, telefono)
                        if ok: st.success("✅ Actualizado")
                    else:
                        nuevo_gid = crear_en_google(new_nombre, new_apellido, telefono)
                        if nuevo_gid: 
                            st.success("✅ Creado")
                            with engine.connect() as conn:
                                conn.execute(text("UPDATE Clientes SET google_id=:g WHERE telefono=:t"), {"g": nuevo_gid, "t": telefono})
                                conn.commit()
                            st.rerun()

    # --- 3. GUARDAR LOCAL ---
    if st.button("💾 GUARDAR CAMBIOS (Solo Local)", key=f"btn_save_loc_{telefono}", use_container_width=True):
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE Clientes 
                SET nombre_corto=:nc, estado=:est, nombre=:n, apellido=:a
                WHERE telefono=:t
            """), {
                "nc": new_corto, "est": new_estado, 
                "n": new_nombre, "a": new_apellido, "t": telefono
            })
            conn.commit()
        st.toast("Datos guardados")
        st.rerun()

    # --- 4. DIRECCIONES ---
    st.markdown("---")
    st.markdown("#### 📍 Direcciones")
    if dirs.empty:
        st.info("No hay direcciones registradas.")
    else:
        for _, row in dirs.iterrows():
            tipo = row.get('tipo_envio', 'GENERAL')
            txt = row.get('direccion_texto') or row.get('direccion') or ""
            badge = "🏢" if tipo == 'AGENCIA' else "🏍️" if tipo == 'MOTO' else "📍"
            st.markdown(f"**{badge} {tipo}:** {txt}")

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
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :txt, (NOW() - INTERVAL '5 hours'), TRUE, :d)"), {"t": tel_db, "txt": texto, "d": data})
        conn.commit()