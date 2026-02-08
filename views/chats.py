import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
import os
import time
import requests
import streamlit.components.v1 as components 
from streamlit_autorefresh import st_autorefresh 
from database import engine 
from utils import (
    enviar_mensaje_media, enviar_mensaje_whatsapp, 
    normalizar_telefono_maestro, buscar_contacto_google, 
    crear_en_google, sincronizar_historial,marcar_leido_waha
)

OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente", 
    "✅ Compró", "👀 Prospecto", "❓ Preguntón", 
    "📉 Pide Rebaja", "📦 Mayorista"
]


def render_chat():
    # 1. Configuración inicial
    st_autorefresh(interval=5000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    # 2. Lógica de la Barra Lateral (SIDEBAR)
    with st.sidebar:
        st.header("Clientes")
        
        # Botón Sincronizar
        if st.button("🔄 Sincronizar Historial"):
            try:
                with st.spinner("Trayendo mensajes..."):
                    res = sincronizar_historial()
                st.success(res)
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Error al sincronizar: {e}")

        # Buscador
        busqueda = st.text_input("🔍 Buscar:", "")
        
        # --- CORRECCIÓN CLAVE: Tabla en minúsculas 'clientes' ---
        query_clientes = """
            SELECT telefono, nombre_corto, estado, 
                   (SELECT COUNT(*) FROM mensajes WHERE telefono = clientes.telefono AND leido = FALSE AND tipo = 'ENTRANTE') as no_leidos
            FROM clientes
            WHERE activo = TRUE
            ORDER BY no_leidos DESC, fecha_registro DESC
        """
        
        try:
            with engine.connect() as conn:
                df_clientes = pd.read_sql(query_clientes, conn)
                
            if busqueda:
                # Filtrado en memoria
                df_clientes = df_clientes[
                    df_clientes['telefono'].astype(str).str.contains(busqueda) | 
                    df_clientes['nombre_corto'].str.lower().str.contains(busqueda.lower(), na=False)
                ]

            if df_clientes.empty:
                st.info("No hay clientes activos.")
            
            # Renderizar lista
            for _, row in df_clientes.iterrows():
                tel = row['telefono']
                nombre = row['nombre_corto'] or tel
                
                # Formato visual
                notif = f"🔴 {row['no_leidos']}" if row['no_leidos'] > 0 else ""
                label = f"{notif} {nombre} ({tel})"
                
                # Botón de selección
                if st.button(label, key=f"btn_{tel}"):
                    st.session_state['chat_actual_telefono'] = tel
                    st.rerun()
                    
        except Exception as e:
            st.error(f"Error cargando clientes: {e}")
            st.code(query_clientes, language="sql")

    # 3. Chat Principal
    if not st.session_state.get('chat_actual_telefono'):
        st.info("👈 Selecciona un chat del menú lateral para comenzar")
        return

# views/chats.py (Dentro de render_chat, justo después de obtener telefono_actual)

    telefono_actual = st.session_state['chat_actual_telefono']
    
    # Obtener info del cliente seleccionado
    with engine.connect() as conn:
        info_cliente = conn.execute(text("SELECT * FROM clientes WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
        
        # 1. Marcar leídos en TU base de datos
        conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
        conn.commit()
    
    # 2. NUEVO: Mandar señal a WhatsApp (Blue Ticks)
    if info_cliente:
        # Usamos un thread o llamada directa para no bloquear UI
        marcar_leido_waha(f"{telefono_actual}@c.us")

    # Cargar Mensajes
    query_msgs = """
        SELECT * FROM mensajes 
        WHERE telefono = :t 
        ORDER BY fecha ASC
    """
    with engine.connect() as conn:
        df_msgs = pd.read_sql(query_msgs, conn, params={"t": telefono_actual})

    # Renderizar Burbujas de Chat
    contenedor_mensajes = st.container()
    with contenedor_mensajes:
        st.markdown("""
        <style>
        .chat-row { display: flex; width: 100%; margin-bottom: 10px; }
        .row-izq { justify-content: flex-start; }
        .row-der { justify-content: flex-end; }
        .bubble { max-width: 70%; padding: 10px 14px; border-radius: 12px; position: relative; font-size: 15px; }
        .bubble-izq { background-color: #333333; color: white; border-bottom-left-radius: 2px; }
        .bubble-der { background-color: #005c4b; color: white; border-bottom-right-radius: 2px; }
        .reply-box { background-color: rgba(0,0,0,0.2); border-left: 4px solid #00a884; padding: 5px; margin-bottom: 5px; border-radius: 4px; font-size: 12px; color: #ddd; }
        .meta { font-size: 10px; color: #aaa; text-align: right; margin-top: 4px; }
        </style>
        """, unsafe_allow_html=True)

        for _, msg in df_msgs.iterrows():
            es_mio = (msg['tipo'] == 'SALIENTE')
            
            clase_row = "row-der" if es_mio else "row-izq"
            clase_bubble = "bubble-der" if es_mio else "bubble-izq"
            
            contenido = msg['contenido'] or ""
            hora = msg['fecha'].strftime("%H:%M") if msg['fecha'] else ""
            
            html_reply = ""
            if msg.get('reply_content'):
                html_reply = f"<div class='reply-box'>↪ {msg['reply_content'][:60]}...</div>"

            html_archivo = ""
            if msg.get('archivo_data'):
                html_archivo = f"<div style='margin-bottom:5px'>📎 <i>Archivo adjunto</i></div>"

            # Lógica para mostrar ticks en mensajes salientes
            icono_estado = ""
            if es_mio:
                estado = msg.get('estado_waha') # Asegúrate de leer esta columna en tu query_msgs
                if estado == 'leido': icono_estado = "🔵 ✓✓"
                elif estado == 'recibido': icono_estado = "☑️ ✓✓"
                elif estado == 'enviado': icono_estado = "☑️ ✓"
                else: icono_estado = "🕒"

            st.markdown(f"""
            <div class='chat-row {clase_row}'>
                <div class='bubble {clase_bubble}'>
                    {html_reply}
                    {html_archivo}
                    {contenido}
                    <div class='meta'>{hora} {icono_estado}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Input Area
    st.markdown("---")
    c1, c2 = st.columns([5, 1])
    with c1:
        txt_input = st.text_input("Escribe un mensaje...", key="input_msg", label_visibility="collapsed")
    with c2:
        uploaded = st.file_uploader("📎", type=["png","jpg","pdf","mp4"], label_visibility="collapsed")

    if st.button("Enviar ➤", use_container_width=True):
        if uploaded:
            enviar_archivo_chat(telefono_actual, uploaded)
        elif txt_input:
            enviar_texto_chat(telefono_actual, txt_input)

def mostrar_info_avanzada(telefono):
    """Ficha de cliente integrada"""
    with engine.connect() as conn:
        res_cliente = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": telefono}).fetchone()
        
        if not res_cliente:
            if st.button("Crear Ficha Rápida"):
                 with engine.connect() as conn:
                    conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo')"), {"t": telefono})
                    conn.commit()
                    st.rerun()
            return

        cl = res_cliente._mapping
        id_cliente = cl.get('id_cliente')
        dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id"), conn, params={"id": id_cliente})

    with st.container():
        c1, c2 = st.columns(2)
        new_corto = c1.text_input("Alias", value=cl.get('nombre_corto') or "", key=f"in_corto_{telefono}")
        
        tags_act = cl.get('etiquetas', '') or ""
        lista = [t for t in tags_act.split(',') if t]
        new_tags = c2.multiselect("Etiquetas", OPCIONES_TAGS, default=[t for t in lista if t in OPCIONES_TAGS], key=f"tag_{telefono}")

    st.markdown("#### 👤 Datos Google")
    col_nom, col_ape, col_btns = st.columns([1.5, 1.5, 1.5])
    new_nombre = col_nom.text_input("Nombre", value=cl.get('nombre') or "", key=f"in_nom_{telefono}")
    new_apellido = col_ape.text_input("Apellido", value=cl.get('apellido') or "", key=f"in_ape_{telefono}")

    with col_btns:
            st.write("") 
            if st.button("📥 Google (Sync)", key=f"btn_s_{telefono}", use_container_width=True):
                with st.spinner("Google..."):
                    norm = normalizar_telefono_maestro(telefono)
                    dat = buscar_contacto_google(norm['db']) 
                    if dat and dat['encontrado']:
                        with engine.connect() as conn:
                            conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                        {"n": dat['nombre'], "a": dat['apellido'], "gid": dat['google_id'], "nc": dat['nombre_completo'], "t": telefono})
                            conn.commit()
                        st.toast("✅ Sync OK"); time.sleep(1); st.rerun()
                    elif new_nombre:
                        gid = crear_en_google(new_nombre, new_apellido, norm['db'])
                        if gid:
                            full = f"{new_nombre} {new_apellido}".strip()
                            with engine.connect() as conn:
                                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid, nombre_corto=:nc WHERE telefono=:t"), 
                                            {"n": new_nombre, "a": new_apellido, "gid": gid, "nc": full, "t": telefono})
                                conn.commit()
                            st.success(f"Creado: {full}"); time.sleep(1); st.rerun()
                        else: st.error("Error Google")
                    else: st.warning("Pon nombre primero")

    if st.button("💾 GUARDAR", key=f"save_{telefono}", type="primary", use_container_width=True):
        t_str = ",".join(new_tags)
        with engine.connect() as conn:
            conn.execute(text("UPDATE Clientes SET nombre_corto=:nc, etiquetas=:tag, nombre=:n, apellido=:a WHERE telefono=:t"), 
                        {"nc": new_corto, "tag": t_str, "n": new_nombre, "a": new_apellido, "t": telefono})
            conn.commit()
        st.toast("Guardado"); time.sleep(0.5); st.rerun()

    st.markdown("---")
    if not dirs.empty:
        for _, row in dirs.iterrows():
            st.markdown(f"📍 **{row.get('tipo_envio','Loc')}:** {row.get('direccion_texto')} ({row.get('distrito')})")

def enviar_texto_chat(telefono, texto):
    ok, r = enviar_mensaje_whatsapp(telefono, texto)
    if ok: guardar_mensaje_saliente(telefono, texto, None); st.rerun()
    else: st.error(r)

def enviar_archivo_chat(telefono, archivo):
    ok, r = enviar_mensaje_media(telefono, archivo.getvalue(), archivo.type, "", archivo.name)
    if ok: guardar_mensaje_saliente(telefono, f"📎 {archivo.name}", archivo.getvalue()); st.rerun()
    else: st.error(r)

def guardar_mensaje_saliente(telefono, texto, data):
    norm = normalizar_telefono_maestro(telefono)
    if not norm: return
    t = norm['db']
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO Clientes (telefono, activo, fecha_registro, nombre_corto) VALUES (:t, TRUE, NOW(), 'Nuevo') ON CONFLICT (telefono) DO NOTHING"), {"t": t})
        conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data) VALUES (:t, 'SALIENTE', :c, NOW(), TRUE, :d)"), {"t": t, "c": texto, "d": data})
        conn.commit()