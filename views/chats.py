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
    crear_en_google, sincronizar_historial, render_chat
)

# Copiamos las mismas opciones para mantener consistencia
OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente", 
    "✅ Compró", "👀 Prospecto", "❓ Preguntón", 
    "📉 Pide Rebaja", "📦 Mayorista"
]

# ==============================================================================
# FUNCIONES DE SINCRONIZACION (WAHA)
# ==============================================================================
def sincronizar_historial(telefono):
    norm = normalizar_telefono_maestro(telefono)
    if not norm: return False, "Teléfono inválido"
    
    target_db = norm['db']
    chat_id_waha = norm['waha']

    WAHA_URL = os.getenv("WAHA_URL", "http://waha:3000") 
    WAHA_API_KEY = os.getenv("WAHA_KEY", "321") 
    
    try:
        headers = {"Content-Type": "application/json", "X-Api-Key": WAHA_API_KEY}
        url = f"{WAHA_URL}/api/messages?chatId={chat_id_waha}&limit=50&downloadMedia=false"
        
        response = requests.get(url, headers=headers, timeout=8)
        
        if response.status_code == 200:
            mensajes_waha = response.json()
            nuevos = 0
            
            with engine.begin() as conn:
                for msg in mensajes_waha:
                    cuerpo = msg.get('body', '')
                    if not cuerpo: continue
                    
                    # Validar remitente basura
                    participant_check = msg.get('from')
                    if not normalizar_telefono_maestro(participant_check): continue 

                    es_mio = msg.get('fromMe', False)
                    tipo_msg = 'SALIENTE' if es_mio else 'ENTRANTE'
                    timestamp = msg.get('timestamp')
                    w_id = msg.get('id', None)
                    
                    # --- CAPTURA DE REPLY ---
                    # WAHA envía 'replyTo' como el ID del mensaje original
                    reply_id = msg.get('replyTo') 
                    # A veces WAHA lo manda como objeto, aseguramos que sea string
                    if isinstance(reply_id, dict): reply_id = reply_id.get('id')
                    # ------------------------

                    if w_id:
                        existe = conn.execute(text("SELECT count(*) FROM mensajes WHERE whatsapp_id = :wid"), {"wid": w_id}).scalar()
                    else:
                        existe = conn.execute(text("""
                            SELECT count(*) FROM mensajes 
                            WHERE telefono = :t AND contenido = :m AND fecha > (NOW() - INTERVAL '24 hours')
                        """), {"t": target_db, "m": cuerpo}).scalar()
                    
                    if existe == 0:
                        conn.execute(text("""
                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id, reply_to_id)
                            VALUES (:t, :tp, :m, to_timestamp(:ts), TRUE, :wid, :rid)
                        """), {
                            "t": target_db, 
                            "tp": tipo_msg, 
                            "m": cuerpo,
                            "ts": timestamp,
                            "wid": w_id,
                            "rid": reply_id # <--- GUARDAMOS EL ID DE LA CITA
                        })
                        nuevos += 1
            
            return True, f"Sync: {nuevos} nuevos."
        elif response.status_code == 401:
            return False, "Error 401: API Key incorrecta."
        else:
            return False, f"Error WAHA: {response.status_code}"
            
    except Exception as e:
        return False, f"Error conexión: {e}"
# ==============================================================================
# Render Chat
# ==============================================================================
def render_chat():
    st_autorefresh(interval=10000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # CSS ACTUALIZADO PARA CITAS
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; border-radius: 8px; margin-bottom: 2px; overflow: hidden; text-overflow: ellipsis; }
    
    .chat-bubble { padding: 10px 15px; border-radius: 12px; margin-bottom: 8px; max-width: 75%; color: white; font-size: 15px; position: relative; }
    .incoming { background-color: #262730; margin-right: auto; border-bottom-left-radius: 2px; }
    .outgoing { background-color: #004d40; margin-left: auto; border-bottom-right-radius: 2px; }
    
    /* ESTILO PARA EL MENSAJE CITADO */
    .reply-context {
        background-color: rgba(0,0,0,0.2);
        border-left: 4px solid #00bc8c; /* Verde estilo WhatsApp */
        padding: 5px 8px;
        border-radius: 4px;
        margin-bottom: 6px;
        font-size: 12px;
        color: #ddd;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        display: flex;
        flex-direction: column;
    }
    .reply-author { font-weight: bold; color: #00bc8c; margin-bottom: 2px; }
    
    .chat-meta { font-size: 10px; opacity: 0.7; margin-top: 4px; display: block; text-align: right; }
    .tag-badge { padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-right: 4px; color:black; font-weight:bold; }
    .tag-spam { background-color: #ffcccc; } .tag-vip { background-color: #d4edda; } .tag-warn { background-color: #fff3cd; }
    </style>
    """, unsafe_allow_html=True)

    col_lista, col_chat = st.columns([1, 2.5])

    # --- SIDEBAR ---
    with col_lista:
        st.subheader("Bandeja")
        filtro = st.text_input("🔍 Buscar", placeholder="Teléfono o nombre")
        
        with st.expander("➕ Nuevo Chat"):
            num_manual = st.text_input("Número")
            if st.button("Ir") and num_manual:
                norm = normalizar_telefono_maestro(num_manual)
                if norm:
                    st.session_state['chat_actual_telefono'] = norm['db']
                    st.rerun()
                else: st.error("Inválido")

        q = """
            SELECT m.telefono, MAX(m.fecha) as f, 
            COALESCE(MAX(c.nombre_corto), m.telefono) as nom, MAX(c.etiquetas) as tags
            FROM mensajes m LEFT JOIN Clientes c ON m.telefono = c.telefono
            WHERE LENGTH(m.telefono) < 14 
        """
        if filtro: q += f" AND (m.telefono ILIKE '%%{filtro}%%' OR c.nombre_corto ILIKE '%%{filtro}%%')"
        q += " GROUP BY m.telefono ORDER BY f DESC LIMIT 20"
        
        with engine.connect() as conn:
            chats = conn.execute(text(q)).fetchall()

        for c in chats:
            tag_icon = "🚫" if "SPAM" in (c.tags or "") else "👤"
            tipo = "primary" if st.session_state['chat_actual_telefono'] == c.telefono else "secondary"
            if st.button(f"{tag_icon} {c.nom}", key=f"ch_{c.telefono}", type=tipo):
                st.session_state['chat_actual_telefono'] = c.telefono
                st.rerun()

    # --- CHAT ACTIVO ---
    with col_chat:
        tel_activo = st.session_state['chat_actual_telefono']
        if tel_activo:
            norm = normalizar_telefono_maestro(tel_activo)
            titulo = norm['corto'] if norm else tel_activo
            
            with engine.connect() as conn:
                cli = conn.execute(text("SELECT * FROM Clientes WHERE telefono=:t"), {"t": tel_activo}).fetchone()

            # Header
            c1, c2, c3 = st.columns([3, 0.5, 0.5])
            with c1: 
                st.markdown(f"### {titulo}")
                if cli and cli.etiquetas:
                    html_tags = ""
                    for tag in cli.etiquetas.split(','):
                        cls = "tag-spam" if "SPAM" in tag else "tag-vip" if "VIP" in tag else "tag-warn"
                        html_tags += f"<span class='tag-badge {cls}'>{tag}</span>"
                    st.markdown(html_tags, unsafe_allow_html=True)
            
            with c2:
                if st.button("🔄", help="Sincronizar"):
                    ok, msg = sincronizar_historial(tel_activo)
                    if ok: st.toast(msg); time.sleep(1); st.rerun()
                    else: st.error(msg)
            
            with c3:
                ver_ficha = st.toggle("ℹ️", False)

            st.divider()
            
            if ver_ficha: mostrar_info_avanzada(tel_activo)

            # --- LECTURA INTELIGENTE CON JOIN PARA CITAS ---
            # Hacemos LEFT JOIN consigo misma para traer el texto del mensaje original
            query_msgs = """
                SELECT 
                    m.*, 
                    orig.contenido as reply_texto,
                    orig.tipo as reply_tipo
                FROM mensajes m
                LEFT JOIN mensajes orig ON m.reply_to_id = orig.whatsapp_id
                WHERE m.telefono = :t 
                ORDER BY m.fecha ASC
            """

            with engine.connect() as conn:
                # Marcar leídos
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": tel_activo})
                conn.commit()
                msgs = pd.read_sql(text(query_msgs), conn, params={"t": tel_activo})

            cont = st.container(height=500)
            with cont:
                for _, m in msgs.iterrows():
                    cls = "outgoing" if m['tipo'] == 'SALIENTE' else "incoming"
                    body = m['contenido']
                    
                    # Generar HTML del archivo si existe
                    media_html = ""
                    if m['archivo_data']:
                        # Mostramos un placeholder o imagen si pudiéramos convertir bytes a base64 fácil aquí
                        # Por simplicidad usamos st.image abajo, pero para HTML puro:
                        body = "📄 [Archivo Adjunto]"

                    # --- HTML DE LA CITA (REPLY) ---
                    reply_html = ""
                    if m['reply_to_id'] and m['reply_texto']:
                        autor_reply = "Tú" if m['reply_tipo'] == 'SALIENTE' else "Cliente"
                        texto_corto = (m['reply_texto'][:50] + '...') if len(m['reply_texto']) > 50 else m['reply_texto']
                        reply_html = f"""
                            <div class="reply-context">
                                <span class="reply-author">{autor_reply}</span>
                                <span>{texto_corto}</span>
                            </div>
                        """
                    # -------------------------------

                    # Renderizado final
                    st.markdown(f"""
                        <div class='chat-bubble {cls}'>
                            {reply_html}
                            {body}
                            <span class='chat-meta'>{m['fecha'].strftime('%H:%M')}</span>
                        </div>
                    """, unsafe_allow_html=True)

                    # Si hay imagen, la mostramos debajo del texto (limitación de st.markdown con bytes)
                    if m['archivo_data']:
                        try: st.image(io.BytesIO(m['archivo_data']), width=200)
                        except: pass
                
                components.html("<script>var x=window.parent.document.querySelectorAll('.stChatMessage'); if(x.length>0)x[x.length-1].scrollIntoView();</script>", height=0)

            # Input
            with st.form("send_form", clear_on_submit=True):
                c_in, c_btn = st.columns([4, 1])
                txt = c_in.text_input("Mensaje", key="txt_in")
                adj = st.file_uploader("📎", label_visibility="collapsed")
                if c_btn.form_submit_button("🚀"):
                    if adj: enviar_archivo_chat(tel_activo, adj)
                    elif txt: enviar_texto_chat(tel_activo, txt)


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