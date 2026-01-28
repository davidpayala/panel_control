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
    crear_en_google
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
        # Pedimos 100 mensajes para asegurar que actualizamos historial reciente
        url = f"{WAHA_URL}/api/messages?chatId={chat_id_waha}&limit=100&downloadMedia=false"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            mensajes_waha = response.json()
            nuevos = 0
            actualizados = 0
            
            with engine.begin() as conn:
                for msg in mensajes_waha:
                    cuerpo = msg.get('body', '')
                    if not cuerpo: continue
                    
                    participant_check = msg.get('from')
                    if not normalizar_telefono_maestro(participant_check): continue 

                    es_mio = msg.get('fromMe', False)
                    tipo_msg = 'SALIENTE' if es_mio else 'ENTRANTE'
                    timestamp = msg.get('timestamp')
                    w_id = msg.get('id', None)
                    
                    # Captura de Reply
                    reply_id = msg.get('replyTo') 
                    if isinstance(reply_id, dict): reply_id = reply_id.get('id')

                    if w_id:
                        # 1. INTENTAR ACTUALIZAR PRIMERO (Por si ya existe pero le falta el reply)
                        res = conn.execute(text("""
                            UPDATE mensajes 
                            SET reply_to_id = :rid, contenido = :m 
                            WHERE whatsapp_id = :wid
                        """), {"rid": reply_id, "m": cuerpo, "wid": w_id})
                        
                        if res.rowcount > 0:
                            actualizados += 1
                        else:
                            # 2. SI NO ACTUALIZÓ, ENTONCES INSERTAMOS
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id, reply_to_id)
                                VALUES (:t, :tp, :m, to_timestamp(:ts), TRUE, :wid, :rid)
                            """), {
                                "t": target_db, "tp": tipo_msg, "m": cuerpo, 
                                "ts": timestamp, "wid": w_id, "rid": reply_id
                            })
                            nuevos += 1
                    else:
                        # Fallback simple
                        existe = conn.execute(text("SELECT count(*) FROM mensajes WHERE telefono=:t AND contenido=:m AND fecha > (NOW() - INTERVAL '24h')"), 
                                            {"t": target_db, "m": cuerpo}).scalar()
                        if existe == 0:
                            conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido) VALUES (:t, :tp, :m, NOW(), TRUE)"), 
                                        {"t": target_db, "tp": tipo_msg, "m": cuerpo})
                            nuevos += 1
            
            return True, f"Sync: {nuevos} nuevos, {actualizados} actualizados."
        
        elif response.status_code == 401: return False, "Error 401 API Key"
        else: return False, f"Error WAHA: {response.status_code}"
            
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

    # CSS OPTIMIZADO PARA CITAS
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; border-radius: 8px; margin-bottom: 2px; overflow: hidden; text-overflow: ellipsis; }
    
    .chat-bubble { 
        padding: 12px 16px; 
        border-radius: 12px; 
        margin-bottom: 8px; 
        max-width: 80%;
        color: white; 
        font-size: 15px; 
        position: relative;
        display: flex;
        flex-direction: column;
        line-height: 1.4;
    }
    
    .incoming { background-color: #262730; margin-right: auto; border-bottom-left-radius: 2px; }
    .outgoing { background-color: #004d40; margin-left: auto; border-bottom-right-radius: 2px; }
    
    .reply-context {
        background-color: rgba(0, 0, 0, 0.25);
        border-left: 4px solid #00e676;
        border-radius: 6px;
        padding: 8px 10px;
        margin-bottom: 8px;
        font-size: 0.85em;
        display: flex;
        flex-direction: column;
    }
    
    .reply-author { font-weight: bold; color: #00e676; margin-bottom: 2px; font-size: 0.9em; }
    .reply-text { color: #eeeeee; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; opacity: 0.9; }
    
    .chat-meta { font-size: 10px; opacity: 0.6; margin-top: 4px; align-self: flex-end; }
    
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

            # HEADER
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

            # LECTURA MENSAJES (CON CITA)
            query_msgs = """
                SELECT m.*, orig.contenido as reply_texto, orig.tipo as reply_tipo
                FROM mensajes m LEFT JOIN mensajes orig ON m.reply_to_id = orig.whatsapp_id
                WHERE m.telefono = :t ORDER BY m.fecha ASC
            """
            with engine.connect() as conn:
                conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": tel_activo})
                conn.commit()
                msgs = pd.read_sql(text(query_msgs), conn, params={"t": tel_activo})

            cont = st.container(height=500)
            with cont:
                for _, m in msgs.iterrows():
                    cls = "outgoing" if m['tipo'] == 'SALIENTE' else "incoming"
                    body = m['contenido']
                    
                    if m['archivo_data']: body = "📄 [Archivo Adjunto]"

                    reply_html = ""
                    if m['reply_to_id'] and m['reply_texto']:
                        autor = "Tú" if m['reply_tipo'] == 'SALIENTE' else "Cliente"
                        txt_r = (m['reply_texto'][:60] + '...') if len(m['reply_texto']) > 60 else m['reply_texto']
                        
                        # Sin indentación para evitar bug de código
                        reply_html = f"""<div class="reply-context"><span class="reply-author">{autor}</span><span class="reply-text">{txt_r}</span></div>"""

                    # Renderizado SIN INDENTACIÓN para arreglar el bug visual
                    st.markdown(f"""
<div class='chat-bubble {cls}'>
{reply_html}
<span>{body}</span>
<span class='chat-meta'>{m['fecha'].strftime('%H:%M')}</span>
</div>
""", unsafe_allow_html=True)
                    
                    if m['archivo_data']:
                        try: st.image(io.BytesIO(m['archivo_data']), width=200)
                        except: pass
                
                components.html("<script>var x=window.parent.document.querySelectorAll('.stChatMessage'); if(x.length>0)x[x.length-1].scrollIntoView();</script>", height=0)

            # INPUT
            with st.form("send_form", clear_on_submit=True):
                c_in, c_btn = st.columns([4, 1])
                txt = c_in.text_input("Mensaje", key="txt_in")
                adj = st.file_uploader("📎", label_visibility="collapsed")
                if c_btn.form_submit_button("🚀"):
                    if adj: enviar_archivo_chat(tel_activo, adj)
                    elif txt: enviar_texto_chat(tel_activo, txt)

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

    # EDICIÓN
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