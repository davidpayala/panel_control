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
    crear_en_google, sincronizar_historial
)

OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente", 
    "✅ Compró", "👀 Prospecto", "❓ Preguntón", 
    "📉 Pide Rebaja", "📦 Mayorista"
]

def render_chat():
    if st.sidebar.button("🔄 Sincronizar Historial Completo"):
        with st.spinner("Trayendo mensajes antiguos de WAHA..."):
            msg = sincronizar_historial()
        st.success(msg)
        time.sleep(2)
        st.rerun()

    st_autorefresh(interval=10000, key="chat_autorefresh")
    st.title("💬 Chat Center")

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    # CSS (Sin indentación extraña)
    st.markdown("""
    <style>
    div.stButton > button:first-child { text-align: left; width: 100%; border-radius: 8px; margin-bottom: 2px; overflow: hidden; text-overflow: ellipsis; }
    .chat-bubble { padding: 10px 15px; border-radius: 12px; margin-bottom: 8px; max-width: 80%; color: white; font-size: 15px; position: relative; display: flex; flex-direction: column;}
    .incoming { background-color: #262730; margin-right: auto; border-bottom-left-radius: 2px; }
    .outgoing { background-color: #004d40; margin-left: auto; border-bottom-right-radius: 2px; }
    .reply-context { background-color: rgba(0,0,0,0.25); border-left: 4px solid #00e676; padding: 6px 8px; border-radius: 4px; margin-bottom: 6px; font-size: 0.85em; display: flex; flex-direction: column; }
    .reply-author { font-weight: bold; color: #00e676; margin-bottom: 2px; font-size: 0.9em; }
    .reply-text { color: #eeeeee; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; opacity: 0.9; }
    .chat-meta { font-size: 10px; opacity: 0.7; margin-top: 4px; align-self: flex-end; }
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
            WHERE LENGTH(m.telefono) < 16 
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

            # LECTURA MENSAJES
            query_msgs = """
                SELECT m.*, 
                       orig.contenido as reply_texto_join, 
                       orig.tipo as reply_tipo
                FROM mensajes m 
                LEFT JOIN mensajes orig ON m.reply_to_id = orig.whatsapp_id
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

                    # HTML COMPACTO PARA EL REPLY
                    reply_html = ""
                    texto_cita = m['reply_content'] if 'reply_content' in m and m['reply_content'] else m['reply_texto_join']

                    if m['reply_to_id'] and texto_cita:
                        autor = "Respuesta"
                        if m['reply_tipo']: autor = "Tú" if m['reply_tipo'] == 'SALIENTE' else "Cliente"
                        txt_r = (texto_cita[:60] + '...') if len(texto_cita) > 60 else texto_cita
                        # Sin sangría ni saltos de línea para evitar bug
                        reply_html = f'<div class="reply-context"><span class="reply-author">{autor}</span><span class="reply-text">{txt_r}</span></div>'

                    # HTML FINAL EN UNA SOLA LÍNEA
                    html_burbuja = f"<div class='chat-bubble {cls}'>{reply_html}<span>{body}</span><span class='chat-meta'>{m['fecha'].strftime('%H:%M')}</span></div>"
                    
                    st.markdown(html_burbuja, unsafe_allow_html=True)
                    
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

    # --- ZONA DE DIAGNÓSTICO (AÑADIR AL FINAL DE RENDER_CHAT) ---
    with st.expander("🛠️ DIAGNÓSTICO DB (Ver todos los mensajes)"):
        with engine.connect() as conn:
            # Mostramos los últimos 10 mensajes tal cual están en la base de datos
            raw_msgs = pd.read_sql(text("SELECT id_mensaje, telefono, contenido, whatsapp_id, fecha FROM mensajes ORDER BY fecha DESC LIMIT 10"), conn)
            st.dataframe(raw_msgs)
            
            # Contar clientes
            count = conn.execute(text("SELECT count(*) FROM Clientes")).scalar()
            st.write(f"Total Clientes en DB: {count}")
            
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