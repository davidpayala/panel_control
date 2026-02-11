import streamlit as st
import pandas as pd
from sqlalchemy import text
import time
import os
import threading
import base64
import zipfile
import io
import requests
from datetime import datetime, timedelta
from database import engine 

# --- CONFIGURACIÓN ---
WAHA_URL = os.getenv("WAHA_URL")
WAHA_KEY = os.getenv("WAHA_KEY")

try:
    from utils import marcar_chat_como_leido_waha as marcar_leido_waha 
except ImportError:
    def marcar_leido_waha(*args): pass

def marcar_leido_api(telefono, sesion):
    if not WAHA_URL: return
    url = f"{WAHA_URL.rstrip('/')}/api/sendSeen"
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    payload = {"session": sesion, "chatId": f"{telefono}@c.us"}
    try: requests.post(url, json=payload, headers=headers, timeout=5)
    except: pass

def mandar_mensaje_api(telefono, texto, sesion):
    if not WAHA_URL: return False, "Falta WAHA_URL"
    url = f"{WAHA_URL.rstrip('/')}/api/sendText"
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    payload = {"session": sesion, "chatId": f"{telefono}@c.us", "text": texto}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if r.status_code in [200, 201]: return True, ""
        return False, r.text
    except Exception as e:
        return False, str(e)

def get_table_name(conn):
    try:
        conn.execute(text("SELECT 1 FROM clientes LIMIT 1"))
        return "clientes"
    except:
        return "\"Clientes\""

# ==========================================
# 🌟 MAGIA MULTIMEDIA
# ==========================================
def generar_html_media(archivo_bytes):
    if not archivo_bytes: return ""
    try:
        b = bytes(archivo_bytes)
        b64 = base64.b64encode(b).decode('utf-8')
        mime, ext, nombre_archivo = 'application/octet-stream', 'bin', 'Documento'

        if b.startswith(b'\xff\xd8'): mime, ext = 'image/jpeg', 'jpg'
        elif b.startswith(b'\x89PNG'): mime, ext = 'image/png', 'png'
        elif b'WEBP' in b[:50]: mime, ext = 'image/webp', 'webp'
        elif b.startswith(b'OggS'): mime, ext = 'audio/ogg', 'ogg'
        elif b'ftyp' in b[:20]: mime, ext = 'video/mp4', 'mp4'
        elif b.startswith(b'%PDF'): mime, ext = 'application/pdf', 'pdf'
        elif b.startswith(b'GIF8'): mime, ext = 'image/gif', 'gif'
        elif b.startswith(b'ID3') or b.startswith(b'\xff\xfb'): mime, ext = 'audio/mpeg', 'mp3'
        elif b.startswith(b'PK\x03\x04'): 
            try:
                with zipfile.ZipFile(io.BytesIO(b)) as z:
                    json_filename = next((name for name in z.namelist() if name.endswith('.json')), None)
                    if json_filename:
                        json_data = z.read(json_filename).decode('utf-8')
                        json_b64 = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
                        lottie_html = f"""<!DOCTYPE html><html><head><script src="https://unpkg.com/@lottiefiles/lottie-player@latest/dist/lottie-player.js"></script></head><body style="margin:0;overflow:hidden;background:transparent;"><lottie-player src="data:application/json;base64,{json_b64}" background="transparent" speed="1" style="width:150px;height:150px;" loop autoplay></lottie-player></body></html>"""
                        lottie_html_b64 = base64.b64encode(lottie_html.encode('utf-8')).decode('utf-8')
                        return f"<iframe src='data:text/html;base64,{lottie_html_b64}' width='150' height='150' frameborder='0' scrolling='no' allowtransparency='true' style='margin-bottom: 5px; pointer-events: none;'></iframe>"
            except Exception: pass 
            mime, ext = 'application/zip', 'zip'

        if mime.startswith('image/'): return f"<img src='data:{mime};base64,{b64}' style='max-width: 200px; max-height: 200px; border-radius: 8px; margin-bottom: 5px; object-fit: contain; background: transparent;' />"
        elif mime.startswith('audio/'): return f"<audio controls style='max-width: 250px; height: 40px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></audio>"
        elif mime.startswith('video/'): return f"<video controls style='max-width: 250px; border-radius: 8px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></video>"
        else:
            icono_texto = "🎞️ Sticker Animado (Descargar ZIP)" if ext == 'zip' else "📄 Descargar Archivo"
            return f"<a href='data:{mime};base64,{b64}' download='{nombre_archivo}.{ext}' style='display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.05); padding: 10px; border-radius: 8px; text-decoration: none; color: inherit; font-size: 13px; font-weight: bold; margin-bottom: 5px; border: 1px solid rgba(0,0,0,0.1);'>{icono_texto}</a>"
    except Exception: return "<div style='color: red; font-size: 11px;'>Error cargando archivo</div>"

# ==========================================
# 🕵️ VIGÍA INVISIBLE
# ==========================================
try:
    run_poller = st.fragment(run_every=3) 
except AttributeError:
    run_poller = lambda f: f

@run_poller
def poller_cambios_db():
    st.markdown("<div style='display:none;'>vigia_activo</div>", unsafe_allow_html=True)
    try:
        with engine.connect() as conn: 
            conn.commit()
            version_actual = conn.execute(text("SELECT version FROM sync_estado WHERE id = 1")).scalar() or 0
            
            if 'db_version' not in st.session_state:
                st.session_state['db_version'] = version_actual
            elif st.session_state['db_version'] != version_actual:
                st.session_state['db_version'] = version_actual
                st.rerun()
    except Exception:
        pass

# ==========================================
# VISTA PRINCIPAL
# ==========================================
def render_chat():
    st.title("💬 Chat Center")

    poller_cambios_db()

    def cambiar_chat(telefono):
        st.session_state['chat_actual_telefono'] = telefono

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']
    col_lista, col_chat = st.columns([35, 65])

    # --- BANDEJA AGRUPADA ---
    with col_lista:
        st.subheader("Bandeja")
        if st.button("🔄 Forzar Recarga", use_container_width=True):
            st.rerun()
        st.divider()

        try:
            with engine.connect() as conn:
                tabla = get_table_name(conn)
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                query = f"""
                    SELECT c.telefono, COALESCE(c.nombre_corto, c.telefono) as nombre, c.whatsapp_internal_id, c.estado,
                           (SELECT COUNT(*) FROM mensajes m WHERE m.telefono = c.telefono AND m.leido = FALSE AND m.tipo = 'ENTRANTE') as no_leidos,
                           (SELECT MAX(fecha) FROM mensajes m WHERE m.telefono = c.telefono) as ultima_interaccion
                    FROM {tabla} c
                    WHERE c.activo = TRUE
                """
                
                if busqueda:
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    filtro = f" AND (COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    if busqueda_limpia: filtro += f" OR c.telefono ILIKE '%{busqueda_limpia}%')"
                    else: filtro += f" OR c.telefono ILIKE '%{busqueda}%')"
                    query += filtro
                
                query += " ORDER BY no_leidos DESC, ultima_interaccion DESC NULLS LAST, c.fecha_registro DESC LIMIT 150"
                df_clientes = pd.read_sql(text(query), conn)

            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No se encontraron chats.")
                else:
                    # --- 🚀 TU CONFIGURACIÓN DE ETAPAS ---
                    cat_map = {
                        "ETAPA_2": ["Venta motorizado", "Venta agencia", "Venta express moto"],
                        "ETAPA_1": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
                        "ETAPA_3": ["En camino moto", "En camino agencia", "Contraentrega agencia"],
                        "ETAPA_4": ["Pendiente agradecer", "Problema post"],
                        "ETAPA_0": ["Sin empezar"]
                    }
                    
                    def asignar_categoria(estado):
                        # Si es nulo o vacío -> ETAPA_0 (al final)
                        if not estado or str(estado).strip() == "":
                            return "ETAPA_0"
                        
                        # Buscamos coincidencias exactas
                        for cat, estados in cat_map.items():
                            if estado in estados: return cat
                        
                        # Si tiene un estado raro que no está en la lista -> ETAPA_0
                        return "ETAPA_0"
                        
                    df_clientes['categoria'] = df_clientes['estado'].apply(asignar_categoria)
                    
                    # 🚀 EL ORDEN EXACTO QUE PEDISTE
                    orden_categorias = ["ETAPA_2", "ETAPA_1", "ETAPA_3", "ETAPA_4", "ETAPA_0"]

                    for cat in orden_categorias:
                        df_cat = df_clientes[df_clientes['categoria'] == cat]
                        if not df_cat.empty:
                            no_leidos_cat = int(df_cat['no_leidos'].sum())
                            badge = f" :red-background[**{no_leidos_cat}**]" if no_leidos_cat > 0 else ""
                            
                            chat_activo_aqui = telefono_actual in df_cat['telefono'].values
                            # Se expande si hay mensajes nuevos, si el chat actual está aquí, o si es la ETAPA_2 (Ventas)
                            expandido = (no_leidos_cat > 0) or chat_activo_aqui or (cat == "ETAPA_2")
                            
                            with st.expander(f"{cat} ({len(df_cat)}){badge}", expanded=expandido):
                                for _, row in df_cat.iterrows():
                                    t_row = row['telefono']
                                    c_leidos = row['no_leidos']
                                    
                                    icono = "🔴" if c_leidos > 0 else "👤"
                                    texto_leidos = f" **({c_leidos})**" if c_leidos > 0 else ""
                                    label = f"{icono} {row['nombre']}{texto_leidos}"
                                    
                                    tipo = "primary" if telefono_actual == t_row else "secondary"
                                    
                                    if st.button(label, key=f"c_{t_row}", use_container_width=True, type=tipo, on_click=cambiar_chat, args=(t_row,)):
                                        pass 

        except Exception as e:
            st.error("Reconectando lista...")

    # --- CHAT ---
    with col_chat:
        if not telefono_actual:
            st.info("👈 Selecciona un chat de la izquierda.")
        else:
            try:
                with engine.connect() as conn:
                    unreads_query = conn.execute(text("SELECT COUNT(*), MAX(session_name) FROM mensajes WHERE telefono=:t AND tipo='ENTRANTE' AND leido=FALSE"), {"t": telefono_actual}).fetchone()
                    if unreads_query and unreads_query[0] > 0:
                        sesion_unread = unreads_query[1] if unreads_query[1] else 'default'
                        conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
                        conn.commit()
                        try: threading.Thread(target=marcar_leido_api, args=(telefono_actual, sesion_unread)).start()
                        except: pass

                with engine.connect() as conn:
                    tabla = get_table_name(conn)
                    info = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
                    nombre = info.nombre_corto if info and info.nombre_corto else telefono_actual
                    wsp_id = info.whatsapp_internal_id if hasattr(info, 'whatsapp_internal_id') else "Desconocido"
                    # Estado actual para mostrarlo en el header
                    estado_actual_cliente = info.estado if hasattr(info, 'estado') and info.estado else "Sin estado"
                    
                    msgs = pd.read_sql(text("""
                        SELECT * FROM (
                            SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha DESC LIMIT 100
                        ) sub ORDER BY fecha ASC
                    """), conn, params={"t": telefono_actual})

                st.subheader(f"👤 {nombre}")
                # Mostramos el estado actual en la cabecera
                st.caption(f"📱 {telefono_actual} | 🆔 {wsp_id} | 🏷️ **{estado_actual_cliente}**")
                
                if msgs.empty: 
                    st.caption("Inicio de la conversación.")
                else:
                    html_blocks = []
                    ultima_fecha = None
                    hoy = datetime.now().date()
                    ayer = hoy - timedelta(days=1)

                    for _, m in msgs.iterrows():
                        try: fecha_msg = m['fecha'].date() if pd.notna(m['fecha']) else None
                        except: fecha_msg = None

                        if fecha_msg and fecha_msg != ultima_fecha:
                            if fecha_msg == hoy: texto_fecha = "Hoy"
                            elif fecha_msg == ayer: texto_fecha = "Ayer"
                            else: texto_fecha = fecha_msg.strftime("%d/%m/%Y")
                            
                            html_blocks.append(f"<div class='date-separator'><span>{texto_fecha}</span></div>")
                            ultima_fecha = fecha_msg

                        es_mio = (m['tipo'] == 'SALIENTE')
                        clase_row = "msg-mio" if es_mio else "msg-otro"
                        clase_bub = "b-mio" if es_mio else "b-otro"
                        hora = m['fecha'].strftime("%H:%M") if pd.notna(m['fecha']) else ""
                        
                        icono_estado = ""
                        if es_mio:
                            estado = m.get('estado_waha', 'pendiente')
                            if estado == 'leido': icono_estado = "<span class='check-read'>✓✓</span>"
                            elif estado == 'recibido': icono_estado = "<span class='check-sent'>✓✓</span>"
                            elif estado == 'enviado': icono_estado = "<span class='check-sent'>✓</span>"
                            else: icono_estado = "🕒"

                        etiqueta_sess = ""
                        if 'session_name' in m and pd.notna(m['session_name']):
                            s_name = str(m['session_name']).strip().lower()
                            if s_name == 'principal': etiqueta_sess = "<span class='session-tag'>KM</span>"
                            elif s_name == 'default': etiqueta_sess = "<span class='session-tag'>LENTES</span>"

                        reply_html = ""
                        if pd.notna(m.get('reply_content')) and str(m['reply_content']).strip() != "":
                            reply_html = f"<div class='reply-box'>↪️ {str(m['reply_content'])}</div>"

                        media_html = generar_html_media(m.get('archivo_data'))
                        
                        contenido_str = str(m['contenido']) if pd.notna(m['contenido']) else ""
                        if contenido_str in ["📷 Archivo Multimedia", "📷 Archivo", "📷 Archivo (Recuperado)"] and media_html:
                            contenido_str = ""
                            
                        texto_html = f"<div style='white-space: pre-wrap;'>{contenido_str}</div>" if contenido_str.strip() else ""

                        html_msg = f"<div class='msg-row {clase_row}'><div class='bubble {clase_bub}'>{reply_html}{media_html}{texto_html}<div class='meta'>{hora} {icono_estado}{etiqueta_sess}</div></div></div>"
                        html_blocks.append(html_msg)

                    html_blocks.reverse()

                    css_y_html = f"""<style>
.chat-container {{ display: flex; flex-direction: column-reverse; height: 550px; overflow-y: auto; padding: 10px; border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 10px; background-image: url('https://user-images.githubusercontent.com/15075759/28719144-86dc0f70-73b1-11e7-911d-60d70fcded21.png'); background-color: transparent; }}
.msg-row {{ display: flex; margin-bottom: 5px; }}
.msg-mio {{ justify-content: flex-end; }}
.msg-otro {{ justify-content: flex-start; }}
.bubble {{ padding: 8px 12px; border-radius: 10px; font-size: 15px; max-width: 80%; display: flex; flex-direction: column; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
.b-mio {{ background-color: #dcf8c6; color: black; border-top-right-radius: 0; }}
.b-otro {{ background-color: #ffffff; color: black; border-top-left-radius: 0; }}
.meta {{ font-size: 10px; color: #777; text-align: right; margin-top: 3px; display: inline-block; }}
.check-read {{ color: #34B7F1; font-weight: bold; font-size: 12px; }}
.check-sent {{ color: #999; font-size: 12px; }}
.reply-box {{ background-color: rgba(0, 0, 0, 0.05); border-left: 4px solid #34B7F1; padding: 6px 8px; border-radius: 4px; font-size: 13px; margin-bottom: 6px; color: #555; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; text-overflow: ellipsis; }}
.b-mio .reply-box {{ border-left-color: #075E54; background-color: rgba(0, 0, 0, 0.08); }}
.date-separator {{ display: flex; justify-content: center; margin: 15px 0; }}
.date-separator span {{ background-color: #e1f3fb; color: #555; padding: 4px 12px; border-radius: 10px; font-size: 12px; font-weight: bold; box-shadow: 0 1px 0.5px rgba(0,0,0,0.13); }}
.session-tag {{ margin-left: 6px; padding: 1px 4px; border-radius: 4px; font-size: 9px; font-weight: 800; color: #666; background-color: rgba(0,0,0,0.06); }}
.b-mio .session-tag {{ background-color: rgba(0,0,0,0.08); color: #444; }}
</style><div class='chat-container'>{''.join(html_blocks)}</div>"""
                    st.markdown(css_y_html, unsafe_allow_html=True)

                # --- LÓGICA DE SELECCIÓN DE SESIÓN ---
                ultima_sesion = None
                if not msgs.empty and 'session_name' in msgs.columns:
                    sesiones_validas = msgs['session_name'].dropna().astype(str).str.strip().str.lower()
                    sesiones_validas = sesiones_validas[sesiones_validas != ""]
                    if not sesiones_validas.empty:
                        ultima_sesion = sesiones_validas.iloc[-1]

                idx_sesion = 0
                if ultima_sesion == 'default': idx_sesion = 1

                st.write("") 
                c_sel, c_warn = st.columns([30, 70])
                with c_sel:
                    sesion_elegida = st.selectbox(
                        "Línea de envío:", 
                        options=["principal", "default"], 
                        index=idx_sesion,
                        format_func=lambda x: "📱 KM (Principal)" if x == "principal" else "👓 LENTES (Default)",
                        key=f"sess_{telefono_actual}",
                        label_visibility="collapsed"
                    )
                with c_warn:
                    if ultima_sesion and ultima_sesion != sesion_elegida:
                        nombre_ult = "KM" if ultima_sesion == 'principal' else "LENTES"
                        st.markdown(f"<div style='color: #856404; background-color: #fff3cd; border: 1px solid #ffeeba; padding: 6px 10px; border-radius: 5px; font-size: 13px; font-weight: bold;'>⚠️ OJO: El último mensaje fue en {nombre_ult}.</div>", unsafe_allow_html=True)

                txt = st.chat_input("Escribe un mensaje...")
                
                if txt:
                    ok, res = mandar_mensaje_api(telefono_actual, txt, sesion_elegida)
                    if ok:
                        st.rerun()
                    else:
                        st.error(f"Error al enviar: {res}")

            except Exception as e:
                st.error("Error en el chat")