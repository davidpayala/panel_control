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
    from utils import normalizar_telefono_maestro 
except ImportError:
    def marcar_leido_waha(*args): pass
    def normalizar_telefono_maestro(t): return {"db": "".join(filter(str.isdigit, str(t)))}

# ==========================================
# 📡 RESOLUTOR API PARA LIDs
# ==========================================
def resolver_telefono_api(lid, session):
    if not WAHA_URL or not lid: return None
    try:
        lid_safe = lid.replace('@', '%40')
        url = f"{WAHA_URL.rstrip('/')}/api/{session}/lids/{lid_safe}"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            pn = data.get('pn')
            if pn:
                return pn.split('@')[0]
    except: pass
    return None

def mandar_mensaje_api(telefono, texto, sesion):
    if not WAHA_URL: return False, "Falta WAHA_URL"
    try:
        res_norm = normalizar_telefono_maestro(telefono)
        if isinstance(res_norm, dict):
            telefono_final = res_norm.get('db') 
        else:
            telefono_final = str(res_norm) if res_norm else "".join(filter(str.isdigit, str(telefono)))
            
        if not telefono_final: return False, "Número inválido"

        url = f"{WAHA_URL.rstrip('/')}/api/sendText"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        payload = {"session": sesion, "chatId": f"{telefono_final}@c.us", "text": texto}
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
    except Exception: pass

def render_boton_chat(row, cat, telefono_actual, cambiar_chat_func):
    t_row = row['telefono']
    c_leidos = row['no_leidos']
    icono = "🔴" if c_leidos > 0 else "👤"
    texto_leidos = f" **({c_leidos})**" if c_leidos > 0 else ""
    
    # Mostrar el estado real si está en la bandeja de "Nuevos" o "Otros"
    extra = f" [{row['estado']}]" if cat in ["📁 Otros Estados", "🔴 Mensajes Nuevos"] else ""
    
    label = f"{icono} {row['nombre']}{extra}{texto_leidos}"
    tipo = "primary" if telefono_actual == t_row else "secondary"
    st.button(label, key=f"c_{t_row}", use_container_width=True, type=tipo, on_click=cambiar_chat_func, args=(t_row,))

# ==========================================
# VISTA PRINCIPAL
# ==========================================
def render_chat():
    c_tit, c_time = st.columns([80, 20])
    c_tit.title("💬 Chat Center")
    
    lima_time = datetime.utcnow() - timedelta(hours=5)
    c_time.caption(f"🔄 {lima_time.strftime('%H:%M:%S')}")

    poller_cambios_db()

    def cambiar_chat(telefono):
        st.session_state['chat_actual_telefono'] = telefono

    if 'chat_actual_telefono' not in st.session_state:
        st.session_state['chat_actual_telefono'] = None

    telefono_actual = st.session_state['chat_actual_telefono']
    col_lista, col_chat = st.columns([35, 65])

    # --- BANDEJA DE ENTRADA ---
    with col_lista:
        c_h1, c_h2 = st.columns([85, 15])
        with c_h1:
            st.subheader("Bandeja")
        with c_h2:
            with st.expander("🧹"):
                if st.button("✅ Confirmar", help="Marcar TODO como leído", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE leido = FALSE AND tipo = 'ENTRANTE'"))
                    st.rerun()

        with st.expander("➕ Iniciar Nuevo Chat"):
            with st.form("form_nuevo_chat", clear_on_submit=True):
                nuevo_numero = st.text_input("Número (con código de país, ej: 51999888777):")
                nueva_sesion = st.selectbox(
                    "Línea de envío:", 
                    options=["principal", "default"], 
                    format_func=lambda x: "📱 KM (Principal)" if x == "principal" else "👓 LENTES (Default)"
                )
                nuevo_mensaje = st.text_area("Mensaje inicial:")
                btn_enviar_nuevo = st.form_submit_button("Enviar Mensaje", type="primary", use_container_width=True)

                if btn_enviar_nuevo:
                    if not nuevo_numero.strip() or not nuevo_mensaje.strip():
                        st.error("Ingresa el número y el mensaje.")
                    else:
                        ok, res = mandar_mensaje_api(nuevo_numero, nuevo_mensaje, nueva_sesion)
                        if ok:
                            # Normalizar para establecerlo como chat activo
                            res_norm = normalizar_telefono_maestro(nuevo_numero)
                            num_final = res_norm.get('db') if isinstance(res_norm, dict) else str(res_norm)
                            if not num_final:
                                num_final = "".join(filter(str.isdigit, str(nuevo_numero)))
                                
                            st.session_state['chat_actual_telefono'] = num_final
                            st.success("Enviado. Cargando chat...")
                            time.sleep(1.5) # Pausa breve para dar tiempo al webhook de registrarlo en la BD
                            st.rerun()
                        else:
                            st.error(f"Error al enviar: {res}")

        try:
            with engine.connect() as conn:
                conn.commit() 
                tabla = get_table_name(conn)
                busqueda = st.text_input("🔍 Buscar:", placeholder="Nombre o teléfono...")
                
                query = f"""
                    SELECT c.telefono, COALESCE(c.nombre_corto, c.telefono) as nombre, c.whatsapp_internal_id, c.estado,
                           COALESCE(SUM(CASE WHEN m.leido = FALSE AND m.tipo = 'ENTRANTE' THEN 1 ELSE 0 END), 0) as no_leidos,
                           MAX(m.fecha) as ultima_interaccion
                    FROM {tabla} c
                    LEFT JOIN mensajes m ON c.telefono = m.telefono
                    WHERE c.activo = TRUE
                """
                
                if busqueda:
                    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
                    filtro = f" AND (COALESCE(c.nombre_corto,'') ILIKE '%{busqueda}%'"
                    if busqueda_limpia: filtro += f" OR c.telefono ILIKE '%{busqueda_limpia}%')"
                    else: filtro += f" OR c.telefono ILIKE '%{busqueda}%')"
                    query += filtro
                
                query += " GROUP BY c.telefono, c.nombre_corto, c.whatsapp_internal_id, c.estado ORDER BY no_leidos DESC, ultima_interaccion DESC NULLS LAST"
                df_clientes = pd.read_sql(text(query), conn)

            with st.container(height=600):
                if df_clientes.empty:
                    st.info("No se encontraron chats.")
                else:
                    cat_map = {
                        "💰 Venta realizada": ["Venta motorizado", "Venta agencia", "Venta express moto"],
                        "🗣️ Conversación": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
                        "🚚 En camino": ["En camino moto", "En camino agencia", "Contraentrega agencia"],
                        "🛡️ Post-Venta": ["Pendiente agradecer", "Problema post"],
                        "🆕 Sin empezar": ["Sin empezar"]
                    }
                    
                    # 🚀 LÓGICA DE PRIORIDAD: Si no está leído, va al grupo de Nuevos
                    def asignar_categoria(row):
                        if row['no_leidos'] > 0: return "🔴 Mensajes Nuevos"
                        estado = row['estado']
                        if not estado or str(estado).strip() == "": return "🆕 Sin empezar"
                        for cat, estados in cat_map.items():
                            if estado in estados: return cat
                        return "📁 Otros Estados"
                        
                    df_clientes['categoria'] = df_clientes.apply(asignar_categoria, axis=1)
                    
                    # ORDEN MODIFICADO: Prioridad alta arriba, Sin empezar al fondo
                    orden_categorias = [
                        "🔴 Mensajes Nuevos",
                        "💰 Venta realizada", 
                        "🗣️ Conversación", 
                        "🚚 En camino", 
                        "🛡️ Post-Venta", 
                        "📁 Otros Estados",
                        "🆕 Sin empezar"
                    ]

                    for cat in orden_categorias:
                        df_cat = df_clientes[df_clientes['categoria'] == cat]
                        if not df_cat.empty:
                            
                            if cat == "🆕 Sin empezar":
                                df_cat = df_cat.head(30)
                                
                            no_leidos_cat = int(df_cat['no_leidos'].sum())
                            badge = f" :red-background[**{no_leidos_cat}**]" if no_leidos_cat > 0 else ""
                            chat_activo_aqui = telefono_actual in df_cat['telefono'].values
                            
                            # Expandido automático si son nuevos o si estoy viendo este chat
                            expandido = (cat == "🔴 Mensajes Nuevos") or chat_activo_aqui or (cat == "💰 Venta realizada")
                            
                            with st.expander(f"{cat} ({len(df_cat)}){badge}", expanded=expandido):
                                if cat == "🗣️ Conversación":
                                    sub_estados_ordenados = ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"]
                                    for sub in sub_estados_ordenados:
                                        df_sub = df_cat[df_cat['estado'] == sub]
                                        if not df_sub.empty:
                                            st.markdown(f"<div style='font-size: 11px; color: #777; margin-top: 10px; margin-bottom: 2px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;'>📌 {sub}</div>", unsafe_allow_html=True)
                                            for _, row in df_sub.iterrows():
                                                render_boton_chat(row, cat, telefono_actual, cambiar_chat)
                                else:
                                    for _, row in df_cat.iterrows():
                                        render_boton_chat(row, cat, telefono_actual, cambiar_chat)

        except Exception as e:
            st.error(f"Error cargando lista: {e}")

    # --- CHAT ---
    with col_chat:
        if not telefono_actual:
            st.info("👈 Selecciona un chat.")
        else:
            try:
                # AUTO-RESOLUCIÓN LIDs (Se mantiene)
                if telefono_actual.startswith("LID_"):
                    with st.spinner("🕵️‍♂️ Consultando número real en WAHA..."):
                        with engine.connect() as conn:
                            info = conn.execute(text(f"SELECT whatsapp_internal_id FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
                            if info and info.whatsapp_internal_id and info.whatsapp_internal_id.endswith("@lid"):
                                num_real = resolver_telefono_api(info.whatsapp_internal_id, "default")
                                if not num_real: num_real = resolver_telefono_api(info.whatsapp_internal_id, "principal")
                                
                                if num_real:
                                    norm = normalizar_telefono_maestro(num_real)
                                    real_db = norm.get('db') if isinstance(norm, dict) else norm
                                    if real_db:
                                        with engine.begin() as t_conn:
                                            existente = t_conn.execute(text(f"SELECT id_cliente FROM {tabla} WHERE telefono=:t"), {"t": real_db}).fetchone()
                                            if existente:
                                                t_conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o"), {"n": real_db, "o": telefono_actual})
                                                t_conn.execute(text(f"UPDATE {tabla} SET estado='Duplicado', activo=FALSE, whatsapp_internal_id=:fake WHERE telefono=:o"), {"fake": f"MERGED_{telefono_actual}", "o": telefono_actual})
                                                t_conn.execute(text(f"UPDATE {tabla} SET whatsapp_internal_id=:lid WHERE telefono=:n"), {"lid": info.whatsapp_internal_id, "n": real_db})
                                            else:
                                                t_conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o"), {"n": real_db, "o": telefono_actual})
                                                t_conn.execute(text(f"UPDATE {tabla} SET telefono=:n WHERE telefono=:o"), {"n": real_db, "o": telefono_actual})
                                        st.session_state['chat_actual_telefono'] = real_db
                                        st.rerun()

                # Marcar leido
                with engine.connect() as conn:
                    conn.commit() 
                    unreads_query = conn.execute(text("SELECT COUNT(*), MAX(session_name) FROM mensajes WHERE telefono=:t AND tipo='ENTRANTE' AND leido=FALSE"), {"t": telefono_actual}).fetchone()
                    if unreads_query and unreads_query[0] > 0:
                        sesion_unread = unreads_query[1] if unreads_query[1] else 'default'
                        conn.execute(text("UPDATE mensajes SET leido=TRUE WHERE telefono=:t AND tipo='ENTRANTE'"), {"t": telefono_actual})
                        conn.commit()
                        try: threading.Thread(target=marcar_leido_api, args=(telefono_actual, sesion_unread)).start()
                        except: pass

                # Cargar datos
                with engine.connect() as conn:
                    conn.commit() 
                    info = conn.execute(text(f"SELECT * FROM {tabla} WHERE telefono=:t"), {"t": telefono_actual}).fetchone()
                    nombre = info.nombre_corto if info and info.nombre_corto else telefono_actual
                    estado_actual_cliente = info.estado if hasattr(info, 'estado') and info.estado else "Sin empezar"
                    
                    msgs = pd.read_sql(text("""
                        SELECT * FROM (
                            SELECT * FROM mensajes WHERE telefono=:t ORDER BY fecha DESC LIMIT 100
                        ) sub ORDER BY fecha ASC
                    """), conn, params={"t": telefono_actual})

                # --- HEADER ---
                st.subheader(f"👤 {nombre}")
                c_head_1, c_head_2 = st.columns([40, 60])
                with c_head_1: st.caption(f"📱 {telefono_actual}")
                with c_head_2:
                    OPCIONES_ESTADO = [
                        "Sin empezar",
                        "Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional",
                        "Venta motorizado", "Venta agencia", "Venta express moto",
                        "En camino moto", "En camino agencia", "Contraentrega agencia",
                        "Pendiente agradecer", "Problema post"
                    ]
                    try: idx_estado = OPCIONES_ESTADO.index(estado_actual_cliente)
                    except: idx_estado = 0
                    
                    nuevo_estado = st.selectbox("Estado del Cliente:", OPCIONES_ESTADO, index=idx_estado, key=f"st_{telefono_actual}", label_visibility="collapsed")
                    if nuevo_estado != estado_actual_cliente:
                        with engine.begin() as conn:
                            conn.execute(text(f"UPDATE {tabla} SET estado = :e WHERE telefono = :t"), {"e": nuevo_estado, "t": telefono_actual})
                        st.rerun()

                # =========================================
                # 🚀 MOTOR DE RENDERIZADO UNIFICADO Y OPTIMIZADO
                # =========================================
                html_blocks = []
                imagenes_galeria = []
                
                if not msgs.empty:
                    ultima_fecha = None
                    ahora_lima = datetime.utcnow() - timedelta(hours=5)
                    hoy = ahora_lima.date()
                    ayer = hoy - timedelta(days=1)

                    # Bucle Único (Procesa HTML y Galería a la vez)
                    for _, m in msgs.iterrows():
                        # --- FECHAS ---
                        try: fecha_msg = m['fecha'].date() if pd.notna(m['fecha']) else None
                        except: fecha_msg = None

                        if fecha_msg and fecha_msg != ultima_fecha:
                            if fecha_msg == hoy: texto_fecha = "Hoy"
                            elif fecha_msg == ayer: texto_fecha = "Ayer"
                            else: texto_fecha = fecha_msg.strftime("%d/%m/%Y")
                            html_blocks.append(f"<div class='date-separator'><span>{texto_fecha}</span></div>")
                            ultima_fecha = fecha_msg

                        # --- CONFIG BURBUJA ---
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

                        # --- PROCESAMIENTO MULTIMEDIA OPTIMIZADO ---
                        media_html = ""
                        raw_data = m.get('archivo_data')
                        if raw_data is not None and not pd.isna(raw_data):
                            try:
                                b = bytes(raw_data)
                                if b:
                                    b64 = base64.b64encode(b).decode('utf-8')
                                    mime, ext, nombre_archivo = 'application/octet-stream', 'bin', 'Documento'

                                    if b.startswith(b'\xff\xd8'): mime, ext = 'image/jpeg', 'jpg'
                                    elif b.startswith(b'\x89PNG'): mime, ext = 'image/png', 'png'
                                    elif b'WEBP' in b[:50]: mime, ext = 'image/webp', 'webp'
                                    elif b.startswith(b'OggS'): mime, ext = 'audio/ogg', 'ogg'
                                    elif b'ftyp' in b[:20]: mime, ext = 'video/mp4', 'mp4'
                                    elif b.startswith(b'%PDF'): mime, ext = 'application/pdf', 'pdf'

                                    # Si es imagen -> Va a la galería Y al chat
                                    if mime.startswith('image/'):
                                        media_html = f"<img src='data:{mime};base64,{b64}' style='max-width: 200px; max-height: 200px; border-radius: 8px; margin-bottom: 5px; object-fit: contain; background: transparent; cursor: default;' />"
                                        fecha_corta = m['fecha'].strftime("%d/%m %H:%M") if pd.notna(m['fecha']) else ""
                                        imagenes_galeria.append({"bytes": b, "caption": fecha_corta})
                                    # Otros formatos
                                    elif mime.startswith('audio/'): media_html = f"<audio controls style='max-width: 250px; height: 40px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></audio>"
                                    elif mime.startswith('video/'): media_html = f"<video controls style='max-width: 250px; border-radius: 8px; margin-bottom: 5px;'><source src='data:{mime};base64,{b64}' type='{mime}'></video>"
                                    else:
                                        media_html = f"<a href='data:{mime};base64,{b64}' download='{nombre_archivo}.{ext}' style='display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.05); padding: 10px; border-radius: 8px; text-decoration: none; color: inherit; font-size: 13px; font-weight: bold; margin-bottom: 5px; border: 1px solid rgba(0,0,0,0.1);'>📄 Descargar Archivo</a>"
                            except:
                                media_html = "<div style='color: gray; font-size: 10px;'>Archivo corrupto</div>"

                        contenido_str = str(m['contenido']) if pd.notna(m['contenido']) else ""
                        if contenido_str in ["📷 Archivo Multimedia", "📷 Archivo", "📷 Archivo (Recuperado)"] and media_html:
                            contenido_str = ""
                        texto_html = f"<div style='white-space: pre-wrap;'>{contenido_str}</div>" if contenido_str.strip() else ""
                        
                        html_msg = f"<div class='msg-row {clase_row}'><div class='bubble {clase_bub}'>{reply_html}{media_html}{texto_html}<div class='meta'>{hora} {icono_estado}{etiqueta_sess}</div></div></div>"
                        html_blocks.append(html_msg)

                    html_blocks.reverse() # Invertir para CSS flex-reverse

                # --- RENDERIZAR GALERÍA ---
                if imagenes_galeria:
                    with st.expander(f"🖼️ Galería de Imágenes ({len(imagenes_galeria)})"):
                        st.caption("Haz click en las flechas de la imagen para ver en pantalla completa.")
                        cols = st.columns(4)
                        for i, img in enumerate(reversed(imagenes_galeria)):
                            with cols[i % 4]:
                                st.image(img['bytes'], caption=img['caption'], use_container_width=True)
                
                st.divider()

                # --- RENDERIZAR CHAT ---
                if not msgs.empty:
                    css_y_html = f"""<style>
.chat-container {{ display: flex; flex-direction: column-reverse; height: 500px; overflow-y: auto; padding: 10px; border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 10px; background-image: url('https://user-images.githubusercontent.com/15075759/28719144-86dc0f70-73b1-11e7-911d-60d70fcded21.png'); background-color: transparent; }}
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
                else:
                    st.caption("Inicio de la conversación.")

                # --- INPUT DE ESCRITURA ---
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
                st.error(f"Error detallado en el chat: {str(e)}")