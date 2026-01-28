import streamlit as st
import os
import requests
import base64
import time
import pandas as pd
from sqlalchemy import text
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from database import engine
import re
from streamlit_autorefresh import st_autorefresh

# ==============================================================================
# CONFIGURACIÓN WAHA (WhatsApp)
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_SESSION = os.getenv("WAHA_SESSION", "default") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# 🏆 FUNCIÓN MAESTRA DE NORMALIZACIÓN (FILTROS WAHA ESTRICTOS)
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    """
    Recibe: String o Diccionario (Objeto Mensaje WAHA)
    Aplica las reglas de WAHA para filtrar IDs de sistema, grupos y canales.
    Retorna: Diccionario estandarizado o None si no es un usuario válido.
    """
    if not entrada: return None

    raw_id = ""

    # --- 1. EXTRACCIÓN DEL ID CRUDO ---
    if isinstance(entrada, dict):
        # Prioridad: Buscar el ID serializado completo para analizar el sufijo (@...)
        # WAHA suele tener: id._serialized, o from, o to
        
        # Si es un mensaje saliente (fromMe=True), el cliente es 'to'
        if entrada.get('fromMe', False):
            raw_id = entrada.get('to', '')
        else:
            # Si es entrante, el cliente es 'from'
            raw_id = entrada.get('from', '')
            
        # Si falló, buscamos en id.remote o participant
        if not raw_id:
            raw_id = entrada.get('id', {}).get('remote', '') or entrada.get('participant', '')

        # Fallback final: user
        if not raw_id:
            raw_id = str(entrada.get('user', ''))
    else:
        raw_id = str(entrada)

    # --- 2. FILTROS DE SEGURIDAD (SEGÚN DOCS WAHA) ---
    # Si detectamos estos sufijos, NO es un cliente real (CRM), es sistema/grupo.
    
    # ❌ Estados (Status)
    if 'status@broadcast' in raw_id: return None
    
    # ❌ Grupos (Groups)
    if '@g.us' in raw_id: return None
    
    # ❌ Canales (Channels / Newsletter)
    if '@newsletter' in raw_id: return None
    
    # ❌ LIDs (Hidden User IDs - No sirven para enviar mensajes normales)
    if '@lid' in raw_id: return None

    # ✅ Permitidos:
    # @c.us (Cuentas de usuario estándar)
    # @s.whatsapp.net (Formato antiguo/interno, compatible con @c.us)
    # Sin sufijo (Asumimos que es un número ingresado manualmente por el usuario)

    # --- 3. LIMPIEZA DEL FORMATO ---
    # Nos quedamos solo con la parte izquierda del @
    cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id

    # Quitamos caracteres no numéricos (+, espacios, guiones)
    solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
    
    if not solo_numeros: return None

    # --- 4. VALIDACIÓN DE LONGITUD (IMPORTANTE) ---
    # Un ID de canal o timestamp puede tener muchos dígitos.
    # Un teléfono real raramente pasa de 15 dígitos.
    if len(solo_numeros) > 15: return None  # Filtra basura como '24176488382510'
    if len(solo_numeros) < 7: return None   # Demasiado corto

    # --- 5. ESTANDARIZACIÓN (PERÚ) ---
    full = solo_numeros
    local = solo_numeros
    
    # Caso Perú sin código (9 dígitos) -> Agregar 51
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    # Caso Perú con código (11 dígitos empezando con 51)
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    return {
        "db": full,                  # ID único para BD
        "waha": f"{full}@c.us",      # ID para API WAHA (Siempre @c.us para enviar)
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}" if len(local)==9 else f"+{full}",
        "corto": local
    }
# ==============================================================================
# FUNCIONES DE ENVÍO
# ==============================================================================
def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL: return False, "⚠️ Falta WAHA_URL"
    norm = normalizar_telefono_maestro(numero)
    if not norm: return False, "❌ Número inválido o no es un usuario"
    
    url = f"{WAHA_URL}/api/sendText"
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY 
    
    payload = {"session": WAHA_SESSION, "chatId": norm['waha'], "text": texto}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code in [200, 201]: return True, r.json()
        return False, f"WAHA {r.status_code}: {r.text}"
    except Exception as e: return False, str(e)

def enviar_mensaje_media(telefono, archivo_bytes, mime_type, caption, filename):
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False, "Número inválido"

        media_b64 = base64.b64encode(archivo_bytes).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{media_b64}"

        url = f"{WAHA_URL}/api/sendImage"
        payload = {
            "session": WAHA_SESSION,
            "chatId": norm['waha'],
            "file": {
                "mimetype": mime_type,
                "filename": filename,
                "url": data_uri
            },
            "caption": caption
        }
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

        response = requests.post(url, json=payload, headers=headers, timeout=30)
        if response.status_code == 201: return True, response.json()
        return False, f"Error {response.status_code}: {response.text}"
    except Exception as e: return False, str(e)

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

# ==============================================================================
# LÓGICA GOOGLE (Contactos)
# ==============================================================================
def get_google_service():
    if not os.path.exists('token.json'):
        token_content = os.getenv("GOOGLE_TOKEN_JSON")
        if token_content:
            with open("token.json", "w") as f: f.write(token_content)
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/contacts'])
            return build('people', 'v1', credentials=creds)
        except: return None
    return None

# --- LA FUNCIÓN QUE CORREGÍ PARA QUE FUNCIONE EL WEBHOOK ---
def buscar_contacto_google(telefono_input):
    """
    Busca un contacto en Google probando TODOS los formatos posibles:
    1. 51999...
    2. +51999...
    3. 999...
    4. 999 999 999 (Formato con espacios)
    """
    srv = get_google_service()
    if not srv: return None
    
    norm = normalizar_telefono_maestro(telefono_input)
    if not norm: return None
    
    # Generamos el formato con espacios (Ej: 908 593 211)
    local = norm['corto']
    formato_espacios = f"{local[:3]} {local[3:6]} {local[6:]}" if len(local) == 9 else local
    
    intentos = [
        norm['db'],        # 51908593211
        f"+{norm['db']}",  # +51908593211
        norm['corto'],     # 908593211
        formato_espacios   # 908 593 211 <--- NUEVO INTENTO CLAVE
    ]
    
    # Eliminar duplicados manteniendo orden
    intentos = list(dict.fromkeys(intentos))
    print(f"🔎 Buscando en Google variantes: {intentos}")

    for query in intentos:
        try:
            res = srv.people().searchContacts(
                query=query, 
                readMask='names,phoneNumbers,metadata'
            ).execute()
            
            if 'results' in res and len(res['results']) > 0:
                person = res['results'][0]['person']
                google_id = person.get('resourceName', '').replace('people/', '')
                names = person.get('names', [])
                
                if names:
                    nombre = names[0].get('givenName', '')
                    apellido = names[0].get('familyName', '')
                    nombre_completo = names[0].get('displayName', '')
                else:
                    nombre = "Google Contact"
                    apellido = ""
                    nombre_completo = "Google Contact"
                
                return {
                    "encontrado": True,
                    "nombre": nombre,
                    "apellido": apellido,
                    "nombre_completo": nombre_completo,
                    "google_id": google_id
                }
        except Exception as e:
            continue 
            
    return None

def crear_en_google(nombre, apellido, telefono):
    srv = get_google_service()
    if not srv: return None
    norm = normalizar_telefono_maestro(telefono)
    tel_google = norm['google'] if norm else telefono

    try:
        res = srv.people().createContact(body={
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": tel_google}]
        }).execute()
        return res.get('resourceName')
    except: return None

def actualizar_en_google(gid, nombre, apellido, telefono):
    srv = get_google_service()
    if not srv: return False
    norm = normalizar_telefono_maestro(telefono)
    tel_google = norm['google'] if norm else telefono

    try:
        c = srv.people().get(resourceName=gid, personFields='names,phoneNumbers').execute()
        c['names'] = [{"givenName": nombre, "familyName": apellido}]
        c['phoneNumbers'] = [{"value": tel_google}]
        srv.people().updateContact(resourceName=gid, updatePersonFields='names,phoneNumbers', body=c).execute()
        return True
    except: return False

def sincronizar_desde_google_batch():
    service = get_google_service()
    if not service:
        st.error("No hay conexión con Google.")
        return
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT id_cliente, telefono FROM Clientes WHERE (nombre IS NULL OR nombre = '') AND activo = TRUE"), conn)
        if df.empty: return
        
        st.info(f"Sincronizando {len(df)} contactos...")
        agenda = {}
        try:
            page = None
            while True:
                res = service.people().connections().list(resourceName='people/me', pageSize=1000, personFields='names,phoneNumbers', pageToken=page).execute()
                for p in res.get('connections', []):
                    phones = p.get('phoneNumbers', [])
                    names = p.get('names', [])
                    if phones and names:
                        for ph in phones:
                            norm = normalizar_telefono_maestro(ph.get('value'))
                            if norm:
                                agenda[norm['db']] = {'n': names[0].get('givenName',''), 'a': names[0].get('familyName',''), 'gid': p.get('resourceName')}
                page = res.get('nextPageToken')
                if not page: break
        except: pass

        for idx, row in df.iterrows():
            norm_cliente = normalizar_telefono_maestro(row['telefono'])
            if norm_cliente and norm_cliente['db'] in agenda:
                d = agenda[norm_cliente['db']]
                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"),
                             {"n": d['n'], "a": d['a'], "gid": d['gid'], "id": row['id_cliente']})
        conn.commit()
        st.success("✅ Sincronización completada.")
        time.sleep(1)
        st.rerun()

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================
def agregar_al_carrito(sku, nombre, cantidad, precio, es_inventario, stock_max=None):
    if 'carrito' not in st.session_state: st.session_state.carrito = []
    if es_inventario:
        cant_en_carrito = sum(item['cantidad'] for item in st.session_state.carrito if item['sku'] == sku)
        if (cant_en_carrito + cantidad) > stock_max:
            st.error(f"❌ Stock insuficiente. Disp: {stock_max}")
            return
    st.session_state.carrito.append({
        "sku": sku, "descripcion": nombre, "cantidad": int(cantidad),
        "precio": float(precio), "subtotal": float(precio * cantidad), "es_inventario": es_inventario
    })
    st.success(f"Añadido: {nombre}")

def generar_feed_facebook():
    with engine.connect() as conn:
        query = text("""
            SELECT v.sku as id, p.marca || ' ' || p.modelo || ' ' || p.nombre as title,
            'Lentes de contacto ' || p.marca as description,
            CASE WHEN (v.stock_interno + v.stock_externo) > 0 THEN 'in_stock' ELSE 'out_of_stock' END as availability,
            'new' as condition, v.precio || ' PEN' as price,
            'https://kmlentes.pe/?s=' || v.sku as link, p.url_imagen as image_link, p.marca as brand
            FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
            WHERE p.url_imagen IS NOT NULL AND p.url_imagen != ''
        """)
        df_feed = pd.read_sql(query, conn)
    if not os.path.exists('static'): os.makedirs('static')
    df_feed.to_csv("static/feed_facebook.csv", index=False)
    return len(df_feed)

def actualizar_estados(df_modificado):
    if df_modificado.empty: return
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for idx, row in df_modificado.iterrows():
                conn.execute(text("UPDATE Clientes SET estado=:e, fecha_seguimiento=:f WHERE id_cliente=:id"),
                    {"e": row['estado'], "f": row['fecha_seguimiento'], "id": row['id_cliente']})
            trans.commit()
            st.success("✅ Estados actualizados.")
            time.sleep(0.5)
            st.rerun()
        except: trans.rollback()

# ... (manten tus imports anteriores de requests, os, json, etc)

def verificar_numero_waha(telefono):
    """
    Consulta a WAHA si el número tiene cuenta de WhatsApp.
    Retorna: True (Existe), False (No existe), None (Error al consultar)
    """
    try:
        url = f"{WAHA_URL}/api/contacts/check-exists"
        payload = {"phone": f"{telefono}@c.us"}
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            # WAHA puede devolver {"exists": true} o similar
            return data.get("exists", False)
        return None # Error de API, mejor no asumir que no existe
    except Exception as e:
        print(f"⚠️ Error verificando número: {e}")
        return None
    
# ... (al final del archivo utils.py) ...

def generar_nombre_ia(alias, nombre_real):
    """
    Analiza el alias y el nombre real para extraer un Primer Nombre limpio.
    Retorna "" si parece ser un nombre de empresa o inválido.
    """
    # 1. Palabras que indican que NO es un nombre de pila válido
    PALABRAS_PROHIBIDAS = [
        'CLIENTE', 'LENTES', 'MAYOR', 'MAYORISTA', 'POR MAYOR', 'OPTICA', 'VENTA', 
        'TIENDA', 'ALMACEN', 'CONTACTO', 'PROSPECTO', 'DR', 'DRA', 'SR', 'SRA', 
        'MISS', 'MR', 'DON', 'DOÑA', 'AGENCIA', 'ENVIOS', 'PEDIDOS', 'INFO', 
        'ADMIN', 'GENERAL', 'GRUPO', 'LISTA', 'SPAM', 'ESTAFA', 'NO CONTESTA', 
        'DOBLE', 'TRIPLE', 'PACK', 'PROMO', 'OFERTA', 'CONSULTA', 'NULL', 'NONE',
        'FACEBOOK', 'INSTAGRAM', 'TIKTOK', 'WEB', 'K&M'
    ]

    def es_nombre_valido(palabra):
        if not palabra: return False
        p = palabra.upper()
        # Filtros
        if len(p) <= 3: return False # Nombres muy cortos como "Al", "X", "Yo" suelen ser basura
        if not p.isalpha(): return False # Tiene numeros o simbolos
        if p in PALABRAS_PROHIBIDAS: return False
        return True

    # 2. Prioridad: Primero intentamos con el Nombre Real (Google), luego con el Alias
    candidatos = [nombre_real, alias]
    
    for texto in candidatos:
        if not texto: continue
        
        # Limpieza básica
        limpio = str(texto).replace('-', ' ').replace('_', ' ').replace('.', ' ').strip()
        palabras = limpio.split()
        
        if not palabras: continue
        
        primer_palabra = palabras[0]
        
        # Validación
        if es_nombre_valido(primer_palabra):
            return primer_palabra.capitalize() # ¡Encontramos un nombre!
            
    return "" # No se encontró nada que parezca un nombre humano