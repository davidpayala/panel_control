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