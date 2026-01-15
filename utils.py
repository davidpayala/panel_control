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

# ==============================================================================
# CONFIGURACIÓN WAHA (WhatsApp)
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_SESSION = os.getenv("WAHA_SESSION", "default") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# 🏆 FUNCIÓN MAESTRA DE NORMALIZACIÓN
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    """
    Recibe cualquier cosa (+51 999 999, 999999, 51999..., objetos WAHA)
    Retorna un diccionario con todos los formatos estandarizados.
    """
    if not entrada: return None

    # 1. Limpieza brutal: Solo dejar números
    sucio = str(entrada)
    if isinstance(entrada, dict):
        sucio = str(entrada.get('user') or entrada.get('_serialized') or "")
    
    # Quitar todo lo que no sea número
    solo_numeros = "".join(filter(str.isdigit, sucio))
    
    if not solo_numeros: return None
    
    # 2. Lógica Perú (Detectar 9 dígitos)
    full = solo_numeros
    local = solo_numeros
    
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    # 3. Retornar paquete con formatos listos
    return {
        "db": full,                  # 51986203398
        "waha": f"{full}@c.us",      # 51986203398@c.us
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}", # +51 986 203 398
        "corto": local               # 986203398
    }

# ==============================================================================
# FUNCIONES DE ENVÍO (WAHA)
# ==============================================================================
def formatear_numero_waha(numero):
    norm = normalizar_telefono_maestro(numero)
    if norm: return norm['db']
    return str(numero)

def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL: return False, "⚠️ Falta WAHA_URL en .env"
    number_clean = formatear_numero_waha(numero)
    chat_id = f"{number_clean}@c.us"
    
    url = f"{WAHA_URL}/api/sendText"
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY 
    
    payload = {"session": WAHA_SESSION, "chatId": chat_id, "text": texto}
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code in [200, 201]: return True, r.json()
        return False, f"Error WAHA {r.status_code}: {r.text}"
    except Exception as e: return False, str(e)

def subir_archivo_meta(archivo_bytes, mime_type):
    try:
        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        return f"data:{mime_type};base64,{b64_data}", None
    except Exception as e: return None, str(e)

def enviar_mensaje_media(telefono, media_uri, tipo_archivo, caption="", filename="archivo"):
    if not WAHA_URL: return False, "Falta URL WAHA"
    number_clean = formatear_numero_waha(telefono)
    chat_id = f"{number_clean}@c.us"
    
    url = f"{WAHA_URL}/api/sendFile"
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    
    payload = {
        "session": WAHA_SESSION, "chatId": chat_id,
        "file": {"mimetype": tipo_archivo, "filename": filename, "data": media_uri},
        "caption": caption
    }
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code in [200, 201]: return True, r.json()
        return False, f"Error WAHA: {r.text}"
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

# --- LA FUNCIÓN QUE FALTABA ---
def buscar_contacto_google(query):
    """Busca un contacto en Google por nombre o teléfono"""
    srv = get_google_service()
    if not srv: return None
    try:
        # Intenta buscar usando la API searchContacts
        res = srv.people().searchContacts(query=query, readMask='names,phoneNumbers').execute()
        if 'results' in res and len(res['results']) > 0:
            return res['results'][0]['person']
    except:
        pass # Si falla la búsqueda, retornamos None
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