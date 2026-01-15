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
# FUNCIONES DE ENVÍO (WAHA)
# ==============================================================================

def formatear_numero_waha(numero):
    """WAHA espera formato internacional sin símbolos: 51999888777"""
    n = str(numero).replace("+", "").replace(" ", "").replace("-", "").strip()
    if len(n) == 9: # Caso Perú sin 51
        return f"51{n}"
    return n

# --- 1. ENVIAR TEXTO ---
def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL:
        return False, "⚠️ Falta WAHA_URL en .env"

    number_clean = formatear_numero_waha(numero)
    chat_id = f"{number_clean}@c.us"
    
    url = f"{WAHA_URL}/api/sendText"
    
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY 
    
    payload = {
        "session": WAHA_SESSION,
        "chatId": chat_id,
        "text": texto
    }

    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 201 or r.status_code == 200:
            return True, r.json()
        else:
            return False, f"Error WAHA {r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)

# --- 2. PREPARAR ARCHIVO ---
def subir_archivo_meta(archivo_bytes, mime_type):
    # En WAHA Free (NOWEB), el envío de archivos está limitado.
    # Dejamos esto preparado por si usas WEBJS o Plus en el futuro.
    try:
        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{b64_data}"
        return data_uri, None
    except Exception as e:
        return None, str(e)

# --- 3. ENVIAR MULTIMEDIA ---
def enviar_mensaje_media(telefono, media_id, tipo_archivo, caption="", filename="archivo"):
    if not WAHA_URL: return False, "Falta URL WAHA"

    number_clean = formatear_numero_waha(telefono)
    chat_id = f"{number_clean}@c.us"
    
    # Nota: Esto fallará en el motor NOWEB gratis (Error 422).
    # Funcionará si cambias a WEBJS o pagas Plus.
    url = f"{WAHA_URL}/api/sendFile"
    
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    
    payload = {
        "session": WAHA_SESSION,
        "chatId": chat_id,
        "file": {
            "mimetype": tipo_archivo,
            "filename": filename,
            "data": media_id # Base64
        },
        "caption": caption
    }

    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 201 or r.status_code == 200:
            return True, r.json()
        else:
            return False, f"Error WAHA: {r.text}"
    except Exception as e:
        return False, str(e)

@st.cache_data(show_spinner=False)
def obtener_imagen_whatsapp(media_id):
    return None

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

# ==============================================================================
# LÓGICA GOOGLE (Contactos) - CORREGIDA
# ==============================================================================

def normalizar_celular(numero):
    if not numero: return ""
    solo_numeros = "".join(filter(str.isdigit, str(numero)))
    if len(solo_numeros) == 11 and solo_numeros.startswith("51"): return solo_numeros[2:]
    return solo_numeros[-9:] if len(solo_numeros) > 9 else solo_numeros

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
                            clean = normalizar_celular(ph.get('value'))
                            if clean: agenda[clean] = {'n': names[0].get('givenName',''), 'a': names[0].get('familyName',''), 'gid': p.get('resourceName')}
                page = res.get('nextPageToken')
                if not page: break
        except: pass

        for idx, row in df.iterrows():
            tel = normalizar_celular(row['telefono'])
            if tel in agenda:
                d = agenda[tel]
                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"),
                             {"n": d['n'], "a": d['a'], "gid": d['gid'], "id": row['id_cliente']})
        conn.commit()
        st.success("✅ Sincronización completada.")
        time.sleep(1)
        st.rerun()

def crear_en_google(nombre, apellido, telefono):
    srv = get_google_service()
    if not srv: return None
    try:
        res = srv.people().createContact(body={"names":[{"givenName":nombre,"familyName":apellido}],"phoneNumbers":[{"value":telefono}]}).execute()
        return res.get('resourceName')
    except: return None

def actualizar_en_google(gid, nombre, apellido, telefono):
    srv = get_google_service()
    if not srv: return False
    try:
        c = srv.people().get(resourceName=gid, personFields='names,phoneNumbers').execute()
        c['names'] = [{"givenName": nombre, "familyName": apellido}]
        c['phoneNumbers'] = [{"value": telefono}]
        srv.people().updateContact(resourceName=gid, updatePersonFields='names,phoneNumbers', body=c).execute()
        return True
    except: return False

def buscar_contacto_google(telefono):
    service = get_google_service()
    if not service: return None
    
    tel_busqueda = normalizar_celular(telefono)
    if len(tel_busqueda) < 7: return None

    try:
        page_token = None
        while True:
            results = service.people().connections().list(
                resourceName='people/me', 
                pageSize=1000, 
                personFields='phoneNumbers', 
                pageToken=page_token
            ).execute()
            
            for person in results.get('connections', []):
                for phone in person.get('phoneNumbers', []):
                    if normalizar_celular(phone.get('value', '')) == tel_busqueda:
                        return {
                            'resourceName': person.get('resourceName'), 
                            'etag': person.get('etag')
                        }
            
            page_token = results.get('nextPageToken')
            if not page_token: break
    except Exception as e:
        return None
        
    return None