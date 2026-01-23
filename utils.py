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
        # Intenta sacar el numero si viene dentro de un objeto de Waha
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
        "db": full,                  # 51986203398 (Para ID SQL)
        "waha": f"{full}@c.us",      # 51986203398@c.us (Para enviar mensajes)
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}", # +51 986 203 398 (Display)
        "corto": local               # 986203398 (Nombre corto)
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

def enviar_mensaje_media(telefono, archivo_bytes, mime_type, caption, filename):
    """
    Envía una imagen/archivo usando WAHA Plus (Método Base64 directo).
    No requiere subir el archivo a un servidor intermedio.
    """
    try:
        # 1. Convertir el archivo a Base64
        media_b64 = base64.b64encode(archivo_bytes).decode('utf-8')
        
        # 2. Construir la Data URI (Ej: data:image/jpeg;base64,.....)
        data_uri = f"data:{mime_type};base64,{media_b64}"

        url = f"{WAHA_URL}/api/sendImage" # O /api/sendFile si es PDF
        
        # Si no es imagen, usamos el endpoint genérico de archivos
        if "image" not in mime_type:
            url = f"{WAHA_URL}/api/sendFile"

        payload = {
            "chatId": f"{telefono}@c.us",
            "file": {
                "mimetype": mime_type,
                "filename": filename,
                "url": data_uri # <--- AQUÍ ESTÁ EL TRUCO
            },
            "caption": caption,
            "session": "default" # Cambia esto si usas múltiples sesiones
        }

        headers = {
            "Content-Type": "application/json",
            "X-Api-Key": WAHA_KEY
        }

        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        if response.status_code == 201:
            return True, response.json()
        else:
            return False, f"Error {response.status_code}: {response.text}"

    except Exception as e:
        return False, str(e)

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