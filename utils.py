import streamlit as st
import os
import requests
import urllib.parse
import time
import pandas as pd
import base64
from sqlalchemy import text
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# --- IMPORTANTE: Usamos la conexión centralizada ---
from database import engine

# ==============================================================================
# CONFIGURACIÓN WAHA (WhatsApp HTTP API)
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_SESSION = os.getenv("WAHA_SESSION", "default") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def agregar_al_carrito(sku, nombre, cantidad, precio, es_inventario, stock_max=None):
    # Nota: Esta función manipula st.session_state, asegúrate de que 'carrito' exista en app.py
    if 'carrito' not in st.session_state:
        st.session_state.carrito = []

    # Validar stock si es de inventario
    if es_inventario:
        cant_en_carrito = sum(item['cantidad'] for item in st.session_state.carrito if item['sku'] == sku)
        if (cant_en_carrito + cantidad) > stock_max:
            st.error(f"❌ No hay suficiente stock. Disponibles: {stock_max}, En carrito: {cant_en_carrito}")
            return

    st.session_state.carrito.append({
        "sku": sku,
        "descripcion": nombre,
        "cantidad": int(cantidad),
        "precio": float(precio),
        "subtotal": float(precio * cantidad),
        "es_inventario": es_inventario
    })
    st.success(f"Añadido: {nombre}")


# ==============================================================================
# LÓGICA WHATSAPP (WAHA)
# ==============================================================================

def formatear_numero_waha(numero):
    """WAHA espera formato internacional sin símbolos: 51999888777"""
    n = str(numero).replace("+", "").replace(" ", "").replace("-", "").strip()
    if len(n) == 9: # Caso Perú sin 51
        return f"51{n}"
    return n

# --- FUNCIÓN 1: ENVIAR TEXTO SIMPLE ---
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

# --- FUNCIÓN 2: PREPARAR ARCHIVO (REEMPLAZA A SUBIR_ARCHIVO_META) ---
def subir_archivo_meta(archivo_bytes, mime_type):
    """
    WAHA envía archivos en Base64 o URL. Aquí convertimos a Base64.
    Mantenemos el nombre 'subir_archivo_meta' para no romper tu código en compras.py
    """
    try:
        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{b64_data}"
        return data_uri, None
    except Exception as e:
        return None, str(e)

# --- FUNCIÓN 3: ENVIAR MULTIMEDIA ---
def enviar_mensaje_media(telefono, media_id, tipo_archivo, caption="", filename="archivo"):
    """
    'media_id' aquí es el Data URI (base64 completo) que generamos arriba.
    """
    if not WAHA_URL: return False, "Falta URL WAHA"

    number_clean = formatear_numero_waha(telefono)
    chat_id = f"{number_clean}@c.us"
    
    url = f"{WAHA_URL}/api/sendFile"
    
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    
    payload = {
        "session": WAHA_SESSION,
        "chatId": chat_id,
        "file": {
            "mimetype": tipo_archivo,
            "filename": filename,
            "data": media_id # Aquí va el string largo base64
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

# Función para descargar fotos (Placeholder para WAHA)
@st.cache_data(show_spinner=False)
def obtener_imagen_whatsapp(media_id):
    # WAHA maneja esto diferente, por ahora retornamos None para evitar errores
    return None


# ==============================================================================
# OTRAS FUNCIONES (FACEBOOK / ESTADOS / GOOGLE)
# ==============================================================================

def generar_feed_facebook():
    with engine.connect() as conn:
        query = text("""
            SELECT 
                v.sku as id, 
                p.marca || ' ' || p.modelo || ' ' || p.nombre || ' ' || COALESCE(v.nombre_variante, '') as title,
                'Lentes de contacto ' || p.marca || ' color ' || p.nombre || '. Disponibles en kmlentes.pe' as description,
                CASE WHEN (v.stock_interno + v.stock_externo) > 0 THEN 'in_stock' ELSE 'out_of_stock' END as availability,
                'new' as condition,
                v.precio || ' PEN' as price,
                'https://kmlentes.pe/?s=' || v.sku || '&post_type=product' as link,
                p.url_imagen as image_link,
                p.marca as brand,
                v.sku as mpn
            FROM Variantes v
            JOIN Productos p ON v.id_producto = p.id_producto
            WHERE p.url_imagen IS NOT NULL 
              AND p.url_imagen != ''
        """)
        df_feed = pd.read_sql(query, conn)

    if not os.path.exists('static'):
        os.makedirs('static')
        
    ruta_archivo = "static/feed_facebook.csv" 
    df_feed.to_csv(ruta_archivo, index=False)
    return len(df_feed)


def actualizar_estados(df_modificado):
    if df_modificado.empty: return

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for idx, row in df_modificado.iterrows():
                conn.execute(
                    text("UPDATE Clientes SET estado=:e, fecha_seguimiento=:f WHERE id_cliente=:id"),
                    {
                        "e": row['estado'], 
                        "f": row['fecha_seguimiento'], 
                        "id": row['id_cliente']
                    }
                )
            trans.commit()
            st.success("✅ Estados actualizados correctamente.")
            time.sleep(0.5)
            st.rerun()
        except Exception as e:
            trans.rollback()
            st.error(f"Error al actualizar: {e}")

# ==============================================================================
# LÓGICA GOOGLE
# ==============================================================================

def normalizar_celular(numero):
    if not numero: return ""
    solo_numeros = "".join(filter(str.isdigit, str(numero)))
    if len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        return solo_numeros[2:]
    if len(solo_numeros) > 9:
        return solo_numeros[-9:]
    return solo_numeros

def get_google_service():
    if not os.path.exists('token.json'):
        token_content = os.getenv("GOOGLE_TOKEN_JSON")
        if token_content:
            with open("token.json", "w") as f:
                f.write(token_content)
    
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/contacts'])
            return build('people', 'v1', credentials=creds)
        except Exception as e:
            return None
    return None

def sincronizar_desde_google_batch():
    service = get_google_service()
    if not service:
        st.error("No hay conexión con Google (token.json).")
        return

    with engine.connect() as conn:
        df_pendientes = pd.read_sql(text("""
            SELECT id_cliente, telefono, nombre_corto, google_id 
            FROM Clientes 
            WHERE (nombre IS NULL OR nombre = '') AND activo = TRUE
        """), conn)
        
        if df_pendientes.empty:
            st.success("¡Todos tus clientes ya tienen nombre y apellido!")
            return

        st.info(f"Sincronizando {len(df_pendientes)} contactos con Google...")
        progress_bar = st.progress(0)
        total = len(df_pendientes)
        actualizados = 0

        agenda_google = {}
        try:
            page_token = None
            while True:
                results = service.people().connections().list(
                    resourceName='people/me', pageSize=1000, personFields='names,phoneNumbers', pageToken=page_token
                ).execute()
                
                for person in results.get('connections', []):
                    phones = person.get('phoneNumbers', [])
                    names = person.get('names', [])
                    if phones and names:
                        g_nombre = names[0].get('givenName', '')
                        g_apellido = names[0].get('familyName', '')
                        g_id = person.get('resourceName')
                        for phone in phones:
                            clean_num = normalizar_celular(phone.get('value'))
                            if clean_num:
                                agenda_google[clean_num] = {'nombre': g_nombre, 'apellido': g_apellido, 'google_id': g_id}
                
                page_token = results.get('nextPageToken')
                if not page_token: break
        except Exception as e:
            st.error(f"Error descargando agenda de Google: {e}")
            return

        try:
            for idx, row in df_pendientes.iterrows():
                telefono_db = normalizar_celular(row['telefono'])
                if telefono_db in agenda_google:
                    datos_google = agenda_google[telefono_db]
                    conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"), 
                                {"n": datos_google['nombre'], "a": datos_google['apellido'], "gid": datos_google['google_id'], "id": row['id_cliente']})
                    actualizados += 1
                progress_bar.progress((idx + 1) / total)
            
            conn.commit() 
            st.success(f"✅ Éxito: Se actualizaron {actualizados} clientes desde Google.")
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"Error actualizando DB: {e}")

def buscar_contacto_google(telefono):
    service = get_google_service()
    if not service: return None
    tel_busqueda = normalizar_celular(telefono)
    if len(tel_busqueda) < 7: return None

    page_token = None
    while True:
        results = service.people().connections().list(
            resourceName='people/me', pageSize=1000, personFields='names,phoneNumbers', pageToken=page_token
        ).execute()
        for person in results.get('connections', []):
            for phone in person.get('phoneNumbers', []):
                if normalizar_celular(phone.get('value', '')) == tel_busqueda:
                    return {'resourceName': person.get('resourceName'), 'etag': person.get('etag')}
        page_token = results.get('nextPageToken')
        if not page_token: break
    return None

def crear_en_google(nombre, apellido, telefono):
    service = get_google_service()
    if not service: return None
    try:
        resultado = service.people().createContact(body={
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": telefono}]
        }).execute()
        return resultado.get('resourceName')
    except Exception as e:
        st.error(f"Error Google Create: {e}")
        return None

def actualizar_en_google(google_id, nombre, apellido, telefono):
    service = get_google_service()
    if not service: return False
    try:
        contacto = service.people().get(resourceName=google_id, personFields='names,phoneNumbers').execute()
        contacto['names'] = [{"givenName": nombre, "familyName": apellido}]
        contacto['phoneNumbers'] = [{"value": telefono}]
        service.people().updateContact(resourceName=google_id, updatePersonFields='names,phoneNumbers', body=contacto).execute()
        return True
    except Exception as e:
        return False