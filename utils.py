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
# CONFIGURACIÓN WAHA
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_SESSION = os.getenv("WAHA_SESSION", "default") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# 🏆 NORMALIZACIÓN BLINDADA
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    """
    Convierte cualquier formato de teléfono a un estándar DB (solo números).
    Maneja diccionarios de WAHA, cadenas con +51, etc.
    """
    if not entrada: return None
    raw_id = ""

    # 1. Extraer el ID si es un objeto (payload de WAHA)
    if isinstance(entrada, dict):
        if entrada.get('fromMe', False):
            raw_id = entrada.get('to', '')
        else:
            raw_id = entrada.get('from', '')
            
        if not raw_id:
            raw_id = entrada.get('id', {}).get('remote', '') or entrada.get('participant', '')
        if not raw_id:
            raw_id = str(entrada.get('user', ''))
    else:
        raw_id = str(entrada)

    # 2. Filtros de seguridad (ignorar grupos, estados, newsletters)
    if 'status@broadcast' in raw_id: return None
    if '@g.us' in raw_id: return None
    if '@newsletter' in raw_id: return None
    if '@lid' in raw_id: return None

    # 3. Limpieza de caracteres
    cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id
    solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
    
    if not solo_numeros: return None
    
    # 4. Validaciones de longitud básica
    if len(solo_numeros) > 15: return None
    if len(solo_numeros) < 7: return None

    full = solo_numeros
    local = solo_numeros
    
    # 5. Lógica específica para Perú (ajustar según país si es necesario)
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    return {
        "db": full,           # Para Base de Datos (ej: 51999888777)
        "waha": f"{full}@c.us", # Para API WAHA
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}" if len(local)==9 else f"+{full}",
        "corto": local
    }


def get_headers():
    headers = {"Content-Type": "application/json"}
    if WAHA_KEY:
        headers["X-Api-Key"] = WAHA_KEY
    return headers

def marcar_chat_como_leido_waha(chat_id):
    """Envía la orden a WhatsApp para poner los ticks azules"""
    try:
        url = f"{WAHA_URL}/api/{WAHA_SESSION}/chats/{chat_id}/messages/read"
        requests.post(url, headers=get_headers(), json={})
    except Exception:
        pass


# ==============================================================================
# PROCESAMIENTO DE MENSAJES (SYNC)
# ==============================================================================
def map_ack_status(ack_value):
    """Convierte el código numérico de ACK de WhatsApp a texto"""
    s_ack = str(ack_value).upper()
    if s_ack in ['3', '4', 'READ', 'PLAYED']: return 'leido'     # Azul
    if s_ack in ['2', 'RECEIVED', 'DELIVERED']: return 'recibido' # Gris doble
    if s_ack in ['1', 'SENT']: return 'enviado'  # Gris simple
    return 'pendiente'

def procesar_mensaje_sync(conn, msg, telefono_db):
    try:
        wid = msg.get('id')
        from_me = msg.get('fromMe', False)
        
        # --- EXTRACCIÓN ROBUSTA DE CONTENIDO ---
        body = msg.get('body', '')
        # Si el body está vacío, buscar en _data (común en mensajes multimedia o forwards)
        if not body and '_data' in msg:
            data = msg['_data']
            if 'caption' in data: body = data['caption']
            elif 'message' in data:
                m = data['message']
                if 'conversation' in m: body = m['conversation']
                elif 'extendedTextMessage' in m: body = m['extendedTextMessage'].get('text', '')
                elif 'imageMessage' in m: body = m['imageMessage'].get('caption', '📷 [Imagen]')
        
        # --- FIX TIMESTAMP ---
        timestamp = msg.get('timestamp')
        if timestamp and float(timestamp) > 9999999999:
             timestamp = float(timestamp) / 1000.0

        # --- ESTADO Y TIPO ---
        ack_raw = msg.get('ack', 0)
        estado_waha = map_ack_status(ack_raw) if from_me else None
        tipo = 'SALIENTE' if from_me else 'ENTRANTE'
        
        # 1. Crear Cliente
        conn.execute(text("""
            INSERT INTO clientes (telefono, nombre_corto, estado, activo, fecha_registro)
            VALUES (:t, 'Whatsapp Sync', 'Sin empezar', TRUE, NOW())
            ON CONFLICT (telefono) DO NOTHING
        """), {"t": telefono_db})

        # 2. Insertar Mensaje (Evitar duplicados)
        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:w"), {"w": wid}).scalar()
        
        if not existe:
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id, estado_waha)
                VALUES (:t, :tipo, :c, to_timestamp(:ts), :l, :wid, :est)
            """), {
                "t": telefono_db,
                "tipo": tipo,
                "c": body or "[Archivo adjunto]",
                "ts": timestamp,
                "l": True, 
                "wid": wid,
                "est": estado_waha
            })
        else:
            # Actualizar estado (ticks) si es mensaje mío
            if from_me:
                conn.execute(text("UPDATE mensajes SET estado_waha = :est WHERE whatsapp_id = :wid"), 
                             {"est": estado_waha, "wid": wid})
                
    except Exception as e:
        print(f"Error procesando msg {msg.get('id')}: {e}")

# ==============================================================================
# ENVÍO Y MEDIA
# ==============================================================================
def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL: return False, "Falta WAHA_URL"
    norm = normalizar_telefono_maestro(numero)
    if not norm: return False, "Número inválido"
    
    url = f"{WAHA_URL}/api/sendText"
    payload = {"session": WAHA_SESSION, "chatId": norm['waha'], "text": texto}
    try:
        r = requests.post(url, headers=get_headers(), json=payload, timeout=10)
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
        response = requests.post(url, json=payload, headers=get_headers(), timeout=30)
        if response.status_code == 201: return True, response.json()
        return False, f"Error {response.status_code}: {response.text}"
    except Exception as e: return False, str(e)

def subir_archivo_meta(archivo_bytes, mime_type):
    # Función auxiliar para compatibilidad
    try:
        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        return f"data:{mime_type};base64,{b64_data}", None
    except Exception as e: return None, str(e)

# ==============================================================================
# SINCRONIZACIÓN CORREGIDA (SESSION FIX)
# ==============================================================================
def sincronizar_historial(limit=100):
    try:
        # Usamos la sesión configurada (probablemente 'principal')
        url_chats = f"{WAHA_URL}/api/{WAHA_SESSION}/chats"
        params_chats = {"limit": 50, "sortBy": "messageTimestamp"}
        
        r = requests.get(url_chats, headers=get_headers(), params=params_chats)
        if r.status_code != 200:
            return f"Error: WAHA respondió {r.status_code}. ¿La sesión '{WAHA_SESSION}' existe?"
        
        chats = r.json()
        total_msgs = 0
        
        with engine.connect() as conn:
            for chat in chats:
                chat_id = chat.get('id', '')
                norm = normalizar_telefono_maestro(chat_id)
                if not norm: continue
                
                telefono_db = norm['db']
                
                # Descarga de mensajes
                url_msgs = f"{WAHA_URL}/api/{WAHA_SESSION}/chats/{chat_id}/messages"
                params_msgs = {
                    "limit": limit,
                    "downloadMedia": "false" # False para hacerlo rápido primero
                }
                
                r_msgs = requests.get(url_msgs, headers=get_headers(), params=params_msgs)
                if r_msgs.status_code == 200:
                    mensajes = r_msgs.json()
                    for msg in reversed(mensajes):
                        procesar_mensaje_sync(conn, msg, telefono_db)
                        total_msgs += 1
            
            conn.commit()
            
        return f"✅ Sync OK (Sesión: {WAHA_SESSION}) - {total_msgs} msgs."
    except Exception as e:
        return f"Error crítico: {e}"

# ==============================================================================
# GOOGLE
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

def buscar_contacto_google(telefono_input):
    srv = get_google_service()
    if not srv: return None
    norm = normalizar_telefono_maestro(telefono_input)
    if not norm: return None
    
    local = norm['corto']
    intentos = [norm['db'], f"+{norm['db']}", local]
    if len(local) == 9: intentos.append(f"{local[:3]} {local[3:6]} {local[6:]}")
    
    for query in list(dict.fromkeys(intentos)):
        try:
            res = srv.people().searchContacts(query=query, readMask='names,phoneNumbers,metadata').execute()
            if res.get('results'):
                person = res['results'][0]['person']
                names = person.get('names', [])
                return {
                    "encontrado": True,
                    "nombre": names[0].get('givenName', '') if names else "Google",
                    "apellido": names[0].get('familyName', '') if names else "",
                    "nombre_completo": names[0].get('displayName', '') if names else "Google Contact",
                    "google_id": person.get('resourceName', '').replace('people/', '')
                }
        except: continue
    return None

def crear_en_google(nombre, apellido, telefono):
    srv = get_google_service()
    if not srv: return None
    norm = normalizar_telefono_maestro(telefono)
    if not norm: return None
    try:
        srv.people().createContact(body={
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": norm['google']}]
        }).execute()
        return True 
    except: return None

def actualizar_en_google(gid, nombre, apellido, telefono):
    # Restaurado para vistas/clientes.py
    srv = get_google_service()
    if not srv: return False
    norm = normalizar_telefono_maestro(telefono)
    if not norm: return False

    try:
        c = srv.people().get(resourceName=gid, personFields='names,phoneNumbers').execute()
        c['names'] = [{"givenName": nombre, "familyName": apellido}]
        c['phoneNumbers'] = [{"value": norm['google']}]
        srv.people().updateContact(resourceName=gid, updatePersonFields='names,phoneNumbers', body=c).execute()
        return True
    except: return False

# ==============================================================================
# UTILIDADES EXTRA (IA / VERIFICACIÓN) - RESTAURADAS
# ==============================================================================
def generar_nombre_ia(alias, nombre_real):
    # Función para limpiar nombres basura
    PALABRAS_PROHIBIDAS = [
        'CLIENTE', 'LENTES', 'MAYOR', 'MAYORISTA', 'OPTICA', 'VENTA', 
        'TIENDA', 'ALMACEN', 'CONTACTO', 'DR', 'DRA', 'SR', 'SRA', 
        'ADMIN', 'GRUPO', 'SPAM', 'ESTAFA', 'NO CONTESTA', 'K&M'
    ]
    def es_nombre_valido(palabra):
        if not palabra: return False
        p = palabra.upper()
        if len(p) <= 2: return False 
        if not p.isalpha(): return False 
        if p in PALABRAS_PROHIBIDAS: return False
        return True

    candidatos = [nombre_real, alias]
    for texto in candidatos:
        if not texto: continue
        limpio = str(texto).replace('-', ' ').replace('_', ' ').replace('.', ' ').strip()
        palabras = limpio.split()
        if not palabras: continue
        primer_palabra = palabras[0]
        if es_nombre_valido(primer_palabra):
            return primer_palabra.capitalize()
    return ""

def verificar_numero_waha(telefono):
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False 

        url = f"{WAHA_URL}/api/contacts/check-exists"
        payload = {"phone": norm['waha']}
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            return resp.json().get("exists", False)
        return None
    except Exception: return None