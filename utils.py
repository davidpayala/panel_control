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
# 🏆 NORMALIZACIÓN BLINDADA (Internacionales OK)
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    if not entrada: return None
    raw_id = ""

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

    # Filtros de seguridad WAHA
    if 'status@broadcast' in raw_id: return None
    if '@g.us' in raw_id: return None
    if '@newsletter' in raw_id: return None
    if '@lid' in raw_id: return None

    cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id
    solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
    
    if not solo_numeros: return None
    
    # Validaciones de longitud
    if len(solo_numeros) > 15: return None
    if len(solo_numeros) < 7: return None

    full = solo_numeros
    local = solo_numeros
    
    # Lógica Perú
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    return {
        "db": full,
        "waha": f"{full}@c.us",
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}" if len(local)==9 else f"+{full}",
        "corto": local
    }

# ==============================================================================
# ENVÍO Y MEDIA
# ==============================================================================
def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL: return False, "⚠️ Falta WAHA_URL"
    norm = normalizar_telefono_maestro(numero)
    if not norm: return False, "❌ Número inválido"
    
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

def subir_archivo_meta(archivo_bytes, mime_type):
    # Función auxiliar para compatibilidad
    try:
        b64_data = base64.b64encode(archivo_bytes).decode('utf-8')
        return f"data:{mime_type};base64,{b64_data}", None
    except Exception as e: return None, str(e)

# ==============================================================================
# SINCRONIZACIÓN (CAPTURA REPLY TEXT)
# ==============================================================================
def sincronizar_historial():
    """
    Descarga los últimos mensajes de cada chat en WAHA y los guarda en la DB local.
    """
    sessions = ['default', 'principal'] # Tus sesiones
    total_guardados = 0
    
    headers = {}
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY

    for session in sessions:
        try:
            # 1. Obtener lista de chats (limitado a 100 chats más recientes)
            url_chats = f"{WAHA_URL}/api/{session}/chats?limit=100&sortBy=messageTimestamp"
            r_chats = requests.get(url_chats, headers=headers)
            
            if r_chats.status_code != 200:
                print(f"Error obteniendo chats de {session}: {r_chats.text}")
                continue
                
            chats = r_chats.json()
            
            for chat in chats:
                chat_id = chat.get('id') # ej: 51999...@c.us
                if not chat_id or '@g.us' in chat_id: continue # Saltamos grupos si quieres
                
                # Limpiar numero
                telefono = chat_id.replace('@c.us', '').replace('@s.whatsapp.net', '')
                
                # 2. Obtener mensajes del chat (limitado a 50 por chat para no saturar)
                url_msgs = f"{WAHA_URL}/api/{session}/chats/{chat_id}/messages?limit=50&downloadMedia=false"
                r_msgs = requests.get(url_msgs, headers=headers)
                
                if r_msgs.status_code != 200: continue
                
                mensajes = r_msgs.json()
                
                # 3. Guardar en DB
                with engine.connect() as conn:
                    # Asegurar que el cliente existe
                    conn.execute(text("""
                        INSERT INTO "Clientes" (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, 'Importado', 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO NOTHING
                    """), {"t": telefono})
                    
                    for msg in mensajes:
                        whatsapp_id = msg.get('id')
                        # Verificar si ya existe para no duplicar
                        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:w"), 
                                            {"w": whatsapp_id}).scalar()
                        
                        if not existe:
                            timestamp = msg.get('timestamp')
                            fecha_msg = datetime.datetime.fromtimestamp(timestamp) if timestamp else datetime.datetime.now()
                            
                            es_mio = msg.get('fromMe', False)
                            tipo = 'SALIENTE' if es_mio else 'ENTRANTE'
                            contenido = msg.get('body', '')
                            
                            # Manejo básico de adjuntos en historial (sin descargar binario para ir rápido)
                            has_media = msg.get('hasMedia', False)
                            if has_media and not contenido:
                                contenido = "📷 [Archivo Histórico]"

                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id)
                                VALUES (:t, :tipo, :c, :f, :l, :w)
                            """), {
                                "t": telefono,
                                "tipo": tipo,
                                "c": contenido,
                                "f": fecha_msg,
                                "l": True, # Asumimos leido porque es historial
                                "w": whatsapp_id
                            })
                            total_guardados += 1
                    conn.commit()
                    
        except Exception as e:
            print(f"Error sincronizando {session}: {e}")

    return f"Sincronización completada. {total_guardados} mensajes nuevos importados."

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