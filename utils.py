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
# 🏆 NORMALIZACIÓN
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
    
    # Ajustamos límites para permitir números internacionales largos (hasta 15)
    # y cortos válidos (min 7)
    if len(solo_numeros) > 15: return None
    if len(solo_numeros) < 7: return None

    full = solo_numeros
    local = solo_numeros
    
    # Lógica PERÚ (Solo si cumple patrones peruanos)
    if len(solo_numeros) == 9:
        full = f"51{solo_numeros}"
        local = solo_numeros
    elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
        full = solo_numeros
        local = solo_numeros[2:]
    
    # Para otros países, 'full' y 'local' quedan igual, permitiendo el acceso.
    
    return {
        "db": full,
        "waha": f"{full}@c.us",
        "google": f"+51 {local[:3]} {local[3:6]} {local[6:]}" if len(local)==9 else f"+{full}",
        "corto": local
    }

# ==============================================================================
# ENVÍO
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

# ==============================================================================
# SINCRONIZACIÓN (CAPTURANDO CONTENIDO DEL REPLY)
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
        url = f"{WAHA_URL}/api/messages?chatId={chat_id_waha}&limit=100&downloadMedia=false"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            mensajes_waha = response.json()
            nuevos = 0
            actualizados = 0
            
            with engine.begin() as conn:
                for msg in mensajes_waha:
                    cuerpo = msg.get('body', '')
                    if not cuerpo: continue
                    
                    participant_check = msg.get('from')
                    if not normalizar_telefono_maestro(participant_check): continue 

                    es_mio = msg.get('fromMe', False)
                    tipo_msg = 'SALIENTE' if es_mio else 'ENTRANTE'
                    timestamp = msg.get('timestamp')
                    w_id = msg.get('id', None)
                    
                    # --- CAPTURA AVANZADA DE REPLY ---
                    reply_id = None
                    reply_body = None
                    
                    raw_reply = msg.get('replyTo')
                    
                    if isinstance(raw_reply, dict):
                        reply_id = raw_reply.get('id')
                        reply_body = raw_reply.get('body') 
                    elif isinstance(raw_reply, str):
                        reply_id = raw_reply

                    if w_id:
                        # 1. UPSERT (Actualizar si existe, Insertar si no)
                        res = conn.execute(text("""
                            UPDATE mensajes 
                            SET reply_to_id = :rid, reply_content = :rbody, contenido = :m 
                            WHERE whatsapp_id = :wid
                        """), {"rid": reply_id, "rbody": reply_body, "m": cuerpo, "wid": w_id})
                        
                        if res.rowcount > 0:
                            actualizados += 1
                        else:
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id, reply_to_id, reply_content)
                                VALUES (:t, :tp, :m, to_timestamp(:ts), TRUE, :wid, :rid, :rbody)
                            """), {
                                "t": target_db, "tp": tipo_msg, "m": cuerpo, 
                                "ts": timestamp, "wid": w_id, "rid": reply_id, "rbody": reply_body
                            })
                            nuevos += 1
                    else:
                        # Fallback simple
                        existe = conn.execute(text("SELECT count(*) FROM mensajes WHERE telefono=:t AND contenido=:m AND fecha > (NOW() - INTERVAL '24h')"), 
                                            {"t": target_db, "m": cuerpo}).scalar()
                        if existe == 0:
                            conn.execute(text("INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido) VALUES (:t, :tp, :m, NOW(), TRUE)"), 
                                        {"t": target_db, "tp": tipo_msg, "m": cuerpo})
                            nuevos += 1
            
            return True, f"Sync: {nuevos} nuevos, {actualizados} act."
        
        elif response.status_code == 401: return False, "Error 401 API Key"
        else: return False, f"Error WAHA: {response.status_code}"
            
    except Exception as e:
        return False, f"Error conexión: {e}"

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