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
# Definimos las sesiones que existen en tu sistema
# Puedes agregar más a esta lista si creas nuevas en el futuro
SESIONES_ACTIVAS = ["principal", "default"]
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
    if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
    return headers

def marcar_chat_como_leido_waha(chat_id):
    """Intenta marcar como leído en todas las sesiones posibles"""
    for sesion in SESIONES_ACTIVAS:
        try:
            url = f"{WAHA_URL}/api/{sesion}/chats/{chat_id}/messages/read"
            requests.post(url, headers=get_headers(), json={}, timeout=2)
        except: pass

# ==============================================================================
# PROCESAMIENTO DE MENSAJES (SYNC)
# ==============================================================================
def map_ack_status(ack_value):
    s_ack = str(ack_value).upper()
    if s_ack in ['3', '4', 'READ', 'PLAYED']: return 'leido'
    if s_ack in ['2', 'RECEIVED', 'DELIVERED']: return 'recibido'
    if s_ack in ['1', 'SENT']: return 'enviado'
    return 'pendiente'

def procesar_mensaje_sync(conn, msg, telefono_db):
    try:
        wid = msg.get('id')
        from_me = msg.get('fromMe', False)
        
        # Extracción de contenido
        body = msg.get('body', '')
        if not body and '_data' in msg:
            data = msg['_data']
            if 'caption' in data: body = data['caption']
            elif 'message' in data:
                m = data['message']
                if 'conversation' in m: body = m['conversation']
                elif 'extendedTextMessage' in m: body = m['extendedTextMessage'].get('text', '')
                elif 'imageMessage' in m: body = m['imageMessage'].get('caption', '📷 [Imagen]')
        
        # Timestamp Fix
        timestamp = msg.get('timestamp')
        if timestamp and float(timestamp) > 9999999999:
             timestamp = float(timestamp) / 1000.0

        ack_raw = msg.get('ack', 0)
        estado_waha = map_ack_status(ack_raw) if from_me else None
        tipo = 'SALIENTE' if from_me else 'ENTRANTE'
        
        # 1. Crear Cliente
        conn.execute(text("""
            INSERT INTO clientes (telefono, nombre_corto, estado, activo, fecha_registro)
            VALUES (:t, 'Whatsapp Sync', 'Sin empezar', TRUE, NOW())
            ON CONFLICT (telefono) DO NOTHING
        """), {"t": telefono_db})

        # 2. Insertar Mensaje
        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:w"), {"w": wid}).scalar()
        
        if not existe:
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, whatsapp_id, estado_waha)
                VALUES (:t, :tipo, :c, to_timestamp(:ts), :l, :wid, :est)
            """), {
                "t": telefono_db,
                "tipo": tipo,
                "c": body or "[Adjunto]",
                "ts": timestamp,
                "l": True, 
                "wid": wid,
                "est": estado_waha
            })
        else:
            if from_me:
                conn.execute(text("UPDATE mensajes SET estado_waha = :est WHERE whatsapp_id = :wid"), 
                             {"est": estado_waha, "wid": wid})
                
    except Exception as e:
        print(f"Error procesando msg {msg.get('id')}: {e}")

# ==============================================================================
# ENVÍO Y MEDIA (MULTI-SESIÓN INTELIGENTE)
# ==============================================================================
def verificar_sesion_chat(chat_id_waha):
    """Descubre en qué sesión existe este chat"""
    for sesion in SESIONES_ACTIVAS:
        try:
            url = f"{WAHA_URL}/api/{sesion}/contacts/check-exists"
            r = requests.post(url, headers=get_headers(), json={"phone": chat_id_waha}, timeout=3)
            if r.status_code == 200 and r.json().get('exists', False):
                return sesion
        except: continue
    return "principal" # Fallback por defecto

def enviar_mensaje_whatsapp(numero, texto):
    if not WAHA_URL: return False, "Falta WAHA_URL"
    norm = normalizar_telefono_maestro(numero)
    if not norm: return False, "Número inválido"
    
    # Intentamos primero con la sesión por defecto 'principal' para rapidez
    # Si quisieras exactitud total, descomenta la línea de abajo (pero es más lento):
    # sesion_a_usar = verificar_sesion_chat(norm['waha'])
    sesion_a_usar = "principal" 

    url = f"{WAHA_URL}/api/{sesion_a_usar}/sendText"
    payload = {"chatId": norm['waha'], "text": texto}
    
    try:
        r = requests.post(url, headers=get_headers(), json=payload, timeout=10)
        
        # Si falla (ej: 404 sesión no encontrada o chat no existe), probamos la otra sesión
        if r.status_code != 200 or "error" in r.text.lower():
            sesion_backup = "default" if sesion_a_usar == "principal" else "principal"
            url_bk = f"{WAHA_URL}/api/{sesion_backup}/sendText"
            r2 = requests.post(url_bk, headers=get_headers(), json=payload, timeout=10)
            if r2.status_code in [200, 201]:
                return True, r2.json()
                
        if r.status_code in [200, 201]: return True, r.json()
        return False, f"WAHA {r.status_code}: {r.text}"
    except Exception as e: return False, str(e)

def enviar_mensaje_media(telefono, archivo_bytes, mime_type, caption, filename):
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return False, "Número inválido"

        media_b64 = base64.b64encode(archivo_bytes).decode('utf-8')
        data_uri = f"data:{mime_type};base64,{media_b64}"
        
        # Probamos primero principal, luego default
        sesiones_orden = ["principal", "default"]
        
        last_error = ""
        for sesion in sesiones_orden:
            url = f"{WAHA_URL}/api/{sesion}/sendImage"
            payload = {
                "chatId": norm['waha'],
                "file": {"mimetype": mime_type, "filename": filename, "url": data_uri},
                "caption": caption
            }
            try:
                response = requests.post(url, json=payload, headers=get_headers(), timeout=30)
                if response.status_code == 201: 
                    return True, response.json()
                last_error = f"{sesion}: {response.text}"
            except Exception as e:
                last_error = str(e)
                
        return False, f"Fallo en ambas sesiones. {last_error}"
    except Exception as e: return False, str(e)

# ==============================================================================
# SINCRONIZACIÓN MULTI-SESIÓN (BARRIDO COMPLETO)
# ==============================================================================
def sincronizar_historial(limit=100):
    reporte = []
    total_global = 0
    
    with engine.connect() as conn:
        # BUCLE PRINCIPAL: REVISA CADA SESIÓN
        for sesion in SESIONES_ACTIVAS:
            try:
                # 1. Obtener chats de la sesión actual
                url_chats = f"{WAHA_URL}/api/{sesion}/chats"
                params_chats = {"limit": 50, "sortBy": "messageTimestamp"}
                
                r = requests.get(url_chats, headers=get_headers(), params=params_chats)
                
                if r.status_code != 200:
                    reporte.append(f"⚠️ {sesion}: Error {r.status_code}")
                    continue
                
                chats = r.json()
                msgs_sesion = 0
                
                for chat in chats:
                    chat_id = chat.get('id', '')
                    norm = normalizar_telefono_maestro(chat_id)
                    if not norm: continue
                    telefono_db = norm['db']
                    
                    # 2. Descargar mensajes
                    url_msgs = f"{WAHA_URL}/api/{sesion}/chats/{chat_id}/messages"
                    params_msgs = { "limit": limit, "downloadMedia": "false" }
                    
                    r_m = requests.get(url_msgs, headers=get_headers(), params=params_msgs)
                    if r_m.status_code == 200:
                        mensajes = r_m.json()
                        for msg in reversed(mensajes):
                            procesar_mensaje_sync(conn, msg, telefono_db)
                            msgs_sesion += 1
                
                total_global += msgs_sesion
                reporte.append(f"✅ {sesion}: {msgs_sesion} msgs")
                
            except Exception as e:
                reporte.append(f"❌ {sesion}: Error {str(e)}")
        
        conn.commit()
            
    return f"Sync Finalizado | Total: {total_global} | Detalle: {', '.join(reporte)}"

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
    """Verifica en todas las sesiones si existe el número"""
    for sesion in SESIONES_ACTIVAS:
        try:
            norm = normalizar_telefono_maestro(telefono)
            if not norm: continue
            url = f"{WAHA_URL}/api/{sesion}/contacts/check-exists"
            resp = requests.post(url, json={"phone": norm['waha']}, headers=get_headers(), timeout=5)
            if resp.status_code == 200 and resp.json().get("exists", False):
                return True
        except: continue
    return None