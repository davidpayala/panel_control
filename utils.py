import streamlit as st
import os
import requests
import pandas as pd
from sqlalchemy import text
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from database import engine
import json

# ==============================================================================
# CONFIGURACIÓN GENERAL
# ==============================================================================
WAHA_URL = os.getenv("WAHA_URL") 
WAHA_KEY = os.getenv("WAHA_KEY") 

# ==============================================================================
# 🏆 1. NORMALIZACIÓN Y FORMATO (CRÍTICO PARA CHATS)
# ==============================================================================
def normalizar_telefono_maestro(entrada):
    """
    Convierte cualquier formato de teléfono a un estándar DB (solo números).
    Maneja diccionarios de WAHA, cadenas con +51, etc.
    Devuelve un diccionario con formatos útiles.
    """
    if not entrada: return None
    raw_id = ""

    # 1. Extraer el ID si es un objeto (payload de WAHA o dict propio)
    if isinstance(entrada, dict):
        if 'db' in entrada: return entrada # Ya estaba normalizado
        
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

    # 2. Filtros de seguridad
    if 'status@broadcast' in raw_id: return None
    if '@g.us' in raw_id: return None
    
    # 3. Limpieza de caracteres
    cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id
    solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
    
    if not solo_numeros: return None
    
    # 4. Validaciones de longitud básica
    if len(solo_numeros) > 15 or len(solo_numeros) < 7: return None

    full = solo_numeros
    local = solo_numeros
    
    # 5. Lógica específica para Perú (51)
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

# ==============================================================================
# 🔍 2. FUNCIONES DE GOOGLE CONTACTS (LAS QUE FALTABAN)
# ==============================================================================
def get_google_service():
    """Autenticación silenciosa con Google"""
    try:
        # Busca el secreto en las variables de entorno de Railway
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not creds_json: return None
        
        info = json.loads(creds_json)
        creds = Credentials.from_authorized_user_info(info, ['https://www.googleapis.com/auth/contacts'])
        return build('people', 'v1', credentials=creds)
    except:
        return None

def buscar_contacto_google(telefono):
    """
    Busca un contacto en Google por número de teléfono.
    Retorna un diccionario con los datos encontrados o 'encontrado': False.
    """
    datos = normalizar_telefono_maestro(telefono)
    if not datos: return {'encontrado': False}
    
    service = get_google_service()
    if not service: return {'encontrado': False, 'error': 'No auth'}

    try:
        # Buscamos por el número local (999...) y el internacional (51999...)
        queries = [datos['corto'], datos['db']]
        
        for q in queries:
            results = service.people().searchContacts(
                query=q, readMask='names,phoneNumbers,emailAddresses'
            ).execute()
            
            if results.get('results'):
                person = results['results'][0]['person']
                names = person.get('names', [])
                nombre_completo = names[0].get('displayName', 'Sin Nombre') if names else "Sin Nombre"
                
                # Intentar separar nombre y apellido simple
                partes = nombre_completo.split()
                nombre = partes[0] if partes else ""
                apellido = " ".join(partes[1:]) if len(partes) > 1 else ""

                return {
                    'encontrado': True,
                    'nombre_completo': nombre_completo,
                    'nombre': nombre,
                    'apellido': apellido,
                    'google_id': person.get('resourceName'),
                    'telefono_google': q
                }
    except Exception as e:
        print(f"Error Google Search: {e}")
        
    return {'encontrado': False}

def crear_en_google(nombre, apellido, telefono, email=None):
    """Crea un contacto en Google Contacts"""
    service = get_google_service()
    if not service: return False

    try:
        body = {
            "names": [{"givenName": nombre, "familyName": apellido}],
            "phoneNumbers": [{"value": telefono}],
        }
        if email:
            body["emailAddresses"] = [{"value": email}]

        service.people().createContact(body=body).execute()
        return True
    except:
        return False

# ==============================================================================
# 💬 3. FUNCIONES WAHA (API)
# ==============================================================================
def marcar_chat_como_leido_waha(chat_id):
    if not WAHA_URL: return
    try:
        # Aseguramos formato correcto
        if "@" not in chat_id: chat_id = f"{chat_id}@c.us"
        
        url = f"{WAHA_URL.rstrip('/')}/api/sendSeen"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        requests.post(url, json={"session": "default", "chatId": chat_id}, headers=headers, timeout=3)
    except: pass

def obtener_perfil_waha(telefono):
    """Intenta obtener la foto y el estado de WAHA"""
    if not WAHA_URL: return None
    try:
        norm = normalizar_telefono_maestro(telefono)
        if not norm: return None
        
        url = f"{WAHA_URL.rstrip('/')}/api/contacts/{norm['waha']}"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            return r.json()
    except: pass
    return None