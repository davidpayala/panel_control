from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google # <--- IMPORTANTE AGREGAR


app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def descargar_media(media_url):
    try:
        url_final = media_url
        if not media_url.startswith("http"):
             url_final = f"{WAHA_URL}{media_url}"
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        r = requests.get(url_final, headers=headers, timeout=10)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        print(f"❌ Error descargando media: {e}")
        return None

def obtener_numero_crudo(payload):
    """
    Busca el número real, ignorando los IDs técnicos (@lid)
    """
    # 1. Intentamos sacar data del objeto key
    alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
    from_val = payload.get('from')
    author = payload.get('author')
    participant = payload.get('participant')
    
    candidatos = [alt, from_val, author, participant]
    
    for cand in candidatos:
        cand_str = str(cand)
        # FILTRO: Si es un LID (ID técnico), lo ignoramos
        if '@lid' in cand_str:
            continue
            
        # ACEPTAR: Si tiene formato de usuario normal
        if '51' in cand_str and ('@c.us' in cand_str or len(cand_str) > 9):
            return cand
            
    # Si todo falla, devolvemos el 'from' original aunque sea raro, 
    # pero limpiamos el @lid si viene pegado
    return from_val.replace('@lid', '') if from_val else None

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    print("🔔 WEBHOOK RECIBIDO: Iniciando proceso...") # LOG 1
    
    # 1. Validación de Seguridad
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        print("⛔ Error: Api Key incorrecta")
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data:
        print("⚠️ Error: Body vacío")
        return jsonify({"status": "empty"}), 200

    # 2. Solo procesar mensajes
    if data.get('event') != 'message':
        # Ignoramos eventos de estado para no llenar el log
        return jsonify({"status": "ignored_event"}), 200

    payload = data.get('payload', {})
    
    # 3. Obtener y Normalizar Número
    numero_crudo = obtener_numero_crudo(payload)
    print(f"📞 Número Crudo detectado: {numero_crudo}") # LOG 2
    
    formatos = normalizar_telefono_maestro(numero_crudo)
    
    if not formatos:
        print("❌ Error: No se pudo normalizar el número. Ignorando.")
        return jsonify({"status": "ignored_bad_number"}), 200

    telefono_db = formatos['db']       # 51986203398
    telefono_corto = formatos['corto'] # 986203398
    print(f"✅ Teléfono Normalizado: DB={telefono_db} | CORTO={telefono_corto}") # LOG 3

    # 4. Preparar Datos
    nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or "Cliente WhatsApp"
    
    # --- LÓGICA GOOGLE ---
    print(f"🔎 Buscando en Google Contacts: {telefono_corto}...")
    datos_google = buscar_contacto_google(telefono_corto)
    
    if datos_google and datos_google['encontrado']:
        print(f"✅ Encontrado en Google: {datos_google['nombre_completo']}")
        # Usamos datos de Google
        nombre_final = datos_google['nombre']
        apellido_final = datos_google['apellido']
        nombre_corto_final = datos_google['nombre_completo'] # Ej: David Perez
        google_id_final = datos_google['google_id']
    else:
        print("Build: No encontrado en Google. Usando datos de WhatsApp.")
        # Usamos datos de WhatsApp como fallback
        nombre_final = nombre_wsp
        apellido_final = "" # WhatsApp no separa apellidos
        nombre_corto_final = nombre_wsp
        google_id_final = None

    # Fechas
    try:
        tz = pytz.timezone('America/Lima')
        fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')
    except:
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')

    # Media (igual que antes)
    body = payload.get('body', '')
    has_media = payload.get('hasMedia', False)
    archivo_bytes = None
    if has_media:
        media_info = payload.get('media', {})
        media_url = media_info.get('url')
        if media_url: archivo_bytes = descargar_media(media_url)
        body = "📷 Archivo" if archivo_bytes else "⚠️ Error media"

    # 5. GUARDAR EN BASE DE DATOS
    try:
        with engine.connect() as conn:
            
            # PASO A: Crear/Actualizar Cliente con datos RICOS
            # Usamos ON CONFLICT DO UPDATE para que, si ya existe pero sin Google ID, se actualice.
            print(f"👤 Procesando cliente: {telefono_db}")
            
            conn.execute(text("""
                INSERT INTO Clientes (
                    telefono, codigo_contacto, nombre_corto, 
                    nombre, apellido, google_id,
                    medio_contacto, estado, fecha_seguimiento, activo, fecha_registro
                )
                VALUES (
                    :tel, :tel, :corto, 
                    :nom, :ape, :gid,
                    'WhatsApp', 'Sin empezar', :fec, TRUE, (NOW() - INTERVAL '5 hours')
                )
                ON CONFLICT (telefono) DO UPDATE SET
                    google_id = COALESCE(Clientes.google_id, EXCLUDED.google_id),
                    nombre = COALESCE(NULLIF(Clientes.nombre, 'Cliente WhatsApp'), EXCLUDED.nombre),
                    nombre_corto = COALESCE(NULLIF(Clientes.nombre_corto, 'Cliente WhatsApp'), EXCLUDED.nombre_corto)
            """), {
                "tel": telefono_db,
                "corto": nombre_corto_final,
                "nom": nombre_final,
                "ape": apellido_final,
                "gid": google_id_final,
                "fec": fecha_hoy
            })
            
            # PASO B: Guardar Mensaje (Igual que antes)
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
            """), {
                "tel": telefono_db, 
                "txt": body, 
                "data": archivo_bytes
            })
            
            conn.commit()
            print(f"✅ Guardado exitoso. Cliente: {nombre_corto_final}")
            
    except Exception as e:
        print(f"❌ Error DB: {e}")
        # Retornamos éxito a WAHA para no trabar la cola, pero logueamos el error
        return jsonify({"status": "error_db", "detail": str(e)}), 200

    return jsonify({"status": "success"}), 200