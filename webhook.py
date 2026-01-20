from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google, crear_en_google, actualizar_en_google

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
        r = requests.get(url_final, headers=headers, timeout=5)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ Error leve descargando media: {e}")
        return None

def obtener_numero_crudo(payload):
    """Busca el número real, ignorando los IDs técnicos (@lid)"""
    alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
    from_val = payload.get('from')
    author = payload.get('author')
    participant = payload.get('participant')
    
    candidatos = [alt, from_val, author, participant]
    for cand in candidatos:
        cand_str = str(cand)
        if '@lid' in cand_str: continue 
        if '51' in cand_str and ('@c.us' in cand_str or len(cand_str) > 9):
            return cand
    return from_val.replace('@lid', '') if from_val else None

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # 1. Seguridad
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or data.get('event') != 'message':
        return jsonify({"status": "ignored_event"}), 200

    payload = data.get('payload', {})
    origen = payload.get('from', '')

    # --- 🛡️ FILTRO 0: ESTADOS (STORIES) ---
    # ESTE ES EL CAMBIO CLAVE: Bloqueamos todo lo que venga de status@broadcast
    if 'status@broadcast' in origen:
        print("🙈 Estado/Historia ignorado.")
        return jsonify({"status": "ignored_status"}), 200

    # --- 🛡️ FILTRO 1: SISTEMA ---
    tipo_interno = payload.get('_data', {}).get('type')
    lista_negra = ['e2e_notification', 'notification_template', 'call_log', 'ciphertext', 'revoked', 'gp2', 'protocol', 'unknown']
    if tipo_interno in lista_negra:
        return jsonify({"status": "ignored_system_msg"}), 200
    
    # 2. Obtener Número
    numero_crudo = obtener_numero_crudo(payload)
    formatos = normalizar_telefono_maestro(numero_crudo)
    
    if not formatos:
        return jsonify({"status": "ignored_bad_number"}), 200

    telefono_db = formatos['db']
    telefono_corto = formatos['corto']

    # 3. Google Search (Silent)
    nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or "Cliente WhatsApp"
    nombre_final = nombre_wsp
    apellido_final = ""
    nombre_corto_final = nombre_wsp
    google_id_final = None

    try:
        datos_google = buscar_contacto_google(telefono_db) 
        if datos_google and datos_google['encontrado']:
            nombre_final = datos_google['nombre']
            apellido_final = datos_google['apellido']
            nombre_corto_final = datos_google['nombre_completo']
            google_id_final = datos_google['google_id']
    except Exception:
        pass 

    # 4. PROCESAR CONTENIDO (MEDIA + TEXTO)
    body = payload.get('body', '')
    has_media = payload.get('hasMedia', False)
    archivo_bytes = None
    
    if has_media:
        try:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            if media_url: 
                archivo_bytes = descargar_media(media_url)
                if archivo_bytes:
                    if not body: body = "📷 Archivo Multimedia"
                # Si falla descarga, no ponemos texto de error
            else:
                pass 
        except Exception:
             pass 

    # --- 🛡️ FILTRO 2: SPAM / SPOTIFY ---
    if body and "spotify.com" in body.lower():
        return jsonify({"status": "ignored_spotify"}), 200

    # --- 🛡️ FILTRO 3: VACÍO ---
    if not body and not archivo_bytes:
        return jsonify({"status": "ignored_empty"}), 200

    # 5. GUARDAR EN DB
    try:
        try:
            tz = pytz.timezone('America/Lima')
            fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')
        except:
            fecha_hoy = datetime.now().strftime('%Y-%m-%d')

        with engine.connect() as conn:
            # Upsert Cliente
            conn.execute(text("""
                INSERT INTO Clientes (
                    telefono, codigo_contacto, nombre_corto, 
                    nombre, apellido, google_id,
                    medio_contacto, estado, fecha_seguimiento, activo, fecha_registro
                ) VALUES (
                    :tel, :tel, :corto, 
                    :nom, :ape, :gid,
                    'WhatsApp', 'Sin empezar', :fec, TRUE, (NOW() - INTERVAL '5 hours')
                )
                ON CONFLICT (telefono) DO UPDATE SET
                    google_id = COALESCE(Clientes.google_id, EXCLUDED.google_id),
                    nombre = COALESCE(NULLIF(Clientes.nombre, 'Cliente WhatsApp'), EXCLUDED.nombre),
                    nombre_corto = COALESCE(NULLIF(Clientes.nombre_corto, 'Cliente WhatsApp'), EXCLUDED.nombre_corto)
            """), {
                "tel": telefono_db, "corto": nombre_corto_final,
                "nom": nombre_final, "ape": apellido_final, "gid": google_id_final,
                "fec": fecha_hoy
            })
            
            # Insertar Mensaje
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
            """), {
                "tel": telefono_db, "txt": body, "data": archivo_bytes
            })
            
            conn.commit()
            print(f"✅ Guardado: {nombre_corto_final}")
            
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return jsonify({"status": "error_db", "detail": str(e)}), 200

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)