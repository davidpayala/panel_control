from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro

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
    # Intentamos sacar el número de todas las formas posibles que usa WAHA
    alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
    if alt: return alt
    
    from_val = payload.get('from')
    author = payload.get('author')
    participant = payload.get('participant')
    
    # Prioridad al 'from' si no es grupo, sino al author/participant
    candidatos = [from_val, author, participant]
    
    for cand in candidatos:
        if cand and '51' in str(cand) and ('@c.us' in str(cand) or len(str(cand)) > 9):
            return cand
    return from_val

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
    
    try:
        tz = pytz.timezone('America/Lima')
        fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')
    except:
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')

    body = payload.get('body', '')
    has_media = payload.get('hasMedia', False)
    archivo_bytes = None
    
    if has_media:
        print("📷 Mensaje tiene multimedia, descargando...")
        media_info = payload.get('media', {})
        media_url = media_info.get('url')
        if media_url: 
            archivo_bytes = descargar_media(media_url)
            body = "📷 Archivo Multimedia" if archivo_bytes else "⚠️ Error descargando media"
        else:
            body = "📷 Multimedia (URL no disponible)"

    # 5. GUARDAR EN BASE DE DATOS (ORDEN CORREGIDO)
    try:
        with engine.connect() as conn:
            print("💾 Conectando a DB...")
            
            # PASO A: PRIMERO aseguramos el Cliente (Para evitar error de Foreign Key)
            print(f"👤 Intentando crear/verificar cliente: {telefono_db}")
            conn.execute(text("""
                INSERT INTO Clientes (
                    telefono, codigo_contacto, nombre_corto, nombre, 
                    medio_contacto, estado, fecha_seguimiento, activo,
                    fecha_registro 
                )
                VALUES (
                    :tel, :tel, :corto, :nom_wsp, 
                    'WhatsApp', 'Sin empezar', :fec, TRUE,
                    (NOW() - INTERVAL '5 hours')
                )
                ON CONFLICT (telefono) DO NOTHING
            """), {
                "tel": telefono_db,
                "corto": telefono_corto,
                "nom_wsp": nombre_wsp,
                "fec": fecha_hoy
            })
            
            # PASO B: LUEGO guardamos el Mensaje
            print(f"✉️ Guardando mensaje de: {telefono_db}")
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
            """), {
                "tel": telefono_db, 
                "txt": body, 
                "data": archivo_bytes
            })
            
            conn.commit()
            print(f"✅ ¡ÉXITO TOTAL! Mensaje guardado para {telefono_db}")
            
    except Exception as e:
        print(f"❌❌ ERROR GRAVE EN DB: {e}")
        # Importante: No devolvemos error 500 a WAHA para evitar bucles infinitos, 
        # pero registramos el error en los logs de Railway.
        return jsonify({"status": "db_error", "detail": str(e)}), 200

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)