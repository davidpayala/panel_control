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
        print(f"❌ Error media: {e}")
        return None

def obtener_numero_crudo(payload):
    """Extrae el número bruto de donde sea que WAHA lo esconda"""
    alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
    if alt: return alt
    candidatos = [payload.get('participant'), payload.get('author'), payload.get('from')]
    for cand in candidatos:
        if cand and '51' in str(cand) and '@c.us' in str(cand):
            return cand
    return payload.get('from')

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 1. Obtener y Normalizar
        numero_crudo = obtener_numero_crudo(payload)
        formatos = normalizar_telefono_maestro(numero_crudo)
        
        if not formatos:
            return jsonify({"status": "ignored"}), 200

        telefono_db = formatos['db']       
        telefono_corto = formatos['corto'] 

        # 2. Datos Adicionales
        nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or "Desconocido"
        try:
            tz = pytz.timezone('America/Lima')
            fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')
        except:
            fecha_hoy = datetime.now().strftime('%Y-%m-%d')

        # 3. Media
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        archivo_bytes = None
        if has_media:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            if media_url: archivo_bytes = descargar_media(media_url)
            body = "📷 Archivo" if archivo_bytes else "⚠️ Error media"

        # 4. Guardar en DB
        try:
            with engine.connect() as conn:
                # Insertar Mensaje
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": telefono_db, "txt": body, "data": archivo_bytes})
                
                # Insertar Cliente (CORREGIDO: fecha_registro)
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
                conn.commit()
            print(f"✅ Guardado: {telefono_db}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)