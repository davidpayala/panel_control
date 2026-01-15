from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro # <--- USAMOS TU NUEVA FUNCIÓN MAESTRA

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
    # 1. Prioridad: remoteJidAlt (El que descubrimos en los logs)
    alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
    if alt: return alt

    # 2. Otros candidatos
    candidatos = [payload.get('participant'), payload.get('author'), payload.get('from')]
    for cand in candidatos:
        if cand and '51' in str(cand) and '@c.us' in str(cand):
            return cand
            
    # 3. Fallback
    return payload.get('from')

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 1. OBTENER EL CANDIDATO CRUDO
        numero_crudo = obtener_numero_crudo(payload)
        
        # 2. NORMALIZACIÓN MAESTRA (La función que creamos en utils.py)
        # Esto te devuelve el diccionario: {'db': '51...', 'corto': '9...', 'google': '+51...'}
        formatos = normalizar_telefono_maestro(numero_crudo)
        
        if not formatos:
            print(f"⚠️ No se pudo procesar número: {numero_crudo}")
            return jsonify({"status": "ignored"}), 200

        telefono_db = formatos['db']       # 51986203398
        telefono_corto = formatos['corto'] # 986203398

        # 3. PREPARAR OTROS DATOS
        nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or "Desconocido"
        
        # Fecha Hoy (Intentando usar zona horaria Perú)
        try:
            tz = pytz.timezone('America/Lima')
            fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')
        except:
            fecha_hoy = datetime.now().strftime('%Y-%m-%d')

        # 4. DESCARGAR MEDIA (Si hay)
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
                # A. Insertar Mensaje
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": telefono_db, "txt": body, "data": archivo_bytes})
                
                # B. Insertar Cliente (Con todos los campos que pediste)
                # ON CONFLICT DO NOTHING asegura que si ya existe, no borre lo que editaste manualmente
                conn.execute(text("""
                    INSERT INTO Clientes (
                        telefono, 
                        codigo_contacto, 
                        nombre_corto, 
                        nombre, 
                        medio_contacto, 
                        estado, 
                        fecha_seguimiento, 
                        activo,
                        fecha_creacion
                    )
                    VALUES (
                        :tel,          -- telefono (51986...)
                        :tel,          -- codigo_contacto (51986...)
                        :corto,        -- nombre_corto (986...)
                        :nom_wsp,      -- nombre (Del WhatsApp)
                        'WhatsApp',    -- medio_contacto
                        'Sin empezar', -- estado
                        :fec,          -- fecha_seguimiento
                        TRUE,
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
            print(f"✅ Guardado: {telefono_db} ({nombre_wsp})")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)