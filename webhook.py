from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json

app = Flask(__name__)

# Configuración
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
        print(f"❌ Excepción media: {e}")
        return None

# --- FUNCIÓN DE SEGURIDAD NUCLEAR ---
def extraer_string_seguro(campo_raw):
    """
    Convierte CUALQUIER cosa (Dict, Objeto, None) en un string seguro.
    Evita el error 'AttributeError: dict object has no attribute split'
    """
    if campo_raw is None:
        return ""
    
    if isinstance(campo_raw, dict):
        # Si es un objeto, intentamos sacar el ID serializado o el usuario
        return str(campo_raw.get('_serialized') or campo_raw.get('user') or campo_raw.get('id') or "")
    
    # Si ya es texto, lo devolvemos asegurando que sea string
    return str(campo_raw)

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    # Debug: Imprime lo que llega para que lo veas en los logs si falla algo más
    # print(json.dumps(data, indent=2), flush=True)

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 1. EXTRACCIÓN SEGURA (Esto arregla el crash)
        # Pasamos todo por la función de seguridad. Ya no importa si llega como objeto o texto.
        sender_str = extraer_string_seguro(payload.get('from'))
        participant_str = extraer_string_seguro(payload.get('participant'))
        author_str = extraer_string_seguro(payload.get('author'))

        # 2. LOGICA DE SELECCIÓN (Detectar Empresas/LID)
        numero_final = sender_str # Por defecto usamos el 'from'

        # Si viene de una empresa (@lid) o grupo (@g.us), el número real suele estar en 'participant'
        # Buscamos cual de los campos extra tiene un formato de celular peruano (empieza con 51 y termina en c.us)
        if '@lid' in sender_str or '@g.us' in sender_str:
            if participant_str and '51' in participant_str and '@c.us' in participant_str:
                numero_final = participant_str
            elif author_str and '51' in author_str and '@c.us' in author_str:
                numero_final = author_str
        
        # 3. LIMPIEZA FINAL
        # Quitamos @c.us, @lid y los sufijos de dispositivo (:8)
        # Al usar .split sobre una variable que YA pasamos por extraer_string_seguro, es imposible que falle.
        try:
            telefono_limpio = numero_final.split('@')[0].split(':')[0]
        except:
            telefono_limpio = "Error_Parsing"

        # -------------------------------------------------------
        
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        
        archivo_bytes = None
        
        if has_media:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            mimetype = media_info.get('mimetype', '')
            if media_url:
                archivo_bytes = descargar_media(media_url)
                if archivo_bytes:
                    tipo_icono = "📷" if "image" in mimetype else "📎"
                    body = f"{tipo_icono} Archivo recibido"
                else:
                    body = "⚠️ Error imagen"
            else:
                body = "📷 https://www.spanishdict.com/translate/vac%C3%ADa"

        # Guardar en Base de Datos
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {
                    "tel": telefono_limpio, 
                    "txt": body,
                    "data": archivo_bytes
                })
                conn.commit()
            print(f"✅ Guardado mensaje de: {telefono_limpio} (Original: {sender_str})")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)