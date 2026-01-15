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

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json

    # (Opcional) Debug para ver si ya sale bien
    # print(json.dumps(data, indent=2), flush=True)

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 1. OBTENER DATOS CRUDOS
        sender_raw = payload.get('from', '')
        participant_raw = payload.get('participant')
        author_raw = payload.get('author')

        # 2. NORMALIZAR PARTICIPANT (Aquí estaba el error)
        # Si participant es un diccionario (Objeto), sacamos el string de adentro
        participant_str = ""
        if isinstance(participant_raw, dict):
            participant_str = participant_raw.get('_serialized', '')
        elif isinstance(participant_raw, str):
            participant_str = participant_raw
        
        # Hacemos lo mismo para author por si acaso
        author_str = ""
        if isinstance(author_raw, dict):
            author_str = author_raw.get('_serialized', '')
        elif isinstance(author_raw, str):
            author_str = author_raw

        # 3. ELEGIR EL MEJOR NÚMERO (Prioridad al real sobre el LID)
        sender_final = sender_raw # Por defecto

        # Si el 'from' tiene @lid o es grupo, buscamos el número real
        if '@lid' in sender_raw or '@g.us' in sender_raw:
            if participant_str and '51' in participant_str:
                sender_final = participant_str
            elif author_str and '51' in author_str:
                sender_final = author_str
        
        # 4. LIMPIEZA FINAL (Quitar @c.us, :dispositivo, etc)
        # Convertimos a string por seguridad antes de hacer split
        sender_final = str(sender_final)
        sender_limpio = sender_final.split('@')[0].split(':')[0]

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
                    "tel": sender_limpio, 
                    "txt": body,
                    "data": archivo_bytes
                })
                conn.commit()
            print(f"✅ Guardado mensaje de: {sender_limpio}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)