from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json 

app = Flask(__name__)

# Clave de seguridad opcional (si la configuraste en WAHA)
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL")


def descargar_media(media_url):
    """Descarga la imagen/archivo desde la URL que nos da WAHA"""
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
    # 1. Seguridad
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
# -------------------------------------------------------------
    # 🕵️‍♂️ ZONA DE DIAGNÓSTICO (RAYOS X)
    # -------------------------------------------------------------
    # Esto imprimirá TODO lo que llega a los logs de Railway
    print(f"\n🛑 --- NUEVO MENSAJE RECIBIDO ---", flush=True)
    print(json.dumps(data, indent=2), flush=True) 
    # -----


    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # --- CORRECCIÓN DE NÚMEROS DE EMPRESA (LID) ---
        # 1. Capturamos el ID CRUDO (Sin tocarlo)
        sender_raw = payload.get('from', 'Desconocido')
        participant = payload.get('participant', 'N/A')
        author = payload.get('author', 'N/A')
        
# Si es un LID (Empresa) o Grupo, el número real suele estar en 'participant'
        if '@lid' in sender_raw or '@g.us' in sender_raw:
            if participant and '51' in participant:
                sender_final = participant
            elif author and '51' in author:
                sender_final = author

        # Limpieza final estándar (quitar @c.us y :dispositivo)
        sender_limpio = sender_final.split('@')[0].split(':')[0]

        # 3. Preparar el cuerpo del mensaje
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        
        archivo_bytes = None
        if has_media:
            # ... (Lógica de descarga igual que antes) ...
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

        # 4. AGREGAR INFORMACIÓN DE DEBUG AL TEXTO
        # Esto te permitirá ver en tu app qué números llegaron realmente
        debug_info = f"\n\n🔍 [DEBUG INFO]\nFrom: {sender_raw}\nParticipant: {participant}\nUsado: {sender_limpio}"
        body_con_debug = body + debug_info

        # Guardar en Base de Datos
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {
                    "tel": sender_limpio,  # Guardamos el número que creemos correcto
                    "txt": body_con_debug, # Guardamos el texto con la "trampa" visual
                    "data": archivo_bytes
                })
                conn.commit()
            print(f"✅ Guardado: {sender_limpio}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)