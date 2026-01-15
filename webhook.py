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

# --- FUNCIÓN SALVAVIDAS ---
def extraer_numero_seguro(campo_raw):
    """
    Convierte CUALQUIER cosa (Diccionario, Objeto, None) en un string '51999@c.us'
    """
    if campo_raw is None:
        return ""
    
    # Si es un objeto (Diccionario), sacamos el ID serializado o el usuario
    if isinstance(campo_raw, dict):
        # WAHA suele mandar '_serialized' o 'user'
        return str(campo_raw.get('_serialized') or campo_raw.get('user') or "")
    
    # Si ya es texto, lo devolvemos tal cual
    return str(campo_raw)

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json

    # 1. IMPRIMIR DATOS (Para que veas la estructura real en los logs)
    # print(f"📩 PAYLOAD RAW: {json.dumps(data, indent=2)}") 

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 2. EXTRACCIÓN SEGURA (Convertimos TODO a texto primero)
        # Esto evita el crash "dict object has no attribute split"
        from_str = extraer_numero_seguro(payload.get('from'))
        participant_str = extraer_numero_seguro(payload.get('participant'))
        author_str = extraer_numero_seguro(payload.get('author'))

        # 3. LÓGICA DE SELECCIÓN (Prioridad al número real 51...)
        numero_final = from_str # Por defecto
        
        candidatos = [participant_str, author_str, from_str]
        
        # Buscamos cual de todos tiene el formato de celular Perú ("51" y "@c.us")
        for cand in candidatos:
            if '51' in cand and '@c.us' in cand:
                numero_final = cand
                print(f"🎯 Número corregido detectado: {numero_final}")
                break
        
        # 4. LIMPIEZA FINAL
        # Ahora que 'numero_final' es 100% texto, el split funcionará
        try:
            telefono_limpio = numero_final.split('@')[0].split(':')[0]
        except Exception as e:
            print(f"⚠️ Error limpiando número: {e}")
            telefono_limpio = "Error"

        # -------------------------------------------------------

        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        
        archivo_bytes = None
        
        # Lógica de Imágenes
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
            print(f"✅ Guardado mensaje de: {telefono_limpio}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)