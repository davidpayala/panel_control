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

def limpiar_dato(dato_crudo):
    """
    Convierte diccionarios, objetos o nulos en un string limpio.
    Ej: {'_serialized': '51999@c.us'} -> '51999@c.us'
    """
    if dato_crudo is None:
        return ""
    if isinstance(dato_crudo, dict):
        return str(dato_crudo.get('_serialized') or dato_crudo.get('user') or "")
    return str(dato_crudo)

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # --- LÓGICA "CAZADOR DE NÚMEROS" ---
        # Recopilamos todos los posibles lugares donde WAHA esconde el número
        candidatos = [
            payload.get('participant'), # Aquí suele estar el real en empresas
            payload.get('author'),      # A veces aquí
            payload.get('from')         # Aquí suele venir el LID (malo)
        ]
        
        numero_elegido = None

        # 1. BÚSQUEDA PRIORITARIA: Buscamos un número peruano real (51...c.us)
        for candidato in candidatos:
            s_cand = limpiar_dato(candidato)
            # Si contiene '51' Y contiene '@c.us', ¡ES UN NÚMERO REAL!
            if '51' in s_cand and '@c.us' in s_cand:
                numero_elegido = s_cand
                print(f"🎯 Número real encontrado en campo oculto: {numero_elegido}")
                break # Ya lo encontramos, dejamos de buscar
        
        # 2. FALLBACK: Si no encontramos ninguno con formato peruano, usamos el 'from'
        if not numero_elegido:
            numero_elegido = limpiar_dato(payload.get('from'))
            print(f"⚠️ No se halló número 51... Usando el por defecto: {numero_elegido}")

        # 3. LIMPIEZA FINAL (Quitar @c.us, :dispositivo, etc)
        try:
            # Quitamos todo lo que esté después del @ o del :
            telefono_limpio = numero_elegido.split('@')[0].split(':')[0]
        except:
            telefono_limpio = "Error_Parsing"

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
            print(f"✅ Guardado: {telefono_limpio}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)