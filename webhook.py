from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests

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
    
    # ---------------------------------------------------------
    # 🕵️‍♂️ DEBUG: Descomenta esto si sigue fallando para ver qué llega
    # print(f"📩 PAYLOAD RAW: {data}", flush=True) 
    # ---------------------------------------------------------

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # --- CORRECCIÓN DE NÚMEROS DE EMPRESA (LID) ---
        sender_raw = payload.get('from', '')
        
        # A veces las empresas mandan desde '12345@lid'. Eso no sirve para responder.
        # El número real suele venir en 'author' o 'participant'.
        if '@lid' in sender_raw:
            # Intentamos buscar el número real en otros campos
            numero_alternativo = payload.get('author') or payload.get('participant')
            if numero_alternativo:
                print(f"🔄 Corrigiendo ID de Empresa: Cambiando {sender_raw} por {numero_alternativo}")
                sender_raw = numero_alternativo

        # --- LIMPIEZA ESTÁNDAR ---
        # 1. Quitar dominio (@c.us, @s.whatsapp.net, @lid)
        sender = sender_raw.split('@')[0]
        
        # 2. Quitar sufijo de dispositivo (:8, :24)
        if ':' in sender:
            sender = sender.split(':')[0]

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
                    tipo_icono = "📷 Imagen" if "image" in mimetype else "📎 Archivo"
                    body = f"{tipo_icono} recibida"
                else:
                    body = "⚠️ Error descargando imagen"
            else:
                body = "📷 [Imagen] (URL no disponible)"

        # Guardar en Base de Datos
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {
                    "tel": sender, 
                    "txt": body,
                    "data": archivo_bytes
                })
                conn.commit()
            print(f"✅ Mensaje de {sender} guardado.")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)