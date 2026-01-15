from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json

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

def limpiar_numero(valor_raw):
    if not valor_raw: return ""
    texto = str(valor_raw)
    if isinstance(valor_raw, dict):
        texto = str(valor_raw.get('user') or valor_raw.get('_serialized') or "")
    return texto.split('@')[0].split(':')[0]

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # 1. LÓGICA DE RECUPERACIÓN DE NÚMERO (La que ya funciona)
        candidato_alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
        candidatos = [candidato_alt, payload.get('participant'), payload.get('author'), payload.get('from')]
        
        numero_final = "Desconocido"
        for cand in candidatos:
            limpio = limpiar_numero(cand)
            if limpio.startswith('51') and len(limpio) >= 11:
                numero_final = limpio
                break
        
        if numero_final == "Desconocido":
            numero_final = limpiar_numero(payload.get('from'))

        # 2. CAPTURAR NOMBRE DE WHATSAPP (NotifyName)
        # Esto es el nombre que el usuario se puso en su perfil
        nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or "Cliente Nuevo"

        # 3. GUARDAR MENSAJE
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        archivo_bytes = None
        
        if has_media:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            if media_url: archivo_bytes = descargar_media(media_url)
            body = "📷 Archivo" if archivo_bytes else "⚠️ Error media"

        try:
            with engine.connect() as conn:
                # A. Insertar mensaje
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": numero_final, "txt": body, "data": archivo_bytes})
                
                # B. Asegurar que el cliente exista (UPSERT LIGERO)
                # Si el numero no existe en clientes, lo crea con el nombre de WhatsApp.
                # Si ya existe, NO toca el nombre (para respetar si tú lo editaste manualmente).
                conn.execute(text("""
                    INSERT INTO Clientes (nombre, telefono, activo, fecha_creacion)
                    VALUES (:nom, :tel, TRUE, (NOW() - INTERVAL '5 hours'))
                    ON CONFLICT (telefono) DO NOTHING
                """), {"nom": nombre_wsp, "tel": numero_final})
                
                conn.commit()
            print(f"✅ Guardado: {numero_final} ({nombre_wsp})")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)