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

def limpiar_numero(valor_raw):
    """
    Extrae el texto de cualquier objeto y lo limpia hasta dejar solo el número.
    Ej: "51999@s.whatsapp.net" -> "51999"
    Ej: {'user': '51999'} -> "51999"
    """
    if not valor_raw: return ""
    
    # 1. Si es objeto, sacamos texto
    texto = str(valor_raw)
    if isinstance(valor_raw, dict):
        texto = str(valor_raw.get('user') or valor_raw.get('_serialized') or "")
        
    # 2. Cortamos basura (@c.us, @lid, :8, @s.whatsapp.net)
    return texto.split('@')[0].split(':')[0]

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # ==================================================================
        # 🎯 LÓGICA DE SELECCIÓN DE NÚMERO (PRIORIDAD AL 'ALT')
        # ==================================================================
        
        # 1. Extraemos posibles candidatos de todos los rincones
        # Tu descubrimiento: _data.key.remoteJidAlt
        candidato_alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
        
        # Los normales
        candidato_participant = payload.get('participant')
        candidato_author = payload.get('author')
        candidato_from = payload.get('from') # Este suele ser el LID malo
        
        # Lista en orden de prioridad
        candidatos = [candidato_alt, candidato_participant, candidato_author, candidato_from]
        
        numero_final = "Desconocido"
        
        for cand in candidatos:
            limpio = limpiar_numero(cand)
            # REGLA: Debe empezar con 51 (Perú) y tener longitud de celular (aprox 11)
            # Ignoramos los que empiezan con 319 (LID de empresas)
            if limpio.startswith('51') and len(limpio) >= 11:
                numero_final = limpio
                print(f"✅ Número real detectado desde origen oculto: {numero_final}")
                break
        
        # Fallback: Si no encontramos ningún 51, usamos el 'from' (aunque sea LID)
        if numero_final == "Desconocido":
            numero_final = limpiar_numero(candidato_from)

        # ==================================================================

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
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": numero_final, "txt": body, "data": archivo_bytes})
                conn.commit()
            print(f"💾 Guardado en DB: {numero_final}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)