from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz # Asegúrate de tener pytz en requirements.txt, si no usa datetime normal

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

def limpiar_numero_full(valor_raw):
    """Devuelve el número COMPLETO con 51 (ej: 51986203398)"""
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
        
        # 1. OBTENER NÚMERO (Usando la lógica Alt que ya funcionaba)
        candidato_alt = payload.get('_data', {}).get('key', {}).get('remoteJidAlt')
        candidatos = [candidato_alt, payload.get('participant'), payload.get('author'), payload.get('from')]
        
        numero_full = "Desconocido"
        for cand in candidatos:
            limpio = limpiar_numero_full(cand)
            if limpio.startswith('51') and len(limpio) >= 11:
                numero_full = limpio
                break
        
        if numero_full == "Desconocido":
            numero_full = limpiar_numero_full(payload.get('from'))

        # 2. PREPARAR DATOS PARA TABLA CLIENTES
        # A. Numero Corto (986203398)
        if numero_full.startswith("51") and len(numero_full) == 11:
            numero_corto = numero_full[2:] 
        else:
            numero_corto = numero_full # Por si acaso

        # B. Nombre WhatsApp (PushName)
        nombre_wsp = payload.get('_data', {}).get('notifyName') or payload.get('pushName') or ""
        
        # C. Fecha Hoy (2026-01-15)
        # Usamos zona horaria Perú si es posible, sino UTC
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')

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
                # INSERT MENSAJE
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": numero_full, "txt": body, "data": archivo_bytes})
                
                # INSERT / UPDATE CLIENTE (Tus reglas específicas)
                # Solo insertamos si NO existe. Si existe, no tocamos nada (ON CONFLICT DO NOTHING)
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
                        :tel,          -- telefono (con 51)
                        :cod,          -- codigo_contacto (con 51)
                        :corto,        -- nombre_corto (9 digitos)
                        :nom_wsp,      -- nombre (Wsp placeholder)
                        'WhatsApp',    -- medio_contacto
                        'Sin empezar', -- estado
                        :fec,          -- fecha_seguimiento (YYYY-MM-DD)
                        TRUE,
                        (NOW() - INTERVAL '5 hours')
                    )
                    ON CONFLICT (telefono) DO NOTHING
                """), {
                    "tel": numero_full,
                    "cod": numero_full,
                    "corto": numero_corto,
                    "nom_wsp": nombre_wsp,
                    "fec": fecha_hoy
                })
                
                conn.commit()
            print(f"✅ Guardado: {numero_full} ({numero_corto})")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)