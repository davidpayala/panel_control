from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

# OBTENER VARIABLES
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def descargar_media_plus(media_url):
    """
    Intenta descargar la imagen corrigiendo la URL si es necesario.
    """
    try:
        if not media_url: return None
        
        url_final = media_url
        
        # 1. Corrección de URL:
        # Si WAHA nos da una URL interna (localhost o waha:3000), 
        # la forzamos a usar la URL PÚBLICA que definiste en las variables.
        if WAHA_URL:
            # Si es ruta relativa (/api/files...)
            if not media_url.startswith("http"):
                base = WAHA_URL.rstrip('/')
                path = media_url.lstrip('/')
                url_final = f"{base}/{path}"
            
            # OJO: Si viene como http://waha... o http://localhost...
            # Reemplazamos el dominio por tu WAHA_URL público para asegurar conexión
            elif "localhost" in media_url or "waha:" in media_url:
                # Esto es un hack para Railway: Reemplazar el inicio por el dominio público
                # Ej: http://localhost:3000/api/files/x -> https://mi-waha.railway/api/files/x
                path_real = media_url.split('/api/')[-1] # Extraer lo que sigue a /api/
                base = WAHA_URL.rstrip('/')
                url_final = f"{base}/api/{path_real}"

        print(f"📥 Intentando descargar desde: {url_final}")
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=15)
        
        if r.status_code == 200:
            print(f"✅ Descarga OK: {len(r.content)} bytes")
            return r.content
        else:
            print(f"❌ Falló descarga ({r.status_code}): {r.text[:100]}")
            return None
            
    except Exception as e:
        print(f"⚠️ Excepción en descarga: {e}")
        return None

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook Activo. Estado: OK", 200

@app.route('/webhook', methods=['POST'])
# ... (imports y resto del código igual) ...

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # 1. LOG DE DIAGNÓSTICO
    print("🔵 [WEBHOOK] Solicitud recibida")
    
    # 2. Seguridad
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key and api_key != WAHA_KEY:
        print("⛔ API Key incorrecta")
        return jsonify({"error": "Unauthorized"}), 401

    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            payload = evento.get('payload')
            if not payload: continue

            # Validar remitente
            remitente = payload.get('from', '')
            if 'status@broadcast' in remitente: continue

            # Obtener número
            numero_crudo = obtener_numero_crudo(payload)
            formatos = normalizar_telefono_maestro(numero_crudo)
            if not formatos: continue
            
            telefono_db = formatos['db']
            telefono_corto = formatos['corto']
            
            print(f"📩 Mensaje de: {telefono_corto}")

            # --- CORRECCIÓN DEL ERROR DE MEDIA ---
            media_url = payload.get('mediaUrl')
            
            # PROTECCIÓN CONTRA NULOS AQUÍ:
            if not media_url:
                # Usamos ( ... or {} ) para asegurar que sea un diccionario antes del .get()
                media_obj = payload.get('media') or {} 
                media_url = media_obj.get('url')

            body = payload.get('body', '')
            archivo_bytes = None
            
            # Descargar si hay URL
            if media_url:
                archivo_bytes = descargar_media(media_url)
                if archivo_bytes and not body: 
                    body = "📷 Archivo Multimedia"

            # Guardar en DB (Tu lógica habitual)
            try:
                nombre_final = payload.get('_data', {}).get('notifyName') or "Cliente"
                # (Aquí iría tu lógica de Google Contact si la usas)

                with engine.connect() as conn:
                    # Upsert Cliente
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO UPDATE SET activo = TRUE
                    """), {"t": telefono_db, "n": nombre_final})
                    
                    # Insertar Mensaje
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                        VALUES (:t, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :d)
                    """), {"t": telefono_db, "txt": body, "d": archivo_bytes})
                    
                    conn.commit()
                    print(f"✅ Guardado mensaje de {telefono_corto}")

            except Exception as e:
                print(f"❌ Error DB: {e}")

        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        print(f"🔥 Error Crítico Webhook: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)