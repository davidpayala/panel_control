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
def recibir_mensaje():
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

            try:
                num = remitente.replace('@c.us', '').replace('@s.whatsapp.net', '')
            except: num = "Desconocido"

            print(f"📩 Recibido de: {num}")

            # --- LÓGICA DE MEDIA ---
            media_url = payload.get('mediaUrl')
            
            # Buscar URL en estructura anidada si no está en raíz
            if not media_url:
                media_obj = payload.get('media') or {}
                media_url = media_obj.get('url')

            body = payload.get('body', '')
            archivo_bytes = None
            
            # Intentar descargar
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                
                # --- AQUÍ ESTÁ EL CAMBIO CLAVE PARA QUE NO SALGA VACÍO ---
                if archivo_bytes:
                    # Si descargó bien y no hay texto, ponemos etiqueta
                    if not body: body = "📷 Foto"
                else:
                    # SI FALLÓ LA DESCARGA, GUARDAMOS EL ERROR COMO TEXTO
                    # Así verás en el chat qué pasó
                    error_msg = f"⚠️ Error cargando imagen: {media_url}"
                    if body:
                        body += f"\n({error_msg})"
                    else:
                        body = error_msg

            # Guardar en DB
            try:
                norm = normalizar_telefono_maestro(num)
                if not norm: continue
                tel_db = norm['db']

                nombre = payload.get('_data', {}).get('notifyName') or "Cliente"
                # (Opcional: aquí iría tu lógica de Google Contact)

                with engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, activo, fecha_registro)
                        VALUES (:t, :n, TRUE, NOW())
                        ON CONFLICT (telefono) DO NOTHING
                    """), {"t": tel_db, "n": nombre})
                    
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                        VALUES (:t, 'ENTRANTE', :c, (NOW() - INTERVAL '5 hours'), FALSE, :d)
                    """), {"t": tel_db, "c": body, "d": archivo_bytes})
                    conn.commit()
                    print(f"✅ Guardado mensaje de {tel_db}")

            except Exception as e:
                print(f"❌ Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"🔥 Error Webhook: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)