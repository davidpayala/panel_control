from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
import base64
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

# OBTENER VARIABLES
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- DESCARGA DE MEDIA BLINDADA PARA WAHA PLUS ---
def descargar_media_plus(media_url):
    try:
        if not media_url: return None
        
        # Corrección de URL relativa (si viene como /api/files/...)
        url_final = media_url
        if not media_url.startswith("http"):
             base = WAHA_URL.rstrip('/') if WAHA_URL else ""
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        
        print(f"📥 Descargando media: {url_final}")
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=10)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook Activo 🚀. Usa /webhook en WAHA.", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    print("🔵 [WEBHOOK] Solicitud recibida")
    
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        # Normalizar lista de eventos
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            # Solo procesamos mensajes (ignoramos estados, acks, etc.)
            if evento.get('event') != 'message': continue

            # Extracción segura del payload
            payload = evento.get('payload')
            if not payload: continue # Si payload es None, saltamos

            remitente = payload.get('from', '')
            
            # Filtros básicos
            if 'status@broadcast' in remitente: continue

            # Extraer número
            try:
                num = remitente.replace('@c.us', '').replace('@s.whatsapp.net', '')
            except: num = "Desconocido"

            print(f"📩 Procesando mensaje de: {num}")

            # --- CORRECCIÓN 1: EXTRAER MEDIA DE FORMA SEGURA ---
            # El error anterior ocurría aquí. Ahora usamos una lógica a prueba de fallos.
            media_url = payload.get('mediaUrl')
            
            # Si mediaUrl está vacío, buscamos en el objeto 'media'
            if not media_url:
                # El truco: (payload.get('media') or {}) asegura que si es None, usamos {}
                media_obj = payload.get('media') or {}
                media_url = media_obj.get('url')

            body = payload.get('body', '')
            archivo = None
            
            # --- CORRECCIÓN 2: LLAMADA CORRECTA A LA FUNCIÓN ---
            if media_url:
                archivo = descargar_media_plus(media_url) # Nombre corregido
                if archivo and not body: body = "📷 Foto"

            # Guardar en DB
            try:
                norm = normalizar_telefono_maestro(num)
                if not norm: continue
                tel_db = norm['db']

                # Google Contact (Intento simple)
                nombre = payload.get('_data', {}).get('notifyName') or "Cliente"
                try:
                    gdata = buscar_contacto_google(tel_db)
                    if gdata and gdata['encontrado']: nombre = gdata['nombre_completo']
                except: pass

                with engine.connect() as conn:
                    # Guardar Cliente
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, activo, fecha_registro)
                        VALUES (:t, :n, TRUE, NOW())
                        ON CONFLICT (telefono) DO NOTHING
                    """), {"t": tel_db, "n": nombre})
                    
                    # Guardar Mensaje
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                        VALUES (:t, 'ENTRANTE', :c, (NOW() - INTERVAL '5 hours'), FALSE, :d)
                    """), {"t": tel_db, "c": body, "d": archivo})
                    conn.commit()
                    print(f"✅ Mensaje de {tel_db} guardado.")

            except Exception as e:
                print(f"❌ Error DB para {num}: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        # Esto atrapará cualquier otro error futuro sin tumbar el servidor
        print(f"🔥 Error Crítico en Webhook: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)