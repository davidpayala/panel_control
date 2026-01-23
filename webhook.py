from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
import base64
from datetime import datetime
import pytz 
# Importamos tus utilidades
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
             base = WAHA_URL.rstrip('/')
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        
        print(f"📥 Descargando: {url_final}")
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=10)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

# --- RUTAS DE DIAGNÓSTICO ---

@app.route('/', methods=['GET', 'POST'])
def home():
    # Esta ruta captura si pusiste la URL sin "/webhook" al final
    print("⚠️ ALERTA: WAHA está enviando a la RAÍZ ('/') en lugar de '/webhook'")
    return "Hola WAHA, cambia la URL a /webhook", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    print("🔵 [WEBHOOK RECIBIDO]") 
    
    # 1. DIAGNÓSTICO DE SEGURIDAD (Solo imprimimos, no bloqueamos)
    api_key_recibida = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key_recibida != WAHA_KEY:
        print(f"⚠️ AVISO: Clave recibida '{api_key_recibida}' no coincide con WAHA_KEY local.")
        # NO retornamos 403/401 para permitir que pase y ver si funciona la lógica
    
    data = request.json
    if not data: return jsonify({"status": "empty"}), 200

    # Normalizar lista de eventos
    eventos = data if isinstance(data, list) else [data]

    for evento in eventos:
        if evento.get('event') != 'message': continue

        payload = evento.get('payload', {})
        remitente = payload.get('from', '')
        
        # Filtros básicos
        if 'status@broadcast' in remitente: continue

        # Extraer número
        try:
            num = remitente.replace('@c.us', '').replace('@s.whatsapp.net', '')
        except: num = "Desconocido"

        print(f"📩 Mensaje de: {num}")

        # --- GUARDADO EN DB ---
        try:
            # Procesar Media
            body = payload.get('body', '')
            media_url = payload.get('mediaUrl') or payload.get('media', {}).get('url')
            archivo = None
            
            if media_url:
                archivo = descargar_media_plus(media_url)
                if archivo and not body: body = "📷 Foto"

            # Normalizar número para la DB
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
                print("✅ Guardado en DB")

        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)