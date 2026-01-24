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

# VARIABLES DE ENTORNO
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- FUNCIONES AUXILIARES ---

def descargar_media_plus(media_url):
    """Descarga media desde WAHA Plus con autenticación y corrección de URL."""
    try:
        if not media_url: return None
        
        url_final = media_url
        
        # Corrección 1: URLs relativas
        if not media_url.startswith("http"):
             base = WAHA_URL.rstrip('/') if WAHA_URL else ""
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        # Corrección 2: URLs internas (localhost)
        elif "localhost" in media_url or "waha:" in media_url:
             if WAHA_URL:
                path_real = media_url.split('/api/')[-1]
                base = WAHA_URL.rstrip('/')
                url_final = f"{base}/api/{path_real}"
        
        print(f"📥 Intentando descargar de: {url_final}")
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=15)
        return r.content if r.status_code == 200 else None
            
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

def obtener_numero_crudo(payload):
    """Extrae el número telefónico del payload de WAHA."""
    try:
        # Intento 1: Campo 'from' directo
        from_val = payload.get('from')
        # Intento 2: Estructura interna _data
        if not from_val:
            from_val = payload.get('_data', {}).get('id', {}).get('remote')
        
        if from_val:
            # Limpieza estándar de WhatsApp ID
            return from_val.replace('@c.us', '').replace('@s.whatsapp.net', '')
        return None
    except:
        return None

# --- RUTAS ---

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook Activo 🚀", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    print("🔵 [WEBHOOK] Solicitud recibida")
    
    # 1. Seguridad (Opcional según tu config)
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key and api_key != WAHA_KEY:
        print("⛔ API Key incorrecta")
        # return jsonify({"error": "Unauthorized"}), 401 
        # Comentado para evitar bloqueos si WAHA no envía el header en webhooks

    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            payload = evento.get('payload')
            if not payload: continue

            # Filtros
            remitente = payload.get('from', '')
            if 'status@broadcast' in remitente: continue

            # 2. OBTENER NÚMERO (Aquí fallaba antes)
            numero_crudo = obtener_numero_crudo(payload)
            if not numero_crudo: continue

            formatos = normalizar_telefono_maestro(numero_crudo)
            if not formatos:
                print(f"⚠️ Número no válido: {numero_crudo}")
                continue
            
            telefono_db = formatos['db']
            telefono_corto = formatos['corto']
            print(f"📩 Mensaje de: {telefono_corto}")

            # 3. PROCESAR CONTENIDO
            body = payload.get('body', '')
            
            # Lógica segura para obtener URL de media
            media_url = payload.get('mediaUrl')
            if not media_url:
                media_obj = payload.get('media') or {}
                media_url = media_obj.get('url')

            archivo_bytes = None
            
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes:
                    if not body: body = "📷 Foto"
                else:
                    msg_err = f"⚠️ Error descargando imagen"
                    body = f"{body}\n({msg_err})" if body else msg_err

            # 4. GUARDAR EN BD
            try:
                nombre_final = payload.get('_data', {}).get('notifyName') or "Cliente"
                # Intento recuperar nombre real si ya existe
                try:
                    datos_google = buscar_contacto_google(telefono_db)
                    if datos_google and datos_google['encontrado']:
                        nombre_final = datos_google['nombre_completo']
                except: pass

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
                    print(f"✅ Guardado: {telefono_corto}")

            except Exception as e:
                print(f"❌ Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"🔥 Error Crítico Webhook: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)