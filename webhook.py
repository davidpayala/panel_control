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
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=15)
        return r.content if r.status_code == 200 else None
            
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

def obtener_datos_mensaje(payload):
    """
    Determina quién es el 'Otro' (Cliente) y la dirección del mensaje.
    BLINDADO CONTRA ERRORES NONETYPE.
    """
    try:
        # 1. ¿Lo envié yo?
        from_me = payload.get('fromMe', False)
        
        # Aseguramos que _data sea un diccionario, incluso si viene None
        _data = payload.get('_data') or {}

        if from_me:
            # SI LO ENVIÉ YO (Saliente)
            remote_id = payload.get('to')
            tipo = 'SALIENTE'
            push_name = None 
        else:
            # ME LO ENVIARON (Entrante)
            remote_id = payload.get('from')
            # Fallback seguro
            if not remote_id:
                remote_id = _data.get('id', {}).get('remote')
            
            tipo = 'ENTRANTE'
            push_name = _data.get('notifyName')

        if remote_id:
            # Limpiar sufijos de WhatsApp
            clean_num = remote_id.replace('@c.us', '').replace('@s.whatsapp.net', '')
            if '@g.us' in remote_id: return None, None, None
            
            return clean_num, tipo, push_name
            
        return None, None, None

    except Exception as e:
        print(f"⚠️ Error extrayendo datos: {e}")
        return None, None, None

# --- RUTAS ---

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook Activo 🚀 v2.2 (Anti-Crash)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # Validación API Key (Opcional)
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key and api_key != WAHA_KEY:
        pass 

    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            payload = evento.get('payload')
            if not payload: continue

            # 1. IGNORAR ESTADOS
            if 'status@broadcast' in str(payload.get('from')) or 'status@broadcast' in str(payload.get('to')):
                continue

            # 2. DETERMINAR CLIENTE
            numero_crudo, tipo_msg, push_name = obtener_datos_mensaje(payload)
            if not numero_crudo: continue

            # 3. NORMALIZAR NÚMERO
            formatos = normalizar_telefono_maestro(numero_crudo)
            if not formatos:
                continue
            
            telefono_db = formatos['db']
            telefono_corto = formatos['corto']
            
            print(f"📩 Procesando: {telefono_corto} | Tipo: {tipo_msg}")

            # 4. GESTIÓN DE CONTENIDO Y MEDIA
            body = payload.get('body', '')
            
            media_url = payload.get('mediaUrl')
            if not media_url:
                media_obj = payload.get('media') or {}
                media_url = media_obj.get('url')

            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes:
                    if not body: body = "📷 Archivo Multimedia"
                else:
                    msg_err = f"⚠️ Error descargando imagen"
                    body = f"{body}\n({msg_err})" if body else msg_err

            # --- 5. EXTRAER DATOS DEL REPLY ---
            reply_id = None
            reply_content = None
            
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply
            # ----------------------------------

            # 6. GUARDAR EN BASE DE DATOS
            try:
                nombre_final = push_name or "Cliente"
                
                # Intentar recuperar nombre de Google si es entrante
                if tipo_msg == 'ENTRANTE' and nombre_final == "Cliente":
                     try:
                        datos_google = buscar_contacto_google(telefono_db)
                        if datos_google and datos_google['encontrado']:
                            nombre_final = datos_google['nombre_completo']
                     except: pass

                with engine.connect() as conn:
                    # A) Upsert Cliente
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO UPDATE SET activo = TRUE
                    """), {"t": telefono_db, "n": nombre_final})
                    
                    # B) Insertar Mensaje (CON REPLY)
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content)
                        VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody)
                    """), {
                        "t": telefono_db, 
                        "tipo": tipo_msg, 
                        "txt": body, 
                        "leido": (tipo_msg == 'SALIENTE'), 
                        "d": archivo_bytes,
                        "wid": payload.get('id'),
                        "rid": reply_id,
                        "rbody": reply_content
                    })
                    
                    conn.commit()
                    print(f"✅ {telefono_corto}: Guardado OK.")

            except Exception as e:
                print(f"❌ Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"🔥 Error Crítico Webhook: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)