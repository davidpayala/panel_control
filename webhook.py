from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys # Para forzar logs en Railway
from datetime import datetime
from utils import normalizar_telefono_maestro

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- FUNCIÓN DE LOGGING (Para ver errores en tiempo real) ---
def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

def descargar_media_plus(media_url):
    try:
        if not media_url: return None
        url_final = media_url
        if not media_url.startswith("http"):
             base = WAHA_URL.rstrip('/') if WAHA_URL else ""
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        elif "localhost" in media_url or "waha:" in media_url:
             if WAHA_URL:
                path_real = media_url.split('/api/')[-1]
                base = WAHA_URL.rstrip('/')
                url_final = f"{base}/api/{path_real}"
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        r = requests.get(url_final, headers=headers, timeout=10)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        log_error(f"Media download error: {e}")
        return None

def obtener_datos_mensaje(payload):
    try:
        from_me = payload.get('fromMe', False)
        # Blindaje contra _data nulo
        _data = payload.get('_data') or {}

        if from_me:
            remote_id = payload.get('to')
            tipo = 'SALIENTE'
            push_name = None 
        else:
            remote_id = payload.get('from')
            if not remote_id:
                remote_id = _data.get('id', {}).get('remote')
            tipo = 'ENTRANTE'
            push_name = _data.get('notifyName')

        if remote_id:
            clean_num = remote_id.replace('@c.us', '').replace('@s.whatsapp.net', '')
            if '@g.us' in remote_id: return None, None, None
            return clean_num, tipo, push_name
        return None, None, None
    except: return None, None, None

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V3 (Flush Logs)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
    except Exception as e:
        log_error(f"JSON Error: {e}")
        return jsonify({"status": "error"}), 500

    try:
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            payload = evento.get('payload')
            if not payload: continue

            # Ignorar estados
            remoto = str(payload.get('from', ''))
            if 'status@broadcast' in remoto: continue

            # 1. Procesar Datos
            numero_crudo, tipo_msg, push_name = obtener_datos_mensaje(payload)
            if not numero_crudo: continue

            formatos = normalizar_telefono_maestro(numero_crudo)
            if not formatos:
                log_info(f"⚠️ Ignorado por formato: {numero_crudo}")
                continue
            
            telefono_db = formatos['db']
            
            # 2. Contenido
            body = payload.get('body', '')
            media_url = payload.get('mediaUrl') or payload.get('media', {}).get('url')
            
            archivo_bytes = None
            if media_url:
                log_info(f"📷 Descargando media de {telefono_db}")
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo"

            # 3. Reply
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply

            # 4. Guardar (Sin Google para velocidad)
            try:
                nombre_final = push_name or "Cliente Nuevo"
                
                with engine.connect() as conn:
                    # Upsert Cliente
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO UPDATE SET activo = TRUE
                    """), {"t": telefono_db, "n": nombre_final})
                    
                    # Insert Mensaje
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
                    log_info(f"✅ MENSAJE GUARDADO: {telefono_db}")

            except Exception as e:
                log_error(f"🔥 Error DB escribiendo {telefono_db}: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General Webhook: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)