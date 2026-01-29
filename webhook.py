from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
import base64
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro # Quitamos buscar_contacto_google por ahora

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

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
        r = requests.get(url_final, headers=headers, timeout=10) # Timeout corto
        return r.content if r.status_code == 200 else None
    except: return None

def obtener_datos_mensaje(payload):
    try:
        from_me = payload.get('fromMe', False)
        if from_me:
            remote_id = payload.get('to')
            tipo = 'SALIENTE'
            push_name = None 
        else:
            remote_id = payload.get('from')
            if not remote_id:
                remote_id = payload.get('_data', {}).get('id', {}).get('remote')
            tipo = 'ENTRANTE'
            push_name = payload.get('_data', {}).get('notifyName')

        if remote_id:
            return remote_id, tipo, push_name
        return None, None, None
    except: return None, None, None

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V3 (Debug Mode)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # 1. LOG INICIAL
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        # Imprimir para depurar en Railway
        print(f"📥 Payload recibido: {len(str(data))} bytes")
    except Exception as e:
        print(f"❌ Error leyendo JSON: {e}")
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

            # 2. PROCESAMIENTO
            raw_id, tipo_msg, push_name = obtener_datos_mensaje(payload)
            
            if not raw_id:
                print("⚠️ Saltado: No se pudo extraer ID")
                continue

            # 3. NORMALIZACIÓN
            formatos = normalizar_telefono_maestro(raw_id)
            if not formatos:
                print(f"⚠️ Rechazado por normalizador: {raw_id}")
                continue
            
            telefono_db = formatos['db']
            
            # 4. CONTENIDO
            body = payload.get('body', '')
            media_url = payload.get('mediaUrl')
            if not media_url:
                media_url = payload.get('media', {}).get('url')

            archivo_bytes = None
            if media_url:
                print(f"📷 Descargando media para {telefono_db}...")
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo"

            # 5. REPLY
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply

            # 6. GUARDAR (Sin Google Search para evitar bloqueos)
            try:
                # Nombre por defecto si no hay pushName
                nombre_final = push_name or "Cliente Nuevo"
                
                # --- COMENTADO TEMPORALMENTE PARA EVITAR TIMEOUTS ---
                # if tipo_msg == 'ENTRANTE' and nombre_final == "Cliente Nuevo":
                #    try:
                #       datos = buscar_contacto_google(telefono_db)
                #       if datos and datos['encontrado']: nombre_final = datos['nombre_completo']
                #    except: pass
                # ----------------------------------------------------

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
                    print(f"✅ MENSAJE GUARDADO: {telefono_db}")

            except Exception as e:
                print(f"🔥 Error DB al guardar {telefono_db}: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"🔥 Error General Webhook: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)