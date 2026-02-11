from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys
import json
from datetime import datetime
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# --- 🚑 PARCHE DB: RECONSTRUCCIÓN NUCLEAR ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            # 1. Asegurar tablas básicas
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS session_name VARCHAR(50)"))
            
            # 2. Tabla Logs
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS webhook_logs (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    session_name VARCHAR(50),
                    event_type VARCHAR(50),
                    payload TEXT
                )
            """))

            # 3. VERIFICACIÓN Y RECONSTRUCCIÓN DE SYNC_ESTADO
            try:
                # Intentamos leer la columna version. Si falla, salta al except.
                conn.execute(text("SELECT version FROM sync_estado LIMIT 1"))
            except Exception:
                log_info("⚠️ Tabla sync_estado antigua detectada. Reconstruyendo...")
                # Si falla, borramos la tabla vieja y la creamos bien
                conn.execute(text("DROP TABLE IF EXISTS sync_estado"))
                conn.execute(text("""
                    CREATE TABLE sync_estado (
                        id INT PRIMARY KEY,
                        version INT DEFAULT 0
                    )
                """))
                conn.execute(text("INSERT INTO sync_estado (id, version) VALUES (1, 0)"))
                log_info("✅ Tabla sync_estado reconstruida correctamente.")

    except Exception as e:
        log_error(f"Error en parche DB: {e}")

aplicar_parche_db()

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
    except: return None

def obtener_identidad(payload, session):
    try:
        from_me = payload.get('fromMe', False)
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        routing_id = None
        if from_me: routing_id = payload.get('to')
        else: routing_id = payload.get('from')

        if not routing_id: routing_id = key.get('remoteJid')
        if not routing_id: routing_id = payload.get('participant')

        if not routing_id: return None

        telefono_limpio = "".join(filter(str.isdigit, routing_id.split('@')[0]))
        es_grupo = '@g.us' in routing_id

        return {
            "id_canonico": routing_id,
            "telefono": telefono_limpio,
            "es_grupo": es_grupo
        }
    except Exception as e:
        log_error(f"Error identidad: {e}")
        return None

@app.route('/', methods=['GET'])
def home():
    return "Webhook V36 (Sync Fix) ✅", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        # --- LOGGING DE DIAGNÓSTICO (Con manejo de errores) ---
        try:
            with engine.begin() as conn:
                item = data[0] if isinstance(data, list) else data
                p_str = json.dumps(item, ensure_ascii=False)
                # Recortamos payload si es gigante para evitar errores
                if len(p_str) > 10000: p_str = p_str[:10000] + "...(truncado)"
                
                conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                            {"s": item.get('session', 'unk'), "e": item.get('event', 'unk'), "p": p_str})
                
                # Limpieza automática
                conn.execute(text("DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 50)"))
        except Exception as e:
            log_error(f"Fallo al guardar log de diagnóstico: {e}")
        # ------------------------------------------------------

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            tipo_evento = evento.get('event')
            session_name = evento.get('session', 'default')
            payload = evento.get('payload', {})

            # 1. ACKS
            if tipo_evento == 'message.ack':
                msg_id = payload.get('id')
                ack_status = payload.get('ack') 
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                try:
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), {"e": nuevo_estado, "w": msg_id})
                        conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
                except: pass
                continue 

            # 2. MENSAJES
            if tipo_evento not in ['message', 'message.any', 'message.created']: continue
            if payload.get('from') == 'status@broadcast': continue

            identidad = obtener_identidad(payload, session_name)
            if not identidad: continue

            routing_id = identidad['id_canonico']
            telefono_msg = identidad['telefono']
            # es_grupo = identidad['es_grupo'] # No usado por ahora

            body = payload.get('body', '')
            media_url = payload.get('mediaUrl') or (payload.get('media') or {}).get('url')
            archivo_bytes = descargar_media_plus(media_url) if media_url else None
            if archivo_bytes and not body: body = "📷 Archivo Multimedia"
            
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')

            tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
            whatsapp_id = payload.get('id')
            push_name = (payload.get('_data') or {}).get('notifyName', 'Cliente')
            
            log_info(f"📩 Procesando: {telefono_msg}")

            try:
                with engine.begin() as conn:
                    # A. Cliente
                    cliente_existente = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id = :wid OR telefono = :t"), {"wid": routing_id, "t": telefono_msg}).fetchone()

                    if cliente_existente:
                        conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :wid, activo=TRUE WHERE id_cliente = :id"), {"wid": routing_id, "id": cliente_existente.id_cliente})
                    else:
                        conn.execute(text("INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, estado, activo, fecha_registro) VALUES (:t, :wid, :n, 'Sin empezar', TRUE, NOW())"), {"t": telefono_msg, "wid": routing_id, "n": push_name})

                    # B. Mensaje
                    existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                    
                    if not existe:
                        conn.execute(text("""
                            INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content, estado_waha, session_name)
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                        """), {
                            "t": telefono_msg, "tipo": tipo_msg, "txt": body, "leido": (tipo_msg == 'SALIENTE'), "d": archivo_bytes,
                            "wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado', "sess": session_name
                        })
                    
                    # C. Notificar cambio
                    conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))

            except Exception as e:
                log_error(f"🔥 Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)