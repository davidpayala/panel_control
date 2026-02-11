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

# --- SISTEMA DE LOGS (Para ver errores en Railway) ---
def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# --- 🚑 PARCHE DB: Asegura que las tablas existan ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS session_name VARCHAR(50)"))
            
            # Tabla de logs para depuración interna
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS webhook_logs (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    session_name VARCHAR(50),
                    event_type VARCHAR(50),
                    payload TEXT
                )
            """))

            # Tabla de versión para sincronización
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_estado (
                    id INT PRIMARY KEY,
                    version INT DEFAULT 0
                )
            """))
            conn.execute(text("""
                INSERT INTO sync_estado (id, version)
                VALUES (1, 0) ON CONFLICT (id) DO NOTHING
            """))
        log_info("Base de datos parcheada correctamente.")
    except Exception as e:
        log_error(f"Error parcheando DB: {e}")

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

# --- RUTA DE PRUEBA: Verifica si el Webhook está vivo ---
@app.route('/', methods=['GET'])
def home():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return "Webhook Activo y DB Conectada ✅", 200
    except Exception as e:
        return f"Webhook Activo pero DB Falla ❌: {e}", 500

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: 
            log_error("Recibido POST sin datos JSON")
            return jsonify({"status": "empty"}), 200
        
        # log_info(f"Payload recibido: {str(data)[:200]}...") # Loguea el inicio del mensaje
        
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            tipo_evento = evento.get('event')
            session_name = evento.get('session', 'default')
            payload = evento.get('payload', {})

            # 1. ACKS (Doble Check)
            if tipo_evento == 'message.ack':
                msg_id = payload.get('id')
                ack_status = payload.get('ack') 
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                try:
                    with engine.begin() as conn: # Usamos begin() para autocommit
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), 
                                    {"e": nuevo_estado, "w": msg_id})
                        conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
                except Exception as e:
                    log_error(f"Error guardando ACK: {e}")
                continue 

            # 2. MENSAJES NUEVOS
            if tipo_evento not in ['message', 'message.any', 'message.created']: 
                continue
            
            if payload.get('from') == 'status@broadcast': 
                continue

            # Extraer datos básicos
            routing_id = payload.get('from') # Quien envía
            if payload.get('fromMe'): routing_id = payload.get('to') # Si lo envié yo, el ID es el destino
            
            if not routing_id: 
                log_error("Mensaje sin ID de origen/destino")
                continue

            telefono_msg = "".join(filter(str.isdigit, routing_id.split('@')[0]))
            body = payload.get('body', '')
            
            # Descargar multimedia si existe
            media_url = payload.get('mediaUrl') or (payload.get('media') or {}).get('url')
            archivo_bytes = descargar_media_plus(media_url) if media_url else None
            if archivo_bytes and not body: body = "📷 Archivo Multimedia"
            
            # Identificar respuestas
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')

            tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
            whatsapp_id = payload.get('id')
            push_name = (payload.get('_data') or {}).get('notifyName', 'Cliente')
            
            log_info(f"Procesando mensaje de {telefono_msg} ({tipo_msg}): {body[:30]}")

            try:
                with engine.begin() as conn: # Transacción segura
                    # A. Gestionar Cliente
                    cliente_existente = conn.execute(
                        text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :wid OR telefono = :t"), 
                        {"wid": routing_id, "t": telefono_msg}
                    ).fetchone()

                    if cliente_existente:
                        # Si ya existe, actualizamos su ID interno y lo activamos
                        if not cliente_existente.telefono: # Caso raro
                             conn.execute(text("UPDATE Clientes SET telefono=:t WHERE id_cliente=:id"), {"t": telefono_msg, "id": cliente_existente.id_cliente})
                        
                        conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :wid, activo=TRUE WHERE id_cliente = :id"), 
                                     {"wid": routing_id, "id": cliente_existente.id_cliente})
                    else:
                        # Crear nuevo cliente
                        log_info(f"Creando nuevo cliente: {telefono_msg}")
                        conn.execute(text("""
                            INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, estado, activo, fecha_registro)
                            VALUES (:t, :wid, :n, 'Sin empezar', TRUE, NOW())
                        """), {"t": telefono_msg, "wid": routing_id, "n": push_name})

                    # B. Guardar Mensaje
                    existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                    
                    if not existe:
                        conn.execute(text("""
                            INSERT INTO mensajes (
                                telefono, tipo, contenido, fecha, leido, archivo_data, 
                                whatsapp_id, reply_to_id, reply_content, estado_waha, session_name
                            )
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                        """), {
                            "t": telefono_msg, 
                            "tipo": tipo_msg, 
                            "txt": body, 
                            "leido": (tipo_msg == 'SALIENTE'), 
                            "d": archivo_bytes,
                            "wid": whatsapp_id,
                            "rid": reply_id,
                            "rbody": reply_content,
                            "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado',
                            "sess": session_name
                        })
                        log_info("Mensaje guardado en DB.")
                    
                    # C. Actualizar Versión (Para que Streamlit recargue)
                    conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))

            except Exception as e:
                log_error(f"🔥 Error DB escribiendo mensaje: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General Webhook: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)