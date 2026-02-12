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

# --- 🚑 PARCHE DB ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS session_name VARCHAR(50)"))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS webhook_logs (
                    id SERIAL PRIMARY KEY,
                    fecha TIMESTAMP DEFAULT NOW(),
                    session_name VARCHAR(50),
                    event_type VARCHAR(50),
                    payload TEXT
                )
            """))

            # Tabla Sync
            try:
                conn.execute(text("SELECT version FROM sync_estado LIMIT 1"))
            except Exception:
                conn.execute(text("DROP TABLE IF EXISTS sync_estado"))
                conn.execute(text("CREATE TABLE sync_estado (id INT PRIMARY KEY, version INT DEFAULT 0)"))
                conn.execute(text("INSERT INTO sync_estado (id, version) VALUES (1, 0)"))

    except Exception as e:
        log_error(f"Error parche DB: {e}")

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

# --- 🕵️ DETECTIVE DE IDENTIDAD V38 (UNIFICADOR) ---
def obtener_identidad(payload, session):
    try:
        from_me = payload.get('fromMe', False)
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        # 1. Identificar el ID "Técnico" (Puede ser LID o Normal)
        routing_id = None
        if from_me: routing_id = payload.get('to')
        else: routing_id = payload.get('from')

        if not routing_id: routing_id = key.get('remoteJid')
        if not routing_id: routing_id = payload.get('participant')

        if not routing_id: return None

        # 2. BUSCAR EL NÚMERO REAL (La verdad absoluta)
        # Incluso si el routing_id es un @lid, el número real suele estar en _data['id']['user']
        # o en el propio routing_id si es @c.us
        
        numero_real = ""
        
        # Intento A: Sacarlo del ID si es normal
        if "@s.whatsapp.net" in routing_id or "@c.us" in routing_id:
            numero_real = routing_id.split('@')[0]
            
        # Intento B: Si es LID, buscar en los metadatos ocultos
        elif "@lid" in routing_id:
            # WAHA suele exponer el número real en _data -> id -> user
            user_oculto = _data.get('id', {}).get('user')
            if user_oculto and str(user_oculto).isdigit():
                numero_real = str(user_oculto)
            else:
                # Si falla, intentamos buscar en participants si es grupo, o fallamos
                # Si no encontramos el número real, no podemos unificar, usamos el LID como "número" temporal
                numero_real = routing_id.split('@')[0] 

        # Limpieza final (solo dígitos)
        telefono_limpio = "".join(filter(str.isdigit, numero_real))
        es_grupo = '@g.us' in routing_id

        return {
            "id_canonico": routing_id,  # El ID técnico para responder (puede ser LID)
            "telefono": telefono_limpio, # El ID humano para agrupar (Siempre 519...)
            "es_grupo": es_grupo
        }
    except Exception as e:
        log_error(f"Error identidad: {e}")
        return None

@app.route('/', methods=['GET'])
def home():
    return "Webhook V38 (Phone Unifier) ✅", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
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

            routing_id = identidad['id_canonico'] # Puede ser LID
            telefono_msg = identidad['telefono']  # SIEMPRE es el número (519...)
            
            # Si no pudimos sacar un teléfono válido, saltamos para no ensuciar la DB
            if not telefono_msg or len(telefono_msg) < 5: 
                log_error(f"Teléfono inválido extraído: {telefono_msg}")
                continue

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
            
            log_info(f"📩 Procesando: {telefono_msg} (ID Técnico: {routing_id})")

            try:
                with engine.begin() as conn:
                    # A. LÓGICA DE UNIFICACIÓN DE CLIENTE
                    # Buscamos el cliente POR SU NÚMERO DE TELÉFONO, ignorando el ID técnico
                    cliente_existente = conn.execute(
                        text("SELECT id_cliente, whatsapp_internal_id FROM Clientes WHERE telefono = :t"), 
                        {"t": telefono_msg}
                    ).fetchone()

                    if cliente_existente:
                        # Ya existe el cliente con ese número.
                        # Solo actualizamos el whatsapp_internal_id si el que tenemos es viejo o nulo.
                        # PREFERENCIA: Si el ID actual en DB es LID y llega uno normal, ¿cambiamos?
                        # Por ahora, mantenemos el ID más reciente que funcione.
                        conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :wid, activo=TRUE WHERE id_cliente = :id"), 
                                     {"wid": routing_id, "id": cliente_existente.id_cliente})
                    else:
                        # Cliente Nuevo: Lo insertamos con su teléfono como clave
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
                            "t": telefono_msg, # Guardamos siempre asociado al NÚMERO UNIFICADO
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