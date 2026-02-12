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

# --- 🧠 NUEVO EXTRACTOR DE IDs (Separa LID y Teléfono) ---
def extraer_ids_complejos(payload, session):
    try:
        from_me = payload.get('fromMe', False)
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        # 1. Buscar Routing ID (Quién envía/recibe técnicamente)
        routing_id = payload.get('to') if from_me else payload.get('from')
        if not routing_id: routing_id = key.get('remoteJid')
        if not routing_id: routing_id = payload.get('participant')

        lid_capturado = None
        telefono_capturado = None

        # 2. Análisis del Routing ID
        if routing_id:
            if '@lid' in routing_id:
                lid_capturado = routing_id
            elif '@c.us' in routing_id or '@s.whatsapp.net' in routing_id:
                telefono_capturado = routing_id.split('@')[0]

        # 3. Búsqueda profunda del teléfono (si tenemos LID, el teléfono suele estar en _data)
        if not telefono_capturado:
            posible_user = _data.get('id', {}).get('user')
            if posible_user and str(posible_user).isdigit():
                telefono_capturado = str(posible_user)
        
        # 4. Búsqueda profunda del LID (si tenemos teléfono, el LID puede estar en participant o keys ocultas)
        if not lid_capturado:
            # A veces viene en remoteJidAlt o participantAlt
            alt_id = key.get('participant') # A veces en grupos el LID viene aquí
            if alt_id and '@lid' in alt_id:
                lid_capturado = alt_id

        es_grupo = '@g.us' in (routing_id or "")

        return {
            "lid": lid_capturado,       # El ID @lid (o None)
            "telefono": telefono_capturado, # El número puro '519...' (o None)
            "es_grupo": es_grupo,
            "routing_final": routing_id # Para saber a dónde responder si falla todo
        }
    except Exception as e:
        log_error(f"Error extrayendo IDs: {e}")
        return None

@app.route('/', methods=['GET'])
def home():
    return "Webhook V39 (Logic Master) ✅", 200

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

            # --- APLICACIÓN DE TU LÓGICA MAESTRA ---
            ids = extraer_ids_complejos(payload, session_name)
            if not ids: continue

            wspid_lid = ids['lid']
            telefono_num = ids['telefono'] # Ya viene sin el @...
            
            # Datos comunes del mensaje
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

            id_cliente_final = None # Para asociar el mensaje

            try:
                with engine.begin() as conn:
                    
                    # CASO 1: Tengo LID y Tengo Teléfono
                    if wspid_lid and telefono_num:
                        # 1. Busca wspid@lid en whatsapp_internal_id
                        cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        
                        if cliente_lid:
                            # 1.1 Si encontró coincidencia
                            tel_db = cliente_lid.telefono
                            if tel_db == telefono_num:
                                # 1.1.1 Iguales -> Ese es el cliente
                                id_cliente_final = cliente_lid.id_cliente
                            else:
                                # 1.1.2 Diferente o vacío -> Actualiza teléfono
                                conn.execute(text("UPDATE Clientes SET telefono = :t, activo=TRUE WHERE id_cliente = :id"), {"t": telefono_num, "id": cliente_lid.id_cliente})
                                id_cliente_final = cliente_lid.id_cliente
                        else:
                            # 1.2 No encontró por LID, busca por Teléfono
                            cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                            
                            if cliente_tel:
                                # 1.2.1 Hay coincidencia -> Reemplazo whatsapp_internal_id con LID
                                conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :lid, activo=TRUE WHERE id_cliente = :id"), {"lid": wspid_lid, "id": cliente_tel.id_cliente})
                                id_cliente_final = cliente_tel.id_cliente
                            else:
                                # 1.2.2 No hay coincidencia -> Creo nuevo cliente
                                res = conn.execute(text("""
                                    INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro)
                                    VALUES (:lid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente
                                """), {"lid": wspid_lid, "t": telefono_num, "n": push_name}).fetchone()
                                id_cliente_final = res.id_cliente

                    # CASO 2: Tengo LID pero NO Teléfono
                    elif wspid_lid and not telefono_num:
                        # 2. Busca LID
                        cliente_lid = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        
                        if cliente_lid:
                            # 2.1 Coincidencia -> Ese es el cliente
                            id_cliente_final = cliente_lid.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            # 2.2 No coincidencia -> Creo nuevo con teléfono vacío
                            res = conn.execute(text("""
                                INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro)
                                VALUES (:lid, '', :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente
                            """), {"lid": wspid_lid, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # CASO 3: Tengo Teléfono pero NO LID
                    elif telefono_num and not wspid_lid:
                        # 3. Busca Teléfono
                        cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                        
                        if cliente_tel:
                            # 3.1 Coincidencia -> Ese es el cliente
                            id_cliente_final = cliente_tel.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            # 3.2 No coincidencia -> Creo nuevo con ID vacío (o uso el routing_id temporalmente si es c.us)
                            # NOTA: Tu regla dice "whatsapp_internal_id vacío", pero para poder responder necesitamos algo.
                            # Usaremos el routing_id original (que será tipo 519...@c.us) como fallback técnico, o vacío si prefieres estricto.
                            # Siguiendo tu regla estricta "crea... con el whatsapp_internal_id vacío":
                            # PERO OJO: Si lo dejo vacío, no podré responderle. Usaré el routing_id (ID Tradicional) como fallback.
                            wsp_id_fallback = ids['routing_final'] 
                            
                            res = conn.execute(text("""
                                INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro)
                                VALUES (:wid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente
                            """), {"wid": wsp_id_fallback, "t": telefono_num, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # --- GUARDADO DEL MENSAJE ---
                    # Ahora guardamos el mensaje asociado al teléfono que decidimos usar
                    if id_cliente_final:
                        # Recuperamos el teléfono final para la tabla mensajes (por compatibilidad con tu estructura actual)
                        telefono_final_msg = telefono_num
                        if not telefono_final_msg and wspid_lid:
                             # Si no hay numero, usamos el LID o buscamos en DB (caso raro)
                             telefono_final_msg = wspid_lid 

                        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                        
                        if not existe:
                            conn.execute(text("""
                                INSERT INTO mensajes (
                                    telefono, tipo, contenido, fecha, leido, archivo_data, 
                                    whatsapp_id, reply_to_id, reply_content, estado_waha, session_name
                                )
                                VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                            """), {
                                "t": telefono_final_msg, # Usamos el dato consolidado
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