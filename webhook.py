from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- LOGGING ---
def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# --- 🚑 PARCHE DB ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_wsp_id ON Clientes(whatsapp_internal_id)"))
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre VARCHAR(100)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS apellido VARCHAR(100)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id VARCHAR(100)"))
    except: pass

aplicar_parche_db()

# --- FUNCIONES AUXILIARES ---
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

# ==============================================================================
# 🧠 CEREBRO V24: SEPARACIÓN ID vs TELÉFONO
# ==============================================================================
def obtener_identidad(payload, session):
    """
    ID CANONICO: El que usamos para responder (remoteJid / LID).
    TELEFONO: El número humano extraído del Alt o del ID.
    """
    try:
        from_me = payload.get('fromMe', False)
        
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        # --- 1. DETERMINAR ID DE RUTEO (Para whatsapp_internal_id) ---
        # Este NO debe cambiarse por el Alt, debe ser el origen real (LID o lo que sea)
        routing_id = ""
        
        if from_me:
             # Si lo envié yo, el destino es remoteJid o to
             routing_id = key.get('remoteJid') or payload.get('to')
        else:
             # Si me lo enviaron
             routing_id = key.get('remoteJid') or payload.get('from')
             # Nota: En grupos, remoteJid es el grupo. El participant es quien escribe.
             # Pero para identificar el CHAT, usamos remoteJid.

        if not routing_id: return None

        # --- 2. DETERMINAR TELÉFONO VISUAL (El Tesoro Escondido) ---
        # Aquí sí buscamos el Alt para sacar el número real
        
        alt_source = key.get('remoteJidAlt') or key.get('participantAlt') or _data.get('participantAlt')
        
        telefono_limpio = ""
        
        if alt_source and '@' in alt_source:
             # ¡Tesoro encontrado! Usamos este para el teléfono
             telefono_limpio = "".join(filter(str.isdigit, alt_source.split('@')[0]))
        else:
             # Si no hay tesoro, intentamos sacarlo del participant o del ID
             part = payload.get('participant')
             fuente_num = part if (part and '@lid' not in part) else routing_id
             telefono_limpio = "".join(filter(str.isdigit, fuente_num.split('@')[0]))

        # Detectar grupo
        es_grupo = '@g.us' in routing_id

        return {
            "id_canonico": routing_id, # Ej: 2513...@lid
            "telefono": telefono_limpio, # Ej: 5350610509
            "es_grupo": es_grupo
        }

    except Exception as e:
        log_error(f"Error identidad: {e}")
        return None

# ==============================================================================
# RUTAS FLASK
# ==============================================================================
@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V24 (LID Routing + Phone Alt)", 200

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

            if tipo_evento == 'message.ack':
                # ... (Lógica de ACK igual) ...
                msg_id = payload.get('id')
                ack_status = payload.get('ack') 
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                try:
                    with engine.connect() as conn:
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), {"e": nuevo_estado, "w": msg_id})
                        conn.commit()
                except: pass
                continue 

            if tipo_evento not in ['message', 'message.any', 'message.created']: continue
            if payload.get('from') == 'status@broadcast': continue

            # 1. OBTENER IDENTIDAD
            identidad = obtener_identidad(payload, session_name)
            if not identidad: continue
            
            chat_id = identidad['id_canonico'] # El ID Técnico (LID)
            telefono_msg = identidad['telefono'] # El Número Real
            es_grupo = identidad['es_grupo']

            log_info(f"🔑 [{session_name}] ID: {chat_id} | Tel: {telefono_msg}")

            # 2. CONTENIDO
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"
            
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply

            # 3. LÓGICA DB (Linkeado Inteligente)
            try:
                tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
                whatsapp_id = payload.get('id')
                
                _data = payload.get('_data') or {}
                push_name = _data.get('pushName') or _data.get('notifyName') or _data.get('verifiedBizName')
                nombre_wsp = push_name or "Cliente Nuevo"
                if tipo_msg == 'SALIENTE': nombre_wsp = f"Chat {telefono_msg}"

                with engine.connect() as conn:
                    
                    # A) Buscar por ID TÉCNICO (Prioridad Absoluta)
                    cliente_db = conn.execute(
                        text("SELECT id_cliente, telefono, whatsapp_internal_id FROM Clientes WHERE whatsapp_internal_id = :wid"), 
                        {"wid": chat_id}
                    ).fetchone()

                    telefono_final_db = telefono_msg 

                    if cliente_db:
                        # EXISTE POR ID -> Todo perfecto
                        telefono_final_db = cliente_db.telefono
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:id"), {"id": cliente_db.id_cliente})
                    
                    else:
                        # B) NO EXISTE POR ID -> Buscar por TELÉFONO (Linkeo)
                        # Aquí ocurre la magia: Si encontramos el número real, actualizamos su ID al nuevo LID
                        cliente_tel = conn.execute(
                            text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), 
                            {"t": telefono_msg}
                        ).fetchone()

                        if cliente_tel:
                             # ¡Es él! Pero está usando un ID nuevo (LID). Actualizamos el ID.
                             log_info(f"🔄 Actualizando ID Routing: {telefono_msg} -> {chat_id}")
                             conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :wid, activo=TRUE WHERE id_cliente = :id"), 
                                         {"wid": chat_id, "id": cliente_tel.id_cliente})
                        else:
                            # NUEVO TOTAL
                            nombre_final = f"Grupo {telefono_msg[-4:]}" if es_grupo else nombre_wsp
                            log_info(f"🆕 Nuevo Cliente (LID): {chat_id} | Tel: {telefono_msg}")
                            
                            conn.execute(text("""
                                INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, estado, activo, fecha_registro)
                                VALUES (:t, :wid, :n, 'Sin empezar', TRUE, NOW())
                            """), {"t": telefono_msg, "wid": chat_id, "n": nombre_final})
                            
                            # Sync Google (Solo nuevos)
                            if not es_grupo and tipo_msg == 'ENTRANTE':
                                try:
                                    datos_google = buscar_contacto_google(telefono_msg)
                                    if datos_google and datos_google['encontrado']:
                                        conn.execute(text("""
                                            UPDATE Clientes 
                                            SET nombre=:nom, apellido=:ape, google_id=:gid, nombre_corto=:comp
                                            WHERE whatsapp_internal_id=:wid
                                        """), {
                                            "nom": datos_google['nombre'], "ape": datos_google['apellido'],
                                            "gid": datos_google['google_id'], "comp": datos_google['nombre_completo'],
                                            "wid": chat_id
                                        })
                                except: pass

                    # GUARDAR MENSAJE
                    existe_msg = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                    
                    if not existe_msg:
                        conn.execute(text("""
                            INSERT INTO mensajes (
                                telefono, tipo, contenido, fecha, leido, archivo_data, 
                                whatsapp_id, reply_to_id, reply_content, estado_waha
                            )
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est)
                        """), {
                            "t": telefono_final_db, # Mantenemos el telefono para agrupar visualmente
                            "tipo": tipo_msg, 
                            "txt": body, 
                            "leido": (tipo_msg == 'SALIENTE'), 
                            "d": archivo_bytes,
                            "wid": whatsapp_id,
                            "rid": reply_id,
                            "rbody": reply_content,
                            "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado'
                        })
                    else:
                         conn.execute(text("""
                            UPDATE mensajes SET 
                                reply_to_id = :rid, reply_content = :rbody, archivo_data = COALESCE(mensajes.archivo_data, :d)
                            WHERE whatsapp_id = :wid
                        """), {"wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "d": archivo_bytes})
                    
                    conn.commit()

            except Exception as e:
                log_error(f"🔥 Error DB [{session_name}]: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)