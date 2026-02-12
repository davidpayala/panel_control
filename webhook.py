from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys
import json
from datetime import datetime

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# --- 🛡️ IMPORTACIÓN SEGURA DE UTILS ---
try:
    from utils import normalizar_telefono_maestro
except ImportError:
    # Fallback básico por si falla utils
    def normalizar_telefono_maestro(t): return {"db": "".join(filter(str.isdigit, str(t)))}

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
            except:
                conn.execute(text("DROP TABLE IF EXISTS sync_estado"))
                conn.execute(text("CREATE TABLE sync_estado (id INT PRIMARY KEY, version INT DEFAULT 0)"))
                conn.execute(text("INSERT INTO sync_estado (id, version) VALUES (1, 0)"))
    except: pass

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

# ==============================================================================
# 🕵️ FUNCIONES ESPECIALISTAS (INDEPENDIENTES)
# ==============================================================================

def obtener_lid_waha(payload):
    """
    Busca ÚNICAMENTE un ID que termine en @lid.
    Recorre todos los rincones posibles del JSON.
    """
    try:
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}
        
        # Lista de candidatos donde podría estar el LID
        candidatos = [
            payload.get('from'),
            payload.get('to'),
            payload.get('participant'),
            key.get('remoteJid'),
            key.get('participant'),
            _data.get('lid') # A veces viene directo aquí
        ]

        for c in candidatos:
            if c and isinstance(c, str) and '@lid' in c:
                return c # ¡Encontrado! Devuelve ej: 214924743712877@lid
                
        return None
    except:
        return None

def obtener_telefono_waha(payload):
    """
    Busca ÚNICAMENTE un ID que termine en @s.whatsapp.net o @c.us.
    Devuelve solo el número limpio.
    """
    try:
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        # 1. Prioridad Máxima: remoteJidAlt (Aquí se esconde el teléfono cuando escriben desde PC/Link)
        alt = key.get('remoteJidAlt')
        if alt and isinstance(alt, str):
            if '@s.whatsapp.net' in alt or '@c.us' in alt:
                return alt.split('@')[0]

        # 2. Otros candidatos estándar
        candidatos = [
            payload.get('from'),
            payload.get('to'),
            key.get('remoteJid'),
            payload.get('participant')
        ]

        for c in candidatos:
            if c and isinstance(c, str):
                if '@s.whatsapp.net' in c or '@c.us' in c:
                    return c.split('@')[0]
        
        # 3. Último recurso: _data.id.user (Suele ser el número puro)
        user_id = _data.get('id', {}).get('user')
        if user_id and str(user_id).isdigit():
            return str(user_id)

        return None
    except:
        return None

# ==============================================================================
# 🚀 WEBHOOK PRINCIPAL
# ==============================================================================

@app.route('/', methods=['GET'])
def home():
    return "Webhook V43 (Funciones Independientes) ✅", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        # Logging Diagnóstico
        try:
            with engine.begin() as conn:
                item = data[0] if isinstance(data, list) else data
                p_str = json.dumps(item, ensure_ascii=False)[:5000]
                conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                            {"s": item.get('session', 'unk'), "e": item.get('event', 'unk'), "p": p_str})
                conn.execute(text("DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 50)"))
        except: pass

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            tipo_evento = evento.get('event')
            session_name = evento.get('session', 'default')
            payload = evento.get('payload', {})

            # --- GESTIÓN DE ACKS ---
            if tipo_evento == 'message.ack':
                # (Código de ACK estándar...)
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

            if tipo_evento not in ['message', 'message.any', 'message.created']: continue
            if payload.get('from') == 'status@broadcast': continue

            # ==================================================================
            # 🧠 USO DE LAS NUEVAS FUNCIONES INDEPENDIENTES
            # ==================================================================
            
            # 1. Buscamos LID
            wspid_lid = obtener_lid_waha(payload) # Ej: 214...77@lid o None
            
            # 2. Buscamos Teléfono Crudo
            telefono_crudo = obtener_telefono_waha(payload) # Ej: 51963... o None
            
            # 3. Normalizamos Teléfono (Siempre 51...)
            telefono_num = None
            if telefono_crudo:
                norm = normalizar_telefono_maestro(telefono_crudo)
                if isinstance(norm, dict): telefono_num = norm.get('db')
                else: telefono_num = norm

            # LOG PARA VER QUÉ ENCONTRÓ
            log_info(f"🔎 Analisis -> Tel: {telefono_num} | LID: {wspid_lid}")

            # Datos del mensaje
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

            id_cliente_final = None

            try:
                with engine.begin() as conn:
                    # ==========================================================
                    # 🚦 TU LÓGICA DE REGISTRO (1, 2, 3)
                    # ==========================================================

                    # CASO 1: Tengo LID y Tengo Teléfono
                    if wspid_lid and telefono_num:
                        # 1. Busca LID en BD
                        cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        
                        if cliente_lid:
                            # 1.1 Encontró LID -> Verifica teléfono
                            if cliente_lid.telefono == telefono_num:
                                # 1.1.1 Iguales -> Todo bien
                                id_cliente_final = cliente_lid.id_cliente
                            else:
                                # 1.1.2 Diferente o vacío -> Actualiza teléfono
                                conn.execute(text("UPDATE Clientes SET telefono = :t, activo=TRUE WHERE id_cliente = :id"), {"t": telefono_num, "id": cliente_lid.id_cliente})
                                id_cliente_final = cliente_lid.id_cliente
                        else:
                            # 1.2 No encontró LID -> Busca Teléfono
                            cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                            
                            if cliente_tel:
                                # 1.2.1 Encontró Teléfono -> Asigna LID
                                conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :lid, activo=TRUE WHERE id_cliente = :id"), {"lid": wspid_lid, "id": cliente_tel.id_cliente})
                                id_cliente_final = cliente_tel.id_cliente
                            else:
                                # 1.2.2 Nada -> Crea nuevo completo
                                res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:lid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"lid": wspid_lid, "t": telefono_num, "n": push_name}).fetchone()
                                id_cliente_final = res.id_cliente

                    # CASO 2: Solo LID
                    elif wspid_lid and not telefono_num:
                        cliente_lid = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        if cliente_lid:
                            id_cliente_final = cliente_lid.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:lid, '', :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"lid": wspid_lid, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # CASO 3: Solo Teléfono
                    elif telefono_num and not wspid_lid:
                        cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                        if cliente_tel:
                            id_cliente_final = cliente_tel.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            # Fallback: Usamos routing_id como ID técnico si no hay LID
                            fallback_id = ids['routing_final'] if 'ids' in locals() and ids else payload.get('from')
                            res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:wid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"wid": fallback_id, "t": telefono_num, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # ==========================================================
                    # 💾 GUARDAR EL MENSAJE
                    # ==========================================================
                    if id_cliente_final:
                        # Para la tabla mensajes, usamos el teléfono consolidado
                        # Si no hay teléfono, usamos el LID para que no falle el INSERT (aunque lo ideal es tener teléfono)
                        t_msg = telefono_num if telefono_num else wspid_lid
                        
                        if not t_msg: t_msg = "DESCONOCIDO"

                        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                        if not existe:
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content, estado_waha, session_name)
                                VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                            """), {
                                "t": t_msg, "tipo": tipo_msg, "txt": body, "leido": (tipo_msg == 'SALIENTE'), "d": archivo_bytes,
                                "wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado', "sess": session_name
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