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
# 🕵️ LOGICA MAESTRA PARA EXTRAER EL NÚMERO (LID FIX FINAL)
# ==============================================================================
def resolver_numero_real(payload, session):
    try:
        # 1. Extraer datos profundos siempre
        _data = payload.get('_data', {}) or {}
        key = _data.get('key', {}) or {}
        
        # --- PRIORIDAD ABSOLUTA: CAMPOS "ALT" (Desencriptan el LID) ---
        # Funcionan tanto para mensajes ENTRANTES como SALIENTES (fromMe)
        
        # Caso A: Chat Privado con Privacidad (remoteJidAlt)
        if key.get('remoteJidAlt'):
            return key.get('remoteJidAlt').replace('@s.whatsapp.net', '').replace('@c.us', '')
            
        # Caso B: Grupos/Comunidades (participantAlt)
        if key.get('participantAlt'):
            return key.get('participantAlt').replace('@s.whatsapp.net', '').replace('@c.us', '')

        # --- NIVEL 2: STANDARD ---
        
        # Si lo envié yo y no había Alt, usamos el 'to'
        if payload.get('fromMe', False):
             return payload.get('to', '').split('@')[0]
        
        raw_participant = payload.get('participant')
        raw_from = payload.get('from')

        # Si hay participante (Grupos normales), ese es el que escribe
        if raw_participant and '@lid' not in raw_participant:
            return raw_participant.replace('@c.us', '').replace('@s.whatsapp.net', '')

        # Si es chat privado normal
        if raw_from and '@g.us' not in raw_from and '@lid' not in raw_from:
            return raw_from.replace('@c.us', '').replace('@s.whatsapp.net', '')

        # --- NIVEL 3: FALLBACK ---
        candidato = raw_participant or raw_from or ""
        
        # Si terminamos con un LID y no encontramos el Alt, intentamos resolverlo por API
        if '@lid' in candidato and WAHA_URL:
            try:
                lid_clean = candidato
                url_api = f"{WAHA_URL}/api/{session}/lids/{lid_clean}"
                headers = {"X-Api-Key": WAHA_KEY} if WAHA_KEY else {}
                r = requests.get(url_api, headers=headers, timeout=3)
                if r.status_code == 200:
                    data_lid = r.json()
                    if 'pn' in data_lid: 
                        return data_lid['pn'].replace('@c.us', '').replace('@s.whatsapp.net', '')
            except: pass

        return candidato.split('@')[0]

    except Exception as e:
        log_error(f"Error resolviendo numero: {e}")
        return payload.get('from', '').split('@')[0]

# ==============================================================================
# RUTAS FLASK
# ==============================================================================
@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V19 (Sent LID Fix)", 200

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

            # --- A) ACKs ---
            if tipo_evento == 'message.ack':
                msg_id = payload.get('id')
                ack_status = payload.get('ack') 
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                try:
                    with engine.connect() as conn:
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), 
                                    {"e": nuevo_estado, "w": msg_id})
                        conn.commit()
                except: pass
                continue 

            # --- B) MENSAJES ---
            if tipo_evento not in ['message', 'message.any', 'message.created']:
                continue

            # IGNORAR ESTADOS
            if payload.get('from') == 'status@broadcast': continue

            # 1. Resolver Teléfono
            telefono_real = resolver_numero_real(payload, session_name)
            
            formatos = normalizar_telefono_maestro(telefono_real)
            if formatos:
                telefono_db = formatos['db']
            else:
                telefono_db = "".join(filter(str.isdigit, telefono_real))
                if len(telefono_db) < 5: continue 

            log_info(f"📩 [{session_name}] Msg procesado: {telefono_db}")

            # 2. Contenido
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"

            # 3. Reply
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply

            # 4. LÓGICA DE REGISTRO RÁPIDO
            try:
                _data = payload.get('_data') or {}
                
                # Extracción de Nombre
                push_name = _data.get('pushName')
                notify_name = _data.get('notifyName')
                biz_name = _data.get('verifiedBizName')
                
                nombre_wsp = push_name or notify_name or biz_name or "Cliente Nuevo"
                
                tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
                whatsapp_id = payload.get('id')

                with engine.connect() as conn:
                    
                    # 4.1 Verificar Cliente
                    cliente_existente = conn.execute(
                        text("SELECT id_cliente, google_id FROM Clientes WHERE telefono=:t"), 
                        {"t": telefono_db}
                    ).fetchone()
                    
                    nuevo_cliente = False

                    if not cliente_existente:
                        # CREAR
                        log_info(f"🆕 Cliente Nuevo: {telefono_db} | Nombre: {nombre_wsp}")
                        conn.execute(text("""
                            INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                            VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        """), {"t": telefono_db, "n": nombre_wsp})
                        nuevo_cliente = True
                    else:
                        # REACTIVAR
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE telefono=:t"), {"t": telefono_db})

                    # 4.2 Sync Google (Solo nuevos)
                    if nuevo_cliente:
                        try:
                            datos_google = buscar_contacto_google(telefono_db)
                            if datos_google and datos_google['encontrado']:
                                log_info(f"🔗 Google Sync: {datos_google['nombre_completo']}")
                                conn.execute(text("""
                                    UPDATE Clientes 
                                    SET nombre = :nom, apellido = :ape, google_id = :gid, nombre_corto = :completo
                                    WHERE telefono = :t
                                """), {
                                    "nom": datos_google['nombre'],
                                    "ape": datos_google['apellido'],
                                    "gid": datos_google['google_id'],
                                    "completo": datos_google['nombre_completo'],
                                    "t": telefono_db
                                })
                            else:
                                log_info(f"ℹ️ No en Google. Usando: {nombre_wsp}")
                        except Exception as e_google:
                            log_error(f"Error Sync Google: {e_google}")

                    # 4.3 Guardar Mensaje
                    existe_msg = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                    
                    if existe_msg:
                        conn.execute(text("""
                            UPDATE mensajes SET 
                                reply_to_id = :rid,
                                reply_content = :rbody,
                                archivo_data = COALESCE(mensajes.archivo_data, :d)
                            WHERE whatsapp_id = :wid
                        """), {"wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "d": archivo_bytes})
                    else:
                        conn.execute(text("""
                            INSERT INTO mensajes (
                                telefono, tipo, contenido, fecha, leido, archivo_data, 
                                whatsapp_id, reply_to_id, reply_content, estado_waha
                            )
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est)
                        """), {
                            "t": telefono_db, 
                            "tipo": tipo_msg, 
                            "txt": body, 
                            "leido": (tipo_msg == 'SALIENTE'), 
                            "d": archivo_bytes,
                            "wid": whatsapp_id,
                            "rid": reply_id,
                            "rbody": reply_content,
                            "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado'
                        })
                    
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