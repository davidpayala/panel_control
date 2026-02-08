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
            # ... tus parches anteriores ...
            # NUEVO: Columna para estado del mensaje (ack)
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
    except: pass

aplicar_parche_db()

# --- FUNCIONES ---
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

def resolver_numero_real(payload, session):
    try:
        # ### CAMBIO CRÍTICO: Detectar dirección del mensaje
        # Si fromMe es True (Saliente), el contacto es 'to'.
        # Si fromMe es False (Entrante), el contacto es 'from'.
        if payload.get('fromMe'):
            raw_target = payload.get('to', '')
        else:
            raw_target = payload.get('from', '')
            
        _data = payload.get('_data') or {}
        
        # Usamos raw_target en lugar de raw_from para la lógica
        if '@c.us' in raw_target and not '@lid' in raw_target:
             return raw_target.replace('@c.us', '')

        candidate = _data.get('id', {}).get('remote', '')
        if '@s.whatsapp.net' in candidate: 
            return candidate.replace('@s.whatsapp.net', '')
        
        candidate_user = _data.get('id', {}).get('user', '')
        if candidate_user and candidate_user.isdigit() and len(candidate_user) < 16:
            return candidate_user

        if '@lid' in raw_target and WAHA_URL:
            lid_clean = raw_target
            url_api = f"{WAHA_URL}/api/{session}/lids/{lid_clean}"
            headers = {"X-Api-Key": WAHA_KEY} if WAHA_KEY else {}
            try:
                r = requests.get(url_api, headers=headers, timeout=5)
                if r.status_code == 200:
                    pn = r.json().get('pn')
                    if pn: return pn.replace('@c.us', '').replace('@s.whatsapp.net', '')
            except: pass

        return raw_target.replace('@c.us', '').replace('@lid', '')
    except:
        # Fallback simple
        return payload.get('from', '').replace('@c.us', '')

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V12 (Outgoing & Reply Fix)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            # ### CAMBIO: Aceptar message.any para ver mensajes salientes
            tipo_evento = evento.get('event')
            # NUEVO: Manejo de ACKs (Confirmaciones de lectura/entrega)
            if tipo_evento == 'message.ack':
                payload = evento.get('payload', {})
                msg_id = payload.get('id')
                ack_status = payload.get('ack') # 1: enviado, 2: recibido, 3: leido, etc.
                
                # Mapeo de estados de WAHA a texto legible
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                
                try:
                    with engine.connect() as conn:
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), 
                                    {"e": nuevo_estado, "w": msg_id})
                        conn.commit()
                        log_info(f"✅ ACK Actualizado: {msg_id} -> {nuevo_estado}")
                except Exception as e:
                    log_error(f"Error actualizando ACK: {e}")
                continue # Saltar al siguiente evento

            # 1. RESOLVER NÚMERO (Usando la nueva lógica corregida)
            telefono_real = resolver_numero_real(payload, session_name)
            
            formatos = normalizar_telefono_maestro(telefono_real)
            if formatos:
                telefono_db = formatos['db']
            else:
                telefono_db = "".join(filter(str.isdigit, telefono_real))
                if len(telefono_db) < 5: continue

            log_info(f"📩 [{session_name}] Procesando: {telefono_db} (Evento: {tipo_evento})")

            # 2. CONTENIDO
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"

            # 3. REPLY (MEJORADO)
            reply_id = None
            reply_content = None
            
            # Intento 1: replyTo directo
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply
            
            # Intento 2: Buscar en _data.quotedMsg si el contenido sigue vacío
            if not reply_content:
                quoted = payload.get('_data', {}).get('quotedMsg', {})
                if quoted:
                    reply_content = quoted.get('body') or quoted.get('caption')
                    # Si no teníamos ID, tratamos de sacarlo de _data (estructura variable)
                    if not reply_id: 
                         reply_id = quoted.get('id') # A veces es un objeto, cuidado aquí
            
            # 4. GUARDAR
            try:
                _data = payload.get('_data') or {}
                push_name = _data.get('notifyName')
                nombre_final = push_name or "Cliente Nuevo"
                
                tipo_msg = 'ENTRANTE'
                if payload.get('fromMe'): tipo_msg = 'SALIENTE'

                # Solo buscamos en Google si es entrante y no es muy largo (evitar grupos raros)
                if tipo_msg == 'ENTRANTE' and "Cliente" in nombre_final and len(telefono_db) <= 13:
                     try:
                        datos_google = buscar_contacto_google(telefono_db)
                        if datos_google and datos_google['encontrado']:
                            nombre_final = datos_google['nombre_completo']
                     except: pass

                whatsapp_id = payload.get('id')

                with engine.connect() as conn:
                    # A) Clientes
                    existe_cli = conn.execute(text("SELECT 1 FROM Clientes WHERE telefono=:t"), {"t": telefono_db}).scalar()
                    if not existe_cli:
                        # Si es mensaje saliente (yo escribo a alguien nuevo), también creamos el cliente
                        conn.execute(text("""
                            INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                            VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        """), {"t": telefono_db, "n": nombre_final})
                    else:
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE telefono=:t"), {"t": telefono_db})

                    # B) Mensajes
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
                                whatsapp_id, reply_to_id, reply_content
                            )
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody)
                        """), {
                            "t": telefono_db, 
                            "tipo": tipo_msg, 
                            "txt": body, 
                            "leido": (tipo_msg == 'SALIENTE'), 
                            "d": archivo_bytes,
                            "wid": whatsapp_id,
                            "rid": reply_id,
                            "rbody": reply_content
                        })
                    
                    conn.commit()
                    log_info(f"✅ [{session_name}] Guardado OK: {telefono_db} ({tipo_msg})")

            except Exception as e:
                log_error(f"🔥 Error DB [{session_name}]: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error Webhook: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)