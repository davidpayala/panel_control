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
    """
    Estrategia robusta para extraer el teléfono real del remitente.
    Basado en: https://waha.devlike.pro/docs/how-to/receive-messages/
    """
    try:
        # 1. Determinar dirección (Saliente vs Entrante)
        from_me = payload.get('fromMe', False)
        raw_target = None
        
        if from_me:
            # Si lo envié yo, el destinatario está en 'to'
            raw_target = payload.get('to')
        else:
            # Si es entrante, WAHA puede poner el ID en 'participant' (grupos) o 'from' (privado)
            # 'participant' tiene prioridad si existe, porque identifica al usuario específico
            if payload.get('participant'):
                raw_target = payload.get('participant')
            else:
                raw_target = payload.get('from')
        
        # 2. Fallback a datos internos (_data) si falla lo estándar
        if not raw_target:
            _data = payload.get('_data') or {}
            raw_target = _data.get('id', {}).get('remote') or _data.get('participant')

        if not raw_target: return None

        # 3. Lógica Especial: LIDs (Identificadores ocultos de WhatsApp)
        if '@lid' in raw_target and WAHA_URL:
            # Intentamos resolver el LID al número real consultando a WAHA
            try:
                lid_clean = raw_target
                url_api = f"{WAHA_URL}/api/{session}/lids/{lid_clean}"
                headers = {"X-Api-Key": WAHA_KEY} if WAHA_KEY else {}
                r = requests.get(url_api, headers=headers, timeout=3)
                if r.status_code == 200:
                    data_lid = r.json()
                    # El 'pn' (Phone Number) es lo que buscamos
                    if 'pn' in data_lid:
                        raw_target = data_lid['pn']
            except: 
                pass # Si falla, seguimos con lo que tenemos

        # 4. Limpieza final de dominios
        # Quitamos @c.us, @s.whatsapp.net, @g.us, etc.
        numero_limpio = raw_target
        for sufijo in ['@c.us', '@s.whatsapp.net', '@g.us', '@lid', '@broadcast']:
            numero_limpio = numero_limpio.replace(sufijo, '')
        
        # Aseguramos que solo queden números
        if '@' in numero_limpio: numero_limpio = numero_limpio.split('@')[0]
        
        return numero_limpio

    except Exception as e:
        log_error(f"Error resolviendo numero: {e}")
        return payload.get('from', '').replace('@c.us', '')

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V14 (Phone Fix)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            # 1. EXTRACCIÓN SEGURA DE DATOS (CORRECCIÓN CRÍTICA)
            tipo_evento = evento.get('event')
            session_name = evento.get('session', 'default') # Extraemos la sesión
            payload = evento.get('payload', {})             # Definimos payload aquí para todos

            # 2. MANEJO DE ACKs (Confirmaciones)
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
                        log_info(f"✅ ACK [{session_name}]: {msg_id} -> {nuevo_estado}")
                except Exception as e:
                    log_error(f"Error ACK: {e}")
                continue 

            # 3. FILTRO DE EVENTOS
            # Solo procesamos mensajes creados o upserts
            if tipo_evento not in ['message', 'message.any', 'message.created']:
                continue

            # 4. RESOLVER NÚMERO
            telefono_real = resolver_numero_real(payload, session_name)
            
            formatos = normalizar_telefono_maestro(telefono_real)
            if formatos:
                telefono_db = formatos['db']
            else:
                telefono_db = "".join(filter(str.isdigit, telefono_real))
                if len(telefono_db) < 5: continue

            log_info(f"📩 [{session_name}] Msg de: {telefono_db} (Raw: {telefono_real})")

            # 5. CONTENIDO
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"

            # 6. REPLY (Respuestas citadas)
            reply_id = None
            reply_content = None
            
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply
            
            if not reply_content:
                quoted = payload.get('_data', {}).get('quotedMsg', {})
                if quoted:
                    reply_content = quoted.get('body') or quoted.get('caption')
                    if not reply_id: 
                         reply_id = quoted.get('id')
            
            # 7. GUARDAR EN DB
            try:
                _data = payload.get('_data') or {}
                push_name = _data.get('notifyName')
                nombre_final = push_name or "Cliente Nuevo"
                
                tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'

                # Auto-Identificación con Google Contacts (Solo entrantes desconocidos)
                if tipo_msg == 'ENTRANTE' and "Cliente" in nombre_final:
                     try:
                        datos_google = buscar_contacto_google(telefono_db)
                        if datos_google and datos_google['encontrado']:
                            nombre_final = datos_google['nombre_completo']
                     except: pass

                whatsapp_id = payload.get('id')

                with engine.connect() as conn:
                    # Upsert Cliente
                    existe_cli = conn.execute(text("SELECT 1 FROM Clientes WHERE telefono=:t"), {"t": telefono_db}).scalar()
                    if not existe_cli:
                        conn.execute(text("""
                            INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                            VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        """), {"t": telefono_db, "n": nombre_final})
                    else:
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE telefono=:t"), {"t": telefono_db})

                    # Upsert Mensaje
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
                    log_info(f"✅ [{session_name}] Guardado: {telefono_db}")

            except Exception as e:
                log_error(f"🔥 Error DB [{session_name}]: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)