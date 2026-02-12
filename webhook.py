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

# --- 🛡️ ZONA DE SEGURIDAD: IMPORTACIÓN ROBUSTA ---
try:
    from utils import normalizar_telefono_maestro
    log_info("✅ Utils importado correctamente.")
except ImportError as e:
    log_error(f"⚠️ Alerta: No se pudo importar utils ({e}). Usando modo respaldo.")
    
    def normalizar_telefono_maestro(entrada):
        if not entrada: return None
        raw_id = str(entrada)
        if isinstance(entrada, dict):
            raw_id = entrada.get('from', '') or entrada.get('to', '') or entrada.get('participant', '') or str(entrada.get('user', ''))
            
        cadena_limpia = raw_id.split('@')[0] if '@' in raw_id else raw_id
        solo_numeros = "".join(filter(str.isdigit, cadena_limpia))
        
        if not solo_numeros or len(solo_numeros) < 7: return None
        
        full, local = solo_numeros, solo_numeros
        if len(solo_numeros) == 9:
            full = f"51{solo_numeros}"
        elif len(solo_numeros) == 11 and solo_numeros.startswith("51"):
            local = solo_numeros[2:]
            
        return {"db": full, "waha": f"{full}@c.us", "corto": local}

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

# --- 🧠 EXTRACTOR IDs MAESTRO (V42 - remoteJidAlt Hunter) ---
def extraer_ids_complejos(payload, session):
    try:
        from_me = payload.get('fromMe', False)
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        # 1. Routing ID (Quién envía/recibe técnicamente)
        routing_id = payload.get('to') if from_me else payload.get('from')
        if not routing_id: routing_id = key.get('remoteJid')
        if not routing_id: routing_id = payload.get('participant')

        lid_capturado = None
        telefono_crudo = None

        # 2. Análisis del ID principal
        if routing_id:
            if '@lid' in routing_id:
                lid_capturado = routing_id
            elif '@c.us' in routing_id or '@s.whatsapp.net' in routing_id:
                telefono_crudo = routing_id.split('@')[0]

        # 3. 🕵️‍♂️ BÚSQUEDA PROFUNDA DE TELÉFONO (Aquí estaba la clave)
        if not telefono_crudo:
            # Opción A: remoteJidAlt (La que encontraste tú)
            alt_jid = key.get('remoteJidAlt')
            if alt_jid and ('@c.us' in alt_jid or '@s.whatsapp.net' in alt_jid):
                telefono_crudo = alt_jid.split('@')[0]
            
            # Opción B: _data.id.user
            if not telefono_crudo:
                posible_user = _data.get('id', {}).get('user')
                if posible_user and str(posible_user).isdigit():
                    telefono_crudo = str(posible_user)

        # 4. 🕵️‍♂️ BÚSQUEDA PROFUNDA DE LID
        if not lid_capturado:
            # A veces el LID viene escondido si el principal es el teléfono
            alt_part = key.get('participant')
            if alt_part and '@lid' in alt_part:
                lid_capturado = alt_part
            
            # O en remoteJid si el teléfono salió del Alt
            elif routing_id and '@lid' in routing_id:
                lid_capturado = routing_id

        # 5. NORMALIZACIÓN ESTRICTA
        telefono_normalizado = None
        if telefono_crudo:
            res = normalizar_telefono_maestro(telefono_crudo)
            if isinstance(res, dict):
                telefono_normalizado = res.get('db')
            else:
                telefono_normalizado = res

        # Si aún así no tenemos teléfono, pero tenemos LID, no podemos inventar el teléfono.
        # Retornamos lo que tengamos.
        
        return {
            "lid": lid_capturado,
            "telefono": telefono_normalizado, # SIEMPRE 51xxxxxx (si se encontró)
            "routing_final": routing_id
        }
    except Exception as e:
        log_error(f"Error IDs: {e}")
        return None

@app.route('/', methods=['GET'])
def home():
    return "Webhook V42 (AltJid Hunter) ✅", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        # LOGGING (No tocar)
        try:
            with engine.begin() as conn:
                item = data[0] if isinstance(data, list) else data
                p_str = json.dumps(item, ensure_ascii=False)
                if len(p_str) > 10000: p_str = p_str[:10000]
                conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                            {"s": item.get('session', 'unk'), "e": item.get('event', 'unk'), "p": p_str})
                conn.execute(text("DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 50)"))
        except: pass

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            tipo_evento = evento.get('event')
            session_name = evento.get('session', 'default')
            payload = evento.get('payload', {})

            if tipo_evento == 'message.ack':
                # ... (Lógica de ACK igual) ...
                continue 

            if tipo_evento not in ['message', 'message.any', 'message.created']: continue
            if payload.get('from') == 'status@broadcast': continue

            # --- LÓGICA DE IDENTIDAD MEJORADA ---
            ids = extraer_ids_complejos(payload, session_name)
            if not ids: continue

            wspid_lid = ids['lid']
            telefono_num = ids['telefono'] # Ahora sí vendrá lleno gracias a remoteJidAlt
            
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
            log_info(f"📩 Procesando: {telefono_num} | LID: {wspid_lid}")

            try:
                with engine.begin() as conn:
                    # CASO 1: LID + Teléfono (El caso ideal que ahora sí capturaremos)
                    if wspid_lid and telefono_num:
                        cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        if cliente_lid:
                            if cliente_lid.telefono == telefono_num:
                                id_cliente_final = cliente_lid.id_cliente
                            else:
                                conn.execute(text("UPDATE Clientes SET telefono = :t, activo=TRUE WHERE id_cliente = :id"), {"t": telefono_num, "id": cliente_lid.id_cliente})
                                id_cliente_final = cliente_lid.id_cliente
                        else:
                            cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                            if cliente_tel:
                                conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :lid, activo=TRUE WHERE id_cliente = :id"), {"lid": wspid_lid, "id": cliente_tel.id_cliente})
                                id_cliente_final = cliente_tel.id_cliente
                            else:
                                res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:lid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"lid": wspid_lid, "t": telefono_num, "n": push_name}).fetchone()
                                id_cliente_final = res.id_cliente

                    # CASO 2: LID solo (No debería pasar si remoteJidAlt funciona)
                    elif wspid_lid and not telefono_num:
                        cliente_lid = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        if cliente_lid:
                            id_cliente_final = cliente_lid.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:lid, '', :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"lid": wspid_lid, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # CASO 3: Teléfono solo
                    elif telefono_num and not wspid_lid:
                        cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                        if cliente_tel:
                            id_cliente_final = cliente_tel.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            wsp_id_fallback = ids['routing_final'] 
                            res = conn.execute(text("INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, estado, activo, fecha_registro) VALUES (:wid, :t, :n, 'Sin empezar', TRUE, NOW()) RETURNING id_cliente"), {"wid": wsp_id_fallback, "t": telefono_num, "n": push_name}).fetchone()
                            id_cliente_final = res.id_cliente

                    # GUARDADO DEL MENSAJE (Asociado al teléfono correcto)
                    if id_cliente_final:
                        telefono_final_msg = telefono_num
                        # Si falló la extracción de teléfono, usamos el LID como peor caso
                        if not telefono_final_msg and wspid_lid: telefono_final_msg = wspid_lid 

                        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                        if not existe:
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content, estado_waha, session_name)
                                VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                            """), {
                                "t": telefono_final_msg, "tipo": tipo_msg, "txt": body, "leido": (tipo_msg == 'SALIENTE'), "d": archivo_bytes,
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