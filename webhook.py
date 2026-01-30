from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys
import time
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- LOGGING ---
def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# --- 🚑 PARCHE DE BASE DE DATOS ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            # Convertimos columnas a TEXTO para que acepten LIDs largos si es necesario
            conn.execute(text("ALTER TABLE mensajes ALTER COLUMN telefono TYPE VARCHAR(50)"))
            conn.execute(text("ALTER TABLE \"Clientes\" ALTER COLUMN telefono TYPE VARCHAR(50)"))
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

# --- 🕵️‍♂️ DETECTIVE DE NÚMEROS REALES ---
def resolver_numero_real(payload, session):
    """
    Intenta descubrir el número de teléfono real si viene un LID (8204...).
    """
    try:
        # 1. Datos básicos
        raw_from = payload.get('from', '')
        _data = payload.get('_data') or {}
        
        # Si NO es un LID (es un número normal corto), retornamos eso.
        if '@c.us' in raw_from and not '@lid' in raw_from:
             return raw_from.replace('@c.us', '')

        # 2. Si es LID, buscamos en _data (Truco para empresas)
        # A veces el número real está en _data.id.remote o _data.id.user
        candidate = _data.get('id', {}).get('remote', '')
        if '@s.whatsapp.net' in candidate: 
            return candidate.replace('@s.whatsapp.net', '')
        
        candidate_user = _data.get('id', {}).get('user', '')
        if candidate_user and candidate_user.isdigit() and len(candidate_user) < 16:
            return candidate_user

        # 3. Si sigue siendo oculto, PREGUNTAMOS A WAHA (API)
        # WAHA tiene un endpoint para "traducir" LIDs a Números
        if '@lid' in raw_from and WAHA_URL:
            lid_clean = raw_from
            # Llamada a API: /api/{session}/lids/{lid}
            url_api = f"{WAHA_URL}/api/{session}/lids/{lid_clean}"
            headers = {"X-Api-Key": WAHA_KEY} if WAHA_KEY else {}
            
            try:
                r = requests.get(url_api, headers=headers, timeout=5)
                if r.status_code == 200:
                    data = r.json() # { "lid": "...", "pn": "51999...@c.us" }
                    pn = data.get('pn')
                    if pn:
                        log_info(f"🕵️ LID Resuelto: {lid_clean} -> {pn}")
                        return pn.replace('@c.us', '').replace('@s.whatsapp.net', '')
            except Exception as e:
                log_error(f"Fallo resolviendo LID: {e}")

        # 4. Si todo falla, devolvemos el ID original limpio
        return raw_from.replace('@c.us', '').replace('@lid', '')

    except:
        return payload.get('from', '').replace('@c.us', '')

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V9 (Business/LID Solver)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            session_name = evento.get('session', 'default')
            payload = evento.get('payload')
            if not payload: continue

            if 'status@broadcast' in str(payload.get('from')): continue

            # --- PASO CRÍTICO: RESOLVER QUIÉN ES ---
            telefono_real = resolver_numero_real(payload, session_name)
            
            # Normalización
            formatos = normalizar_telefono_maestro(telefono_real)
            if formatos:
                telefono_db = formatos['db']
            else:
                # Fallback para números internacionales raros
                telefono_db = "".join(filter(str.isdigit, telefono_real))
                if len(telefono_db) < 5: continue

            log_info(f"📩 [{session_name}] Procesando: {telefono_db}")

            # Contenido
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"

            # Reply
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply
            
            # Guardar
            try:
                # PushName o Google
                _data = payload.get('_data') or {}
                push_name = _data.get('notifyName')
                nombre_final = push_name or "Cliente Nuevo"

                # Búsqueda Google (Solo si parece un número normal)
                tipo_msg = 'ENTRANTE' # Asumimos entrante si llega al webhook
                if payload.get('fromMe'): tipo_msg = 'SALIENTE'

                if tipo_msg == 'ENTRANTE' and "Cliente" in nombre_final and len(telefono_db) <= 13:
                     try:
                        datos_google = buscar_contacto_google(telefono_db)
                        if datos_google and datos_google['encontrado']:
                            nombre_final = datos_google['nombre_completo']
                     except: pass

                with engine.connect() as conn:
                    # 1. Clientes
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO UPDATE SET activo = TRUE
                    """), {"t": telefono_db, "n": nombre_final})
                    
                    # 2. Mensajes (Upsert)
                    conn.execute(text("""
                        INSERT INTO mensajes (
                            telefono, tipo, contenido, fecha, leido, archivo_data, 
                            whatsapp_id, reply_to_id, reply_content
                        )
                        VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody)
                        ON CONFLICT (whatsapp_id) DO UPDATE SET
                            reply_to_id = EXCLUDED.reply_to_id,
                            reply_content = EXCLUDED.reply_content,
                            archivo_data = COALESCE(mensajes.archivo_data, EXCLUDED.archivo_data)
                    """), {
                        "t": telefono_db, 
                        "tipo": tipo_msg, 
                        "txt": body, 
                        "leido": (tipo_msg == 'SALIENTE'), 
                        "d": archivo_bytes,
                        "wid": payload.get('id'),
                        "rid": reply_id,
                        "rbody": reply_content
                    })
                    conn.commit()
                    log_info(f"✅ [{session_name}] Guardado OK: {telefono_db}")

            except Exception as e:
                log_error(f"🔥 Error DB [{session_name}]: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error Webhook: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)