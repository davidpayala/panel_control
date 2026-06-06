from dotenv import load_dotenv
load_dotenv()
from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import sys
import json
import random
from datetime import datetime
import io
from PIL import Image
import threading

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# 🛠️ CORRECCIÓN: Agregamos buscar_contacto_google a la importación
try:
    from utils import normalizar_telefono_maestro, crear_en_google, buscar_contacto_google
except ImportError:
    def normalizar_telefono_maestro(t): return {"db": "".join(filter(str.isdigit, str(t)))}
    def crear_en_google(n, a, t): return False
    def buscar_contacto_google(t): return None

def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS id_etapa INTEGER"))
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

# 🛠️ CORRECCIÓN: Ahora recibe id_cliente y guarda el vínculo en la BD
def sync_google_fondo(id_cliente, nombre, telefono):
    """Guarda el contacto en Google Contacts de forma asíncrona y lo vincula"""
    if not telefono or "LID_" in str(telefono): 
        return
    
    norm = normalizar_telefono_maestro(telefono)
    if norm:
        tel_google = norm.get('google', norm['db'])
        exito = crear_en_google(nombre, "", tel_google)
        
        if exito:
            log_info(f"✅ Sincronización Google exitosa para: {nombre} ({tel_google})")
            
            # Buscar el contacto recién creado para obtener su ID
            res_g = buscar_contacto_google(norm['db'])
            if res_g and res_g.get('encontrado'):
                g_id = res_g['google_id']
                try:
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE Clientes SET google_id = :gid WHERE id_cliente = :id"), {"gid": g_id, "id": id_cliente})
                    log_info(f"🔗 Cliente {id_cliente} vinculado permanentemente con Google ID: {g_id}")
                except Exception as e:
                    log_error(f"❌ Error al guardar el Google ID en la BD: {e}")
        else:
            log_error(f"❌ Falló sincronización con Google para: {nombre}")

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

def comprimir_imagen_waha(image_bytes, max_bytes=2097152):
    if not image_bytes or len(image_bytes) <= max_bytes:
        return image_bytes

    try:
        img = Image.open(io.BytesIO(image_bytes))
        formato = img.format
        
        if formato not in ['JPEG', 'PNG', 'WEBP']:
            return image_bytes

        nuevo_ancho = int(img.width * 0.5)
        nuevo_alto = int(img.height * 0.5)
        
        try:
            resample_filter = Image.Resampling.LANCZOS
        except AttributeError:
            resample_filter = Image.ANTIALIAS
            
        img = img.resize((nuevo_ancho, nuevo_alto), resample_filter)

        output = io.BytesIO()
        if formato == 'PNG':
            img.save(output, format='PNG', optimize=True)
        else:
            img.save(output, format=formato, quality=70, optimize=True)

        return output.getvalue()
    except Exception as e:
        log_error(f"Error al comprimir imagen: {e}")
        return None

# ==============================================================================
# 🕵️ FUNCIONES LOCALES (MEJORADAS PARA LLAMADAS Y NOMBRES)
# ==============================================================================
def obtener_lid_local(payload):
    try:
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}
        candidatos = [
            payload.get('from'), payload.get('to'), payload.get('participant'),
            key.get('remoteJid'), key.get('participant'), _data.get('lid'),
            _data.get('chatId')
        ]
        for c in candidatos:
            if c and isinstance(c, str) and '@lid' in c: return c
        return None
    except: return None

def obtener_telefono_local(payload):
    try:
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}
        
        call_creator = payload.get('callCreator') or _data.get('callCreator') or payload.get('peerJid')
        if call_creator and isinstance(call_creator, str) and ('@s.whatsapp.net' in call_creator or '@c.us' in call_creator):
            return call_creator.split('@')[0]

        alt = key.get('remoteJidAlt')
        if alt and isinstance(alt, str) and ('@s.whatsapp.net' in alt or '@c.us' in alt):
            return alt.split('@')[0]

        candidatos = [payload.get('from'), payload.get('to'), key.get('remoteJid'), payload.get('participant')]
        for c in candidatos:
            if c and isinstance(c, str) and ('@s.whatsapp.net' in c or '@c.us' in c):
                return c.split('@')[0]
        
        user_id = _data.get('id', {}).get('user')
        if user_id and str(user_id).isdigit(): return str(user_id)
        
        return None
    except: return None

def resolver_telefono_api(lid, session):
    if not WAHA_URL or not lid: return None
    try:
        lid_safe = lid.replace('@', '%40')
        url = f"{WAHA_URL.rstrip('/')}/api/{session}/lids/{lid_safe}"
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            pn = data.get('pn')
            if pn:
                log_info(f"✨ API Resuelta: {lid} es {pn}")
                return pn.split('@')[0]
    except Exception as e:
        log_error(f"Error API WAHA LIDs: {e}")
    return None

def obtener_nombre_waha(contact_id, session):
    """Consulta la API de WAHA para obtener el nombre configurado por el usuario en WhatsApp"""
    if not WAHA_URL or not contact_id: return None
    try:
        url = f"{WAHA_URL.rstrip('/')}/api/contacts/contact"
        params = {"contactId": contact_id, "session": session}
        headers = {"Content-Type": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        r = requests.get(url, params=params, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            nombre = data.get('name') or data.get('pushname') or data.get('shortName')
            if nombre:
                log_info(f"✨ Nombre obtenido de API WAHA: {nombre}")
                return nombre
    except Exception as e:
        log_error(f"Error API WAHA Contacts: {e}")
    return None

# ==============================================================================
# 🚀 WEBHOOK PRINCIPAL V54 (Fix Vinculación y Extracción Nombres)
# ==============================================================================

@app.route('/', methods=['GET'])
def home():
    return "Webhook V54 (Smart Sync & Name Fix) ✅", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200
        
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

            if tipo_evento not in ['message', 'message.any', 'message.created', 'call.received']: 
                continue
            if payload.get('from') == 'status@broadcast': continue

            wspid_lid = obtener_lid_local(payload)
            telefono_crudo = obtener_telefono_local(payload)
            
            telefono_num = None
            if telefono_crudo:
                norm = normalizar_telefono_maestro(telefono_crudo)
                if isinstance(norm, dict): telefono_num = norm.get('db')
                else: telefono_num = norm

            log_info(f"🏁 Inicio Proceso: Tel={telefono_num} | LID={wspid_lid}")

            body = "📞 Llamada entrante" if tipo_evento == 'call.received' else payload.get('body', '')
            media_url = payload.get('mediaUrl') or (payload.get('media') or {}).get('url')
            archivo_bytes = descargar_media_plus(media_url) if media_url else None
            
            if archivo_bytes:
                archivo_bytes = comprimir_imagen_waha(archivo_bytes)
                
            if archivo_bytes and not body: body = "📷 Archivo Multimedia"
            
            tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
            whatsapp_id = payload.get('id')
            reply_id = (payload.get('replyTo') or {}).get('id')
            reply_content = (payload.get('replyTo') or {}).get('body')

            # --- LÓGICA INTELIGENTE DE NOMBRES (CORREGIDA) ---
            wsp_id_contact = payload.get('from') if tipo_msg == 'ENTRANTE' else payload.get('to')
            
            # 1. Buscar el nombre en todas las posibles rutas de WAHA
            _data = payload.get('_data') or {}
            nombre_wsp = payload.get('pushName') or _data.get('notifyName') or _data.get('pushname') or _data.get('name')
            
            # 2. Si no viene en el mensaje, consultar a la API de WAHA
            if not nombre_wsp and wsp_id_contact:
                nombre_wsp = obtener_nombre_waha(wsp_id_contact, session_name)
            
            # 3. Determinar Nombre Corto y Nombre IA
            nombre_corto_final = nombre_wsp if nombre_wsp and nombre_wsp.strip() else "Cliente Nuevo"
            nombre_ia_final = nombre_corto_final.split()[0] if nombre_corto_final != "Cliente Nuevo" else ""

            id_cliente_final = None

            try:
                with engine.begin() as conn:
                    # CASO A: WAHA envía LID + Teléfono Numérico
                    if wspid_lid and telefono_num:
                        cliente_tel = conn.execute(text("SELECT id_cliente, whatsapp_internal_id FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                        if cliente_tel:
                            if cliente_tel.whatsapp_internal_id != wspid_lid:
                                conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :lid, activo=TRUE WHERE id_cliente = :id"), {"lid": wspid_lid, "id": cliente_tel.id_cliente})
                            else:
                                conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": cliente_tel.id_cliente})
                            id_cliente_final = cliente_tel.id_cliente
                        else:
                            cliente_lid = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                            if cliente_lid:
                                conn.execute(text("UPDATE Clientes SET telefono = :t, activo=TRUE WHERE id_cliente = :id"), {"t": telefono_num, "id": cliente_lid.id_cliente})
                                id_cliente_final = cliente_lid.id_cliente
                            else:
                                try:
                                    res = conn.execute(text("""
                                        INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                                        VALUES (:lid, :t, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                                        RETURNING id_cliente
                                    """), {"lid": wspid_lid, "t": telefono_num, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                                    id_cliente_final = res.id_cliente
                                    # 🛠️ CORRECCIÓN: Pasamos el ID del cliente al hilo de Google
                                    threading.Thread(target=sync_google_fondo, args=(id_cliente_final, nombre_corto_final, telefono_num)).start()
                                except Exception as e:
                                    if "UniqueViolation" in str(e): id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).scalar()
                                    else: raise e

                    # CASO B: WAHA solo envía LID
                    elif wspid_lid and not telefono_num:
                        cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid"), {"lid": wspid_lid}).fetchone()
                        if cliente_lid:
                            if cliente_lid.telefono and len(cliente_lid.telefono) > 5:
                                id_cliente_final = cliente_lid.id_cliente
                                conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                            else:
                                tel_api = resolver_telefono_api(wspid_lid, session_name)
                                if tel_api:
                                    norm_api = normalizar_telefono_maestro(tel_api)
                                    final_tel = norm_api.get('db') if isinstance(norm_api, dict) else norm_api
                                    try:
                                        conn.execute(text("UPDATE Clientes SET telefono = :t, activo=TRUE WHERE id_cliente = :id"), {"t": final_tel, "id": cliente_lid.id_cliente})
                                    except Exception as e:
                                        if "UniqueViolation" in str(e): id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono=:t"), {"t": final_tel}).scalar()
                                if not id_cliente_final: id_cliente_final = cliente_lid.id_cliente
                        else:
                            tel_api = resolver_telefono_api(wspid_lid, session_name)
                            if tel_api:
                                norm_api = normalizar_telefono_maestro(tel_api)
                                final_tel = norm_api.get('db') if isinstance(norm_api, dict) else norm_api
                                cliente_tel_api = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": final_tel}).fetchone()
                                if cliente_tel_api:
                                    conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :lid, activo=TRUE WHERE id_cliente = :id"), {"lid": wspid_lid, "id": cliente_tel_api.id_cliente})
                                    id_cliente_final = cliente_tel_api.id_cliente
                                else:
                                    try:
                                        res = conn.execute(text("""
                                            INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                                            VALUES (:lid, :t, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                                            RETURNING id_cliente
                                        """), {"lid": wspid_lid, "t": final_tel, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                                        id_cliente_final = res.id_cliente
                                        # 🛠️ CORRECCIÓN: Pasamos el ID del cliente al hilo de Google
                                        threading.Thread(target=sync_google_fondo, args=(id_cliente_final, nombre_corto_final, final_tel)).start()
                                    except Exception as e:
                                        if "UniqueViolation" in str(e): id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": final_tel}).scalar()
                                        else: raise e
                            else:
                                    fake = f"LID_{wspid_lid.split('@')[0]}"
                                    try:
                                        res = conn.execute(text("""
                                            INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                                            VALUES (:lid, :f, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                                            RETURNING id_cliente
                                        """), {"lid": wspid_lid, "f": fake, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                                        id_cliente_final = res.id_cliente
                                    except: id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE whatsapp_internal_id=:lid"), {"lid": wspid_lid}).scalar()

                    # CASO C: WAHA solo envía Teléfono Numérico
                    elif telefono_num and not wspid_lid:
                        cliente_tel = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).fetchone()
                        if cliente_tel:
                            id_cliente_final = cliente_tel.id_cliente
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        else:
                            fallback_id = payload.get('from')
                            try:
                                res = conn.execute(text("""
                                    INSERT INTO Clientes (whatsapp_internal_id, telefono, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                                    VALUES (:wid, :t, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                                    RETURNING id_cliente
                                """), {"wid": fallback_id, "t": telefono_num, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                                id_cliente_final = res.id_cliente
                                # 🛠️ CORRECCIÓN: Pasamos el ID del cliente al hilo de Google
                                threading.Thread(target=sync_google_fondo, args=(id_cliente_final, nombre_corto_final, telefono_num)).start()
                            except Exception as e:
                                if "UniqueViolation" in str(e): id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).scalar()
                                else: raise e

                    # GUARDADO Y SINCRONIZACIÓN CON TABLA SECUNDARIA
                    if id_cliente_final:
                        t_msg = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente = :id"), {"id": int(id_cliente_final)}).scalar()
                        if not t_msg: t_msg = "DESCONOCIDO"

                        # --- GARANTIZAR QUE EL TELÉFONO EXISTA EN LA TABLA SECUNDARIA ---
                        if t_msg and not str(t_msg).startswith("LID_") and t_msg != "DESCONOCIDO":
                            tel_existe = conn.execute(text("SELECT 1 FROM telefonoscliente WHERE id_cliente = :id AND telefono = :t AND activo = TRUE"), {"id": int(id_cliente_final), "t": t_msg}).scalar()
                            if not tel_existe:
                                conn.execute(text("UPDATE telefonoscliente SET es_principal = FALSE WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                                conn.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, es_principal) VALUES (:id, :t, TRUE)"), {"id": int(id_cliente_final), "t": t_msg})
                        # ----------------------------------------------------------------

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
                        
                        # --- LÓGICA DE DETECCIÓN ZOMBIE ---
                        if tipo_msg == 'ENTRANTE':
                            texto_limpio = body.strip().lower()
                            
                            if archivo_bytes or "archivo multimedia" in texto_limpio:
                                conn.execute(text("UPDATE Clientes SET nivel_zombie = 0 WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                            else:
                                es_clave = conn.execute(text("SELECT 1 FROM respuestas_automaticas WHERE LOWER(frase_clave) = :t LIMIT 1"), {"t": texto_limpio}).scalar()
                                
                                if es_clave:
                                    conn.execute(text("UPDATE Clientes SET nivel_zombie = 1, ultimo_msg_zombie = (NOW() - INTERVAL '5 hours') WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                                else:
                                    conn.execute(text("UPDATE Clientes SET nivel_zombie = 0 WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
            except Exception as e:
                log_error(f"🔥 Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)