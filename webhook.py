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
            res_g = buscar_contacto_google(norm['db'])
            if res_g and res_g.get('encontrado'):
                g_id = res_g['google_id']
                g_nom = res_g.get('nombre', nombre)
                g_ape = res_g.get('apellido', '')
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE Clientes 
                            SET google_id = :gid, nombre = :n, apellido = :a 
                            WHERE id_cliente = :id
                        """), {"gid": g_id, "n": g_nom, "a": g_ape, "id": id_cliente})
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
# 🕵️ FUNCIONES LOCALES CORREGIDAS
# ==============================================================================
# 🛠️ CORRECCIÓN: Se recibe from_me como parámetro para no fallar en la extracción
def obtener_lid_local(payload, from_me=False):
    try:
        me_lid = payload.get('me', {}).get('lid', '')
        target = payload.get('to') if from_me else payload.get('from')

        _data = payload.get('_data') or {}
        key = _data.get('key') or {}
        remote_id = _data.get('id', {}).get('remote')

        candidatos = [
            remote_id, target, payload.get('participant'),
            key.get('remoteJid'), _data.get('lid'), _data.get('chatId')
        ]
        for c in candidatos:
            if c and isinstance(c, str) and '@lid' in c:
                if c != me_lid:  # EVITAMOS DEVOLVER TU PROPIO LID
                    return c
        return None
    except: return None

# 🛠️ CORRECCIÓN: Se recibe from_me robusto para definir bien el target
def obtener_telefono_local(payload, from_me=False):
    try:
        me_id = payload.get('me', {}).get('id', '')
        me_phone = me_id.split('@')[0] if me_id else ''

        target = payload.get('to') if from_me else payload.get('from')

        _data = payload.get('_data') or {}
        key = _data.get('key') or {}
        remote_id = _data.get('id', {}).get('remote')

        call_creator = payload.get('callCreator') or _data.get('callCreator') or payload.get('peerJid')
        if call_creator and isinstance(call_creator, str) and ('@s.whatsapp.net' in call_creator or '@c.us' in call_creator):
            num = call_creator.split('@')[0]
            if num != me_phone: return num

        candidatos = [
            remote_id, target, key.get('remoteJidAlt'), 
            key.get('remoteJid'), payload.get('participant')
        ]
        for c in candidatos:
            if c and isinstance(c, str) and ('@s.whatsapp.net' in c or '@c.us' in c):
                num = c.split('@')[0]
                if num != me_phone: return num

        user_id = _data.get('id', {}).get('user')
        if user_id and str(user_id).isdigit():
            if str(user_id) != me_phone: return str(user_id)

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
# 🚀 WEBHOOK PRINCIPAL
# ==============================================================================

@app.route('/', methods=['GET'])
def home():
    return "Webhook V57 (Fix UI Chat Invertido) ✅", 200

@app.route('/api/alertas', methods=['POST'])
def recibir_alerta():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        log_error(f"🚨 ALERTA CRÍTICA RECIBIDA: {data}")

        with engine.begin() as conn:
            p_str = json.dumps(data, ensure_ascii=False)
            session_name = data.get('sesion', 'SISTEMA')
            conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                        {"s": session_name, "e": "ALERTA_CRITICA", "p": p_str})

        return jsonify({"status": "success"}), 200
    except Exception as e:
        log_error(f"🔥 Error procesando alerta: {e}")
        return jsonify({"status": "error"}), 500

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

            msg_id_obj = payload.get('id')
            whatsapp_id = msg_id_obj.get('_serialized') if isinstance(msg_id_obj, dict) else str(msg_id_obj)

            from_me = payload.get('fromMe')
            if from_me is None:
                if isinstance(msg_id_obj, dict) and 'fromMe' in msg_id_obj:
                    from_me = msg_id_obj.get('fromMe')
                else:
                    from_me = str(whatsapp_id).startswith('true_') or payload.get('_data', {}).get('id', {}).get('fromMe', False)

            if tipo_evento == 'engine.event':
                continue

            is_broadcast = (
                payload.get('from') == 'status@broadcast' or 
                payload.get('to') == 'status@broadcast' or
                payload.get('_data', {}).get('id', {}).get('remote') == 'status@broadcast' or
                'status@broadcast' in str(whatsapp_id)
            )

            tipo_msg_waha = payload.get('type') or payload.get('_data', {}).get('type', '')
            subtipo = payload.get('subtype') or payload.get('_data', {}).get('subtype', '')

            es_sistema = (
                tipo_msg_waha in ['notification_template', 'e2e_notification', 'gp2', 'system', 'protocol'] or
                subtipo in ['biz_me_account_type_is_hosted', 'ephemeral_setting']
            )

            if is_broadcast or es_sistema or (not payload.get('body') and not payload.get('hasMedia') and tipo_evento != 'call.received'):
                continue

            if not from_me and tipo_evento not in ['message.ack']:
                try:
                    with engine.begin() as conn:
                        p_str = json.dumps(evento, ensure_ascii=False)[:5000]
                        conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                                    {"s": session_name, "e": tipo_evento, "p": p_str})
                        conn.execute(text("DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 50)"))
                except Exception as e:
                    log_error(f"Error DB Log Raw: {e}")

            if tipo_evento == 'message.ack':
                ack_status = payload.get('ack') 
                estado_map = {1: 'enviado', 2: 'recibido', 3: 'leido', 4: 'reproducido'}
                nuevo_estado = estado_map.get(ack_status, 'pendiente')
                try:
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE mensajes SET estado_waha = :e WHERE whatsapp_id = :w"), {"e": nuevo_estado, "w": whatsapp_id})
                        conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
                except: pass
                continue 

            if tipo_evento == 'message.edited':
                msg_id = payload.get('editedMessageId')
                new_body = payload.get('body', '')
                try:
                    with engine.begin() as conn:
                        old_msg = conn.execute(text("SELECT contenido FROM mensajes WHERE whatsapp_id = :wid"), {"wid": msg_id}).scalar()
                        if old_msg:
                            import re
                            partes = old_msg.split('<!--HISTORIAL-->')
                            texto_previo = partes[0].strip()
                            historial_acumulado = partes[1] if len(partes) > 1 else ""
                            if historial_acumulado:
                                m = re.search(r'<div class="items-historial"[^>]*>(.*?)</div>\s*</details>', historial_acumulado, re.DOTALL)
                                historial_acumulado = m.group(1) if m else ""

                            ahora_str = datetime.now().strftime("%d/%m %I:%M %p")
                            nuevo_item = f"<div style='margin-bottom: 6px;'><i>{ahora_str}:</i><br><s>{texto_previo}</s></div>"
                            historial_final = nuevo_item + historial_acumulado
                            nuevo_contenido = f"{new_body}<!--HISTORIAL--><div class='historial-edicion' style='margin-top: 5px; font-size: 11px;'><details style='cursor: pointer; color: #666; background: rgba(0,0,0,0.05); padding: 4px; border-radius: 4px;'><summary style='outline: none; font-weight: bold;'>✏️ Ver historial</summary><div class='items-historial' style='margin-top: 5px; padding-top: 5px; border-top: 1px dashed #ccc; color: #888;'>{historial_final}</div></details></div>"
                            
                            conn.execute(text("UPDATE mensajes SET contenido = :nuevo WHERE whatsapp_id = :wid"), {"nuevo": nuevo_contenido, "wid": msg_id})
                            conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
                except Exception as e:
                    log_error(f"Error editando mensaje: {e}")
                continue

            if tipo_evento in ['message.revoked', 'message_revoke_everyone']:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE mensajes 
                            SET contenido = contenido || '<br><span style="font-size: 11px; color: #c0392b; background: #fadbd8; padding: 2px 6px; border-radius: 4px; display: inline-block; margin-top: 5px;">🚫 Mensaje eliminado</span>'
                            WHERE whatsapp_id = :wid AND contenido NOT LIKE '%Mensaje eliminado%'
                        """), {"wid": whatsapp_id})
                        conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
                except Exception as e:
                    log_error(f"Error marcando mensaje como eliminado: {e}")
                continue

            if tipo_evento not in ['message', 'message.any', 'message.created', 'call.received']: 
                continue

            if tipo_evento == "message.any" and not from_me:
                continue
            if tipo_evento == "message" and from_me:
                continue

            # 🛠️ CORRECCIÓN: Le pasamos from_me a las funciones de abajo
            wspid_lid = obtener_lid_local(payload, from_me)
            telefono_crudo = obtener_telefono_local(payload, from_me)

            telefono_num = None
            if telefono_crudo:
                norm = normalizar_telefono_maestro(telefono_crudo)
                if isinstance(norm, dict): telefono_num = norm.get('db')
                else: telefono_num = norm

            log_info(f"🏁 Inicio Proceso: Tel={telefono_num} | LID={wspid_lid}")

            body = "📞 Llamada entrante" if tipo_evento == 'call.received' else payload.get('body', '')
            
            tipo_mensaje_real = payload.get('type') or payload.get('_data', {}).get('type')
            datos_ubicacion = payload.get('location') or payload.get('_data', {}).get('location') or {}

            if tipo_mensaje_real == 'location' or datos_ubicacion:
                lat = datos_ubicacion.get('latitude') or datos_ubicacion.get('lat')
                lng = datos_ubicacion.get('longitude') or datos_ubicacion.get('lng')
                url_mapa = datos_ubicacion.get('url') or payload.get('_data', {}).get('loc')
                
                if url_mapa: body = f"📍 Ubicación compartida: {url_mapa}"
                elif lat and lng: body = f"📍 Ubicación compartida: https://maps.google.com/?q={lat},{lng}"
                else: body = "📍 Ubicación compartida (Google Maps)"
            elif isinstance(body, str) and body.startswith('/9j/'):
                body = "📍 [Ubicación o enlace compartido]"

            has_media = payload.get('hasMedia') or tipo_mensaje_real in ['image', 'video', 'audio', 'document', 'sticker', 'ptt']
            media_url = payload.get('mediaUrl') or (payload.get('media') or {}).get('url')

            if has_media and not media_url and WAHA_URL:
                msg_id_safe = str(whatsapp_id).replace('@', '%40')
                media_url = f"{WAHA_URL.rstrip('/')}/api/{session_name}/messages/{msg_id_safe}/download"

            archivo_bytes = descargar_media_plus(media_url) if media_url else None
            if archivo_bytes: 
                archivo_bytes = comprimir_imagen_waha(archivo_bytes)
            
            if has_media and not body: 
                body = "📷 Archivo Multimedia" if archivo_bytes else "📷 [Multimedia enviada]"

            # 🛠️ CORRECCIÓN: Asignamos explícitamente SALIENTE_BOT para que el Streamlit panel lo ubique del lado derecho
            tipo_msg = 'SALIENTE_BOT' if from_me else 'ENTRANTE'
            
            reply_id = (payload.get('replyTo') or {}).get('id')
            reply_content = (payload.get('replyTo') or {}).get('body')

            wsp_id_contact = payload.get('from') if tipo_msg == 'ENTRANTE' else payload.get('to')
            _data = payload.get('_data') or {}
            nombre_wsp = payload.get('pushName') or _data.get('notifyName') or _data.get('pushname') or _data.get('name')

            if not nombre_wsp and wsp_id_contact:
                nombre_wsp = obtener_nombre_waha(wsp_id_contact, session_name)

            nombre_corto_final = nombre_wsp if nombre_wsp and nombre_wsp.strip() else "Cliente Nuevo"
            nombre_ia_final = nombre_corto_final.split()[0] if nombre_corto_final != "Cliente Nuevo" else ""
            id_cliente_final = None

            try:
                with engine.begin() as conn:
                    if wspid_lid and not telefono_num:
                        tel_api = resolver_telefono_api(wspid_lid, session_name)
                        if tel_api:
                            norm_api = normalizar_telefono_maestro(tel_api)
                            telefono_num = norm_api.get('db') if isinstance(norm_api, dict) else norm_api

                    cliente_tel = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono = :t LIMIT 1"), {"t": telefono_num}).fetchone() if telefono_num else None
                    cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM telefonoscliente WHERE lid = :lid LIMIT 1"), {"lid": wspid_lid}).fetchone() if wspid_lid else None

                    if not cliente_lid and wspid_lid:
                        cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid LIMIT 1"), {"lid": wspid_lid}).fetchone()
                    if not cliente_tel and telefono_num:
                        cliente_tel = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE telefono = :t LIMIT 1"), {"t": telefono_num}).fetchone()

                    # Escenario 1: Colisión y Fusión
                    if cliente_tel and cliente_lid and cliente_tel.id_cliente != cliente_lid.id_cliente:
                        viejo_tel = cliente_lid.telefono
                        if telefono_num:
                            conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o OR telefono=:lid_str"), {"n": telefono_num, "o": viejo_tel, "lid_str": wspid_lid})
                        conn.execute(text("""
                            UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE, lid = :lid, alias = :alias 
                            WHERE id_cliente = :old
                        """), {"new": cliente_tel.id_cliente, "old": cliente_lid.id_cliente, "lid": wspid_lid, "alias": nombre_corto_final})
                        conn.execute(text("UPDATE Clientes SET estado='Duplicado', activo=FALSE, whatsapp_internal_id=NULL WHERE id_cliente=:old"), {"old": cliente_lid.id_cliente})
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:new"), {"new": cliente_tel.id_cliente})
                        id_cliente_final = cliente_tel.id_cliente

                    # Escenario 2: Teléfono existe
                    elif cliente_tel:
                        id_cliente_final = cliente_tel.id_cliente
                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                        if wspid_lid:
                            conn.execute(text("""
                                UPDATE telefonoscliente SET lid = :lid, alias = :alias WHERE id_cliente = :id AND telefono = :t
                            """), {"lid": wspid_lid, "alias": nombre_corto_final, "id": id_cliente_final, "t": telefono_num})

                    # Escenario 3: LID existe
                    elif cliente_lid:
                        id_cliente_final = cliente_lid.id_cliente
                        viejo_tel = cliente_lid.telefono
                        if telefono_num and viejo_tel != telefono_num:
                            conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o OR telefono=:lid_str"), {"n": telefono_num, "o": viejo_tel, "lid_str": wspid_lid})
                            conn.execute(text("""
                                UPDATE telefonoscliente SET telefono=:n, lid=:lid, alias=:alias WHERE id_cliente=:id AND (telefono=:o OR lid=:lid)
                            """), {"n": telefono_num, "lid": wspid_lid, "alias": nombre_corto_final, "o": viejo_tel, "id": id_cliente_final})
                            conn.execute(text("UPDATE Clientes SET telefono=:n, activo=TRUE WHERE id_cliente=:id"), {"n": telefono_num, "id": id_cliente_final})
                        else:
                            conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:id"), {"id": id_cliente_final})
                            conn.execute(text("UPDATE telefonoscliente SET alias=:alias WHERE id_cliente=:id AND lid=:lid"), {"alias": nombre_corto_final, "id": id_cliente_final, "lid": wspid_lid})

                    # Escenario 4: Nuevo contacto
                    else:
                        try:
                            res = conn.execute(text("""
                                INSERT INTO Clientes (telefono, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                                VALUES (:t, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                                RETURNING id_cliente
                            """), {"t": telefono_num, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                            id_cliente_final = res.id_cliente

                            conn.execute(text("""
                                INSERT INTO telefonoscliente (id_cliente, telefono, lid, alias, es_principal, activo)
                                VALUES (:id, :t, :lid, :alias, TRUE, TRUE)
                            """), {"id": id_cliente_final, "t": telefono_num, "lid": wspid_lid, "alias": nombre_corto_final})

                            if telefono_num:
                                threading.Thread(target=sync_google_fondo, args=(id_cliente_final, nombre_corto_final, telefono_num)).start()

                        except Exception as e:
                            if "UniqueViolation" in str(e):
                                if telefono_num: id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).scalar()
                                else: id_cliente_final = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE lid = :lid"), {"lid": wspid_lid}).scalar()
                            else: raise e

                    if id_cliente_final:
                        t_msg = telefono_num if telefono_num else wspid_lid
                        existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                        
                        is_echo = False
                        fue_insertado = False

                        if not existe:
                            # 🛠️ CORRECCIÓN: Ahora evalúa SALIENTE_BOT o SALIENTE_PANEL de manera inteligente
                            if tipo_msg == 'SALIENTE_BOT':
                                match = conn.execute(text("""
                                    SELECT id_mensaje FROM mensajes 
                                    WHERE telefono = :t AND tipo IN ('SALIENTE_BOT', 'SALIENTE_PANEL') AND whatsapp_id IS NULL 
                                      AND contenido = :txt AND fecha >= NOW() - INTERVAL '15 minutes'
                                    LIMIT 1
                                """), {"t": t_msg, "txt": body}).scalar()
                                
                                if match:
                                    conn.execute(text("UPDATE mensajes SET whatsapp_id = :wid, estado_waha = 'enviado' WHERE id_mensaje = :idm"), {"wid": whatsapp_id, "idm": match})
                                    existe = True
                                    log_info("🔗 Mensaje saliente de Bot fusionado correctamente con el Webhook.")

                            elif tipo_msg == 'ENTRANTE':
                                is_echo = conn.execute(text("""
                                    SELECT 1 FROM mensajes 
                                    WHERE telefono = :t AND tipo IN ('SALIENTE_BOT', 'SALIENTE_PANEL') 
                                      AND contenido = :txt AND fecha >= NOW() - INTERVAL '15 minutes'
                                    LIMIT 1
                                """), {"t": t_msg, "txt": body}).scalar()
                                
                                if is_echo:
                                    log_info(f"🚫 Fantasma WEBJS bloqueado. Ignorando eco entrante: {body[:20]}")

                        if not existe and not is_echo:
                            # 🛠️ CORRECCIÓN: "leido" debe comprobar SALIENTE_BOT (fue renombrado)
                            conn.execute(text("""
                                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content, estado_waha, session_name)
                                VALUES (:t, :tipo, :txt, NOW() - INTERVAL '5 hours', :leido, :d, :wid, :rid, :rbody, :est, :sess)
                            """), {
                                "t": t_msg, "tipo": tipo_msg, "txt": body, "leido": (tipo_msg == 'SALIENTE_BOT'), "d": archivo_bytes,
                                "wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado', "sess": session_name
                            })
                            fue_insertado = True

                        conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))

                        if fue_insertado and tipo_msg == 'ENTRANTE':
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