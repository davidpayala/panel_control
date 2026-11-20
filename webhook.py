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
import time

app = Flask(__name__)

WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 
from waha_motor import url_waha, url_media, headers_waha  # motor (NOWEB/WEBJS) por acción

def _dic(valor):
    """Devuelve el valor si es un diccionario; si no, {}. Según el motor y el tipo de evento, WAHA manda
    algunos campos como texto (ej. en las llamadas de WEBJS «_data.id» es un string) y un .get() encadenado
    sobre un string tiraba «'str' object has no attribute 'get'» y se perdía el evento completo."""
    return valor if isinstance(valor, dict) else {}


def log_info(msg):
    print(f"[INFO] {msg}", file=sys.stdout, flush=True)

def log_error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

try:
    from utils import normalizar_telefono_maestro, crear_en_google, buscar_contacto_google, marcar_chat_como_leido_waha
except ImportError:
    def normalizar_telefono_maestro(t): return {"db": "".join(filter(str.isdigit, str(t)))}
    def crear_en_google(n, a, t): return False
    def buscar_contacto_google(t): return None
    def marcar_chat_como_leido_waha(*args): pass

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

def descargar_media_plus(media_url, motor=None):
    """Descarga el archivo desde el contenedor WAHA que lo recibió (motor del evento)."""
    try:
        if not media_url: return None
        url_final = url_media(media_url, motor)
        r = requests.get(url_final, headers=headers_waha(json=False), timeout=10)
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


def _sin_dispositivo(jid):
    """'51941380271:62@s.whatsapp.net' → '51941380271@s.whatsapp.net' (GOWS agrega el n° de dispositivo)."""
    if not jid or not isinstance(jid, str) or '@' not in jid:
        return jid
    usuario, dominio = jid.split('@', 1)
    return f"{usuario.split(':')[0]}@{dominio}"


def _candidatos_chat(payload, from_me):
    """JIDs donde puede venir el contacto, en orden de preferencia, para NOWEB, WEBJS y GOWS.
    En los mensajes propios (fromMe) NOWEB y GOWS ponen el chat en 'from' y dejan 'to' vacío."""
    _data = _dic(payload.get('_data'))
    key = _dic(_data.get('key'))
    info = _dic(_data.get('Info'))  # GOWS
    remote_id = _dic(_data.get('id')).get('remote')
    target = (payload.get('to') or payload.get('from')) if from_me else payload.get('from')
    crudos = [
        remote_id, target, payload.get('participant'),
        key.get('remoteJid'), key.get('remoteJidAlt'),
        info.get('Chat'),
        info.get('RecipientAlt') if from_me else info.get('SenderAlt'),
        None if from_me else info.get('Sender'),
        _data.get('lid'), _data.get('chatId'),
    ]
    return [_sin_dispositivo(c) for c in crudos if c and isinstance(c, str)]


def obtener_lid_local(payload, from_me=False, me=None):
    try:
        me_lid = _sin_dispositivo(_dic(me).get('lid') or _dic(payload.get('me')).get('lid') or '')
        for c in _candidatos_chat(payload, from_me):
            if '@lid' in c and c != me_lid:
                return c
        return None
    except Exception: return None

def obtener_telefono_local(payload, from_me=False, me=None):
    try:
        me_id = _dic(me).get('id') or _dic(payload.get('me')).get('id') or ''
        me_phone = me_id.split('@')[0] if me_id else ''

        _data = _dic(payload.get('_data'))

        call_creator = payload.get('callCreator') or _data.get('callCreator') or payload.get('peerJid')
        if call_creator and isinstance(call_creator, str) and ('@s.whatsapp.net' in call_creator or '@c.us' in call_creator):
            num = _sin_dispositivo(call_creator).split('@')[0]
            if num != me_phone: return num

        for c in _candidatos_chat(payload, from_me):
            if '@s.whatsapp.net' in c or '@c.us' in c:
                num = c.split('@')[0]
                if num != me_phone: return num

        user_id = _dic(_data.get('id')).get('user')
        if user_id and str(user_id).isdigit():
            if str(user_id) != me_phone: return str(user_id)

        return None
    except Exception: return None

def resolver_telefono_api(lid, session):
    if not lid: return None
    try:
        lid_safe = lid.replace('@', '%40')
        url = f"{url_waha('contactos')}/api/{session}/lids/{lid_safe}"
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
    if not contact_id: return None
    try:
        # Ruta correcta de WAHA: GET /api/contacts?contactId=...&session=... (/api/contacts/contact no existe)
        url = f"{url_waha('contactos')}/api/contacts"
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

# Caché en memoria para evitar spam (almacena {telefono: timestamp_ultima_respuesta})
ultima_respuesta_zombie = {}

def enviar_respuesta_zombie_async(telefono, session_name, respuesta, delay=30, msg_id=None):
    """Espera los segundos indicados y envía la respuesta automática, forzando entrega E2E."""
    try:
        ahora = time.time()
        
        # Validación de spam: Si le hemos respondido en los últimos 60 segundos, cancelamos este hilo.
        ultimo_envio = ultima_respuesta_zombie.get(telefono, 0)
        if ahora - ultimo_envio < 60:
            log_info(f"🛑 Hilo Zombie CANCELADO: Se bloqueó respuesta repetitiva a {telefono}.")
            return
            
        ultima_respuesta_zombie[telefono] = ahora

        log_info(f"⏳ Hilo Zombie: Esperando {delay}s para responder a {telefono}...")
        time.sleep(delay)
        
        # 🔥 BLINDAJE DE CHAT ID: Previene el error de JIDs malformados (ej. @lid@c.us)
        telefono_str = str(telefono).strip()
        if "@" in telefono_str:
            chat_id = telefono_str.replace("@c.us", "") if "@lid" in telefono_str else telefono_str
        else:
            chat_id = f"{telefono_str}@c.us"

        url = f"{url_waha('enviar_texto')}/api/sendText"
        payload = {
            "chatId": chat_id,
            "text": respuesta,
            "session": session_name
        }
        
        # 🔥 TRUCO E2E: Citar su mensaje fuerza a WhatsApp a sincronizar las llaves de seguridad
        if msg_id:
            payload["reply_to"] = str(msg_id)
            
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY
        
        res = requests.post(url, json=payload, headers=headers, timeout=15)
        if res.status_code in [200, 201]:
            log_info(f"✅ Respuesta Zombie entregada físicamente a {telefono}")
        else:
            log_error(f"❌ Error API WAHA Zombie: {res.text}")
            
    except Exception as e:
        log_error(f"🔥 Error en hilo de respuesta zombie: {e}")

# ==============================================================================
# 🚀 WEBHOOK PRINCIPAL
# ==============================================================================

@app.route('/', methods=['GET'])
def home():
    return "Webhook V58 (Fix Zombie E2E) ✅", 200

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

def procesar_evento(evento):
    """Procesa UN evento de WAHA. Si falla, solo se pierde este evento (no todo el lote)."""
    tipo_evento = evento.get('event')
    session_name = evento.get('session', 'default')
    motor_evento = evento.get('engine')  # 'NOWEB' / 'WEBJS': de qué contenedor vino el evento
    payload = evento.get('payload', {})

    msg_id_obj = payload.get('id')
    whatsapp_id = msg_id_obj.get('_serialized') if isinstance(msg_id_obj, dict) else str(msg_id_obj)

    from_me = payload.get('fromMe')
    if from_me is None:
        if isinstance(msg_id_obj, dict) and 'fromMe' in msg_id_obj:
            from_me = msg_id_obj.get('fromMe')
        else:
            from_me = str(whatsapp_id).startswith('true_') or _dic(_dic(payload.get('_data')).get('id')).get('fromMe', False)

    if tipo_evento == 'engine.event':
        return

    is_broadcast = (
        payload.get('from') == 'status@broadcast' or 
        payload.get('to') == 'status@broadcast' or
        _dic(_dic(payload.get('_data')).get('id')).get('remote') == 'status@broadcast' or
        'status@broadcast' in str(whatsapp_id)
    )

    tipo_msg_waha = payload.get('type') or _dic(payload.get('_data')).get('type', '')
    subtipo = payload.get('subtype') or _dic(payload.get('_data')).get('subtype', '')

    es_sistema = (
        tipo_msg_waha in ['notification_template', 'e2e_notification', 'gp2', 'system', 'protocol'] or
        subtipo in ['biz_me_account_type_is_hosted', 'ephemeral_setting']
    )

    if is_broadcast or es_sistema or (not payload.get('body') and not payload.get('hasMedia') and tipo_evento != 'call.received'):
        return

    if tipo_evento not in ['message.ack']:
        try:
            with engine.begin() as conn:
                p_str = json.dumps(evento, ensure_ascii=False)[:5000]
                conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), 
                            {"s": session_name, "e": tipo_evento, "p": p_str})
                conn.execute(text("DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 150)"))
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
        return 

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
        return

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
        return

    if tipo_evento not in ['message', 'message.any', 'message.created', 'call.received']: 
        return

    if tipo_evento == "message.any" and not from_me:
        return
    if tipo_evento == "message" and from_me:
        return

    me_evento = evento.get('me')  # WAHA manda la cuenta propia a nivel del evento, no dentro de payload
    wspid_lid = obtener_lid_local(payload, from_me, me_evento)
    telefono_crudo = obtener_telefono_local(payload, from_me, me_evento)

    telefono_num = None
    if telefono_crudo:
        norm = normalizar_telefono_maestro(telefono_crudo)
        if isinstance(norm, dict): telefono_num = norm.get('db')
        else: telefono_num = norm

    log_info(f"🏁 Inicio Proceso: Tel={telefono_num} | LID={wspid_lid}")

    if not telefono_num and not wspid_lid:
        # Sin número ni LID no se sabe de quién es: antes se creaba un cliente vacío. Se descarta y se deja constancia.
        log_error(f"⚠️ Evento sin contacto identificable (motor {motor_evento}, sesión {session_name}, id {whatsapp_id}). Descartado.")
        return

    body = "📞 Llamada entrante" if tipo_evento == 'call.received' else payload.get('body', '')

    tipo_mensaje_real = payload.get('type') or _dic(payload.get('_data')).get('type')
    datos_ubicacion = _dic(payload.get('location')) or _dic(_dic(payload.get('_data')).get('location'))

    if tipo_mensaje_real == 'location' or datos_ubicacion:
        lat = datos_ubicacion.get('latitude') or datos_ubicacion.get('lat')
        lng = datos_ubicacion.get('longitude') or datos_ubicacion.get('lng')
        url_mapa = datos_ubicacion.get('url') or _dic(payload.get('_data')).get('loc')

        if url_mapa: body = f"📍 Ubicación compartida: {url_mapa}"
        elif lat and lng: body = f"📍 Ubicación compartida: https://maps.google.com/?q={lat},{lng}"
        else: body = "📍 Ubicación compartida (Google Maps)"
    elif isinstance(body, str) and body.startswith('/9j/'):
        body = "📍 [Ubicación o enlace compartido]"

    has_media = payload.get('hasMedia') or tipo_mensaje_real in ['image', 'video', 'audio', 'document', 'sticker', 'ptt']
    media_url = payload.get('mediaUrl') or _dic(payload.get('media')).get('url')

    if has_media and not media_url:
        msg_id_safe = str(whatsapp_id).replace('@', '%40')
        media_url = f"/api/{session_name}/messages/{msg_id_safe}/download"

    archivo_bytes = descargar_media_plus(media_url, motor_evento) if media_url else None

    if not archivo_bytes and has_media:
        import base64
        b64_str = _dic(payload.get('_data')).get('body')

        if not b64_str or not isinstance(b64_str, str):
            b64_str = _dic(_dic(_dic(payload.get('_data')).get('message')).get('imageMessage')).get('jpegThumbnail')

        if isinstance(b64_str, str) and b64_str.startswith('/9j/'):
            try:
                archivo_bytes = base64.b64decode(b64_str)
                log_info("📥 ¡Imagen rescatada con éxito usando el Base64 interno de WAHA!")
            except Exception as e:
                log_error(f"Error decodificando base64: {e}")

    if archivo_bytes: 
        archivo_bytes = comprimir_imagen_waha(archivo_bytes)

    if has_media and not body: 
        body = "📷 Archivo Multimedia" if archivo_bytes else "📷 [Multimedia enviada]"

    tipo_msg = 'SALIENTE_BOT' if from_me else 'ENTRANTE'

    reply_id = _dic(payload.get('replyTo')).get('id')
    reply_content = _dic(payload.get('replyTo')).get('body')

    wsp_id_contact = payload.get('from') if tipo_msg == 'ENTRANTE' else payload.get('to')
    _data = _dic(payload.get('_data'))
    nombre_wsp = payload.get('pushName') or _data.get('notifyName') or _data.get('pushname') or _data.get('name')

    if not nombre_wsp and wsp_id_contact:
        nombre_wsp = obtener_nombre_waha(wsp_id_contact, session_name)

    nombre_corto_final = nombre_wsp if nombre_wsp and nombre_wsp.strip() else "Cliente Nuevo"
    nombre_ia_final = nombre_corto_final.split()[0] if nombre_corto_final != "Cliente Nuevo" else ""
    id_cliente_final = None

    # Número oculto: se intenta resolver el LID → teléfono ANTES de abrir la transacción
    # (es una llamada HTTP que puede tardar 5 s; no conviene tener la base bloqueada mientras tanto).
    if wspid_lid and not telefono_num:
        tel_api = resolver_telefono_api(wspid_lid, session_name)
        if tel_api:
            norm_api = normalizar_telefono_maestro(tel_api)
            telefono_num = norm_api.get('db') if isinstance(norm_api, dict) else norm_api

    try:
        with engine.begin() as conn:
            # 🔒 Un contacto a la vez: WAHA puede mandar 2 eventos del mismo contacto al mismo tiempo (ej. el cliente
            # envía texto + foto). Sin este candado, los dos intentaban crear el mismo cliente nuevo, uno chocaba con
            # la llave única y su mensaje se perdía («InFailedSqlTransaction»). El candado se libera al terminar.
            for clave in sorted({c for c in (telefono_num, wspid_lid) if c}):
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"contacto:{clave}"})

            # ℹ️ telefonoscliente.alias está RESERVADO para el nuevo «usuario» de WhatsApp (empieza con @, ej. @maria.lentes).
            # WAHA todavía no lo entrega, así que el webhook NO escribe alias (antes guardaba ahí el nombre de perfil).
            # Lo vigila recurrente/agente_waha_alias.py; cuando WAHA lo soporte, se llenará aquí.
            cliente_tel = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono = :t LIMIT 1"), {"t": telefono_num}).fetchone() if telefono_num else None
            cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM telefonoscliente WHERE lid = :lid LIMIT 1"), {"lid": wspid_lid}).fetchone() if wspid_lid else None

            if not cliente_lid and wspid_lid:
                cliente_lid = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE whatsapp_internal_id = :lid LIMIT 1"), {"lid": wspid_lid}).fetchone()
            if not cliente_tel and telefono_num:
                cliente_tel = conn.execute(text("SELECT id_cliente, telefono FROM Clientes WHERE telefono = :t LIMIT 1"), {"t": telefono_num}).fetchone()

            if cliente_tel and cliente_lid and cliente_tel.id_cliente != cliente_lid.id_cliente:
                viejo_tel = cliente_lid.telefono
                if telefono_num:
                    conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o OR telefono=:lid_str"), {"n": telefono_num, "o": viejo_tel, "lid_str": wspid_lid})
                # El LID es único: se suelta de todas las filas y se asigna a UNA sola (antes se ponía en todas las
                # filas del cliente viejo y, si tenía más de un teléfono, chocaba con la llave única y el mensaje se perdía).
                conn.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE lid = :lid"), {"lid": wspid_lid})
                conn.execute(text("""
                    UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE
                    WHERE id_cliente = :old
                """), {"new": cliente_tel.id_cliente, "old": cliente_lid.id_cliente})
                conn.execute(text("""
                    UPDATE telefonoscliente SET lid = :lid
                    WHERE id_telefono = (SELECT id_telefono FROM telefonoscliente
                                         WHERE id_cliente = :new AND telefono = :t ORDER BY es_principal DESC LIMIT 1)
                """), {"lid": wspid_lid, "new": cliente_tel.id_cliente, "t": telefono_num})
                conn.execute(text("UPDATE Clientes SET estado='Duplicado', activo=FALSE, whatsapp_internal_id=NULL WHERE id_cliente=:old"), {"old": cliente_lid.id_cliente})
                conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:new"), {"new": cliente_tel.id_cliente})
                id_cliente_final = cliente_tel.id_cliente

            elif cliente_tel:
                id_cliente_final = cliente_tel.id_cliente
                conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente = :id"), {"id": id_cliente_final})
                if wspid_lid:
                    # El LID ya puede estar en OTRA fila (típico: la fila «solo LID» que se creó cuando el número
                    # estaba oculto). La llave única de lid hacía fallar este UPDATE y el mensaje se perdía.
                    otras = conn.execute(text("""
                        SELECT id_telefono, id_cliente, telefono, es_principal FROM telefonoscliente
                        WHERE lid = :lid AND NOT (id_cliente = :id AND telefono IS NOT DISTINCT FROM :t)
                    """), {"lid": wspid_lid, "id": id_cliente_final, "t": telefono_num}).fetchall()
                    for o in otras:
                        if o.id_cliente == id_cliente_final and not (o.telefono or "").strip():
                            # fila «solo LID» del mismo cliente: se fusiona con la fila del teléfono
                            conn.execute(text("DELETE FROM telefonoscliente WHERE id_telefono = :i"), {"i": o.id_telefono})
                            if o.es_principal:
                                conn.execute(text("UPDATE telefonoscliente SET es_principal = TRUE WHERE id_cliente = :id AND telefono = :t"),
                                             {"id": id_cliente_final, "t": telefono_num})
                        else:
                            conn.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE id_telefono = :i"), {"i": o.id_telefono})
                    if otras:
                        # el historial guardado con el LID pasa al teléfono real
                        conn.execute(text("UPDATE mensajes SET telefono = :n WHERE telefono = :lid"), {"n": telefono_num, "lid": wspid_lid})
                    conn.execute(text("""
                        UPDATE telefonoscliente SET lid = :lid WHERE id_cliente = :id AND telefono = :t
                    """), {"lid": wspid_lid, "id": id_cliente_final, "t": telefono_num})

            elif cliente_lid:
                id_cliente_final = cliente_lid.id_cliente
                viejo_tel = cliente_lid.telefono
                if telefono_num and viejo_tel != telefono_num:
                    conn.execute(text("UPDATE mensajes SET telefono=:n WHERE telefono=:o OR telefono=:lid_str"), {"n": telefono_num, "o": viejo_tel, "lid_str": wspid_lid})
                    conn.execute(text("""
                        UPDATE telefonoscliente SET telefono=:n, lid=:lid WHERE id_cliente=:id AND (telefono=:o OR lid=:lid)
                    """), {"n": telefono_num, "lid": wspid_lid, "o": viejo_tel, "id": id_cliente_final})
                    conn.execute(text("UPDATE Clientes SET telefono=:n, activo=TRUE WHERE id_cliente=:id"), {"n": telefono_num, "id": id_cliente_final})
                else:
                    conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:id"), {"id": id_cliente_final})

            else:
                try:
                    # Cliente nuevo. Si su número está oculto (solo LID), «telefono» queda VACÍO (NULL) en Clientes y en
                    # telefonoscliente: el LID va en su propia columna y el teléfono se completa solo cuando WhatsApp lo revele.
                    # Savepoint: si choca con una llave única, se deshace solo este intento y la transacción sigue viva.
                    with conn.begin_nested():
                        res = conn.execute(text("""
                            INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, nombre_ia, estado, id_etapa, activo, fecha_registro) 
                            VALUES (:t, :lid, :n, :nia, 'Sin empezar', (SELECT id_etapa FROM EtapasCliente WHERE LOWER(TRIM(subgrupo)) = 'sin empezar' LIMIT 1), TRUE, NOW()) 
                            RETURNING id_cliente
                        """), {"t": telefono_num or None, "lid": wspid_lid, "n": nombre_corto_final, "nia": nombre_ia_final}).fetchone()
                        id_cliente_final = res.id_cliente

                        conn.execute(text("""
                            INSERT INTO telefonoscliente (id_cliente, telefono, lid, es_principal, activo)
                            VALUES (:id, :t, :lid, TRUE, TRUE)
                        """), {"id": id_cliente_final, "t": telefono_num or None, "lid": wspid_lid})

                    if telefono_num:
                        threading.Thread(target=sync_google_fondo, args=(id_cliente_final, nombre_corto_final, telefono_num)).start()

                except Exception as e:
                    if "UniqueViolation" in str(e):
                        log_info(f"♻️ Cliente ya existía (creado por otro evento simultáneo). Se reutiliza.")
                        if telefono_num: id_cliente_final = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), {"t": telefono_num}).scalar()
                        if not id_cliente_final and wspid_lid: id_cliente_final = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE lid = :lid"), {"lid": wspid_lid}).scalar()
                    else: raise e

            if id_cliente_final:
                t_msg = telefono_num if telefono_num else wspid_lid
                existe = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()

                is_echo = False
                fue_insertado = False

                if not existe:
                    if tipo_msg == 'SALIENTE_BOT':
                        match = conn.execute(text("""
                            SELECT id_mensaje FROM mensajes 
                            WHERE telefono = :t AND tipo IN ('SALIENTE_BOT', 'SALIENTE_PANEL') AND whatsapp_id IS NULL 
                              AND contenido = :txt AND fecha >= NOW() - INTERVAL '15 minutes'
                            LIMIT 1
                        """), {"t": t_msg, "txt": body}).scalar()

                        if match:
                            conn.execute(text("""
                                UPDATE mensajes 
                                SET whatsapp_id = :wid, estado_waha = 'enviado', archivo_data = COALESCE(archivo_data, :d) 
                                WHERE id_mensaje = :idm
                            """), {"wid": whatsapp_id, "idm": match, "d": archivo_bytes})
                            existe = True
                            log_info("🔗 Mensaje saliente fusionado (¡Imagen rescatada!).")

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
                    conn.execute(text("""
                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, archivo_data, whatsapp_id, reply_to_id, reply_content, estado_waha, session_name)
                        VALUES (:idc, :t, :tipo, :txt, NOW(), :leido, :d, :wid, :rid, :rbody, :est, :sess)
                    """), {
                        "idc": int(id_cliente_final), "t": t_msg, "tipo": tipo_msg, "txt": body, "leido": (tipo_msg == 'SALIENTE_BOT'), "d": archivo_bytes,
                        "wid": whatsapp_id, "rid": reply_id, "rbody": reply_content, "est": 'recibido' if tipo_msg == 'ENTRANTE' else 'enviado', "sess": session_name
                    })
                    fue_insertado = True

                # Mantiene mensajes.id_cliente al día: llena los que quedaron vacíos (mensajes fusionados, historial
                # que pasó del LID al teléfono) y corrige los de clientes fusionados por duplicado.
                conn.execute(text("""
                    UPDATE mensajes SET id_cliente = :idc
                    WHERE telefono = :t AND id_cliente IS DISTINCT FROM :idc
                """), {"idc": int(id_cliente_final), "t": t_msg})

                conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))

                if fue_insertado and tipo_msg == 'ENTRANTE':
                    import re 
                    texto_limpio = body.strip().lower()
                    texto_limpio = re.sub(r'[áäâà]', 'a', texto_limpio)
                    texto_limpio = re.sub(r'[éëêè]', 'e', texto_limpio)
                    texto_limpio = re.sub(r'[íïîì]', 'i', texto_limpio)
                    texto_limpio = re.sub(r'[óöôò]', 'o', texto_limpio)
                    texto_limpio = re.sub(r'[úüûù]', 'u', texto_limpio)
                    texto_limpio = re.sub(r'[^a-z0-9\s]', '', texto_limpio).strip()

                    estado_cli = conn.execute(text("""
                        SELECT c.nivel_zombie,
                               (SELECT COUNT(*) FROM Ventas v WHERE v.id_cliente = c.id_cliente AND v.anulado = FALSE) as cant_ventas,
                               (SELECT MAX(fecha) FROM mensajes m WHERE (m.telefono = :t OR m.telefono = c.telefono) 
                                AND m.tipo = 'ENTRANTE' AND m.fecha < NOW() - INTERVAL '1 minute') as fecha_ultimo_msg
                        FROM Clientes c WHERE c.id_cliente = :id
                    """), {"id": int(id_cliente_final), "t": t_msg}).fetchone()

                    nivel_actual = estado_cli.nivel_zombie if estado_cli and estado_cli.nivel_zombie else 0
                    ventas = estado_cli.cant_ventas if estado_cli and estado_cli.cant_ventas else 0
                    ultimo_msg = estado_cli.fecha_ultimo_msg

                    es_primera_vez = False
                    if ventas == 0:
                        if not ultimo_msg:
                            es_primera_vez = True
                        else:
                            diferencia = datetime.now() - ultimo_msg
                            if diferencia.days >= 180: 
                                es_primera_vez = True

                    if archivo_bytes or has_media:
                        if nivel_actual > 0:
                            conn.execute(text("UPDATE Clientes SET nivel_zombie = 0 WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                            log_info(f"🚶 Cliente {id_cliente_final} dejó de ser zombie (Envió Multimedia).")
                    else:
                        todas_las_frases = conn.execute(text("SELECT frase_clave, respuesta_nivel_1 FROM respuestas_automaticas")).fetchall()

                        respuesta_auto = None
                        es_frase_zombie = False

                        for row in todas_las_frases:
                            frase_db_limpia = str(row.frase_clave).lower()
                            frase_db_limpia = re.sub(r'[áäâà]', 'a', frase_db_limpia)
                            frase_db_limpia = re.sub(r'[éëêè]', 'e', frase_db_limpia)
                            frase_db_limpia = re.sub(r'[íïîì]', 'i', frase_db_limpia)
                            frase_db_limpia = re.sub(r'[óöôò]', 'o', frase_db_limpia)
                            frase_db_limpia = re.sub(r'[úüûù]', 'u', frase_db_limpia)
                            frase_db_limpia = re.sub(r'[^a-z0-9\s]', '', frase_db_limpia).strip()

                            if texto_limpio == frase_db_limpia:
                                es_frase_zombie = True
                                respuesta_auto = row.respuesta_nivel_1
                                break

                        if es_frase_zombie:
                            conn.execute(text("UPDATE mensajes SET leido = TRUE WHERE telefono = :t AND tipo = 'ENTRANTE' AND leido = FALSE"), {"t": t_msg})
                            threading.Thread(target=marcar_chat_como_leido_waha, args=(t_msg, session_name)).start()

                            if es_primera_vez and nivel_actual == 0:
                                conn.execute(text("UPDATE Clientes SET nivel_zombie = 1, ultimo_msg_zombie = NOW() WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                                log_info(f"🧟 Cliente {id_cliente_final} categorizado como ZOMBIE Lvl 1. Chat marcado como leído.")
                                try:
                                    if respuesta_auto: threading.Thread(target=enviar_respuesta_zombie_async, args=(t_msg, session_name, respuesta_auto, 30, whatsapp_id)).start()
                                except Exception as th_err:
                                    log_error(f"Error iniciando hilo zombie: {th_err}")

                            elif nivel_actual > 0:
                                conn.execute(text("UPDATE Clientes SET ultimo_msg_zombie = NOW() WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                                log_info(f"🧟 Cliente {id_cliente_final} permanece como ZOMBIE (Nivel {nivel_actual}). Chat marcado como leído.")
                                try:
                                    if respuesta_auto: threading.Thread(target=enviar_respuesta_zombie_async, args=(t_msg, session_name, respuesta_auto, 30, whatsapp_id)).start()
                                except Exception as th_err:
                                    log_error(f"Error iniciando hilo zombie: {th_err}")
                        else:
                            if nivel_actual > 0:
                                conn.execute(text("UPDATE Clientes SET nivel_zombie = 0 WHERE id_cliente = :id"), {"id": int(id_cliente_final)})
                                log_info(f"🚶 Cliente {id_cliente_final} despertó y dejó de ser zombie (Escribió texto orgánico).")

    except Exception as e:
        log_error(f"🔥 Error DB: {e}")


@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            try:
                procesar_evento(evento)
            except Exception as e:
                log_error(f"🔥 Error procesando evento {evento.get('event') if isinstance(evento, dict) else type(evento).__name__}: {e}")
                # Se guarda el evento crudo para poder diagnosticarlo (Diagnóstico → logs del webhook)
                try:
                    with engine.begin() as conn:
                        conn.execute(text("INSERT INTO webhook_logs (session_name, event_type, payload) VALUES (:s, :e, :p)"), {
                            "s": (evento.get('session') if isinstance(evento, dict) else None) or 'desconocida',
                            "e": f"ERROR_PROCESO: {str(e)[:120]}",
                            "p": json.dumps(evento, ensure_ascii=False, default=str)[:5000]})
                except Exception:
                    pass

        return jsonify({"status": "success"}), 200

    except Exception as e:
        log_error(f"🔥 Error General: {e}")
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)