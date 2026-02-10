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

# --- 🚑 PARCHE DB: MODELO ID-CENTRIC ---
def aplicar_parche_db():
    try:
        with engine.begin() as conn:
            # 1. Crear columna para el ID Técnico
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS whatsapp_internal_id VARCHAR(150)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_wsp_id ON Clientes(whatsapp_internal_id)"))
            
            # 2. Migración de Datos Antiguos (Backfill)
            # Si un cliente no tiene ID interno, se lo generamos basado en su teléfono
            conn.execute(text("""
                UPDATE Clientes 
                SET whatsapp_internal_id = CONCAT(telefono, '@s.whatsapp.net') 
                WHERE whatsapp_internal_id IS NULL AND telefono IS NOT NULL AND LENGTH(telefono) > 8
            """))
            
            # 3. Columnas auxiliares
            conn.execute(text("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS estado_waha VARCHAR(20)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS nombre VARCHAR(100)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS apellido VARCHAR(100)"))
            conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS google_id VARCHAR(100)"))
    except Exception as e:
        log_error(f"Error migración DB: {e}")

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
# 🧠 CEREBRO V23: EXTRACCIÓN DE ID CANÓNICO
# ==============================================================================
def obtener_identidad(payload, session):
    """
    Devuelve un diccionario con:
    - 'id_canonico': El ID técnico real (ej: 51999...@s.whatsapp.net)
    - 'telefono': El número limpio extraído de ese ID
    - 'es_grupo': Booleano
    """
    try:
        from_me = payload.get('fromMe', False)
        
        # Extracción segura
        _data = payload.get('_data') or {}
        key = _data.get('key') or {}

        candidate_id = ""

        # --- PASO 1: Buscar el "Alt" (El ID real detrás del LID) ---
        # Esto soluciona tus problemas anteriores de raíz.
        alt_id = key.get('remoteJidAlt') or key.get('participantAlt')
        
        if alt_id and '@' in alt_id:
            candidate_id = alt_id
        else:
            # --- PASO 2: Si no hay Alt, buscamos el Standard ---
            if from_me:
                # Si lo envié yo, el ID es el remoteJid (Chat destino)
                candidate_id = key.get('remoteJid') or payload.get('to')
            else:
                # Si es entrante
                # En grupos: participant. En privado: from.
                part = payload.get('participant')
                src = payload.get('from')
                
                if part and '@lid' not in part:
                    candidate_id = part
                else:
                    candidate_id = src

        if not candidate_id: return None

        # --- PASO 3: Normalización ---
        # Queremos formato JID puro: '51999...@s.whatsapp.net' o '123...@g.us'
        # Eliminamos sufijos extraños si existen, pero mantenemos el dominio.
        
        # Si es un LID puro sin Alt, intentamos resolverlo por API (último recurso)
        if '@lid' in candidate_id and WAHA_URL:
             try:
                lid_clean = candidate_id
                url_api = f"{WAHA_URL}/api/{session}/lids/{lid_clean}"
                headers = {"X-Api-Key": WAHA_KEY} if WAHA_KEY else {}
                r = requests.get(url_api, headers=headers, timeout=2) # Timeout rápido
                if r.status_code == 200:
                    data_lid = r.json()
                    if 'pn' in data_lid: 
                        # Convertimos el número retornado a formato JID
                        candidate_id = f"{data_lid['pn']}@s.whatsapp.net"
             except: pass

        # Detectar tipo
        es_grupo = '@g.us' in candidate_id
        
        # Extraer teléfono (solo dígitos) del ID
        telefono_limpio = "".join(filter(str.isdigit, candidate_id.split('@')[0]))
        
        return {
            "id_canonico": candidate_id,
            "telefono": telefono_limpio,
            "es_grupo": es_grupo
        }

    except Exception as e:
        log_error(f"Error obteniendo identidad: {e}")
        return None

# ==============================================================================
# RUTAS FLASK
# ==============================================================================
@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook V23 (ID-Centric Architecture)", 200

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
            if payload.get('from') == 'status@broadcast': continue

            # 1. OBTENER IDENTIDAD (Tu nueva lógica paso 1)
            identidad = obtener_identidad(payload, session_name)
            if not identidad: continue
            
            chat_id = identidad['id_canonico']
            telefono_msg = identidad['telefono']
            es_grupo = identidad['es_grupo']

            log_info(f"🔑 [{session_name}] Procesando ID: {chat_id}")

            # 2. PROCESAR CONTENIDO
            body = payload.get('body', '')
            media_obj = payload.get('media') or {} 
            media_url = payload.get('mediaUrl') or media_obj.get('url')
            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes and not body: body = "📷 Archivo Multimedia"
            
            # Reply info
            reply_id = None
            reply_content = None
            raw_reply = payload.get('replyTo')
            if isinstance(raw_reply, dict):
                reply_id = raw_reply.get('id')
                reply_content = raw_reply.get('body')
            elif isinstance(raw_reply, str):
                reply_id = raw_reply

            # 3. LÓGICA DB (Tu nueva lógica pasos 2, 3 y 4)
            try:
                tipo_msg = 'SALIENTE' if payload.get('fromMe') else 'ENTRANTE'
                whatsapp_id = payload.get('id')
                
                # Datos para creación
                _data = payload.get('_data') or {}
                push_name = _data.get('pushName') or _data.get('notifyName') or _data.get('verifiedBizName')
                nombre_wsp = push_name or "Cliente Nuevo"
                if tipo_msg == 'SALIENTE': nombre_wsp = f"Chat {telefono_msg}" # No usar mi nombre

                with engine.connect() as conn:
                    
                    # --- PASO 2: BUSCAR POR ID INTERNO (La clave del éxito) ---
                    cliente_db = conn.execute(
                        text("SELECT id_cliente, telefono, whatsapp_internal_id FROM Clientes WHERE whatsapp_internal_id = :wid"), 
                        {"wid": chat_id}
                    ).fetchone()

                    telefono_final_db = telefono_msg # Por defecto usamos el del mensaje

                    if cliente_db:
                        # YA EXISTE -> Solo actualizamos actividad
                        telefono_final_db = cliente_db.telefono # Mantenemos el telefono que ya tenia registrado
                        
                        # --- PASO 4: ADVERTENCIA DE CAMBIO DE NUMERO ---
                        # Si el teléfono del mensaje es diferente al de la DB, es raro (pero posible si mantuvo ID)
                        # Aquí podrías lanzar un log o actualizar si quisieras.
                        if telefono_msg and cliente_db.telefono and telefono_msg != cliente_db.telefono:
                             log_info(f"⚠️ Alerta: ID {chat_id} ahora usa tel {telefono_msg} (En DB: {cliente_db.telefono})")
                             # Opcional: Actualizar el telefono en DB automáticamente
                             # conn.execute(text("UPDATE Clientes SET telefono=:t WHERE id_cliente=:id"), {"t": telefono_msg, "id": cliente_db.id_cliente})

                        conn.execute(text("UPDATE Clientes SET activo=TRUE WHERE id_cliente=:id"), {"id": cliente_db.id_cliente})
                    
                    else:
                        # NO EXISTE -> CREAR (Paso 1)
                        # Intentamos ver si existe por telefono antiguo (Legacy fallback)
                        cliente_legacy = conn.execute(
                            text("SELECT id_cliente FROM Clientes WHERE telefono = :t"), 
                            {"t": telefono_msg}
                        ).fetchone()

                        if cliente_legacy:
                             # Es un cliente viejo que no tenía ID interno migrado. Lo actualizamos.
                             log_info(f"🔄 Migrando Cliente Legacy: {telefono_msg} -> {chat_id}")
                             conn.execute(text("UPDATE Clientes SET whatsapp_internal_id = :wid, activo=TRUE WHERE id_cliente = :id"), 
                                         {"wid": chat_id, "id": cliente_legacy.id_cliente})
                        else:
                            # NUEVO ABSOLUTO
                            nombre_final = f"Grupo {telefono_msg[-4:]}" if es_grupo else nombre_wsp
                            log_info(f"🆕 Creando Cliente ID-Centric: {chat_id}")
                            
                            conn.execute(text("""
                                INSERT INTO Clientes (telefono, whatsapp_internal_id, nombre_corto, estado, activo, fecha_registro)
                                VALUES (:t, :wid, :n, 'Sin empezar', TRUE, NOW())
                            """), {"t": telefono_msg, "wid": chat_id, "n": nombre_final})
                            
                            # Sync Google (Solo nuevos no grupos y entrantes)
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

                    # GUARDAR MENSAJE (Usando el telefono para mantener compatibilidad con tu frontend actual)
                    # Nota: Idealmente tu frontend debería buscar por id_cliente o whatsapp_internal_id en el futuro
                    existe_msg = conn.execute(text("SELECT 1 FROM mensajes WHERE whatsapp_id=:wid"), {"wid": whatsapp_id}).scalar()
                    
                    if not existe_msg:
                        conn.execute(text("""
                            INSERT INTO mensajes (
                                telefono, tipo, contenido, fecha, leido, archivo_data, 
                                whatsapp_id, reply_to_id, reply_content, estado_waha
                            )
                            VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d, :wid, :rid, :rbody, :est)
                        """), {
                            "t": telefono_final_db, # Usamos el teléfono asociado al ID
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