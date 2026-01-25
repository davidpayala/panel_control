from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
import base64
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

# VARIABLES DE ENTORNO
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

# --- FUNCIONES AUXILIARES ---

def descargar_media_plus(media_url):
    """Descarga media desde WAHA Plus con autenticación y corrección de URL."""
    try:
        if not media_url: return None
        
        url_final = media_url
        
        # Corrección 1: URLs relativas
        if not media_url.startswith("http"):
             base = WAHA_URL.rstrip('/') if WAHA_URL else ""
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        # Corrección 2: URLs internas (localhost)
        elif "localhost" in media_url or "waha:" in media_url:
             if WAHA_URL:
                path_real = media_url.split('/api/')[-1]
                base = WAHA_URL.rstrip('/')
                url_final = f"{base}/api/{path_real}"
        
        print(f"📥 Intentando descargar de: {url_final}")
        
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=15)
        return r.content if r.status_code == 200 else None
            
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

def obtener_datos_mensaje(payload):
    """
    Determina quién es el 'Otro' (Cliente) y la dirección del mensaje.
    Retorna: (numero_crudo, tipo_mensaje, nombre_push)
    """
    try:
        # 1. ¿Lo envié yo?
        from_me = payload.get('fromMe', False)
        
        if from_me:
            # SI LO ENVIÉ YO (Saliente)
            # El cliente está en 'to' (Para)
            # Ejemplo 'to': '51992270321@c.us'
            remote_id = payload.get('to')
            tipo = 'SALIENTE'
            # En mensajes salientes no suele haber notifyName del cliente, usamos None
            push_name = None 
        else:
            # ME LO ENVIARON (Entrante)
            # El cliente está en 'from' (De)
            remote_id = payload.get('from')
            # Fallback por si la estructura cambia
            if not remote_id:
                remote_id = payload.get('_data', {}).get('id', {}).get('remote')
            
            tipo = 'ENTRANTE'
            push_name = payload.get('_data', {}).get('notifyName')

        if remote_id:
            # Limpiar sufijos de WhatsApp
            clean_num = remote_id.replace('@c.us', '').replace('@s.whatsapp.net', '')
            # Si es un grupo (@g.us), lo ignoramos o manejamos aparte (aquí lo devolvemos tal cual)
            if '@g.us' in remote_id: return None, None, None
            
            return clean_num, tipo, push_name
            
        return None, None, None

    except Exception as e:
        print(f"⚠️ Error extrayendo datos: {e}")
        return None, None, None

# --- RUTAS ---

@app.route('/', methods=['GET', 'POST'])
def home():
    return "Webhook Activo 🚀 v2.0 (Fix Salientes)", 200

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # print("🔵 [WEBHOOK] Evento recibido") # Descomentar para debug intenso
    
    # Validación API Key (Opcional)
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key and api_key != WAHA_KEY:
        # print("⛔ API Key incorrecta")
        pass 

    try:
        data = request.json
        if not data: return jsonify({"status": "empty"}), 200

        eventos = data if isinstance(data, list) else [data]

        for evento in eventos:
            if evento.get('event') != 'message': continue

            payload = evento.get('payload')
            if not payload: continue

            # 1. IGNORAR ESTADOS (Historias)
            # Los estados suelen venir con 'status@broadcast' en from o to
            if 'status@broadcast' in str(payload.get('from')) or 'status@broadcast' in str(payload.get('to')):
                continue

            # 2. DETERMINAR CLIENTE Y DIRECCIÓN (La corrección clave)
            numero_crudo, tipo_msg, push_name = obtener_datos_mensaje(payload)
            
            if not numero_crudo: continue

            # 3. NORMALIZAR NÚMERO
            formatos = normalizar_telefono_maestro(numero_crudo)
            if not formatos:
                # Si no es un número válido (ej. ID raro), lo ignoramos
                # print(f"⚠️ Número ignorado: {numero_crudo}")
                continue
            
            telefono_db = formatos['db']
            telefono_corto = formatos['corto']
            
            print(f"📩 Procesando: {telefono_corto} | Tipo: {tipo_msg}")

            # 4. GESTIÓN DE CONTENIDO Y MEDIA
            body = payload.get('body', '')
            
            # Recuperar URL de media si existe
            media_url = payload.get('mediaUrl')
            if not media_url:
                media_obj = payload.get('media') or {}
                media_url = media_obj.get('url')

            archivo_bytes = None
            if media_url:
                archivo_bytes = descargar_media_plus(media_url)
                if archivo_bytes:
                    if not body: body = "📷 Archivo Multimedia"
                else:
                    msg_err = f"⚠️ Error descargando imagen"
                    body = f"{body}\n({msg_err})" if body else msg_err

            # 5. GUARDAR EN BASE DE DATOS
            try:
                nombre_final = push_name or "Cliente"
                
                # Lógica para recuperar nombre si ya existe (Solo útil si es un cliente nuevo)
                if tipo_msg == 'ENTRANTE' and nombre_final == "Cliente":
                     try:
                        datos_google = buscar_contacto_google(telefono_db)
                        if datos_google and datos_google['encontrado']:
                            nombre_final = datos_google['nombre_completo']
                     except: pass

                with engine.connect() as conn:
                    # A) Upsert del Cliente (Asegurar que existe)
                    # Si ya existe, NO cambiamos el nombre para no perder ediciones manuales
                    conn.execute(text("""
                        INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                        VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                        ON CONFLICT (telefono) DO UPDATE SET activo = TRUE
                    """), {"t": telefono_db, "n": nombre_final})
                    
                    # B) Insertar Mensaje (Evitar duplicados exactos si es posible)
                    # Usamos un truco simple: Si acabamos de enviar una campaña, ya se guardó.
                    # Pero es mejor guardar doble que perder mensajes.
                    # Para producción fina, podríamos verificar ID de mensaje, pero por ahora insertamos.
                    
                    conn.execute(text("""
                        INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                        VALUES (:t, :tipo, :txt, (NOW() - INTERVAL '5 hours'), :leido, :d)
                    """), {
                        "t": telefono_db, 
                        "tipo": tipo_msg, 
                        "txt": body, 
                        "leido": (tipo_msg == 'SALIENTE'), # Si sale de mí, ya está leído
                        "d": archivo_bytes
                    })
                    
                    conn.commit()
                    print(f"✅ {telefono_corto}: Guardado correctamente.")

            except Exception as e:
                print(f"❌ Error DB: {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"🔥 Error Crítico Webhook: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)