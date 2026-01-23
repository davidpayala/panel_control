from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json
from datetime import datetime
import pytz 
from utils import normalizar_telefono_maestro, buscar_contacto_google

app = Flask(__name__)

# TUS VARIABLES DE ENTORNO
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def descargar_media(media_url):
    """
    Descarga media desde WAHA Plus usando autenticación.
    """
    try:
        url_final = media_url
        
        # Si la URL viene relativa (/api/files/...), le pegamos el dominio
        if not media_url.startswith("http"):
             # Quitamos doble barra si existe
             base = WAHA_URL.rstrip('/')
             path = media_url.lstrip('/')
             url_final = f"{base}/{path}"
        
        print(f"📥 Intentando descargar de: {url_final}")
        
        headers = {}
        # IMPORTANTE: WAHA Plus requiere la API Key para descargar archivos
        if WAHA_KEY: 
            headers["X-Api-Key"] = WAHA_KEY   
        
        r = requests.get(url_final, headers=headers, timeout=10)
        
        if r.status_code == 200:
            print(f"✅ Descarga exitosa ({len(r.content)} bytes)")
            return r.content
        else:
            print(f"❌ Falló descarga: {r.status_code} - {r.text}")
            return None
            
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
        return None

def obtener_numero_crudo(payload):
    """Extrae el número del payload de WAHA Plus"""
    try:
        # WAHA Plus suele enviar 'from' directo en el payload o dentro de 'key'
        from_val = payload.get('from')
        if not from_val:
            from_val = payload.get('_data', {}).get('id', {}).get('remote')
        
        if from_val:
            return from_val.replace('@c.us', '').replace('@s.whatsapp.net', '')
        return None
    except:
        return None

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # 1. LOG DE DIAGNÓSTICO (Para ver qué llega)
    print("🔵 [WEBHOOK] Solicitud recibida")
    
    # 2. Seguridad
    api_key = request.headers.get('X-Api-Key')
    # Si WAHA envía la key en el header, validamos. Si no, seguimos (a veces no la envía en webhooks)
    if WAHA_KEY and api_key and api_key != WAHA_KEY:
        print("⛔ API Key incorrecta en Webhook")
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data:
        return jsonify({"status": "empty"}), 200

    # WAHA Plus a veces envía un array de eventos o un objeto único
    # Vamos a normalizarlo a una lista para procesar todo
    eventos = data if isinstance(data, list) else [data]

    for evento in eventos:
        # Solo nos interesan los mensajes
        if evento.get('event') != 'message':
            continue

        payload = evento.get('payload', {})
        
        # DIAGNÓSTICO RAPIDO
        # print(f"📩 Payload crudo: {json.dumps(payload)[:200]}...") 

        # --- FILTRO SISTEMA ---
        # Si no tiene 'from' o es status@broadcast, ignoramos
        remitente = payload.get('from', '')
        if 'status@broadcast' in remitente:
            continue
            
        # 3. Obtener Número
        numero_crudo = obtener_numero_crudo(payload)
        formatos = normalizar_telefono_maestro(numero_crudo)
        
        if not formatos:
            print(f"⚠️ Número no normalizable: {numero_crudo}")
            continue # Saltamos al siguiente evento

        telefono_db = formatos['db']
        telefono_corto = formatos['corto']
        
        # 4. Procesar Contenido
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        media_url = payload.get('mediaUrl') or payload.get('media', {}).get('url')
        
        archivo_bytes = None
        
        # Lógica de Media mejorada para Plus
        if has_media or media_url:
            if media_url:
                archivo_bytes = descargar_media_waha_plus(media_url)
                if archivo_bytes:
                    if not body: body = "📷 Archivo Multimedia"
            else:
                # A veces dice hasMedia: true pero no trae URL (stickers raros, etc)
                pass

        # Filtro final anti-vacíos
        if not body and not archivo_bytes:
            continue

        # 5. GUARDAR EN DB (Tu lógica de siempre)
        try:
            # Buscar nombre (Google Search simple)
            nombre_final = payload.get('_data', {}).get('notifyName') or "Cliente"
            try:
                datos_google = buscar_contacto_google(telefono_db)
                if datos_google and datos_google['encontrado']:
                    nombre_final = datos_google['nombre_completo']
            except: pass

            tz = pytz.timezone('America/Lima')
            fecha_hoy = datetime.now(tz).strftime('%Y-%m-%d')

            with engine.connect() as conn:
                # Upsert Cliente
                conn.execute(text("""
                    INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                    VALUES (:t, :n, 'Sin empezar', TRUE, NOW())
                    ON CONFLICT (telefono) DO UPDATE SET
                    activo = TRUE
                """), {"t": telefono_db, "n": nombre_final})
                
                # Insertar Mensaje
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:t, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :d)
                """), {"t": telefono_db, "txt": body, "d": archivo_bytes})
                
                conn.commit()
                print(f"✅ Guardado mensaje de {telefono_corto}")

        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)