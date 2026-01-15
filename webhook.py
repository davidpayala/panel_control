from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os
import requests
import json

app = Flask(__name__)

# Configuración
WAHA_KEY = os.getenv("WAHA_KEY")
WAHA_URL = os.getenv("WAHA_URL") 

def descargar_media(media_url):
    try:
        url_final = media_url
        if not media_url.startswith("http"):
             url_final = f"{WAHA_URL}{media_url}"
        headers = {}
        if WAHA_KEY: headers["X-Api-Key"] = WAHA_KEY   
        r = requests.get(url_final, headers=headers, timeout=10)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        print(f"❌ Excepción media: {e}")
        return None

# --- FUNCIÓN MAESTRA DE EXTRACCIÓN ---
def extraer_dato_seguro(campo_raw):
    """
    Saca el texto real ('51999@c.us') de cualquier estructura extraña que mande WAHA.
    """
    if campo_raw is None:
        return ""
    
    # Si es un Diccionario (Objeto), buscamos el usuario o el serializado
    if isinstance(campo_raw, dict):
        # Prioridad: 'user' (el número limpio) > '_serialized' (con @c.us)
        return str(campo_raw.get('user') or campo_raw.get('_serialized') or "")
    
    # Si ya es texto, devolvemos texto
    return str(campo_raw)

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # Validación simple
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    if data.get('event') == 'message':
        payload = data.get('payload', {})

        # ==================================================================
        # 🕵️‍♂️ ZONA DE DIAGNÓSTICO (ESTO ES LO QUE QUIERES VER)
        # ==================================================================
        print("\n🔍 --- INSPECCIONANDO DATOS DEL CONTACTO ---")
        print(f"RAW 'from': {json.dumps(payload.get('from'))}")
        print(f"RAW 'participant': {json.dumps(payload.get('participant'))}")
        print(f"RAW 'author': {json.dumps(payload.get('author'))}")
        print("----------------------------------------------------\n", flush=True)
        # ==================================================================
        
        # 1. Convertimos todo a TEXTO PLANO primero (Anti-Crash)
        from_str = extraer_dato_seguro(payload.get('from'))
        participant_str = extraer_dato_seguro(payload.get('participant'))
        author_str = extraer_dato_seguro(payload.get('author'))

        # 2. SELECCIÓN INTELIGENTE DEL NÚMERO
        # Buscamos cuál de los 3 campos tiene un número que empieza con '51' (Perú)
        # y NO es el número raro '319...' (LID)
        
        candidatos = [participant_str, author_str, from_str]
        numero_final = from_str # Valor por defecto (aunque sea el malo)

        for cand in candidatos:
            # Limpiamos basura para comparar solo números
            cand_clean = cand.split('@')[0].split(':')[0]
            
            # REGLA DE ORO: Si empieza con 51 y tiene longitud de celular (11 dígitos aprox)
            if cand_clean.startswith('51') and len(cand_clean) >= 11:
                numero_final = cand_clean
                print(f"🎯 ¡NÚMERO REAL ENCONTRADO!: {numero_final}")
                break
        
        # Si después de todo seguimos con el LID raro, intentamos limpiar lo que quede
        telefono_limpio = numero_final.split('@')[0].split(':')[0]

        # -------------------------------------------------------
        # Procesamiento del cuerpo y multimedia (Igual que siempre)
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        archivo_bytes = None
        
        if has_media:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            mimetype = media_info.get('mimetype', '')
            if media_url:
                archivo_bytes = descargar_media(media_url)
                if archivo_bytes:
                    tipo_icono = "📷" if "image" in mimetype else "📎"
                    body = f"{tipo_icono} Archivo recibido"
                else:
                    body = "⚠️ Error imagen"
            else:
                body = "📷 https://www.spanishdict.com/translate/vac%C3%ADa"

        # Guardar en BD
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {
                    "tel": telefono_limpio, 
                    "txt": body,
                    "data": archivo_bytes
                })
                conn.commit()
            print(f"✅ Guardado en DB como: {telefono_limpio}")
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)