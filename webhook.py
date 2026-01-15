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

# --- FUNCIÓN DE RASTREO (BUSCA EL NÚMERO POR TI) ---
def buscar_numero_recursivo(data, path=""):
    """Recorre todo el JSON buscando cualquier cosa que parezca un celular peruano"""
    if isinstance(data, dict):
        for k, v in data.items():
            buscar_numero_recursivo(v, f"{path}.{k}")
    elif isinstance(data, list):
        for i, item in enumerate(data):
            buscar_numero_recursivo(item, f"{path}[{i}]")
    else:
        # Convertimos a string para analizar
        valor = str(data)
        # Si contiene '51' y tiene longitud suficiente (ignora fechas y timestamps)
        if "51" in valor and len(valor) > 9 and len(valor) < 25:
            # Imprimimos en GRANDE para que lo veas en el log
            print(f"🕵️ ¡PISTA ENCONTRADA! Ruta: {path} || Valor: {valor}", flush=True)

# --- FUNCIÓN SEGURA PARA EVITAR CRASH ---
def extraer_string(valor):
    if isinstance(valor, dict):
        return str(valor.get('user') or valor.get('_serialized') or "")
    return str(valor or "")

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    
    # ==================================================================
    # 🚨 ZONA DE VOLCADO DE DATOS 🚨
    # ==================================================================
    print("\n📦 --- INICIO DEL DUMP COMPLETO DEL JSON ---", flush=True)
    # Imprime todo el JSON bonito
    print(json.dumps(data, indent=2), flush=True)
    print("📦 --- FIN DEL DUMP ---\n", flush=True)

    # EJECUTAR RASTREO AUTOMÁTICO
    print("🔎 Buscando números 51... en todos los rincones:", flush=True)
    buscar_numero_recursivo(data, "ROOT")
    print("------------------------------------------------", flush=True)
    # ==================================================================

    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # LÓGICA DE EMERGENCIA (Para que no crashee mientras miras los logs)
        # Intenta sacar el número de cualquier lado posible
        candidatos = [
            extraer_string(payload.get('participant')),
            extraer_string(payload.get('author')),
            extraer_string(payload.get('from')),
            extraer_string(payload.get('_data', {}).get('notifyName')) # A veces ayuda
        ]
        
        numero_elegido = "Desconocido"
        for cand in candidatos:
            clean = cand.split('@')[0].split(':')[0]
            if clean.startswith('51') and len(clean) >= 11:
                numero_elegido = clean
                break
        
        if numero_elegido == "Desconocido":
            # Si no encontró nada, usa el 'from' aunque sea el LID, para no perder el mensaje
            numero_elegido = extraer_string(payload.get('from')).split('@')[0]

        # Procesamiento normal...
        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        archivo_bytes = None
        
        if has_media:
            media_info = payload.get('media', {})
            media_url = media_info.get('url')
            if media_url: archivo_bytes = descargar_media(media_url)
            body = "📷 Archivo" if archivo_bytes else "⚠️ Error media"

        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, archivo_data)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE, :data)
                """), {"tel": numero_elegido, "txt": body, "data": archivo_bytes})
                conn.commit()
        except Exception as e:
            print(f"❌ Error DB: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)