from flask import Flask, request, jsonify
from sqlalchemy import text
from database import engine
import os

app = Flask(__name__)

# Clave de seguridad opcional (si la configuraste en WAHA)
WAHA_KEY = os.getenv("WAHA_KEY")

@app.route('/webhook', methods=['POST'])
def recibir_mensaje():
    # 1. Verificar seguridad (opcional)
    api_key = request.headers.get('X-Api-Key')
    if WAHA_KEY and api_key != WAHA_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    print(f"📩 Webhook recibido: {data}", flush=True)

    # 2. Filtrar solo mensajes entrantes (ignoramos estados, etc.)
    if data.get('event') == 'message':
        payload = data.get('payload', {})
        
        # Datos del mensaje
        # WAHA envía el número como "51999...@c.us", lo limpiamos
        sender_raw = payload.get('from', '')
        sender = sender_raw.split('@')[0] 
        
        # 2. CRUCIAL: Quitamos el sufijo de dispositivo (:8, :12, etc)
        # Si el número viene como "51999888777:8", esto lo deja en "51999888777"
        if ':' in sender:
            sender = sender.split(':')[0]

        body = payload.get('body', '')
        has_media = payload.get('hasMedia', False)
        
        # Si tiene archivo, cambiamos el texto para avisar (Mejora futura: descargar el archivo)
        if has_media:
            body = "📷 [Archivo/Imagen Recibido] (Ver en celular)"

        # 3. Guardar en Base de Datos
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido)
                    VALUES (:tel, 'ENTRANTE', :txt, (NOW() - INTERVAL '5 hours'), FALSE)
                """), {
                    "tel": sender, 
                    "txt": body
                })
                conn.commit()
            print(f"✅ Mensaje de {sender} guardado en DB.")
        except Exception as e:
            print(f"❌ Error guardando en DB: {e}")

    # Siempre responder 200 OK a WAHA para que sepa que recibimos el mensaje
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    # Railway nos da el puerto en la variable PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)