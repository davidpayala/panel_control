import os
import requests
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. FORZAMOS LA CARGA DEL ARCHIVO .ENV (Sin importar nada más)
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# === PON TUS DATOS AQUÍ ===
MI_NUMERO = "51986203398" 
MI_NOMBRE = "David"
# ==========================

def prueba_disparo():
    print("\n🔧 Iniciando diagnóstico profundo de WAHA...")
    
    # 2. CONEXIÓN A BASE DE DATOS 100% INDEPENDIENTE (No usa database.py)
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ FATAL: No se encontró DATABASE_URL en tu archivo .env")
        return
        
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    motor_seguro = create_engine(db_url)
    
    try:
        # Fíjate que aquí usamos motor_seguro, no engine
        with motor_seguro.connect() as conn:
            grupos = conn.execute(text("SELECT * FROM Grupos_Productos")).fetchall()
            if not grupos:
                print("❌ No hay grupos en la base de datos.")
                return
                
            grupo_elegido = random.choice(grupos)
            
            # Construimos el texto
            mensaje_texto = f"¡Hola {MI_NOMBRE}! 👋\n\n{grupo_elegido.descripcion}\n\n{grupo_elegido.enlace_tienda}"
            
            # Extraemos la imagen si existe
            url_img = grupo_elegido.imagenes[0] if (grupo_elegido.imagenes and len(grupo_elegido.imagenes) > 0) else None

            print(f"📸 URL de imagen a enviar: {url_img}")

            # 3. CONEXIÓN DIRECTA A WAHA
            waha_url = os.getenv("WAHA_URL", "").rstrip('/')
            waha_key = os.getenv("WAHA_KEY", "")
            
            if not waha_url:
                print("❌ Error: No se encontró WAHA_URL en el archivo .env")
                return

            # Payload dinámico: Le pasamos el link directo a Waha
            if url_img:
                payload = {
                    "session": "default",
                    "chatId": f"{MI_NUMERO}@c.us",
                    "file": {"url": url_img},
                    "caption": mensaje_texto
                }
                endpoint = f"{waha_url}/api/sendImage"
            else:
                payload = {
                    "session": "default",
                    "chatId": f"{MI_NUMERO}@c.us",
                    "text": mensaje_texto
                }
                endpoint = f"{waha_url}/api/sendText"

            headers = {"Content-Type": "application/json"}
            if waha_key: headers["X-Api-Key"] = waha_key
            
            print(f"🚀 Disparando directamente a WAHA...")
            
            r = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            
            # 4. IMPRIMIR LA RESPUESTA CRUDA DE WAHA
            print("\n--- RESPUESTA DEL SERVIDOR WAHA ---")
            print(f"Status Code: {r.status_code}")
            print(f"Detalle: {r.text}")
            print("-----------------------------------")
            
            if r.status_code in [200, 201]:
                print("✅ Waha dice que lo aceptó. ¡Revisa tu celular!")
            else:
                print("❌ Waha rechazó el envío.")
                
    except Exception as e:
        print(f"🔥 Error en el script: {e}")

if __name__ == "__main__":
    prueba_disparo()