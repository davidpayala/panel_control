import os
from dotenv import load_dotenv

# =====================================================================
# 1. PRIMERO CARGAMOS EL .ENV (Antes de importar cualquier otra cosa)
# =====================================================================
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# =====================================================================
# 2. AHORA SÍ IMPORTAMOS LAS LIBRERÍAS
# =====================================================================
import requests
import random
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from utils import normalizar_telefono_maestro

def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Bot de Marketing...")

    # ==========================================================
    # 🎲 MODO HUMANO: Retraso aleatorio (1 a 45 min)
    # ==========================================================
    retraso_minutos = random.randint(1, 45)
    retraso_segundos = retraso_minutos * 60
    print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de disparar...")
    time.sleep(retraso_segundos)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Tiempo de espera terminado. Iniciando proceso...")

    # ==========================================================
    # 🔗 CONEXIÓN A BASE DE DATOS BLINDADA
    # ==========================================================
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ FATAL: No se encontró DATABASE_URL en tu archivo .env")
        return
        
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    motor_seguro = create_engine(db_url)

    try:
        with motor_seguro.connect() as conn:
            # A. LEER ÓRDENES DEL PANEL DE CONTROL
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            if not config or not config.bot_activo:
                print("🛑 El bot está APAGADO en el panel de control. Volviendo a dormir.")
                return

            # B. VALIDAR HORARIO COMERCIAL (Ajustado a Perú UTC-5)
            hora_peru = datetime.utcnow() - timedelta(hours=5)
            ahora = hora_peru.time()
            if not (config.hora_inicio <= ahora <= config.hora_fin):
                print(f"⏰ Fuera de horario. Hora actual en Perú: {ahora.strftime('%H:%M:%S')}. Permitido: {config.hora_inicio} a {config.hora_fin}.")
                return

            # C. VALIDAR LÍMITE DE MENSAJES DIARIOS
            enviados_hoy = conn.execute(text("""
            SELECT COUNT(*) FROM mensajes 
            WHERE tipo = 'SALIENTE_BOT' AND fecha::date = CURRENT_DATE
            """)).scalar()
            if enviados_hoy >= config.max_mensajes_dia:
                print(f"📈 Límite diario alcanzado ({enviados_hoy}/{config.max_mensajes_dia}). No se enviarán más hoy.")
                return

            # D. BUSCAR EL GRUPO DE PRODUCTOS
            filtro_sql = "" if config.tipo_objetivo == 'Todos' else "WHERE tipo = :t"
            grupos = conn.execute(text(f"SELECT * FROM Grupos_Productos {filtro_sql}"), {"t": config.tipo_objetivo}).fetchall()
            if not grupos:
                print(f"⚠️ No encontré ningún grupo del tipo '{config.tipo_objetivo}'.")
                return
            grupo_elegido = random.choice(grupos)

            # E. BUSCAR CLIENTE IDEAL (Escudo Anti-Spam 15 días)
            query_clientes = text("""
                SELECT id_cliente, nombre_corto, nombre_ia, telefono 
                FROM Clientes 
                WHERE activo = TRUE 
                  AND estado NOT IN ('Sin empezar', 'Proveedor internacional', 'Proveedor nacional')
                  AND length(telefono) > 6
                  AND telefono NOT IN (
                  SELECT telefono FROM mensajes 
                  WHERE tipo = 'SALIENTE_BOT' AND fecha > NOW() - INTERVAL '15 days'
                  )
            """)
            clientes_validos = conn.execute(query_clientes).fetchall()
            if not clientes_validos:
                print("🛡️ No hay clientes disponibles ahora mismo (todos fueron contactados hace menos de 15 días).")
                return

            cliente = random.choice(clientes_validos)
            norm = normalizar_telefono_maestro(cliente.telefono)
            if not norm:
                print(f"❌ El teléfono de {cliente.nombre_corto} no es válido.")
                return
            telefono_final = norm['db']

            # F. CONSTRUIR EL MENSAJE
            saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
            nombre = cliente.nombre_ia if cliente.nombre_ia else cliente.nombre_corto
            
            mensaje_texto = f"{saludo} {nombre} 👋\n\n{grupo_elegido.descripcion}\n\n{grupo_elegido.enlace_tienda}"
            url_img = grupo_elegido.imagenes[0] if (grupo_elegido.imagenes and len(grupo_elegido.imagenes) > 0) else None

            # G. ENVÍO DIRECTO A WAHA
            waha_url = os.getenv("WAHA_URL", "").rstrip('/')
            waha_key = os.getenv("WAHA_KEY", "")
            
            if url_img:
                payload = {"session": "default", "chatId": f"{telefono_final}@c.us", "file": {"url": url_img}, "caption": mensaje_texto}
                endpoint = f"{waha_url}/api/sendImage"
            else:
                payload = {"session": "default", "chatId": f"{telefono_final}@c.us", "text": mensaje_texto}
                endpoint = f"{waha_url}/api/sendText"

            headers = {"Content-Type": "application/json"}
            if waha_key: headers["X-Api-Key"] = waha_key
            
            print(f"🚀 Disparando mensaje a {nombre} ({telefono_final})...")
            
            trans = conn.begin()
            r = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            
            if r.status_code in [200, 201]:
                # H. REGISTRAR EN LA BASE DE DATOS
                conn.execute(text("""
                    INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido) 
                    VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() - INTERVAL '5 hours'), TRUE)
                """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_texto})
                    
                trans.commit()
                print("✅ ¡Mensaje enviado y registrado en la BD con éxito!")
            else:
                trans.rollback()
                print(f"❌ Falló el envío en la API de WhatsApp. Código: {r.status_code}")

    except Exception as e:
        print(f"🔥 Error catastrófico en el script: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()