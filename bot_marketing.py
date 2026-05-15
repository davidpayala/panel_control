import os
from dotenv import load_dotenv

# =====================================================================
# 1. PRIMERO CARGAMOS EL .ENV
# =====================================================================
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# =====================================================================
# 2. AHORA SÍ IMPORTAMOS LAS LIBRERÍAS
# =====================================================================
import requests
import random
import time
from datetime import datetime, timedelta, timezone
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

            # B. VALIDAR HORARIO COMERCIAL (Ajustado a Perú UTC-5 sin DeprecationWarning)
            hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
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

            # E. BUSCAR CLIENTE IDEAL (Con hasta 3 intentos por si hay números falsos)
            enviado_exitoso = False
            
            for intento in range(3):
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
                    print("🛡️ No hay clientes disponibles ahora mismo (todos contactados recientemente).")
                    return

                cliente = random.choice(clientes_validos)
                norm = normalizar_telefono_maestro(cliente.telefono)
                
                if not norm:
                    print(f"❌ El teléfono de {cliente.nombre_corto} no tiene un formato válido. Desactivando...")
                    conn.execute(text("UPDATE Clientes SET activo = FALSE, notas = 'Formato de teléfono inválido' WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                    conn.commit()
                    continue  # Saltamos al siguiente intento de la lista

                telefono_final = norm['db']
                
                # F. VERIFICACIÓN JUST-IN-TIME CON WAHA
                waha_url = os.getenv("WAHA_URL", "").rstrip('/')
                waha_key = os.getenv("WAHA_KEY", "")
                headers = {"Content-Type": "application/json"}
                if waha_key: headers["X-Api-Key"] = waha_key
                
                print(f"🔎 WAHA: Verificando silenciosamente si {telefono_final} existe en WhatsApp...")
                url_check = f"{waha_url}/api/contacts/checkExists"
                payload_check = {"session": "default", "phone": telefono_final}
                
                try:
                    r_check = requests.post(url_check, json=payload_check, headers=headers, timeout=15)
                    datos_check = r_check.json()
                    
                    if not datos_check.get("numberExists", False):
                        print(f"👻 ¡Fantasma detectado! {telefono_final} NO tiene WhatsApp. Archivando cliente...")
                        conn.execute(text("UPDATE Clientes SET activo = FALSE, notas = 'Número no existe en WhatsApp' WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                        conn.commit()
                        continue  # El número es falso, volvemos arriba a intentar con otro cliente
                    else:
                        print("✅ El número existe y es real. Preparando disparo...")
                except Exception as e:
                    print(f"⚠️ Error al preguntar a WAHA si el número existe: {e}. Asumiremos que existe por si acaso.")

                # G. CONSTRUIR EL MENSAJE
                # Verificamos si existe nombre_ia y que no sea solo espacios
                if cliente.nombre_ia and str(cliente.nombre_ia).strip():
                    saludo_aleatorio = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                    cabecera = f"{saludo_aleatorio} {cliente.nombre_ia} 👋"
                else:
                    # Si el nombre IA está vacío, usamos un saludo estándar sin nombre
                    cabecera = "¡Hola! 👋"
                mensaje_texto = f"{cabecera}\n\n{grupo_elegido.descripcion}\n\n{grupo_elegido.enlace_tienda}"
                url_img = grupo_elegido.imagenes[0] if (grupo_elegido.imagenes and len(grupo_elegido.imagenes) > 0) else None

                # H. ENVÍO DIRECTO A WAHA
                if url_img:
                    payload = {"session": "default", "chatId": f"{telefono_final}@c.us", "file": {"url": url_img}, "caption": mensaje_texto}
                    endpoint = f"{waha_url}/api/sendImage"
                else:
                    payload = {"session": "default", "chatId": f"{telefono_final}@c.us", "text": mensaje_texto}
                    endpoint = f"{waha_url}/api/sendText"
                
                print(f"🚀 Disparando mensaje a {nombre} ({telefono_final})...")
                
                r = requests.post(endpoint, json=payload, headers=headers, timeout=30)
                
                if r.status_code in [200, 201]:
                    # I. REGISTRAR EN LA BASE DE DATOS
                    conn.execute(text("""
                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido) 
                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() - INTERVAL '5 hours'), TRUE)
                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_texto})
                    
                    conn.commit()
                    print("✅ ¡Mensaje enviado y registrado en la BD con éxito!")
                    enviado_exitoso = True
                    break  # Si el envío fue exitoso, rompemos el ciclo 'for' para terminar la tarea de esta hora
                else:
                    conn.rollback()
                    print(f"❌ Falló el envío en la API de WhatsApp. Código: {r.status_code}")
                    break
            
            if not enviado_exitoso:
                print("⚠️ Se agotaron los intentos de envío por esta hora.")

    except Exception as e:
        print(f"🔥 Error catastrófico en el script: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()