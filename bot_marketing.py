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
from utils import normalizar_telefono_maestro, verificar_numero_waha, enviar_mensaje_whatsapp

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
                # E. BUSCAR CLIENTE IDEAL (Apuntando a la nueva tabla de teléfonos)
                query_clientes = text("""
                    SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, t.telefono 
                    FROM clientes c
                    JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                    WHERE c.activo = TRUE 
                      AND c.estado IN ('Sin empezar')
                      AND t.activo = TRUE
                      AND t.es_principal = TRUE
                      AND length(t.telefono) > 6
                      AND t.telefono NOT IN (
                          SELECT telefono FROM mensajes 
                          WHERE tipo = 'SALIENTE_BOT' AND fecha > NOW() - INTERVAL '60 days'
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
                
                # F. VERIFICACIÓN Y LÓGICA DE RECUPERACIÓN DE CLIENTE (Usando utils.py)
                print(f"🔎 WAHA: Verificando silenciosamente si {telefono_final} existe...")
                existe_wsp = verificar_numero_waha(telefono_final)
                
                if existe_wsp is False:
                    print(f"👻 {telefono_final} NO tiene WhatsApp. Buscando alternativa en perfil...")
                    
                    # Buscamos si existe otro número activo para este cliente
                    otro_telefono = conn.execute(text("""
                        SELECT telefono FROM telefonoscliente 
                        WHERE id_cliente = :idc AND telefono != :t AND activo = TRUE 
                        LIMIT 1
                    """), {"idc": cliente.id_cliente, "t": telefono_final}).fetchone()
                    
                    if otro_telefono:
                        nuevo_tel = otro_telefono[0]
                        print(f"🔄 Alternativa encontrada: {nuevo_tel}. Promoviendo a principal...")
                        
                        # 1. Desactivamos el viejo, activamos el nuevo y lo hacemos principal
                        conn.execute(text("UPDATE telefonoscliente SET activo = FALSE WHERE telefono = :t"), {"t": telefono_final})
                        conn.execute(text("UPDATE telefonoscliente SET es_principal = FALSE WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                        conn.execute(text("UPDATE telefonoscliente SET es_principal = TRUE WHERE telefono = :nt"), {"nt": nuevo_tel})
                        
                        # 2. Actualizamos el teléfono en la tabla clientes (para retrocompatibilidad)
                        conn.execute(text("UPDATE Clientes SET telefono = :nt WHERE id_cliente = :idc"), {"nt": nuevo_tel, "idc": cliente.id_cliente})
                        conn.commit()
                        
                        # 3. Reintentamos el envío con el nuevo número (el ciclo 'for' actual ya está en el intento, lo ajustamos:)
                        telefono_final = nuevo_tel
                        print(f"✅ Número actualizado. Continuando con {telefono_final}...")
                    else:
                        print(f"💀 No hay más números. Archivando cliente...")
                        conn.execute(text("UPDATE Clientes SET activo = FALSE, notas = 'Ningún número tiene WhatsApp' WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                        conn.commit()
                        continue # Saltamos a otro cliente
                        
                elif existe_wsp is None:
                    print("⚠️ Error CRÍTICO de conexión con WAHA al intentar verificar el número.")
                    print("Abortando intentos para no archivar por error.")
                    break # Salimos del ciclo para no enviar nada a ciegas
                    
                else:
                    print("✅ El número existe y es real. Preparando disparo...")

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

                # H. ENVÍO DIRECTO A WAHA (USANDO UTILS)
                print(f"🚀 Disparando mensaje a {cliente.nombre_corto} ({telefono_final})...")
                
                # Usamos la navaja suiza de utils.py
                envio_ok = enviar_mensaje_whatsapp(telefono_final, mensaje_texto, url_img)
                
                if envio_ok:
                    # I. REGISTRAR EN LA BASE DE DATOS
                    conn.execute(text("""
                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido) 
                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() - INTERVAL '5 hours'), TRUE)
                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_texto})
                    
                    conn.commit()
                    print("✅ ¡Mensaje enviado y registrado en la BD con éxito!")
                    enviado_exitoso = True
                    break  # Si el envío fue exitoso, rompemos el ciclo 'for'
                else:
                    print("❌ Falló el envío en la API de WhatsApp.")
                    break
            
            if not enviado_exitoso:
                print("⚠️ Se agotaron los intentos de envío por esta hora.")

    except Exception as e:
        print(f"🔥 Error catastrófico en el script: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()