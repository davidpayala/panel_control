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

# 🔥 IMPORTANTE: Agregamos las dos nuevas funciones de estados desde utils
from utils import (
    normalizar_telefono_maestro, 
    verificar_numero_waha, 
    enviar_mensaje_whatsapp, 
    seleccionar_producto_para_estado, 
    subir_estado_whatsapp
)

def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Bot de Marketing...")

    # ==========================================================
    # 🎲 MODO HUMANO: Retraso aleatorio (1 a 45 min)
    # ==========================================================
    retraso_minutos = random.randint(1, 45)
    retraso_segundos = retraso_minutos * 60
    print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de actuar...")
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
            # LEER ÓRDENES DEL PANEL DE CONTROL
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            if not config:
                print("🛑 No hay configuración en la base de datos.")
                return

            # ==================================================================
            # 🎯 TAREA 1: BOT FRANCOTIRADOR (MENSAJES DIRECTOS)
            # ==================================================================
            if config.bot_activo:
                print("\n▶️ INICIANDO TAREA 1: Mensajes Directos")
                # VALIDAR HORARIO COMERCIAL
                hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
                ahora = hora_peru.time()
                if not (config.hora_inicio <= ahora <= config.hora_fin):
                    print(f"⏰ Fuera de horario para mensajes directos. Permitido: {config.hora_inicio} a {config.hora_fin}.")
                else:
                    # VALIDAR LÍMITE DE MENSAJES DIARIOS
                    enviados_hoy = conn.execute(text("""
                        SELECT COUNT(*) FROM mensajes 
                        WHERE tipo = 'SALIENTE_BOT' AND fecha::date = CURRENT_DATE
                    """)).scalar()
                    
                    if enviados_hoy >= config.max_mensajes_dia:
                        print(f"📈 Límite diario alcanzado ({enviados_hoy}/{config.max_mensajes_dia}).")
                    else:
                        # BUSCAR EL GRUPO DE PRODUCTOS
                        filtro_sql = "" if config.tipo_objetivo == 'Todos' else "WHERE tipo = :t"
                        grupos = conn.execute(text(f"SELECT * FROM Grupos_Productos {filtro_sql}"), {"t": config.tipo_objetivo}).fetchall()
                        
                        if not grupos:
                            print(f"⚠️ No encontré ningún grupo del tipo '{config.tipo_objetivo}'.")
                        else:
                            grupo_elegido = random.choice(grupos)

                            # BUSCAR CLIENTE IDEAL (Con hasta 3 intentos)
                            enviado_exitoso = False
                            for intento in range(3):
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
                                    print("🛡️ No hay clientes disponibles ahora mismo.")
                                    break

                                cliente = random.choice(clientes_validos)
                                norm = normalizar_telefono_maestro(cliente.telefono)
                                
                                if not norm:
                                    conn.execute(text("UPDATE Clientes SET activo = FALSE, notas = 'Formato de teléfono inválido' WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                                    conn.commit()
                                    continue 

                                telefono_final = norm['db']
                                existe_wsp = verificar_numero_waha(telefono_final)
                                
                                if existe_wsp is False:
                                    # Lógica de búsqueda de alternativa de teléfono
                                    otro_telefono = conn.execute(text("""
                                        SELECT telefono FROM telefonoscliente 
                                        WHERE id_cliente = :idc AND telefono != :t AND activo = TRUE 
                                        LIMIT 1
                                    """), {"idc": cliente.id_cliente, "t": telefono_final}).fetchone()
                                    
                                    if otro_telefono:
                                        nuevo_tel = otro_telefono[0]
                                        conn.execute(text("UPDATE telefonoscliente SET activo = FALSE WHERE telefono = :t"), {"t": telefono_final})
                                        conn.execute(text("UPDATE telefonoscliente SET es_principal = FALSE WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                                        conn.execute(text("UPDATE telefonoscliente SET es_principal = TRUE WHERE telefono = :nt"), {"nt": nuevo_tel})
                                        conn.execute(text("UPDATE Clientes SET telefono = :nt WHERE id_cliente = :idc"), {"nt": nuevo_tel, "idc": cliente.id_cliente})
                                        conn.commit()
                                        telefono_final = nuevo_tel
                                    else:
                                        conn.execute(text("UPDATE Clientes SET activo = FALSE, notas = 'Ningún número tiene WhatsApp' WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
                                        conn.commit()
                                        continue 
                                        
                                elif existe_wsp is None:
                                    break 

                                # CONSTRUIR Y ENVIAR MENSAJE
                                if cliente.nombre_ia and str(cliente.nombre_ia).strip():
                                    saludo_aleatorio = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                                    cabecera = f"{saludo_aleatorio} {cliente.nombre_ia} 👋"
                                else:
                                    cabecera = "¡Hola! 👋"
                                    
                                mensaje_texto = f"{cabecera}\n\n{grupo_elegido.descripcion}\n\n{grupo_elegido.enlace_tienda}"
                                url_img = grupo_elegido.imagenes[0] if (grupo_elegido.imagenes and len(grupo_elegido.imagenes) > 0) else None

                                envio_ok = enviar_mensaje_whatsapp(telefono_final, mensaje_texto, url_img)
                                
                                if envio_ok:
                                    conn.execute(text("""
                                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido) 
                                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() - INTERVAL '5 hours'), TRUE)
                                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_texto})
                                    conn.commit()
                                    print("✅ ¡Mensaje enviado con éxito!")
                                    enviado_exitoso = True
                                    break 
                                else:
                                    print("❌ Falló el envío en la API de WhatsApp.")
                                    break
                            
                            if not enviado_exitoso:
                                print("⚠️ Se agotaron los intentos de envío de mensajes directos.")
            else:
                print("\n⏸️ TAREA 1 OMITIDA: Los mensajes directos están apagados en el Panel.")

            # ==================================================================
            # 📱 TAREA 2: PUBLICACIÓN DE ESTADOS (NUEVA LÓGICA)
            # ==================================================================
            if getattr(config, 'estados_activo', False):
                print("\n▶️ INICIANDO TAREA 2: Publicación de Estado de WhatsApp")
                
                # 1. El Cerebro elige el producto según las probabilidades
                producto = seleccionar_producto_para_estado(
                    getattr(config, 'prob_natural', 34), 
                    getattr(config, 'prob_fantasia', 33), 
                    getattr(config, 'prob_accesorios', 33)
                )

                if not producto:
                    print("⚠️ No hay productos elegibles (sin stock o ya publicados en los últimos 14 días).")
                else:
                    # 2. PLANTILLA ESTÁTICA (En la Fase 3, aquí conectaremos la IA)
                    texto_estado = f"✨ ¡Mira lo que tenemos en stock!\n{producto['nombre']}\n\nEnvíanos un mensaje para más información. 📲"
                    
                    # 3. Subir a WAHA
                    print(f"🚀 Subiendo estado: {producto['sku']} - {producto['nombre']}")
                    exito, mensaje_estado = subir_estado_whatsapp(
                        session_name="default", 
                        texto=texto_estado, 
                        media_url=producto['imagen']
                    )

                    # 4. Guardar en el historial
                    if exito:
                        # Usamos otra transacción para no interferir con la de arriba
                        with motor_seguro.begin() as trans_estados:
                            trans_estados.execute(text("""
                                INSERT INTO Historial_Estados (sku) VALUES (:sku)
                            """), {"sku": producto['sku']})
                        print(f"✅ ¡Estado subido con éxito y anotado en el historial!")
                    else:
                        print(f"❌ Falló la publicación del estado: {mensaje_estado}")
            else:
                print("\n⏸️ TAREA 2 OMITIDA: La publicación de Estados está apagada en el Panel.")

    except Exception as e:
        print(f"🔥 Error catastrófico en el script: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()