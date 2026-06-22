import os
import requests
import random
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Cargar variables de entorno
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

from utils import (
    normalizar_telefono_maestro, 
    verificar_numero_waha, 
    enviar_mensaje_whatsapp, 
    seleccionar_producto_para_estado, 
    subir_estado_whatsapp,
    generar_texto_estado_ia
)

def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Motor de Marketing Omni-Canal...")

    # Retraso orgánico humano (1 a 55 min)
    retraso_minutos = random.randint(1, 55)
    print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de actuar...")
    time.sleep(retraso_minutos * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Tiempo de espera terminado. Iniciando ráfaga...")

    db_url = os.getenv("DATABASE_URL")
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    motor_seguro = create_engine(db_url)

    try:
        with motor_seguro.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            if not config:
                print("🛑 No hay configuración registrada en la base de datos.")
                return

            hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
            ahora = hora_peru.time()
            dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

            # ==================================================================
            # 🎯 TAREA 1: BOT FRANCOTIRADOR (MENSAJES DIRECTOS CON ENRUTAMIENTO)
            # ==================================================================
            if config.bot_activo:
                print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot)")
                if not dentro_de_horario:
                    print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
                else:
                    enviados_hoy = conn.execute(text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND fecha::date = CURRENT_DATE")).scalar()
                    
                    if enviados_hoy >= config.max_mensajes_dia:
                        print(f"📈 Límite de mensajes diarios alcanzado ({enviados_hoy}/{config.max_mensajes_dia}).")
                    else:
                        filtro_sql = "" if config.tipo_objetivo == 'Todos' else "WHERE tipo = :t"
                        grupos = conn.execute(text(f"SELECT * FROM Grupos_Productos {filtro_sql}"), {"t": config.tipo_objetivo}).fetchall()
                        
                        if grupos:
                            grupo_elegido = random.choice(grupos)
                            
                            # --- CORRECCIÓN VITAL 1: Enrutamiento de Sesión ---
                            # Si el grupo es de Pelucas, dispara por la cuenta 'principal'; si es de Lentes, por 'default'
                            sesion_disparo = "principal" if getattr(grupo_elegido, 'tipo', '') == 'Pelucas' else "default"

                            for intento in range(3):
                                query_clientes = text("""
                                    SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, t.telefono 
                                    FROM clientes c
                                    JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                                    WHERE c.activo = TRUE AND c.estado = 'Sin empezar'
                                      AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6
                                      AND t.telefono NOT IN (SELECT telefono FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND fecha > NOW() - INTERVAL '60 days')
                                """)
                                clientes_validos = conn.execute(query_clientes).fetchall()
                                
                                if not clientes_validos:
                                    print("🛡️ No hay clientes en estado 'Sin empezar' elegibles en este momento.")
                                    break

                                cliente = random.choice(clientes_validos)
                                norm = normalizar_telefono_maestro(cliente.telefono)
                                if not norm: continue 

                                telefono_final = norm['db']
                                if verificar_numero_waha(telefono_final):
                                    saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                                    nom_ia = cliente.nombre_ia.strip() if cliente.nombre_ia else ""
                                    cabecera = f"{saludo} {nom_ia} 👋" if nom_ia else "¡Hola! 👋"
                                        
                                    mensaje_texto = f"{cabecera}\n\n{grupo_elegido.descripcion}\n\n{grupo_elegido.enlace_tienda}"
                                    url_img = grupo_elegido.imagenes[0] if (grupo_elegido.imagenes and len(grupo_elegido.imagenes) > 0) else None

                                    # Disparamos respetando estrictamente la sesión asignada
                                    if enviar_mensaje_whatsapp(telefono_final, mensaje_texto, url_img, session=sesion_disparo):
                                        conn.execute(text("INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW() - INTERVAL '5 hours', TRUE, :sess)"), 
                                                     {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_texto, "sess": sesion_disparo})
                                        conn.commit()
                                        print(f"✅ ¡Mensaje directo enviado a {telefono_final} (Vía: {sesion_disparo})!")
                                        break 
            else:
                print("\n⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado en el Panel.")

            # ==================================================================
            # 📱 TAREA 2: PUBLICACIÓN DE ESTADOS (EVALUACIÓN INDEPENDIENTE)
            # ==================================================================
            if getattr(config, 'estados_activo', False):
                print("\n▶️ INICIANDO TAREA 2: Publicación de Estados en WhatsApp")
                if not dentro_de_horario:
                    print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
                else:
                    prob_nat = getattr(config, 'prob_natural', 34)
                    prob_fan = getattr(config, 'prob_fantasia', 33)
                    prob_acc = getattr(config, 'prob_accesorios', 33)

                    # --- CORRECCIÓN VITAL 2: Evaluación Aislada para 'default' (Solo Lentes) ---
                    prob_lentes_roll = getattr(config, 'prob_sesion_lentes', 100)
                    if random.randint(1, 100) <= prob_lentes_roll:
                        print(f"\n👉 [Cuenta: default / Lentes] Ganó el roll ({prob_lentes_roll}%). Buscando ítem de Lentes...")
                        producto_lentes = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo='Lentes')
                        
                        if producto_lentes:
                            print(f"🧠 Redactando copy con IA para: {producto_lentes['nombre']}...")
                            texto_ia_lentes = generar_texto_estado_ia(producto_lentes)
                            
                            exito, msg_resp = subir_estado_whatsapp("default", texto_ia_lentes, producto_lentes['imagen'])
                            if exito:
                                conn.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_lentes['sku']})
                                conn.commit()
                                print(f"   ✅ ¡Estado de Lentes publicado exitosamente en 'default'!")
                            else:
                                print(f"   ❌ Falló estado en 'default': {msg_resp}")
                    else:
                        print(f"\n⏸️ [Cuenta: default] No superó el roll de probabilidad esta hora.")

                    # --- CORRECCIÓN VITAL 3: Evaluación Aislada para 'principal' (Omnicanal) ---
                    prob_master_roll = getattr(config, 'prob_sesion_principal', 50)
                    if random.randint(1, 100) <= prob_master_roll:
                        # El cerebro decide qué promocionar en esta hora (60% Pelucas, 40% Lentes)
                        macro_elegida_master = random.choices(['Pelucas', 'Lentes'], weights=[60, 40], k=1)[0]
                        print(f"\n👉 [Cuenta: principal / Master] Ganó el roll ({prob_master_roll}%). El altavoz principal promocionará hoy: {macro_elegida_master.upper()}...")
                        
                        producto_master = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo=macro_elegida_master)
                        
                        if producto_master:
                            print(f"🧠 Redactando copy con IA para: {producto_master['nombre']}...")
                            texto_ia_master = generar_texto_estado_ia(producto_master)
                            
                            exito, msg_resp = subir_estado_whatsapp("principal", texto_ia_master, producto_master['imagen'])
                            if exito:
                                conn.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_master['sku']})
                                conn.commit()
                                print(f"   ✅ ¡Estado de {macro_elegida_master} publicado exitosamente en la cuenta 'principal'!")
                            else:
                                print(f"   ❌ Falló estado en 'principal': {msg_resp}")
                    else:
                        print(f"\n⏸️ [Cuenta: principal] No superó el roll de probabilidad esta hora.")
            else:
                print("\n⏸️ TAREA 2 OMITIDA: Los estados automáticos están apagados en el Panel.")

    except Exception as e:
        print(f"🔥 Error en la ejecución del bot de marketing: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()