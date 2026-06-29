import os
import sys
from dotenv import load_dotenv

# 1. REGLA DE ORO: Inyectar variables de entorno ANTES de invocar a database.py
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# 2. AHORA SÍ importamos los módulos de la infraestructura
import requests
import random
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from database import engine  

from utils import (
    normalizar_telefono_maestro, 
    verificar_numero_waha, 
    enviar_mensaje_whatsapp, 
    seleccionar_producto_para_estado, 
    subir_estado_whatsapp,
    generar_texto_producto_ia, # <-- IMPORT CENTRALIZADO ACTUALIZADO
    buscar_producto_aleatorio_en_stock
)

def obtener_subcarpetas_activas(config, macro_categoria):
    activos = []
    if macro_categoria == 'Lentes':
        if getattr(config, 'cat_len_nat', True): activos.append('Estilo Natural')
        if getattr(config, 'cat_len_fan', True): activos.append('Estilo Fantasía')
        if getattr(config, 'cat_len_acc', True): activos.append('Accesorios')
    else:
        if getattr(config, 'cat_pel_nat', True): activos.append('Peluca Natural')
        if getattr(config, 'cat_pel_fan', True): activos.append('Peluca Fantasía')
        if getattr(config, 'cat_pel_acc', True): activos.append('Accesorios Pelucas')
    return activos

# ==============================================================================
# 🚀 MOTOR ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Motor de Marketing Omni-Canal...")

    es_modo_test = "--test" in sys.argv or "--now" in sys.argv

    if not es_modo_test:
        retraso_minutos = random.randint(1, 55)
        print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de actuar... (Tip: Ejecuta con '--test' para saltar)")
        time.sleep(retraso_minutos * 60)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Tiempo de espera terminado. Iniciando evaluación...")
    else:
        print("⚡ [Modo Test activado]: Disparando ráfaga inmediata.")

    try:
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        if not config:
            print("🛑 No hay configuración registrada en la base de datos.")
            return

        hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
        ahora = hora_peru.time()
        dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

        # ==================================================================
        # 🎯 TAREA 1: BOT FRANCOTIRADOR (MENSAJES DIRECTOS)
        # ==================================================================
        if config.bot_activo:
            print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot)")
            if not dentro_de_horario:
                print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
            else:
                obreros = [
                    {"sesion": "default", "macro": "Lentes", "prob": 100},
                    {"sesion": "principal", "macro": random.choice(["Pelucas", "Lentes"]), "prob": 100}
                ]

                for obrero in obreros:
                    print(f"\n--------------------------------------------------")
                    print(f"🤖 Evaluando Obrero de Sesión: '{obrero['sesion']}' ({obrero['macro']})")
                    print(f"--------------------------------------------------")

                    if random.randint(1, 100) > obrero["prob"]: continue

                    with engine.connect() as conn:
                        query_conteo = text("""
                            SELECT COUNT(*) FROM mensajes 
                            WHERE tipo = 'SALIENTE_BOT' 
                              AND COALESCE(session_name, 'default') = :sess 
                              AND fecha::date = CURRENT_DATE
                        """)
                        enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar() or 0

                        if enviados_por_mi >= config.max_mensajes_dia:
                            print(f"📈 [Presupuesto Lleno]: Esta línea ya disparó su tope hoy.")
                            continue

                        carpetas_ok = obtener_subcarpetas_activas(config, obrero['macro'])
                        if not carpetas_ok:
                            print(f"⚠️ Omitido: Todas las subcarpetas de {obrero['macro']} están apagadas.")
                            continue

                        prod_elegido = buscar_producto_aleatorio_en_stock(conn, obrero['macro'], carpetas_ok)
                        if not prod_elegido:
                            print(f"⚠️ Omitido: No hay stock físico disponible para la línea '{obrero['macro']}'.")
                            continue

                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, c.etiquetas, t.telefono 
                            FROM clientes c
                            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                            WHERE c.activo = TRUE 
                              AND c.estado = 'Sin empezar'
                              AND COALESCE(c.excluir_publicidad, FALSE) = FALSE 
                              AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6
                              AND t.telefono NOT IN (
                                  SELECT telefono FROM mensajes 
                                  WHERE tipo = 'SALIENTE_BOT' 
                                    AND fecha > (NOW() - INTERVAL '60 days')
                              )
                            LIMIT 50
                        """)
                        clientes_validos = conn.execute(query_clientes).fetchall()

                    if not clientes_validos:
                        print(f"🛡️ Omitido: No quedan clientes en estado 'Sin empezar' elegibles.")
                        continue

                    prospectos = list(clientes_validos)
                    random.shuffle(prospectos)
                    
                    disparo_exitoso = False
                    prospectos_evaluados = 0

                    for cliente in prospectos[:5]:
                        prospectos_evaluados += 1
                        norm = normalizar_telefono_maestro(cliente.telefono)
                        if not norm: continue

                        telefono_final = norm['db']
                        check_waha = verificar_numero_waha(telefono_final)

                        if check_waha is True:
                            saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                            nom_ia = cliente.nombre_ia.strip() if cliente.nombre_ia else ""
                            cabecera = f"{saludo} {nom_ia} 👋" if nom_ia else "¡Hola! 👋"

                            print(f"🧠 Redactando copy magnético dinámico (IA) para: {prod_elegido.get('nombre', '')[:25]}...")
                            
                            info_prospecto = {"etiquetas": cliente.etiquetas if cliente.etiquetas else ""}
                            
                            # <-- LLAMADA REFACTORIZADA PARA MENSAJE DIRECTO (es_estado=False)
                            cuerpo_ia = generar_texto_producto_ia(prod_elegido, es_estado=False, cliente_info=info_prospecto)

                            mensaje_completo = f"{cabecera}\n\n{cuerpo_ia}"

                            if enviar_mensaje_whatsapp(telefono_final, mensaje_completo, prod_elegido['url_imagen'], session=obrero['sesion']):
                                with engine.begin() as conn_save:
                                    conn_save.execute(text("""
                                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) 
                                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW() - INTERVAL '5 hours', TRUE, :sess)
                                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion']})
                                
                                print(f"✅ ¡Disparo de {obrero['macro']} enviado a {telefono_final} (Vía: {obrero['sesion']})!")
                                disparo_exitoso = True
                                break 
                            else:
                                print(f"   ❌ WAHA rechazó el envío HTTP para {telefono_final}")

                        elif check_waha is False:
                            print(f"   ⚠️ [Intento {prospectos_evaluados}/5] El número {telefono_final} no tiene WhatsApp. Purgando...")
                            with engine.begin() as conn_purge:
                                conn_purge.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE telefono=:t"), {"t": telefono_final})

                    if not disparo_exitoso:
                        print(f"🛑 [Obrero '{obrero['sesion']}'] Fallaron los {prospectos_evaluados} prospectos evaluados.")
        else:
            print("\n⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado en el Panel.")

        # ==================================================================
        # 📱 TAREA 2: ESTADOS DE WHATSAPP
        # ==================================================================
        if getattr(config, 'estados_activo', False):
            print("\n▶️ INICIANDO TAREA 2: Publicación de Estados en WhatsApp")
            if not dentro_de_horario:
                print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
            else:
                prob_nat = getattr(config, 'prob_natural', 34)
                prob_fan = getattr(config, 'prob_fantasia', 33)
                prob_acc = getattr(config, 'prob_accesorios', 33)

                prob_lentes_roll = getattr(config, 'prob_sesion_lentes', 100)
                if random.randint(1, 100) <= prob_lentes_roll:
                    producto_lentes = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo='Lentes')
                    if producto_lentes:
                        # <-- LLAMADA REFACTORIZADA PARA ESTADO (es_estado=True)
                        texto_ia_lentes = generar_texto_producto_ia(producto_lentes, es_estado=True)
                        exito, msg_resp = subir_estado_whatsapp("default", texto_ia_lentes, producto_lentes['imagen'])
                        if exito:
                            with engine.begin() as conn_est:
                                conn_est.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_lentes['sku']})
                            print(f"  ✅ ¡Estado de Lentes publicado en 'default'!")

                prob_master_roll = getattr(config, 'prob_sesion_principal', 100)
                if random.randint(1, 100) <= prob_master_roll:
                    macro_elegida_master = random.choices(['Pelucas', 'Lentes'], weights=[60, 40], k=1)[0]
                    producto_master = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo=macro_elegida_master)
                    if producto_master:
                        # <-- LLAMADA REFACTORIZADA PARA ESTADO (es_estado=True)
                        texto_ia_master = generar_texto_producto_ia(producto_master, es_estado=True)
                        exito, msg_resp = subir_estado_whatsapp("principal", texto_ia_master, producto_master['imagen'])
                        if exito:
                            with engine.begin() as conn_est2:
                                conn_est2.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_master['sku']})
                            print(f"  ✅ ¡Estado de {macro_elegida_master} publicado en 'principal'!")

    except Exception as e:
        print(f"🔥 Error catastrófico en la ejecución de marketing: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()