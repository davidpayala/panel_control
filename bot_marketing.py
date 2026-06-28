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
    generar_texto_estado_ia,
    buscar_producto_aleatorio_en_stock
)

# ==============================================================================
# 🤖 INTELIGENCIA ARTIFICIAL ORIENTADA A LA VENTA DIRECTA
# ==============================================================================
def generar_copy_venta_ia(producto_db, macro_categoria):
    dominio = "pelucat.pe" if macro_categoria == "Pelucas" else "kmlentes.pe"
    tipo_prod = "esta espectacular peluca" if macro_categoria == "Pelucas" else "estos increíbles lentes de contacto"

    prompt = f"""
    Eres un copywriter experto en cierres de ventas por WhatsApp para la marca {dominio}.
    Redacta un mensaje directo, magnético, cálido y sumamente tentador ofreciendo {tipo_prod}:

    Producto: {producto_db['marca']} {producto_db['modelo']} - {producto_db['nombre']}
    Precio normal: S/ {producto_db['precio']}
    Enlace de compra directo: https://{dominio}/producto/{producto_db['sku']}

    Reglas de redacción estrictas:
    1. Máximo 3 párrafos ultracortos.
    2. Usa 2 o 3 emojis llamativos.
    3. Concéntrate en el deseo de compra y la belleza del producto.
    4. Cierra con un fuerte llamado a la acción (CTA) pidiendo que hagan clic en el enlace.
    5. NO saludes al principio (el bot inyectará el saludo dinámicamente arriba).
    6. NO pongas despedidas ni postdatas.
    """

    try:
        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        resp = requests.post(f"{ollama_url}/api/generate", 
                             json={"model": "llama3", "prompt": prompt, "stream": False}, 
                             timeout=25)
        if resp.status_code == 200:
            return resp.json().get("response", "").strip()
    except Exception:
        pass

    return f"¡Mira el hermoso modelo que acaba de reingresar a nuestro almacén!\n\n✨ **{producto_db['marca']} {producto_db['modelo']} - {producto_db['nombre']}** a solo **S/ {producto_db['precio']}**.\n\nConsíguelo antes de que se agote ingresando aquí:\n👉 https://{dominio}/producto/{producto_db['sku']}"

def obtener_posdata_cruzada(macro_actual):
    if macro_actual == 'Lentes':
        return "\n\n---\n✨ **P.D.:** ¿Sabías que también contamos con una hermosa colección de pelucas importadas? Descúbrelas en 👉 https://pelucat.pe"
    else:
        return "\n\n---\n✨ **P.D.:** ¿Sabías que también somos expertos en lentes de contacto? Cambia tu mirada en 👉 https://kmlentes.pe"

def obtener_subcarpetas_activas(config, macro_categoria):
    activos = []
    if macro_categoria == 'Lentes':
        if getattr(config, 'cat_len_nat', True): activos.append('Estilo Natural')
        if getattr(config, 'cat_len_fan', True): activos.append('Estilo Fantasía')
        if getattr(config, 'cat_len_acc', True): activos.append('Accesorios')
    else:
        if getattr(config, 'cat_pel_nat', True): activos.append('Estilo Natural')
        if getattr(config, 'cat_pel_fan', True): activos.append('Estilo Fantasía')
        if getattr(config, 'cat_pel_acc', True): activos.append('Accesorios Pelucas')
    return activos

# ==============================================================================
# 🚀 MOTOR ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Motor de Marketing Omni-Canal...")

    retraso_minutos = random.randint(1, 55)
    print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de actuar...")
    time.sleep(retraso_minutos * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Tiempo de espera terminado. Iniciando evaluación...")

    db_url = os.getenv("DATABASE_URL")
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    motor_seguro = create_engine(db_url)

    try:
        with motor_seguro.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
            if not config: return

            hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
            ahora = hora_peru.time()
            dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

            # ==================================================================
            # 🎯 TAREA 1: BOT FRANCOTIRADOR (CON TAMBOR DE 5 BALAS ANTI-SILENCIO)
            # ==================================================================
            if config.bot_activo:
                print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot - Venta Físicos)")
                if not dentro_de_horario:
                    print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
                else:
                    obreros = [
                        {"sesion": "default", "macro": "Lentes", "prob": 100},
                        {"sesion": "principal", "macro": random.choice(["Pelucas", "Lentes"]), "prob": 100}
                    ]

                    for obrero in obreros:
                        print(f"\n--------------------------------------------------")
                        print(f"🤖 Evaluando Obrero de Sesión: '{obrero['sesion']}'")
                        print(f"--------------------------------------------------")

                        if random.randint(1, 100) > obrero["prob"]: continue

                        query_conteo = text("""
                            SELECT COUNT(*) FROM mensajes 
                            WHERE tipo = 'SALIENTE_BOT' 
                              AND COALESCE(session_name, 'default') = :sess 
                              AND fecha::date = CURRENT_DATE
                        """)
                        enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar()

                        if enviados_por_mi >= config.max_mensajes_dia:
                            print(f"📈 [Presupuesto Lleno]: Esta línea ya disparó su tope hoy.")
                            continue

                        carpetas_ok = obtener_subcarpetas_activas(config, obrero['macro'])
                        if not carpetas_ok:
                            print(f"⚠️ Omitido: Todas las subcarpetas de {obrero['macro']} están apagadas en el Panel.")
                            continue

                        prod_elegido = buscar_producto_aleatorio_en_stock(conn, obrero['macro'], carpetas_ok)
                        if not prod_elegido:
                            print(f"⚠️ Omitido: No hay stock físico crudo disponible para la línea '{obrero['macro']}'.")
                            continue

                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, t.telefono 
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

                        # --- EL TAMBOR DE 5 BALAS ---
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

                                print(f"🧠 Redactando copy magnético para: {prod_elegido['nombre'][:25]}...")
                                cuerpo_ia = generar_copy_venta_ia(prod_elegido, obrero['macro'])
                                posdata = obtener_posdata_cruzada(obrero['macro'])

                                mensaje_completo = f"{cabecera}\n\n{cuerpo_ia}{posdata}"

                                if enviar_mensaje_whatsapp(telefono_final, mensaje_completo, prod_elegido['url_imagen'], session=obrero['sesion']):
                                    conn.execute(text("""
                                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) 
                                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW() - INTERVAL '5 hours', TRUE, :sess)
                                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion']})
                                    conn.commit()
                                    print(f"✅ ¡Disparo de {obrero['macro']} enviado a {telefono_final} (Vía: {obrero['sesion']})!")
                                    disparo_exitoso = True
                                    break # ¡Éxito! Rompe el bucle de intentos y pasa a la siguiente sesión
                                else:
                                    print(f"   ❌ WAHA rechazó el envío HTTP para {telefono_final}")

                            elif check_waha is False:
                                print(f"   ⚠️ [Intento {prospectos_evaluados}/5] El número {telefono_final} no tiene WhatsApp. Purgando del CRM...")
                                conn.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE telefono=:t"), {"t": telefono_final})
                                conn.commit()

                        if not disparo_exitoso:
                            print(f"🛑 [Obrero '{obrero['sesion']}'] Fallaron los {prospectos_evaluados} prospectos evaluados al azar.")
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
                    prob_nat, prob_fan, prob_acc = getattr(config, 'prob_natural', 34), getattr(config, 'prob_fantasia', 33), getattr(config, 'prob_accesorios', 33)

                    prob_lentes_roll = getattr(config, 'prob_sesion_lentes', 100)
                    if random.randint(1, 100) <= prob_lentes_roll:
                        producto_lentes = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo='Lentes')
                        if producto_lentes:
                            texto_ia_lentes = generar_texto_estado_ia(producto_lentes)
                            exito, msg_resp = subir_estado_whatsapp("default", texto_ia_lentes, producto_lentes['imagen'])
                            if exito:
                                conn.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_lentes['sku']})
                                conn.commit()
                                print(f"  ✅ ¡Estado de Lentes publicado en 'default'!")

                    prob_master_roll = getattr(config, 'prob_sesion_principal', 100)
                    if random.randint(1, 100) <= prob_master_roll:
                        macro_elegida_master = random.choices(['Pelucas', 'Lentes'], weights=[60, 40], k=1)[0]
                        producto_master = seleccionar_producto_para_estado(prob_nat, prob_fan, prob_acc, macro_objetivo=macro_elegida_master)
                        if producto_master:
                            texto_ia_master = generar_texto_estado_ia(producto_master)
                            exito, msg_resp = subir_estado_whatsapp("principal", texto_ia_master, producto_master['imagen'])
                            if exito:
                                conn.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": producto_master['sku']})
                                conn.commit()
                                print(f"  ✅ ¡Estado de {macro_elegida_master} publicado en 'principal'!")

    except Exception as e:
        print(f"🔥 Error en la ejecución del bot de marketing: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()