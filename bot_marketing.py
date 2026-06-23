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

# ==============================================================================
# 🤖 INTELIGENCIA ARTIFICIAL ORIENTADA A LA VENTA DIRECTA
# ==============================================================================
def generar_copy_venta_ia(producto_db, macro_categoria):
    """
    Pide a Ollama local un copy persuasivo de venta directa amarrado a su dominio web real.
    producto_db es un diccionario con: nombre, marca, modelo, precio, sku
    """
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
    5. NO saludes al principio (el bot inyectará el saludo con el nombre del cliente dinámicamente arriba).
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

    # Fallback blindado por si tu IA local está apagada
    return f"¡Mira el hermoso modelo que acaba de reingresar a nuestro almacén!\n\n✨ **{producto_db['marca']} {producto_db['modelo']} - {producto_db['nombre']}** a solo **S/ {producto_db['precio']}**.\n\nConsíguelo antes de que se agote ingresando aquí:\n👉 https://{dominio}/producto/{producto_db['sku']}"

def obtener_posdata_cruzada(macro_actual):
    """Genera el pie de página informando sobre la otra línea de negocio"""
    if macro_actual == 'Lentes':
        return "\n\n---\n✨ **P.D.:** ¿Sabías que también contamos con una hermosa colección de pelucas importadas? Descúbrelas en 👉 https://pelucat.pe"
    else:
        return "\n\n---\n✨ **P.D.:** ¿Sabías que también somos expertos en lentes de contacto? Cambia tu mirada en 👉 https://kmlentes.pe"

# ==============================================================================
# 🔎 SELECTOR SQL DE MERCADERÍA EN STOCK PARA MENSAJES DIRECTOS
# ==============================================================================
def buscar_producto_aleatorio_en_stock(conn, macro_categoria, subcategorias_permitidas):
    """Busca 1 producto al azar que tenga stock físico > 0 y pertenezac a las carpetas con check"""
    if not subcategorias_permitidas:
        return None
    
    # Formateo defensivo de lista de strings para SQL
    lista_sql = ', '.join([f"'{s}'" for s in subcategorias_permitidas])
    
    query = text(f"""
        SELECT p.id_producto, p.marca, p.modelo, p.nombre, p.categoria, p.url_imagen, v.sku, v.precio
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        WHERE p.macro_categoria = :macro 
          AND p.categoria IN ({lista_sql})
          AND v.stock_interno > 0
          AND p.url_imagen IS NOT NULL AND p.url_imagen != ''
        ORDER BY RANDOM()
        LIMIT 1
    """)
    row = conn.execute(query, {"macro": macro_categoria}).fetchone()
    if row:
        return dict(row._mapping)
    return None

def obtener_subcarpetas_activas(config, macro_categoria):
    """Filtra las subcategorías según los checks que dejaste encendidos en el panel"""
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
            if not config:
                print("🛑 No hay configuración registrada en la base de datos.")
                return

            hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
            ahora = hora_peru.time()
            dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

            # ==================================================================
            # 🎯 TAREA 1: BOT FRANCOTIRADOR (PRESUPUESTO DIVIDIDO / MEMORIA COMPARTIDA)
            # ==================================================================
            if config.bot_activo:
                print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot)")
                if not dentro_de_horario:
                    print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
                else:
                    # --- REGLA 1: AMBOS OBREROS AL 100% DE INTENTOS POR HORA ---
                    obreros = [
                        {"sesion": "default", "macro": "Lentes", "prob": 100},
                        {"sesion": "principal", "macro": random.choice(["Pelucas", "Lentes"]), "prob": 100} # <--- Freno quitado (pasó de 50 a 100)
                    ]

                    for obrero in obreros:
                        print(f"\n--------------------------------------------------")
                        print(f"🤖 Evaluando Obrero de Sesión: '{obrero['sesion']}'")
                        print(f"--------------------------------------------------")

                        if random.randint(1, 100) > obrero["prob"]:
                            print(f"🎲 No superó su roll de probabilidad horaria ({obrero['prob']}%).")
                            continue

                        # --- REGLA 1: BILLETERA INDEPENDIENTE ---
                        # Contamos cuántos mensajes ha disparado estrictamente ESTA sesión hoy
                        query_conteo = text("""
                            SELECT COUNT(*) FROM mensajes 
                            WHERE tipo = 'SALIENTE_BOT' 
                              AND COALESCE(session_name, 'default') = :sess 
                              AND fecha::date = CURRENT_DATE
                        """)
                        enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar()

                        if enviados_por_mi >= config.max_mensajes_dia:
                            print(f"📈 [Presupuesto Lleno]: Esta línea ya disparó su tope de {config.max_mensajes_dia} mensajes hoy.")
                            continue

                        # 1. Verificar qué subcarpetas le dejaste tildadas en el Panel
                        carpetas_ok = obtener_subcarpetas_activas(config, obrero['macro'])
                        if not carpetas_ok:
                            print(f"⚠️ Todas las subcategorías de {obrero['macro']} están apagadas en el Panel.")
                            continue

                        # 2. Extraer un producto físico con stock real
                        prod_elegido = buscar_producto_aleatorio_en_stock(conn, obrero['macro'], carpetas_ok)
                        if not prod_elegido:
                            print(f"⚠️ No hay stock físico disponible para promocionar {obrero['macro']}.")
                            continue

                        # --- REGLA 2: MEMORIA COMPARTIDA (EL ESCUDO GLOBAL) ---
                        # Nota de ingeniería: El "NOT IN" busca en la tabla mensajes SIN filtrar por sesión.
                        # Si al cliente ya le escribió Lentes, la cuenta Principal lo verá ahí y lo respetará.
                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, t.telefono 
                            FROM clientes c
                            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                            WHERE c.activo = TRUE AND c.estado = 'Sin empezar'
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
                            print(f"🛡️ No quedan clientes elegibles libres de cuarentena omnicanal en estado 'Sin empezar'.")
                            continue

                        cliente = random.choice(clientes_validos)
                        norm = normalizar_telefono_maestro(cliente.telefono)
                        if not norm: continue

                        telefono_final = norm['db']
                        if verificar_numero_waha(telefono_final):
                            saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                            nom_ia = cliente.nombre_ia.strip() if cliente.nombre_ia else ""
                            cabecera = f"{saludo} {nom_ia} 👋" if nom_ia else "¡Hola! 👋"

                            print(f"🧠 Redactando copy magnético para: {prod_elegido['nombre']}...")
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
            else:
                print("\n⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado en el Panel.")

            # ==================================================================
            # 📱 TAREA 2: ESTADOS DE WHATSAPP (Tu código original intacto)
            # ==================================================================
            if getattr(config, 'estados_activo', False):
                print("\n▶️ INICIANDO TAREA 2: Publicación de Estados en WhatsApp")
                if not dentro_de_horario:
                    print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
                else:
                    prob_nat, prob_fan, prob_acc = getattr(config, 'prob_natural', 34), getattr(config, 'prob_fantasia', 33), getattr(config, 'prob_accesorios', 33)

                    # Cuenta default (Lentes)
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

                    # Cuenta principal (Omnicanal)
                    prob_master_roll = getattr(config, 'prob_sesion_principal', 100) # <--- Blindado a 100 por defecto
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