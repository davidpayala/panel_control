import os
import sys
from dotenv import load_dotenv

# 1. REGLA DE ORO: Inyectar variables de entorno ANTES de invocar a database.py
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# 2. Importamos los módulos de la infraestructura
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
    subir_estado_whatsapp,
    generar_texto_producto_ia
)

# ==============================================================================
# 🧠 MOTOR BLINDADO DE SELECCIÓN DE PRODUCTOS (Aislamiento de Categorías)
# ==============================================================================

def buscar_producto_dinamico(conn, col_probabilidad):
    """
    Selecciona un producto con stock asegurando un doble candado:
    Coincidencia exacta de Macro-Categoría + Sub-Categoría con probabilidad > 0.
    """
    # 1. Extraer macro_categoria, subcategoria y probabilidad > 0 (Ignorando mayúsculas/espacios)
    query_pesos = text(f"""
        SELECT TRIM(s.macro_categoria) as macro, TRIM(s.subcategoria) as subcat, MAX(s.{col_probabilidad}) as prob
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        JOIN Subcategorias_Sistema s ON TRIM(p.categoria) ILIKE TRIM(s.subcategoria) 
                                    AND TRIM(p.macro_categoria) ILIKE TRIM(s.macro_categoria)
        WHERE COALESCE(v.stock_interno, 0) > 0
          AND p.url_imagen IS NOT NULL AND TRIM(p.url_imagen) != ''
        GROUP BY TRIM(s.macro_categoria), TRIM(s.subcategoria)
        HAVING COALESCE(MAX(s.{col_probabilidad}), 0) > 0
    """)
    
    categorias_validas = conn.execute(query_pesos).fetchall()
    
    if not categorias_validas:
        return None 
        
    opciones = [(row.macro, row.subcat) for row in categorias_validas]
    pesos = [row.prob for row in categorias_validas]
    
    eleccion = random.choices(opciones, weights=pesos, k=1)[0]
    macro_elegida, cat_elegida = eleccion
    
    condicion_historial = ""
    if 'est_' in col_probabilidad:
        condicion_historial = "AND v.sku NOT IN (SELECT sku FROM Historial_Estados WHERE fecha_publicacion > NOW() - INTERVAL '14 days')"
        
    # --- INYECCIÓN DE LA COLUMNA URL_TIENDA EN EL SELECT ---
    query_prod = text(f"""
        SELECT 
            p.id_producto, p.marca, p.modelo, p.nombre, p.categoria, p.color_principal,
            p.url_imagen, p.url_tienda, v.sku, v.precio, p.macro_categoria,
            s.descripcion_ia as enfoque_ia
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        LEFT JOIN Subcategorias_Sistema s ON TRIM(p.categoria) ILIKE TRIM(s.subcategoria)
        WHERE TRIM(p.categoria) ILIKE :cat
          AND TRIM(p.macro_categoria) ILIKE :macro
          AND COALESCE(v.stock_interno, 0) > 0
          AND p.url_imagen IS NOT NULL AND TRIM(p.url_imagen) != ''
          {condicion_historial}
        ORDER BY RANDOM()
        LIMIT 1
    """)
    
    prod = conn.execute(query_prod, {"cat": cat_elegida, "macro": macro_elegida}).fetchone()
    
    if not prod and 'est_' in col_probabilidad:
        query_rescate = query_prod.text.replace(condicion_historial, "")
        prod = conn.execute(text(query_rescate), {"cat": cat_elegida, "macro": macro_elegida}).fetchone()
        
    if prod:
        return dict(prod._mapping)
    return None

# ==============================================================================
# 🚀 MOTOR ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_francotirador():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🤖 Despertando Motor de Marketing Multi-Terminal...")

    es_modo_test = "--test" in sys.argv or "--now" in sys.argv

    if not es_modo_test:
        retraso_minutos = random.randint(1, 55)
        print(f"⏳ Modo orgánico: Esperando {retraso_minutos} minutos antes de actuar... (Tip: '--test' para saltar)")
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
        # 🎯 TAREA 1: MENSAJES DIRECTOS (Orden: Principal -> Lentes)
        # ==================================================================
        if config.bot_activo:
            print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot)")
            if not dentro_de_horario:
                print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
            else:
                obreros = [
                    {"sesion": "principal", "col_prob": "prob_msg_principal", "nombre_vis": "Principal"},
                    {"sesion": "default", "col_prob": "prob_msg_default", "nombre_vis": "Lentes"}
                ]

                for obrero in obreros:
                    print(f"\n--------------------------------------------------")
                    print(f"🤖 Evaluando Cuenta: '{obrero['nombre_vis']}'")
                    print(f"--------------------------------------------------")

                    with engine.connect() as conn:
                        query_conteo = text("""
                            SELECT COUNT(*) FROM mensajes 
                            WHERE tipo = 'SALIENTE_BOT' 
                              AND COALESCE(session_name, 'default') = :sess 
                              AND fecha::date = CURRENT_DATE
                        """)
                        enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar() or 0

                        if enviados_por_mi >= config.max_mensajes_dia:
                            print(f"📈 [Presupuesto Lleno]: {obrero['nombre_vis']} ya alcanzó su tope diario ({config.max_mensajes_dia}).")
                            continue

                        prod_elegido = buscar_producto_dinamico(conn, obrero['col_prob'])
                        if not prod_elegido:
                            print(f"⚠️ Omitido: No hay stock o todas las categorías tienen 0% de probabilidad en {obrero['nombre_vis']}.")
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
                        print(f"🛡️ Omitido: No quedan clientes pendientes en estado 'Sin empezar'.")
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

                            print(f"🧠 Redactando copy magnético (IA) para: {prod_elegido.get('nombre', '')[:25]}...")
                            info_prospecto = {"etiquetas": cliente.etiquetas if cliente.etiquetas else ""}
                            
                            cuerpo_ia = generar_texto_producto_ia(prod_elegido, es_estado=False, cliente_info=info_prospecto)
                            mensaje_completo = f"{cabecera}\n\n{cuerpo_ia}"

                            if enviar_mensaje_whatsapp(telefono_final, mensaje_completo, prod_elegido['url_imagen'], session=obrero['sesion']):
                                with engine.begin() as conn_save:
                                    conn_save.execute(text("""
                                        INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) 
                                        VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW() - INTERVAL '5 hours', TRUE, :sess)
                                    """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion']})
                                
                                print(f"✅ ¡Disparo exitoso enviado a {telefono_final} (Vía: {obrero['nombre_vis']})!")
                                disparo_exitoso = True
                                break 
                            else:
                                print(f"   ❌ WAHA rechazó el envío HTTP para {telefono_final}")

                        elif check_waha is False:
                            print(f"   ⚠️ [Intento {prospectos_evaluados}/5] El número {telefono_final} no tiene WhatsApp. Purgando...")
                            with engine.begin() as conn_purge:
                                conn_purge.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE telefono=:t"), {"t": telefono_final})

                    if not disparo_exitoso:
                        print(f"🛑 [{obrero['nombre_vis']}] Fallaron los {prospectos_evaluados} prospectos evaluados.")
        else:
            print("\n⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado en el Panel.")

        # ==================================================================
        # 📱 TAREA 2: ESTADOS DE WHATSAPP (Orden: Principal -> Lentes)
        # ==================================================================
        print("\n▶️ INICIANDO TAREA 2: Publicación de Estados en WhatsApp")
        if not dentro_de_horario:
            print(f"⏰ Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
        else:
            cuentas_estados = [
                {"sesion": "principal", "col_prob": "prob_est_principal", "nombre_vis": "Principal"},
                {"sesion": "default", "col_prob": "prob_est_default", "nombre_vis": "Lentes"}
            ]
            
            for cuenta in cuentas_estados:
                print(f"📡 Evaluando Estados para cuenta: '{cuenta['nombre_vis']}'...")
                with engine.connect() as conn:
                    prod_est = buscar_producto_dinamico(conn, cuenta['col_prob'])
                    
                if prod_est:
                    # CORRECCIÓN: Usamos la variable iterativa 'prod_est', no 'producto_lentes'
                    respuestas_ia = generar_texto_producto_ia(prod_est, es_estado=True)
                    
                    texto_wsp = respuestas_ia.get('estado_whatsapp', '')
                    texto_fb = respuestas_ia.get('post_facebook', '') # ¡Guardado en memoria para usarlo luego en Meta!

                    # CORRECCIÓN: Usamos la cuenta dinámica y el url dinámico
                    exito, _ = subir_estado_whatsapp(cuenta['sesion'], texto_wsp, prod_est.get('url_imagen', ''))
                    if exito:
                        with engine.begin() as conn_est:
                            conn_est.execute(text("INSERT INTO Historial_Estados (sku) VALUES (:sku)"), {"sku": prod_est['sku']})
                        print(f"  ✅ ¡Estado publicado exitosamente en {cuenta['nombre_vis']}!")
                else:
                    print(f"  ⚠️ Omitido: No hay stock o todas las probabilidades están en 0% para {cuenta['nombre_vis']}.")

    except Exception as e:
        print(f"🔥 Error catastrófico en la ejecución de marketing: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()