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
    generar_texto_producto_ia,
    publicar_en_facebook_via_webhook
)

# ==============================================================================
# 🧠 MOTOR BLINDADO DE SELECCIÓN DE PRODUCTOS (Aislamiento de Categorías)
# ==============================================================================
def buscar_producto_dinamico(conn, col_probabilidad):
    """
    Selecciona un producto con stock asegurando un doble candado:
    Coincidencia exacta de Macro-Categoría + Sub-Categoría con probabilidad > 0.
    """
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

    try:
        # 1. Obtener configuraciones de intervalos base
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        if not config:
            print("🛑 No hay configuración registrada en la base de datos.")
            return

        # Helper para convertir texto de intervalo en minutos matemáticos
        def mapear_intervalo_a_minutos(texto):
            t_str = str(texto).lower()
            if "30" in t_str: return 30
            if "6" in t_str: return 360
            return 60  # "cada hora" por defecto

        minutos_req_msg = mapear_intervalo_a_minutos(config.intervalo_mensajes)
        minutos_req_est = mapear_intervalo_a_minutos(config.intervalo_estados)

        # 2. Retraso Orgánico Adaptativo Inteligente
        if not es_modo_test:
            min_intervalo_activo = min(minutos_req_msg, minutos_req_est)
            # Si el intervalo más corto es 30 min, el delay no puede superar los 20 min
            max_delay = min(25, max(1, min_intervalo_activo - 5))
            retraso_minutos = random.randint(1, max_delay)
            print(f"⏳ Modo orgánico adaptativo: Esperando {retraso_minutos} minutos antes de evaluar... (Tip: '--test' para saltar)")
            time.sleep(retraso_minutos * 60)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏰ Tiempo de espera terminado. Iniciando evaluación...")
        else:
            print("⚡ [Modo Test activado]: Disparando ráfaga inmediata.")

        # 3. Consultar a PostgreSQL la diferencia real en minutos usando su reloj interno (Evita desfases Python-Server)
        with engine.connect() as conn:
            query_tiempo = text("""
            SELECT 
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_mensajes, NOW() - INTERVAL '1 day')))/60 AS min_pasados_msg,
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_estados, NOW() - INTERVAL '1 day')))/60 AS min_pasados_est,
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_fb, NOW() - INTERVAL '1 day')))/60 AS min_pasados_fb
            FROM Configuracion_Campanas LIMIT 1
            """)
            tiempos = conn.execute(query_tiempo).fetchone()

        min_pasados_msg = tiempos.min_pasados_msg if tiempos else 9999
        min_pasados_est = tiempos.min_pasados_est if tiempos else 9999

        hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
        ahora = hora_peru.time()
        dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

        # Identificar si ya corresponde ejecutar cada tarea (Margen de 2 min de tolerancia por retrasos de ejecución)
        toca_ejecutar_msg = es_modo_test or (min_pasados_msg >= (minutos_req_msg - 2))
        toca_ejecutar_est = es_modo_test or (min_pasados_est >= (minutos_req_est - 2))

        # ==================================================================
        # 🎯 TAREA 1: MENSAJES DIRECTOS (Orden: Principal -> Lentes)
        # ==================================================================
        if not config.bot_activo:
            print("\n⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado en el Panel.")
        elif not toca_ejecutar_msg:
            print(f"\n⏳ [TAREA 1 OMITIDA]: Frecuencia de mensajes configurada '{config.intervalo_mensajes}'. (Solo han transcurrido {int(min_pasados_msg)} min).")
        elif not dentro_de_horario:
            print(f"\n⏰ TAREA 1 OMITIDA: Fuera de horario comercial para Mensajes ({config.hora_inicio} - {config.hora_fin}).")
        else:
            print("\n▶️ INICIANDO TAREA 1: Mensajes Directos (Sniper Bot)")
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

            # Registrar marca de tiempo global del ciclo evaluado de mensajes
            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_mensajes = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📱 TAREA 2: ESTADOS DE WHATSAPP (Orden: Principal -> Lentes)
        # ==================================================================
        if not toca_ejecutar_est:
            print(f"\n⏳ [TAREA 2 OMITIDA]: Frecuencia de estados configurada '{config.intervalo_estados}'. (Solo han transcurrido {int(min_pasados_est)} min).")
        elif not dentro_de_horario:
            print(f"\n⏰ TAREA 2 OMITIDA: Fuera de horario comercial para Estados ({config.hora_inicio} - {config.hora_fin}).")
        else:
            print("\n▶️ INICIANDO TAREA 2: Publicación de Estados en WhatsApp")
            cuentas_estados = [
                {"sesion": "principal", "col_prob": "prob_est_principal", "nombre_vis": "Principal"},
                {"sesion": "default", "col_prob": "prob_est_default", "nombre_vis": "Lentes"}
            ]
            
            for cuenta in cuentas_estados:
                print(f"📡 Evaluando Estados para cuenta: '{cuenta['nombre_vis']}'...")
                with engine.connect() as conn:
                    prod_est = buscar_producto_dinamico(conn, cuenta['col_prob'])
                    
                if prod_est:
                    respuestas_ia = generar_texto_producto_ia(prod_est, es_estado=True)
                    texto_wsp = respuestas_ia.get('estado_whatsapp', '')
                    
                    exito, _ = subir_estado_whatsapp(cuenta['sesion'], texto_wsp, prod_est.get('url_imagen', ''))
                    if exito:
                        with engine.begin() as conn_est:
                            conn_est.execute(text("""
                                INSERT INTO Historial_Estados (sku, session_name, fecha_publicacion) 
                                VALUES (:sku, :sess, NOW())
                            """), {"sku": prod_est['sku'], "sess": cuenta['sesion']})
                else:
                    print(f"  ⚠️ Omitido: No hay stock o todas las probabilidades están en 0% para {cuenta['nombre_vis']}.")

            # Registrar marca de tiempo global del ciclo evaluado de estados
            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_estados = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📘 TAREA 3: POSTS DE FACEBOOK MULTI-PÁGINA (Vía Make.com)
        # ==================================================================
        minutos_req_fb = mapear_intervalo_a_minutos(config.intervalo_fb) if hasattr(config, 'intervalo_fb') else 360
        min_pasados_fb = tiempos.min_pasados_fb if tiempos and hasattr(tiempos, 'min_pasados_fb') else 9999
        toca_ejecutar_fb = es_modo_test or (min_pasados_fb >= (minutos_req_fb - 2))

        if not getattr(config, 'fb_activo', False):
            print("\n⏸️ TAREA 3 OMITIDA: Auto-Publicación Facebook está apagada en el Panel.")
        elif not toca_ejecutar_fb:
            print(f"\n⏳ [TAREA 3 OMITIDA]: Frecuencia de FB configurada '{getattr(config, 'intervalo_fb', 'cada 6 horas')}'. (Han pasado {int(min_pasados_fb)} min).")
        
        # 👇 ESTA ES LA LÍNEA NUEVA QUE BLOQUEA PUBLICACIONES DE MADRUGADA 👇
        elif not dentro_de_horario:
            print(f"\n⏰ TAREA 3 OMITIDA: Fuera de horario comercial para Facebook ({config.hora_inicio} - {config.hora_fin}).")
        
        else:
            print("\n▶️ INICIANDO TAREA 3: Publicaciones Automáticas Multi-Página de Facebook")
            
            paginas_fb = [
                {"nombre": "General", "col_prob": "prob_fb_general", "webhook": getattr(config, 'webhook_fb_general', '')},
                {"nombre": "General", "col_prob": "prob_fb_general", "webhook": getattr(config, 'webhook_fb_general', '')},
                {"nombre": "Pelucas", "col_prob": "prob_fb_pelucas", "webhook": getattr(config, 'webhook_fb_pelucas', '')},
                {"nombre": "Lentes", "col_prob": "prob_fb_lentes", "webhook": getattr(config, 'webhook_fb_lentes', '')}
            ]

            disparo_realizado = False

            for pagina in paginas_fb:
                print(f"📘 Evaluando Fanpage: '{pagina['nombre']}'...")
                
                if not pagina["webhook"]:
                    print(f"  ⚠️ Omitido: No hay URL de Webhook guardada para {pagina['nombre']}.")
                    continue
                
                with engine.connect() as conn:
                    prod_fb = buscar_producto_dinamico(conn, pagina['col_prob'])
                    
                if prod_fb:
                    print(f"  🧠 Redactando copy (IA) para postear {prod_fb.get('nombre', '')} en {pagina['nombre']}...")
                    respuestas_ia = generar_texto_producto_ia(prod_fb, es_estado=True)
                    texto_fb = respuestas_ia.get('post_facebook', respuestas_ia.get('estado_whatsapp', ''))
                    
                    exito_fb, mensaje_fb = publicar_en_facebook_via_webhook(texto_fb, prod_fb.get('url_imagen', ''), pagina["webhook"])
                    
                    if exito_fb:
                        print(f"  ✅ ¡Post inyectado exitosamente en Make.com ({pagina['nombre']})!")
                        disparo_realizado = True
                        
                        # Registrar en el historial de Facebook para el contador del reporte
                        with engine.begin() as conn_hist:
                            conn_hist.execute(text("""
                                INSERT INTO Historial_Facebook (pagina, sku) 
                                VALUES (:pag, :sku)
                            """), {"pag": pagina['nombre'], "sku": prod_fb.get('sku', '')})
                    else:
                        print(f"  ❌ Fallo el envío a Make.com: {mensaje_fb}")
                else:
                    print(f"  ⚠️ Omitido: Cero stock o probabilidad 0% para todas las categorías en FB {pagina['nombre']}.")

            if disparo_realizado:
                with engine.begin() as conn_up:
                    conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_fb = NOW() WHERE id = :id"), {"id": config.id})

    except Exception as e:
        print(f"🔥 Error catastrófico en la ejecución de marketing: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()