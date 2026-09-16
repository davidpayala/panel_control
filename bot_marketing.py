import os
import sys
from dotenv import load_dotenv

# 1. Inyectar variables de entorno
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

# 2. Módulos de infraestructura
import requests
import random
import time
import subprocess # 🛠️ NUEVO: Importado para reiniciar el contenedor WAHA
from datetime import datetime
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
# 🗄️ INICIALIZADOR DEL SISTEMA DE LOGS SQL
# ==============================================================================
def log_mkt(mensaje):
    """Guarda el log en la Base de Datos e imprime en SSH"""
    mensaje_limpio = str(mensaje).lstrip('\n')
    print(mensaje_limpio, flush=True) 
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO logs_marketing (fecha, mensaje) VALUES (NOW(), :msg)"), 
                {"msg": mensaje_limpio}
            )
    except Exception as e:
        try:
            with engine.begin() as conn_fix:
                conn_fix.execute(text("""
                    CREATE TABLE IF NOT EXISTS logs_marketing (
                        id SERIAL PRIMARY KEY,
                        fecha TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                        mensaje TEXT
                    );
                    GRANT ALL PRIVILEGES ON TABLE logs_marketing TO PUBLIC;
                    GRANT ALL PRIVILEGES ON SEQUENCE logs_marketing_id_seq TO PUBLIC;
                """))
                conn_fix.execute(
                    text("INSERT INTO logs_marketing (fecha, mensaje) VALUES (NOW(), :msg)"), 
                    {"msg": mensaje_limpio}
                )
        except Exception as e2:
            print(f"Error crítico escribiendo log en BD: {e2}", flush=True)

# ==============================================================================
# 🧠 MOTOR DE SELECCIÓN DE PRODUCTOS
# ==============================================================================
def buscar_producto_dinamico(conn, col_probabilidad):
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
        producto_dict = dict(prod._mapping)
        producto_dict['contexto_ia_extra'] = f"REGLA DE ORO: ESTE PRODUCTO ES UN/UNA {producto_dict.get('macro_categoria', '').upper()}. HABLA ESTRICTAMENTE DE ESA CATEGORÍA."
        return producto_dict
    return None

# ==============================================================================
# 🚀 MOTOR ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_francotirador():
    log_mkt("🤖 Despertando Motor de Marketing Multi-Terminal...")
    es_modo_test = "--test" in sys.argv or "--now" in sys.argv

    try:
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        if not config:
            log_mkt("🛑 No hay configuración registrada en la base de datos.")
            return

        def obtener_probabilidad(texto):
            try:
                if str(texto).isdigit(): return int(texto)
                else: return 100
            except:
                return 100 

        prob_msg = obtener_probabilidad(config.intervalo_mensajes)
        prob_est = obtener_probabilidad(config.intervalo_estados)
        prob_fb  = obtener_probabilidad(getattr(config, 'intervalo_fb', '100'))
        
        # 🛡️ NUEVO: Límite de mensajes "fríos" diarios configurado en el panel (Por defecto 10 al día = 300 al mes)
        max_mensajes_nuevos_dia = getattr(config, 'max_mensajes_nuevos_dia', 10) 

        if not es_modo_test:
            retraso_minutos = random.randint(1, 25) 
            log_mkt(f"⏳ Esperando {retraso_minutos} minutos (Retraso orgánico)...")
            time.sleep(retraso_minutos * 60)

        with engine.connect() as conn:
            query_tiempo = text("""
            SELECT 
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_mensajes, NOW() - INTERVAL '1 day')))/60 AS min_pasados_msg,
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_estados, NOW() - INTERVAL '1 day')))/60 AS min_pasados_est,
                EXTRACT(EPOCH FROM (NOW() - COALESCE(ultimo_envio_fb, NOW() - INTERVAL '1 day')))/60 AS min_pasados_fb
            FROM Configuracion_Campanas LIMIT 1
            """)
            tiempos = conn.execute(query_tiempo).fetchone()
            
            # Contabilizar los mensajes "nuevos" enviados hoy para no rebasar el límite de Meta
            query_conteo_nuevos = text("""
                SELECT COUNT(DISTINCT m.telefono)
                FROM mensajes m
                WHERE m.tipo = 'SALIENTE_BOT'
                  AND m.fecha::date = CURRENT_DATE
                  AND NOT EXISTS (
                      SELECT 1 FROM mensajes me
                      WHERE me.telefono = m.telefono AND me.tipo = 'ENTRANTE' AND me.fecha < m.fecha
                  )
            """)
            enviados_nuevos_hoy = conn.execute(query_conteo_nuevos).scalar() or 0

        # 🛡️ SEGURO ANTI-TIEMPO NEGATIVO
        min_pasados_msg = tiempos.min_pasados_msg if tiempos and tiempos.min_pasados_msg >= 0 else 9999
        min_pasados_est = tiempos.min_pasados_est if tiempos and tiempos.min_pasados_est >= 0 else 9999
        min_pasados_fb  = tiempos.min_pasados_fb if tiempos and hasattr(tiempos, 'min_pasados_fb') and tiempos.min_pasados_fb >= 0 else 9999

        ahora = datetime.now().time()
        dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

        tiempo_ok_msg = es_modo_test or (min_pasados_msg >= 10)
        tiempo_ok_est = es_modo_test or (min_pasados_est >= 10)
        tiempo_ok_fb  = es_modo_test or (min_pasados_fb >= 10)

        # 🛡️ MEDIDA DE SEGURIDAD Y AUTO-RECUPERACIÓN - VERIFICAR WAHA
        waha_url = os.getenv("WAHA_URL", "http://localhost:3000")
        waha_key = os.getenv("WAHA_KEY", "")
        waha_ok = False
        
        def comprobar_estado_waha():
            try:
                headers = {"Accept": "application/json"}
                if waha_key:
                    headers["X-Api-Key"] = waha_key
                res = requests.get(f"{waha_url}/api/sessions?all=true", headers=headers, timeout=10)
                
                if res.status_code == 200:
                    sesiones = res.json()
                    sesiones_activas = {s.get('name'): s.get('status') for s in sesiones}
                    if sesiones_activas.get('default') == 'WORKING' and sesiones_activas.get('principal') == 'WORKING':
                        return True, "WORKING"
                    return False, f"Las sesiones no están óptimas: {sesiones_activas}"
                return False, f"WAHA respondió con error HTTP {res.status_code}"
            except Exception as e:
                return False, f"Error de red/timeout: {e}"

        log_mkt("🔍 Verificando salud de WAHA y sesiones (default, principal)...")
        waha_ok, detalle_estado = comprobar_estado_waha()

        if waha_ok:
            log_mkt("✅ WAHA operativo. Sesiones 'default' y 'principal' en línea.")
        else:
            log_mkt(f"⚠️ Alerta: {detalle_estado}. Iniciando protocolo de auto-recuperación...")
            try:
                # 2.1 Reiniciar el WAHA
                log_mkt("♻️ Aplicando reinicio al contenedor WAHA...")
                subprocess.run(["docker", "restart", "waha"], capture_output=True, text=True, timeout=30)
                
                # 2.2 Esperar 1 minuto
                log_mkt("⏳ Esperando 60 segundos para que WAHA vuelva a levantar...")
                time.sleep(60)
                
                # 2.3 Revisar de nuevo si funciona
                log_mkt("🔍 Re-evaluando salud de WAHA tras el reinicio...")
                waha_ok, detalle_estado = comprobar_estado_waha()
                
                # 2.4 Tomar decisión final
                if waha_ok:
                    log_mkt("✅ WAHA se recuperó exitosamente. Continuando con las tareas.")
                else:
                    log_mkt(f"❌ WAHA sigue fallando tras el reinicio ({detalle_estado}). Saltando Tareas 1 y 2.")
            except Exception as e:
                log_mkt(f"🔥 Error crítico al intentar reiniciar WAHA: {e}")
                waha_ok = False

        # ==================================================================
        # 🎯 TAREA 1: MENSAJES DIRECTOS CON RESTRICCIÓN DE CONTACTOS FRÍOS
        # ==================================================================
        if not waha_ok:
            log_mkt("⏸️ TAREA 1 OMITIDA: Bloqueo de seguridad activado (WAHA inestable o desconectado).")
        elif not config.bot_activo:
            log_mkt("⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado.")
        elif not tiempo_ok_msg:
            log_mkt(f"⏳ TAREA 1: Aún no pasan los 30 min base (Han pasado {int(min_pasados_msg)} min).")
        elif not dentro_de_horario:
            log_mkt(f"⏰ TAREA 1 OMITIDA: Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
        else:
            log_mkt("▶️ INICIANDO TAREA 1: Evaluando líneas de envío independientes...")
            
            # 🛠️ CORRECCIÓN 1: Extraemos límites totales y probabilidades de forma individual por línea
            obreros = [
                {
                    "sesion": "principal", 
                    "col_prob": "prob_msg_principal", 
                    "nombre_vis": "Principal", 
                    "limite_nuevos": getattr(config, 'max_nuevos_principal', getattr(config, 'max_mensajes_nuevos_dia', 10)),
                    "limite_total": getattr(config, 'max_mensajes_principal', config.max_mensajes_dia),
                    "probabilidad": obtener_probabilidad(getattr(config, 'intervalo_mensajes_principal', config.intervalo_mensajes))
                },
                {
                    "sesion": "default", 
                    "col_prob": "prob_msg_default", 
                    "nombre_vis": "Lentes", 
                    "limite_nuevos": getattr(config, 'max_nuevos_default', getattr(config, 'max_mensajes_nuevos_dia', 10)),
                    "limite_total": getattr(config, 'max_mensajes_default', config.max_mensajes_dia),
                    "probabilidad": obtener_probabilidad(getattr(config, 'intervalo_mensajes_default', config.intervalo_mensajes))
                }
            ]
            
            for obrero in obreros:
                limite_nuevos_sesion = obrero["limite_nuevos"]
                limite_total_sesion = obrero["limite_total"]
                prob_sesion = obrero["probabilidad"]
                
                # 🎲 CORRECCIÓN 2: Dado independiente para cada celular
                dado_msg = random.randint(1, 100)
                if dado_msg > prob_sesion and not es_modo_test:
                    log_mkt(f"🎲 [{obrero['nombre_vis']}] SALTADO: El dado cayó en {dado_msg} (Requerido: <= {prob_sesion}%).")
                    continue
                    
                log_mkt(f"🎯 [{obrero['nombre_vis']}] APROBADO: El dado cayó en {dado_msg} (Requerido: <= {prob_sesion}%).")
                
                with engine.connect() as conn:
                    # Total enviados hoy filtrando de forma estricta por el session_name exacto
                    query_conteo = text("""
                        SELECT COUNT(DISTINCT telefono) FROM mensajes 
                        WHERE tipo = 'SALIENTE_BOT' 
                        AND TRIM(COALESCE(session_name, 'default')) = TRIM(:sess) 
                        AND fecha >= CURRENT_DATE
                    """)
                    enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar() or 0

                    if enviados_por_mi >= limite_total_sesion:
                        log_mkt(f"🚫 [{obrero['nombre_vis']}] Límite total diario alcanzado ({enviados_por_mi}/{limite_total_sesion}).")
                        continue

                    # Nuevos contactos impactados hoy por esta sesión específica
                    query_conteo_nuevos = text("""
                        SELECT COUNT(DISTINCT m.telefono)
                        FROM mensajes m
                        WHERE m.tipo = 'SALIENTE_BOT'
                        AND TRIM(COALESCE(m.session_name, 'default')) = TRIM(:sess)
                        AND m.fecha >= CURRENT_DATE
                        AND NOT EXISTS (
                            SELECT 1 FROM mensajes me
                            WHERE me.telefono = m.telefono AND me.tipo = 'ENTRANTE' AND me.fecha < m.fecha
                        )
                    """)
                    enviados_nuevos_mi_sesion = conn.execute(query_conteo_nuevos, {"sess": obrero["sesion"]}).scalar() or 0

                    # 🛠️ CORRECCIÓN 3: Despliegue visual mostrando el límite total individual en el log
                    log_mkt(f"📊 [{obrero['nombre_vis']}] Fríos hoy: {enviados_nuevos_mi_sesion}/{limite_nuevos_sesion} | Totales hoy: {enviados_por_mi}/{limite_total_sesion}")

                    prod_elegido = buscar_producto_dinamico(conn, obrero['col_prob'])
                    if not prod_elegido: continue

                    # 🧠 Lógica avanzada para detectar clientes calentados vs fríos
                    query_clientes = text("""
                        WITH Prospectos_Random AS (
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, c.etiquetas, t.telefono 
                            FROM clientes c
                            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                            WHERE c.activo = TRUE AND c.estado = 'Sin empezar' AND COALESCE(c.excluir_publicidad, FALSE) = FALSE 
                              AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6
                              AND t.telefono NOT IN (SELECT telefono FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND fecha > NOW() - INTERVAL '60 days')
                            ORDER BY RANDOM()
                            LIMIT 50
                        )
                        SELECT 
                            pr.*,
                            (SELECT COUNT(*) FROM mensajes m2 WHERE m2.telefono = pr.telefono AND m2.tipo = 'ENTRANTE') as total_entrantes,
                            (SELECT session_name FROM mensajes m3 WHERE m3.telefono = pr.telefono AND m3.tipo = 'ENTRANTE' ORDER BY fecha DESC LIMIT 1) as ultima_sesion_entrante
                        FROM Prospectos_Random pr
                    """)
                    clientes_validos = conn.execute(query_clientes).fetchall()

                if not clientes_validos: continue
                prospectos = list(clientes_validos)

                for cliente in prospectos:
                    # -----------------------------------------------------------
                    # 🛡️ FILTRO DE RESTRICCIÓN META (WHATSAPP)
                    # -----------------------------------------------------------
                    es_cliente_frio = (cliente.total_entrantes == 0)

                    if not es_cliente_frio:
                        # Rescate de clientes antiguos asumiendo 'default' si la columna era NULL
                        ultima_ses = cliente.ultima_sesion_entrante or 'default'
                        if ultima_ses != obrero['sesion']:
                            continue
                    else:
                        # Validación independiente por cuenta
                        if enviados_nuevos_mi_sesion >= limite_nuevos_sesion:
                            continue
                    # -----------------------------------------------------------

                    norm = normalizar_telefono_maestro(cliente.telefono)
                    if not norm: continue
                    telefono_final = norm['db']
                    
                    if verificar_numero_waha(telefono_final) is True:
                        saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                        nom_ia = cliente.nombre_ia.strip() if cliente.nombre_ia else ""
                        cabecera = f"{saludo} {nom_ia} 👋" if nom_ia else "¡Hola! 👋"

                        cuerpo_ia = generar_texto_producto_ia(prod_elegido, es_estado=False, cliente_info={"etiquetas": cliente.etiquetas or ""})
                        mensaje_completo = f"{cabecera}\n\n{cuerpo_ia}"

                        if enviar_mensaje_whatsapp(telefono_final, mensaje_completo, prod_elegido['url_imagen'], session=obrero['sesion']):
                            with engine.begin() as conn_save:
                                conn_save.execute(text("""
                                    INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) 
                                    VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW(), TRUE, :sess)
                                """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion']})
                            
                            log_mkt(f"✅ Disparo a {telefono_final} ({obrero['nombre_vis']}). Frío: {'Sí' if es_cliente_frio else 'No'}.")
                            break  # Disparo exitoso: pasa a evaluar al siguiente obrero
                    else:
                        log_mkt(f"🚫 {telefono_final} NO tiene WhatsApp. Bloqueando contacto y excluyendo de publicidad...")
                        with engine.begin() as conn_purge:
                            conn_purge.execute(text("""
                                UPDATE clientes 
                                SET excluir_publicidad = TRUE, activo = FALSE, estado = 'Sin WhatsApp' 
                                WHERE id_cliente = :idc
                            """), {"idc": cliente.id_cliente})
                            conn_purge.execute(text("""
                                UPDATE telefonoscliente 
                                SET activo = FALSE 
                                WHERE id_cliente = :idc AND telefono = :t
                            """), {"idc": cliente.id_cliente, "t": cliente.telefono})

            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_mensajes = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📱 TAREA 2: ESTADOS CON TRAZABILIDAD EXTREMA
        # ==================================================================
        if not waha_ok:
            log_mkt("⏸️ TAREA 2 OMITIDA: Bloqueo de seguridad activado (WAHA inestable o desconectado).")
        elif not tiempo_ok_est:
            log_mkt(f"⏳ TAREA 2 OMITIDA: Aún no pasan los 30 min base (Han pasado {int(min_pasados_est)} min).")
        elif not dentro_de_horario: # 🛠️ NUEVO: Filtro para respetar el horario laboral en estados
            log_mkt(f"⏰ TAREA 2 OMITIDA: Fuera de horario comercial ({config.hora_inicio} - {config.hora_fin}).")
        else:
            dado_est = random.randint(1, 100)
            if dado_est <= prob_est or es_modo_test:
                log_mkt(f"▶️ INICIANDO TAREA 2 (Dado: {dado_est} <= {prob_est}%)")
                cuentas_estados = [
                    {"sesion": "principal", "col_prob": "prob_est_principal"},
                    {"sesion": "default", "col_prob": "prob_est_default"}
                ]
                for cuenta in cuentas_estados:
                    log_mkt(f" 🔍 [TRACE] Evaluando sesión '{cuenta['sesion']}'...")
                    with engine.connect() as conn:
                        prod_est = buscar_producto_dinamico(conn, cuenta['col_prob'])
                        
                    if prod_est:
                        log_mkt(f" 🔍 [TRACE] Producto seleccionado: {prod_est.get('nombre')} (SKU: {prod_est.get('sku')})")
                        
                        respuestas_ia = generar_texto_producto_ia(prod_est, es_estado=True)
                        texto_estado = respuestas_ia.get('estado_whatsapp', '') 
                        
                        log_mkt(f" 📡 [TRACE] Enviando estado a WAHA ({cuenta['sesion']}). URL Imagen: {prod_est.get('url_imagen')}")
                        exito, msg_api = subir_estado_whatsapp(cuenta['sesion'], texto_estado, prod_est.get('url_imagen', ''))
                        
                        log_mkt(f" 🔍 [TRACE] Respuesta WAHA cruda: {msg_api}")
                        
                        if exito or ("error" not in str(msg_api).lower() and "fail" not in str(msg_api).lower()):
                            log_mkt(f" ✅ ¡Estado publicado y registrado en la BD ({cuenta['sesion']})!")
                            with engine.begin() as conn_est:
                                conn_est.execute(text("INSERT INTO Historial_Estados (sku, session_name, fecha_publicacion) VALUES (:sku, :sess, NOW())"), {"sku": prod_est['sku'], "sess": cuenta['sesion']})
                        else:
                            log_mkt(f" ❌ Fallo real en la subida a WAHA ({cuenta['sesion']}): {msg_api}")
                    else:
                        log_mkt(f" ⚠️ [TRACE] No se encontró producto para '{cuenta['sesion']}' (Stock 0 o probabilidad 0%).")
            else:
                log_mkt(f"🎲 TAREA 2 SALTADA: El dado cayó en {dado_est} (Requerido: <= {prob_est}%).")

            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_estados = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📘 TAREA 3: FACEBOOK
        # ==================================================================
        if not getattr(config, 'fb_activo', False):
            log_mkt("⏸️ TAREA 3 OMITIDA: Auto-Publicación Facebook está apagada en el Panel.")
        elif not tiempo_ok_fb:
            log_mkt(f"⏳ TAREA 3: Aún no pasan los 30 min base (Han pasado {int(min_pasados_fb)} min).")
        elif not dentro_de_horario:
            log_mkt("⏰ TAREA 3 OMITIDA: Fuera de horario comercial para Facebook.")
        else:
            dado_fb = random.randint(1, 100)
            if dado_fb <= prob_fb or es_modo_test:
                log_mkt(f"▶️ INICIANDO TAREA 3 (Dado: {dado_fb} <= {prob_fb}%)")
                paginas_fb = [
                    {"nombre": "General", "col_prob": "prob_fb_general", "webhook": getattr(config, 'webhook_fb_general', '')},
                    {"nombre": "Pelucas", "col_prob": "prob_fb_pelucas", "webhook": getattr(config, 'webhook_fb_pelucas', '')},
                    {"nombre": "Lentes", "col_prob": "prob_fb_lentes", "webhook": getattr(config, 'webhook_fb_lentes', '')}
                ]
                
                for pagina in paginas_fb:
                    if not pagina["webhook"] or str(pagina["webhook"]).strip() == "":
                        continue
                    
                    with engine.connect() as conn:
                        prod_fb = buscar_producto_dinamico(conn, pagina['col_prob'])
                        
                    if prod_fb:
                        log_mkt(f" 🧠 Redactando copy (IA) para postear {prod_fb.get('nombre', '')} en {pagina['nombre']}...")
                        respuestas_ia = generar_texto_producto_ia(prod_fb, es_estado=True)
                        texto_fb = respuestas_ia.get('post_facebook', '')
                        
                        exito_fb, mensaje_fb = publicar_en_facebook_via_webhook(texto_fb, prod_fb.get('url_imagen', ''), pagina["webhook"])
                        
                        if exito_fb:
                            log_mkt(f" ✅ ¡Post inyectado exitosamente en Make.com ({pagina['nombre']})!")
                            with engine.begin() as conn_hist:
                                conn_hist.execute(text("INSERT INTO Historial_Facebook (pagina, sku) VALUES (:pag, :sku)"), {"pag": pagina['nombre'], "sku": prod_fb.get('sku', '')})
                        else:
                            log_mkt(f" ❌ Make.com rechazó el envío para {pagina['nombre']}: {mensaje_fb}")
            else:
                log_mkt(f"🎲 TAREA 3 SALTADA: El dado cayó en {dado_fb} (Requerido: <= {prob_fb}%).")
            
            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_fb = NOW() WHERE id = :id"), {"id": config.id})

    except Exception as e:
        log_mkt(f"🔥 Error catastrófico: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()