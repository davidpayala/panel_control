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
# 🗄️ INICIALIZADOR DEL SISTEMA DE LOGS SQL
# ==============================================================================
def log_mkt(mensaje):
    """Guarda el log en la Base de Datos y lo imprime en la terminal SSH"""
    # Limpiamos los saltos de línea iniciales para que la base de datos quede ordenada
    mensaje_limpio = str(mensaje).lstrip('\n')
    print(mensaje_limpio, flush=True) 
    try:
        with engine.begin() as conn:
            # Insertamos con el reloj de Perú
            conn.execute(text("INSERT INTO logs_marketing (fecha, mensaje) VALUES (NOW() - INTERVAL '5 hours', :msg)"), {"msg": mensaje_limpio})
    except Exception:
        pass


# ==============================================================================
# 🧠 MOTOR BLINDADO DE SELECCIÓN DE PRODUCTOS
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
        # Inyectamos contexto fuerte para la IA
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

        # --- LÓGICA DE PROBABILIDAD (Ciclo Base = 30 min) ---
        minutos_base = 30
        
        def obtener_probabilidad(texto):
            try:
                if str(texto).isdigit(): return int(texto)
                else: return 100
            except:
                return 100 

        prob_msg = obtener_probabilidad(config.intervalo_mensajes)
        prob_est = obtener_probabilidad(config.intervalo_estados)
        prob_fb  = obtener_probabilidad(getattr(config, 'intervalo_fb', '100'))

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

        min_pasados_msg = tiempos.min_pasados_msg if tiempos else 9999
        min_pasados_est = tiempos.min_pasados_est if tiempos else 9999
        min_pasados_fb  = tiempos.min_pasados_fb if tiempos and hasattr(tiempos, 'min_pasados_fb') else 9999

        hora_peru = datetime.now(timezone.utc) - timedelta(hours=5)
        ahora = hora_peru.time()
        dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

        tiempo_ok_msg = es_modo_test or (min_pasados_msg >= 10)
        tiempo_ok_est = es_modo_test or (min_pasados_est >= 10)
        tiempo_ok_fb  = es_modo_test or (min_pasados_fb >= 10)

        # ==================================================================
        # 🎯 TAREA 1: MENSAJES DIRECTOS
        # ==================================================================
        if not config.bot_activo:
            log_mkt("⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado.")
        elif not tiempo_ok_msg:
            log_mkt(f"⏳ TAREA 1: Aún no pasan los 30 min base (Han pasado {int(min_pasados_msg)} min).")
        elif not dentro_de_horario:
            log_mkt("⏰ TAREA 1 OMITIDA: Fuera de horario comercial.")
        else:
            dado_msg = random.randint(1, 100)
            if dado_msg <= prob_msg or es_modo_test:
                log_mkt(f"▶️ INICIANDO TAREA 1 (Dado: {dado_msg} <= {prob_msg}%)")
                obreros = [
                    {"sesion": "principal", "col_prob": "prob_msg_principal", "nombre_vis": "Principal"},
                    {"sesion": "default", "col_prob": "prob_msg_default", "nombre_vis": "Lentes"}
                ]
                
                disparo_general_exitoso = False
                for obrero in obreros:
                    with engine.connect() as conn:
                        query_conteo = text("SELECT COUNT(*) FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND COALESCE(session_name, 'default') = :sess AND fecha::date = (NOW() - INTERVAL '5 hours')::date")
                        enviados_por_mi = conn.execute(query_conteo, {"sess": obrero["sesion"]}).scalar() or 0

                        if enviados_por_mi >= config.max_mensajes_dia:
                            continue

                        prod_elegido = buscar_producto_dinamico(conn, obrero['col_prob'])
                        if not prod_elegido: continue

                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, c.etiquetas, t.telefono 
                            FROM clientes c
                            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                            WHERE c.activo = TRUE AND c.estado = 'Sin empezar' AND COALESCE(c.excluir_publicidad, FALSE) = FALSE 
                              AND t.activo = TRUE AND t.es_principal = TRUE AND length(t.telefono) > 6
                              AND t.telefono NOT IN (SELECT telefono FROM mensajes WHERE tipo = 'SALIENTE_BOT' AND fecha > NOW() - INTERVAL '60 days')
                            ORDER BY RANDOM()
                            LIMIT 50
                        """)
                        clientes_validos = conn.execute(query_clientes).fetchall()

                    if not clientes_validos: continue
                    prospectos = list(clientes_validos)

                    for cliente in prospectos[:5]:
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
                                    conn_save.execute(text("INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name) VALUES (:idc, :t, 'SALIENTE_BOT', :c, NOW() - INTERVAL '5 hours', TRUE, :sess)"), 
                                                      {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion']})
                                log_mkt(f"✅ Disparo a {telefono_final} ({obrero['nombre_vis']})!")
                                disparo_general_exitoso = True
                                break 
                        else:
                            log_mkt(f"⚠️ El número {telefono_final} no tiene WhatsApp. Purgando del embudo para siempre...")
                            with engine.begin() as conn_purge:
                                conn_purge.execute(text("UPDATE clientes SET excluir_publicidad = TRUE WHERE id_cliente = :idc"), {"idc": cliente.id_cliente})
            else:
                log_mkt(f"🎲 TAREA 1 SALTADA: El dado cayó en {dado_msg} (Requerido: <= {prob_msg}%).")

            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_mensajes = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📱 TAREA 2: ESTADOS
        # ==================================================================
        if not tiempo_ok_est:
            pass
        elif not dentro_de_horario:
            pass
        else:
            dado_est = random.randint(1, 100)
            if dado_est <= prob_est or es_modo_test:
                log_mkt(f"▶️ INICIANDO TAREA 2 (Dado: {dado_est} <= {prob_est}%)")
                cuentas_estados = [
                    {"sesion": "principal", "col_prob": "prob_est_principal"},
                    {"sesion": "default", "col_prob": "prob_est_default"}
                ]
                for cuenta in cuentas_estados:
                    with engine.connect() as conn:
                        prod_est = buscar_producto_dinamico(conn, cuenta['col_prob'])
                        
                    if prod_est:
                        respuestas_ia = generar_texto_producto_ia(prod_est, es_estado=True)
                        texto_estado = respuestas_ia.get('estado_whatsapp', '')
                        
                        log_mkt(f" 📡 Enviando estado a WAHA ({cuenta['sesion']})...")
                        exito, msg_api = subir_estado_whatsapp(cuenta['sesion'], texto_estado, prod_est.get('url_imagen', ''))
                        
                        if exito or ("error" not in str(msg_api).lower() and "fail" not in str(msg_api).lower()):
                            log_mkt(f" ✅ ¡Estado publicado y registrado en la BD ({cuenta['sesion']})!")
                            with engine.begin() as conn_est:
                                conn_est.execute(text("INSERT INTO Historial_Estados (sku, session_name, fecha_publicacion) VALUES (:sku, :sess, NOW())"), {"sku": prod_est['sku'], "sess": cuenta['sesion']})
                        else:
                            log_mkt(f" ❌ Fallo real en la subida a WAHA: {msg_api}")
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
                disparo_fb = False
                
                for pagina in paginas_fb:
                    if not pagina["webhook"] or str(pagina["webhook"]).strip() == "":
                        log_mkt(f" ⚠️ Omitido: No hay URL de Webhook guardada para la página '{pagina['nombre']}'.")
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
                            disparo_fb = True
                        else:
                            log_mkt(f" ❌ Make.com rechazó el envío para {pagina['nombre']}. Razón: {mensaje_fb}")
                    else:
                        log_mkt(f" ⚠️ Omitido: Cero stock o probabilidad 0% para categorías en FB {pagina['nombre']}.")
            else:
                log_mkt(f"🎲 TAREA 3 SALTADA: El dado cayó en {dado_fb} (Requerido: <= {prob_fb}%).")
            
            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_fb = NOW() WHERE id = :id"), {"id": config.id})

    except Exception as e:
        log_mkt(f"🔥 Error catastrófico: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()