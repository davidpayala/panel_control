import os
import sys
from dotenv import load_dotenv

# 1. Este archivo vive en recurrente/: la raíz del proyecto (database.py, utils.py, .env) es la carpeta superior.
RAIZ_PROYECTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ_PROYECTO not in sys.path:
    sys.path.insert(0, RAIZ_PROYECTO)
os.chdir(RAIZ_PROYECTO)  # rutas relativas (token.json, token_google...) igual que si corriera desde la raíz

# 2. Inyectar variables de entorno
ruta_env = os.path.join(RAIZ_PROYECTO, '.env')
load_dotenv(ruta_env)

# 3. Módulos de infraestructura
import requests
import random
import time
import subprocess
from datetime import datetime
from sqlalchemy import text
from database import engine

from utils import (
    normalizar_telefono_maestro, 
    verificar_numero_waha, 
    enviar_mensaje_whatsapp, 
    subir_estado_whatsapp,
    generar_texto_producto_ia,
    publicar_en_facebook_via_webhook,
    obtener_imagen_jpg_b64,
    es_lid,
)
from marketing_grupos import (
    asegurar_esquema,
    leer_limites,
    enviados_hoy_por_grupo,
    origen_grupo,
    origen_zombie,
    dias_zombie,
    nivel_zombie_por_dias,
    ZOMBIE_DIA_SALIDA,
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
    except Exception:
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
# 👥 GRUPOS DE MARKETING (los calcula agente_semanal.py cada domingo en la tabla bot_grupos)
#   1 Recompra | 2 Escribió sin comprar | 3 Cliente antiguo | 4 Contacto frío
#   Cada grupo tiene su espera mínima (columna espera_dias) y su instrucción de texto para la IA.
#   El filtro estado = 'Sin empezar' se mantiene: los grupos son un filtro ADICIONAL.
# ==============================================================================
SQL_PROSPECTOS = """
    WITH Candidatos AS (
        -- destino = teléfono; si el número está oculto, su LID (así se guardan sus mensajes en la tabla mensajes)
        SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, c.etiquetas, COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid) AS telefono,
               bg.grupo, bg.instruccion_ia, bg.escribio_real, bg.actualizado, bg.prioridad
        FROM clientes c
        JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
        JOIN bot_grupos bg ON bg.id_cliente = c.id_cliente
        WHERE c.activo = TRUE
          AND c.estado = 'Sin empezar'
          AND COALESCE(c.excluir_publicidad, FALSE) = FALSE
          AND COALESCE(c.nivel_zombie, 0) = 0
          AND t.activo = TRUE
          AND t.es_principal = TRUE
          AND length(COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid)) > 6
          -- solo grupos activos que aún tienen cupo hoy en ESTA cuenta (límites del panel)
          AND bg.grupo = ANY(:grupos)
          -- espera propia de cada grupo desde el último mensaje del bot
          AND NOT EXISTS (
              SELECT 1 FROM mensajes m_sal
              WHERE m_sal.telefono = COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid)
                AND m_sal.tipo = 'SALIENTE_BOT'
                AND m_sal.fecha > NOW() - (bg.espera_dias * INTERVAL '1 day')
          )
          -- no interrumpir una conversación: nada (entrante o saliente) en los últimos 14 días
          AND NOT EXISTS (
              SELECT 1 FROM mensajes m_rec
              WHERE m_rec.telefono = COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid) AND m_rec.fecha > NOW() - INTERVAL '14 days'
          )
          -- si compró en los últimos 60 días, lo atiende la post-venta, no el marketing
          AND NOT EXISTS (
              SELECT 1 FROM ventas v
              WHERE v.id_cliente = c.id_cliente AND COALESCE(v.anulado, FALSE) = FALSE
                AND v.fecha_venta > NOW() - INTERVAL '60 days'
          )
    ),
    Clasificados AS (
        SELECT ca.*,
               -- «tibio» = escribió un mensaje REAL (las respuestas automáticas de empresas no cuentan;
               -- el agente semanal las descarta) o escribió después de la última actualización de grupos.
               (ca.escribio_real OR EXISTS (
                   SELECT 1 FROM mensajes m2
                   WHERE m2.telefono = ca.telefono AND m2.tipo = 'ENTRANTE' AND m2.fecha > ca.actualizado
               )) AS tibio,
               us.session_name AS ultima_sesion_entrante
        FROM Candidatos ca
        LEFT JOIN LATERAL (
            SELECT m3.session_name FROM mensajes m3
            WHERE m3.telefono = ca.telefono AND m3.tipo = 'ENTRANTE'
            ORDER BY m3.fecha DESC LIMIT 1
        ) us ON TRUE
    )
    SELECT cl.*, CASE WHEN cl.tibio THEN 1 ELSE 0 END AS total_entrantes
    FROM Clasificados cl
    WHERE
        -- Tibio: solo lo contacta la cuenta con la que conversó por última vez (antes se descartaba en Python
        -- DESPUÉS del LIMIT, y por eso la cuenta Lentes recibía lotes llenos de clientes de Principal).
        (cl.tibio AND TRIM(COALESCE(cl.ultima_sesion_entrante, 'default')) = :sess)
        -- Frío: cualquier cuenta (su cupo lo controla el límite de G3/G4 en el panel).
        OR NOT cl.tibio
    -- primero el grupo con menos avance respecto a su límite (reparte el día entre grupos)
    ORDER BY array_position(CAST(:orden_grupos AS int[]), cl.grupo), RANDOM()
    LIMIT 50
"""


def grupos_listos(conn):
    """Devuelve (ok, motivo). Sin grupos recientes el bot NO envía a ciegas."""
    try:
        existe = conn.execute(text("SELECT to_regclass('public.bot_grupos')")).scalar()
        if not existe:
            return False, "No existe la tabla bot_grupos. Ejecuta recurrente/agente_semanal.py una vez."
        r = conn.execute(text("SELECT COUNT(*), MAX(actualizado) FROM bot_grupos")).fetchone()
        if not r[0]:
            return False, "La tabla bot_grupos está vacía. Revisa el último reporte del agente semanal."
        if r[1] is not None and (datetime.now() - r[1]).days > 21:
            return False, f"Los grupos tienen más de 21 días (última actualización {r[1]:%d/%m/%Y}). Revisa el agente semanal."
        return True, ""
    except Exception as e:
        return False, f"No se pudieron leer los grupos: {e}"

# ==============================================================================
# 🧠 MOTOR DE SELECCIÓN DE PRODUCTOS (Optimizado en RAM)
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
    
    # 🛠️ OPTIMIZACIÓN RAM: NOT EXISTS en lugar de NOT IN
    condicion_historial = ""
    if 'est_' in col_probabilidad:
        condicion_historial = """
            AND NOT EXISTS (
                SELECT 1 FROM Historial_Estados he 
                WHERE he.sku = v.sku AND he.fecha_publicacion > NOW() - INTERVAL '14 days'
            )
        """
        
    query_prod = text(f"""
        SELECT 
            p.id_producto, p.marca, p.modelo, p.nombre, p.categoria, p.color_principal,
            p.url_imagen, p.url_tienda, v.sku, v.precio, p.macro_categoria,
            s.descripcion_ia as enfoque_ia
        FROM Variantes v
        JOIN Productos p ON v.id_producto = p.id_producto
        LEFT JOIN Subcategorias_Sistema s ON TRIM(p.categoria) ILIKE TRIM(s.subcategoria)
                                         AND TRIM(p.macro_categoria) ILIKE TRIM(s.macro_categoria)
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
        contexto_base = f"REGLA DE ORO: ESTE PRODUCTO ES UN/UNA {producto_dict.get('macro_categoria', '').upper()}. HABLA ESTRICTAMENTE DE ESA CATEGORÍA."
        
        query_eventos = text("""
            SELECT nombre_evento, descripcion
            FROM Festividades
            WHERE activo = TRUE
              AND (
                  CURRENT_DATE BETWEEN (make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, mes, dia) - (dias_anticipacion * INTERVAL '1 day'))::date 
                                   AND make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, mes, dia)
                  OR
                  CURRENT_DATE BETWEEN (make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, mes, dia) - (dias_anticipacion * INTERVAL '1 day'))::date 
                                   AND make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, mes, dia)
              )
        """)
        
        eventos = conn.execute(query_eventos).fetchall()
        
        if eventos:
            texto_eventos = " | ".join([f"[{e.nombre_evento}: {e.descripcion}]" for e in eventos])
            contexto_base += f" EVENTOS ACTUALES DETECTADOS (Cruza y adapta el mensaje a estos eventos si aplica): {texto_eventos}"
            
        producto_dict['contexto_ia_extra'] = contexto_base
        return producto_dict
        
    return None

# ==============================================================================
# 🚀 MOTOR ORQUESTADOR PRINCIPAL
# ==============================================================================
def ejecutar_francotirador():
    log_mkt("🤖 Despertando Motor de Marketing Multi-Terminal...")
    
    es_modo_test = "--test" in sys.argv or "--now" in sys.argv
    tarea_objetivo = None
    for arg in sys.argv:
        if arg.startswith("--solo="):
            tarea_objetivo = arg.split("=")[1].strip()
            log_mkt(f"🧪 MODO TEST AISLADO: Se ejecutará únicamente la Tarea {tarea_objetivo}")

    try:
        with engine.begin() as conn:
            asegurar_esquema(conn)  # columna mensajes.origen_bot y tabla bot_grupos_limites
        with engine.connect() as conn:
            config = conn.execute(text("SELECT * FROM Configuracion_Campanas LIMIT 1")).fetchone()
        
        if not config:
            log_mkt("🛑 No hay configuración registrada en la base de datos.")
            return

        def obtener_probabilidad(texto):
            try:
                if str(texto).isdigit(): return int(texto)
                else: return 100
            except Exception:
                return 100 

        prob_msg = obtener_probabilidad(config.intervalo_mensajes)
        prob_est = obtener_probabilidad(config.intervalo_estados)
        prob_fb  = obtener_probabilidad(getattr(config, 'intervalo_fb', '100'))
        
        # 🛠️ CORRECCIÓN: Definimos 'obreros' globalmente para que TAREA 1 y TAREA 4 puedan usarla siempre
        obreros = [
            {
                "sesion": "principal", 
                "col_prob": "prob_msg_principal", 
                "nombre_vis": "Principal", 
                "probabilidad": obtener_probabilidad(getattr(config, 'intervalo_mensajes_principal', config.intervalo_mensajes))
            },
            {
                "sesion": "default", 
                "col_prob": "prob_msg_default", 
                "nombre_vis": "Lentes", 
                "probabilidad": obtener_probabilidad(getattr(config, 'intervalo_mensajes_default', config.intervalo_mensajes))
            }
        ]

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

        min_pasados_msg = tiempos.min_pasados_msg if tiempos and tiempos.min_pasados_msg >= 0 else 9999
        min_pasados_est = tiempos.min_pasados_est if tiempos and tiempos.min_pasados_est >= 0 else 9999
        min_pasados_fb  = tiempos.min_pasados_fb if tiempos and hasattr(tiempos, 'min_pasados_fb') and tiempos.min_pasados_fb >= 0 else 9999

        ahora = datetime.now().time()
        dentro_de_horario = (config.hora_inicio <= ahora <= config.hora_fin)

        tiempo_ok_msg = es_modo_test or (min_pasados_msg >= 10)
        tiempo_ok_est = es_modo_test or (min_pasados_est >= 10)
        tiempo_ok_fb  = es_modo_test or (min_pasados_fb >= 10)

        # ==================================================================
        # 🛡️ VERIFICACIÓN DE SALUD WAHA (solo los motores en uso, ver waha_motor.py)
        # ==================================================================
        waha_key = os.getenv("WAHA_KEY", "")
        waha_ok = False
        
        # Solo se vigilan (y reinician) los motores que alguna acción usa (Opciones → Motor WhatsApp).
        # Así un motor desactivado a propósito (ej. WEBJS) no se vuelve a levantar solo.
        from waha_motor import motores_en_uso, url_motor, CONTENEDORES as CONTENEDORES_WAHA
        sesion_referencia = {"WEBJS": "default", "NOWEB": "principal", "GOWS": "principal"}
        instancias_waha = [
            {"puerto": url_motor(m).rsplit(":", 1)[-1], "url": url_motor(m), "contenedor": CONTENEDORES_WAHA[m],
             "nombre_sesion": sesion_referencia[m]}
            for m in motores_en_uso()
        ]
        
        def comprobar_estado_waha(puerto, nombre_sesion):
            url_base = next((i["url"] for i in instancias_waha if i["puerto"] == puerto), f"http://localhost:{puerto}")
            try:
                headers = {"Accept": "application/json"}
                if waha_key:
                    headers["X-Api-Key"] = waha_key
                
                url = f"{url_base}/api/sessions?all=true"
                res = requests.get(url, headers=headers, timeout=10)
                
                if res.status_code == 200:
                    sesiones = res.json()
                    sesiones_activas = {s.get('name'): s.get('status') for s in sesiones if isinstance(s, dict)}
                    
                    # 1. Verifica si la sesión específica está WORKING
                    if sesiones_activas.get(nombre_sesion) == 'WORKING':
                        return True, f"Sesión '{nombre_sesion}' WORKING"
                    # 2. Respaldo: Si en ese puerto las sesiones tienen otro nombre pero están WORKING
                    if 'WORKING' in sesiones_activas.values():
                        return True, f"Operativo ({sesiones_activas})"
                        
                    return False, f"Sesiones no óptimas en puerto {puerto}: {sesiones_activas}"
                
                return False, f"WAHA respondió con error HTTP {res.status_code}"
            except Exception as e:
                return False, f"Error de red/timeout: {e}"

        # Si estamos probando SOLO Facebook (Tarea 3), no necesitamos evaluar ni reiniciar WAHA
        if tarea_objetivo in ['3', 'fb', 'facebook']:
            log_mkt("⏭️ Verificación de WAHA omitida (Modo exclusivo Facebook).")
        else:
            log_mkt(f"🔍 Verificando salud de WAHA en puertos {', '.join(i['puerto'] for i in instancias_waha)}...")
            contenedores_reiniciados = []
            estados_finales = []

            for instancia in instancias_waha:
                puerto = instancia["puerto"]
                contenedor = instancia["contenedor"]
                sesion = instancia["nombre_sesion"]
                
                ok, detalle_estado = comprobar_estado_waha(puerto, sesion)
                
                if ok:
                    log_mkt(f"✅ WAHA operativo en puerto {puerto}: {detalle_estado}.")
                    estados_finales.append(True)
                else:
                    log_mkt(f"⚠️ Alerta en puerto {puerto} ({contenedor}): {detalle_estado}. Iniciando auto-recuperación...")
                    try:
                        log_mkt(f"♻️ Aplicando reinicio al contenedor {contenedor}...")
                        subprocess.run(["docker", "restart", contenedor], capture_output=True, text=True, timeout=30)
                        contenedores_reiniciados.append(instancia)
                    except Exception as e:
                        log_mkt(f"🔥 Error crítico al intentar reiniciar {contenedor}: {e}")
                        estados_finales.append(False)

            if contenedores_reiniciados:
                log_mkt("⏳ Esperando 60 segundos para que los contenedores reiniciados vuelvan a levantar...")
                time.sleep(60)
                
                for instancia in contenedores_reiniciados:
                    puerto = instancia["puerto"]
                    sesion = instancia["nombre_sesion"]
                    
                    log_mkt(f"🔍 Re-evaluando salud de WAHA en puerto {puerto} tras el reinicio...")
                    ok, detalle_estado = comprobar_estado_waha(puerto, sesion)
                    
                    if ok:
                        log_mkt(f"✅ WAHA en puerto {puerto} se recuperó exitosamente ({detalle_estado}).")
                        estados_finales.append(True)
                    else:
                        log_mkt(f"❌ WAHA en puerto {puerto} sigue fallando ({detalle_estado}).")
                        estados_finales.append(False)

            waha_ok = all(estados_finales) and len(estados_finales) == len(instancias_waha)

            if waha_ok:
                log_mkt("✅ Todos los servicios WAHA operativos. Continuando con las tareas.")
            else:
                log_mkt("❌ Hay inestabilidad en uno o más servicios WAHA. Se bloquearán las tareas dependientes.")

        # ==================================================================
        # 🎯 TAREA 1: MENSAJES DIRECTOS (Optimizado en RAM)
        # ==================================================================
        if tarea_objetivo and tarea_objetivo not in ['1', 'msg', 'mensajes']:
            log_mkt("⏭️ TAREA 1 OMITIDA: Filtrada por parámetro de prueba (--solo).")
        elif not waha_ok:
            log_mkt("⏸️ TAREA 1 OMITIDA: Bloqueo de seguridad activado (WAHA inestable o desconectado).")
        elif not config.bot_activo:
            log_mkt("⏸️ TAREA 1 OMITIDA: El Sniper Bot está apagado.")
        elif not tiempo_ok_msg:
            log_mkt(f"⏳ TAREA 1: Aún no pasan los 10 min mínimos (Han pasado {int(min_pasados_msg)} min).")
        elif not dentro_de_horario:
            log_mkt(f"⏰ TAREA 1 OMITIDA: Fuera de horario comercial ({config.hora_inicio} a {config.hora_fin}).")
        else:
            with engine.connect() as conn_g:
                grupos_ok, motivo_grupos = grupos_listos(conn_g)
                limites_grupo = leer_limites(conn_g)
            if not grupos_ok:
                log_mkt(f"⏸️ TAREA 1 OMITIDA: {motivo_grupos}")
                obreros_tarea1 = []
            else:
                obreros_tarea1 = obreros
            log_mkt("▶️ INICIANDO TAREA 1: Evaluando líneas de envío independientes...")            
            
            for obrero in obreros_tarea1:
                prob_sesion = obrero["probabilidad"]
                
                dado_msg = random.randint(1, 100)
                if dado_msg > prob_sesion and not es_modo_test:
                    log_mkt(f"🎲 [{obrero['nombre_vis']}] SALTADO: El dado cayó en {dado_msg} (Requerido: <= {prob_sesion}%).")
                    continue
                    
                log_mkt(f"🎯 [{obrero['nombre_vis']}] APROBADO: El dado cayó en {dado_msg}.")
                
                with engine.connect() as conn:
                    # 👥 Cupo por grupo (Campañas → Mensajes → Límites por grupo)
                    por_grupo = enviados_hoy_por_grupo(conn, obrero["sesion"])
                    clave_lim = "principal" if obrero["sesion"] == "principal" else "default"
                    estado_grupos, grupos_con_cupo = [], []
                    enviados_hoy = sum(por_grupo.values())
                    for g, lim in sorted(limites_grupo.items()):
                        env_g, max_g = por_grupo.get(g, 0), lim[clave_lim]
                        if not lim["activo"] or max_g <= 0:
                            estado_grupos.append(f"G{g}: off")
                            continue
                        estado_grupos.append(f"G{g}: {env_g}/{max_g}")
                        if env_g < max_g:
                            grupos_con_cupo.append((env_g / max_g, g))
                    # el grupo con menos avance (proporcional a su límite) va primero
                    orden_grupos = [g for _, g in sorted(grupos_con_cupo)]

                    log_mkt(f"📊 [{obrero['nombre_vis']}] Directos hoy: {enviados_hoy} | {' · '.join(estado_grupos)}")

                    if not orden_grupos:
                        log_mkt(f"🚫 [{obrero['nombre_vis']}] Todos los grupos activos llegaron a su límite diario (o están apagados).")
                        continue

                    prod_elegido = buscar_producto_dinamico(conn, obrero['col_prob'])
                    if not prod_elegido: 
                        log_mkt(f"⚠️ [{obrero['nombre_vis']}] Sin producto disponible (stock 0 o probabilidades en 0%).")
                        continue

                    # Prospectos filtrados por GRUPO y por CUENTA (ver SQL_PROSPECTOS arriba)
                    query_clientes = text(SQL_PROSPECTOS)
                    clientes_validos = conn.execute(query_clientes, {
                        "sess": obrero["sesion"],
                        "grupos": orden_grupos,
                        "orden_grupos": orden_grupos,
                    }).fetchall()

                if not clientes_validos: 
                    log_mkt(f"💤 [{obrero['nombre_vis']}] Sin prospectos disponibles hoy en los grupos con cupo ({', '.join(f'G{g}' for g in orden_grupos)}).")
                    continue
                prospectos = list(clientes_validos)

                descartes = {"otra_sesion": 0, "formato_invalido": 0, "sin_waha": 0, "error_api": 0}
                disparo_exitoso = False

                for cliente in prospectos:
                    es_cliente_frio = (cliente.total_entrantes == 0)

                    if not es_cliente_frio:
                        ultima_ses = str(cliente.ultima_sesion_entrante or 'default').strip()
                        if ultima_ses != obrero['sesion']:
                            descartes["otra_sesion"] += 1
                            continue

                    if es_lid(cliente.telefono):
                        # número oculto: se le escribe a su LID tal cual (no es un teléfono, no se normaliza)
                        telefono_final = str(cliente.telefono).strip()
                    else:
                        norm = normalizar_telefono_maestro(cliente.telefono)
                        if not norm: 
                            descartes["formato_invalido"] += 1
                            continue
                        telefono_final = norm['db']
                    
                    if verificar_numero_waha(telefono_final) is True:
                        saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                        nom_ia = cliente.nombre_ia.strip() if cliente.nombre_ia else ""
                        cabecera = f"{saludo} {nom_ia} 👋" if nom_ia else "¡Hola! 👋"

                        # Cada grupo tiene su propio texto: se agrega su instrucción al contexto de la IA
                        prod_cliente = dict(prod_elegido)
                        prod_cliente['contexto_ia_extra'] = f"{prod_elegido.get('contexto_ia_extra', '')} {cliente.instruccion_ia or ''}".strip()
                        cuerpo_ia = generar_texto_producto_ia(prod_cliente, es_estado=False, cliente_info={"etiquetas": cliente.etiquetas or ""})
                        mensaje_completo = f"{cabecera}\n\n{cuerpo_ia}"

                        if enviar_mensaje_whatsapp(telefono_final, mensaje_completo, prod_elegido['url_imagen'], session=obrero['sesion']):
                            with engine.begin() as conn_save:
                                conn_save.execute(text("""
                                    INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name, origen_bot) 
                                    VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() AT TIME ZONE 'America/Lima'), TRUE, :sess, :origen)
                                """), {"idc": cliente.id_cliente, "t": telefono_final, "c": mensaje_completo, "sess": obrero['sesion'],
                                       "origen": origen_grupo(cliente.grupo)})
                            
                            log_mkt(f"✅ Disparo a {telefono_final} ({obrero['nombre_vis']}). Grupo {cliente.grupo}. Frío: {'Sí' if es_cliente_frio else 'No'}.")
                            disparo_exitoso = True
                            break  
                        else:
                            descartes["error_api"] += 1
                    else:
                        descartes["sin_waha"] += 1
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

                if not disparo_exitoso:
                    log_mkt(
                        f"⚠️ [{obrero['nombre_vis']}] Lote de {len(prospectos)} prospectos fallido. "
                        f"Descartes -> Otra Sesión: {descartes['otra_sesion']} | "
                        f"Inválido: {descartes['formato_invalido']} | "
                        f"Sin WA: {descartes['sin_waha']} | "
                        f"Err API: {descartes['error_api']}"
                    )

            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_mensajes = NOW() WHERE id = :id"), {"id": config.id})

        # ==================================================================
        # 📱 TAREA 2: ESTADOS CON TRAZABILIDAD EXTREMA
        # ==================================================================
        if tarea_objetivo and tarea_objetivo not in ['2', 'est', 'estados']:
            log_mkt("⏭️ TAREA 2 OMITIDA: Filtrada por parámetro de prueba (--solo).")
        elif not waha_ok:
            log_mkt("⏸️ TAREA 2 OMITIDA: Bloqueo de seguridad activado (WAHA inestable o desconectado).")
        elif not tiempo_ok_est:
            log_mkt(f"⏳ TAREA 2 OMITIDA: Aún no pasan los 30 min base (Han pasado {int(min_pasados_est)} min).")
        elif not dentro_de_horario:
            log_mkt(f"⏰ TAREA 2 OMITIDA: Fuera de horario comercial ({config.hora_inicio} a {config.hora_fin}).")
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
        # 📘 TAREA 3: FACEBOOK / INSTAGRAM (Vía Make.com Unificado)
        # ==================================================================
        if tarea_objetivo and tarea_objetivo not in ['3', 'fb', 'facebook']:
            log_mkt("⏭️ TAREA 3 OMITIDA: Filtrada por parámetro de prueba (--solo).")
        elif not getattr(config, 'fb_activo', False):
            log_mkt("⏸️ TAREA 3 OMITIDA: Auto-Publicación apagada.")
        elif not tiempo_ok_fb:
            log_mkt(f"⏳ TAREA 3: Aún no pasan los 30 min base (Han pasado {int(min_pasados_fb)} min).")
        elif not dentro_de_horario:
            log_mkt("⏰ TAREA 3 OMITIDA: Fuera de horario comercial.")
        else:
            dado_fb = random.randint(1, 100)
            if dado_fb <= prob_fb or es_modo_test:
                log_mkt(f"▶️ INICIANDO TAREA 3 (Dado: {dado_fb} <= {prob_fb}%)")
                
                secundarias = [
                    {"nombre": "pelucas", "col_prob": "prob_fb_pelucas"},
                    {"nombre": "lentes", "col_prob": "prob_fb_lentes"}
                ]
                random.shuffle(secundarias)
                pagina_elegida = secundarias[0]

                with engine.connect() as conn:
                    prod_fb = buscar_producto_dinamico(conn, pagina_elegida['col_prob'])
                    
                if prod_fb:
                    log_mkt(f" 🧠 Redactando copy (IA) para postear {prod_fb.get('nombre', '')} en la categoría: {pagina_elegida['nombre']}...")
                    respuestas_ia = generar_texto_producto_ia(prod_fb, es_estado=True)
                    texto_fb = respuestas_ia.get('post_facebook', '')
                    
                    imagen_jpg_b64 = obtener_imagen_jpg_b64(prod_fb.get('url_imagen', ''))

                    payload_make = {
                        "codigo_acceso": os.getenv("MAKE_ACCESS_CODE"),
                        "categoria": pagina_elegida["nombre"],
                        "mensaje": texto_fb,
                        "imagen_b64_jpg": imagen_jpg_b64,
                        "url_imagen": prod_fb.get('url_imagen', '')
                    }
                    
                    webhook_url = os.getenv("MAKE_WEBHOOK_FB")
                    try:
                        # 🛠️ CORRECCIÓN: Se usa 'requests' global (sin re-importar aquí adentro)
                        response = requests.post(webhook_url, json=payload_make, timeout=20)
                        
                        if response.status_code == 200:
                            log_mkt(f" ✅ ¡Datos enviados exitosamente a Make.com (Categoría: {pagina_elegida['nombre']})!")
                            with engine.begin() as conn_hist:
                                conn_hist.execute(text("INSERT INTO Historial_Facebook (pagina, sku) VALUES (:pag, :sku)"), {"pag": pagina_elegida['nombre'], "sku": prod_fb.get('sku', '')})
                        else:
                            log_mkt(f" ❌ Make.com rechazó el webhook. Código HTTP {response.status_code}: {response.text}")
                    except Exception as e:
                        log_mkt(f" ❌ Error de conexión con el Webhook de Make: {e}")
            else:
                log_mkt(f"🎲 TAREA 3 SALTADA: El dado cayó en {dado_fb} (Requerido: <= {prob_fb}%).")
            
            with engine.begin() as conn_up:
                conn_up.execute(text("UPDATE Configuracion_Campanas SET ultimo_envio_fb = NOW() WHERE id = :id"), {"id": config.id})
                
        # ==================================================================
        # 🧟 TAREA 4: REACTIVACIÓN DE CLIENTES ZOMBIES (Optimizado en RAM)
        # ==================================================================
        if tarea_objetivo and tarea_objetivo not in ['4', 'zombie', 'zombies']:
            log_mkt("⏭️ TAREA 4 OMITIDA: Filtrada por parámetro de prueba (--solo).")
        elif not waha_ok:
            log_mkt("⏸️ TAREA 4 OMITIDA: Bloqueo de seguridad activado (WAHA inestable o desconectado).")
        elif not config.bot_activo:
            log_mkt("⏸️ TAREA 4 OMITIDA: El Sniper Bot está apagado.")
        elif not dentro_de_horario:
            log_mkt(f"⏰ TAREA 4 OMITIDA: Fuera de horario comercial ({config.hora_inicio} a {config.hora_fin}).")
        else:
            log_mkt("▶️ INICIANDO TAREA 4: Evaluando clientes en categoría Zombie...")
            
            limites_zombie = {
                1: getattr(config, 'max_zombies_lvl1', 1),
                2: getattr(config, 'max_zombies_lvl2', 1),
                3: getattr(config, 'max_zombies_lvl3', 1)
            }
            probabilidades_zombie = {
                1: getattr(config, 'prob_zombie_lvl1', 10),
                2: getattr(config, 'prob_zombie_lvl2', 20),
                3: getattr(config, 'prob_zombie_lvl3', 30)
            }
            
            for obrero in obreros: 
                with engine.connect() as conn:
                    # 🛠️ OPTIMIZACIÓN RAM: Uso de CTEs (WITH) para evitar escanear 'mensajes' fila por fila
                    query_zombies = text("""
                        WITH Zombies_Base AS (
                            SELECT c.id_cliente, c.nombre_corto, c.nombre_ia, c.etiquetas, 
                                   c.nivel_zombie, c.ultimo_msg_zombie,
                                   COALESCE(NULLIF(TRIM(t.telefono), ''), t.lid) AS telefono  -- LID si el número está oculto
                            FROM Clientes c
                            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
                            WHERE c.activo = TRUE 
                              AND c.nivel_zombie > 0 
                              AND COALESCE(c.excluir_publicidad, FALSE) = FALSE 
                              AND t.activo = TRUE 
                              AND t.es_principal = TRUE
                        ),
                        Ultima_Sesion_Zombie AS (
                            SELECT DISTINCT ON (m3.telefono) m3.telefono, m3.session_name
                            FROM mensajes m3
                            JOIN Zombies_Base zb ON m3.telefono = zb.telefono
                            WHERE m3.tipo = 'ENTRANTE'
                            ORDER BY m3.telefono, m3.fecha DESC
                        ),
                        Envios_Hoy_Zombie AS (
                            SELECT m2.telefono, 
                                   COUNT(DISTINCT (EXTRACT(EPOCH FROM m2.fecha) / 300)::INT) as msgs_enviados_hoy
                            FROM mensajes m2
                            JOIN Zombies_Base zb ON m2.telefono = zb.telefono
                            WHERE m2.tipo IN ('SALIENTE_BOT', 'SALIENTE_PANEL')
                              AND m2.fecha >= (NOW() AT TIME ZONE 'America/Lima')::date
                            GROUP BY m2.telefono
                        )
                        SELECT zb.*, 
                               COALESCE(eh.msgs_enviados_hoy, 0) as msgs_enviados_hoy
                        FROM Zombies_Base zb
                        LEFT JOIN Ultima_Sesion_Zombie us ON zb.telefono = us.telefono
                        LEFT JOIN Envios_Hoy_Zombie eh ON zb.telefono = eh.telefono
                        WHERE TRIM(COALESCE(us.session_name, 'default')) = TRIM(:sess)
                    """)
                    zombies_validos = conn.execute(query_zombies, {"sess": obrero["sesion"]}).fetchall()

                if not zombies_validos:
                    continue

                for zombie in zombies_validos:
                    if not zombie.ultimo_msg_zombie:
                        continue
                        
                    # El nivel se CALCULA por la ventana de días (ver marketing_grupos.ZOMBIE_VENTANAS), así los
                    # 3 niveles nunca se cruzan: Lvl 1 = día 0 | Lvl 2 = días 1-6 | Lvl 3 = días 7-13 | día 14 → sale.
                    # Si el cliente vuelve a mandar la frase clave, el webhook reinicia ultimo_msg_zombie y vuelve a Lvl 1.
                    ref_cliente = zombie.telefono or f"id {zombie.id_cliente}"
                    dias_transcurridos = dias_zombie(zombie.ultimo_msg_zombie)
                    nivel_guardado = int(zombie.nivel_zombie or 0)
                    nivel_actual = nivel_zombie_por_dias(dias_transcurridos)

                    if nivel_actual == 0:
                        with engine.begin() as conn_up:
                            conn_up.execute(text("UPDATE Clientes SET nivel_zombie = 0 WHERE id_cliente = :id"), {"id": zombie.id_cliente})
                        log_mkt(f"🚶 Cliente {ref_cliente} dejó de ser zombie (pasaron {ZOMBIE_DIA_SALIDA} días en silencio).")
                        continue

                    if nivel_actual != nivel_guardado:
                        with engine.begin() as conn_up:
                            conn_up.execute(text("UPDATE Clientes SET nivel_zombie = :n WHERE id_cliente = :id"), {"n": nivel_actual, "id": zombie.id_cliente})
                        log_mkt(f"🔄 Cliente {ref_cliente}: Zombie Lvl {nivel_guardado} → Lvl {nivel_actual} (día {dias_transcurridos}).")

                    if not zombie.telefono:
                        continue  # sin teléfono ni LID: no hay a dónde escribir

                    limite_usuario = limites_zombie.get(nivel_actual, 1)
                    if zombie.msgs_enviados_hoy >= limite_usuario:
                        continue 
                        
                    prob_z = probabilidades_zombie.get(nivel_actual, 10)
                    dado_z = random.randint(1, 100)
                    
                    if dado_z > prob_z and not es_modo_test:
                        continue 
                        
                    es_primer_contacto_hoy = (zombie.msgs_enviados_hoy == 0)
                    
                    with engine.connect() as conn:
                        prod_zombie = buscar_producto_dinamico(conn, obrero['col_prob'])
                    if not prod_zombie: 
                        continue

                    cuerpo_ia = generar_texto_producto_ia(
                        prod_zombie, 
                        es_estado=False, 
                        cliente_info={"etiquetas": zombie.etiquetas}, 
                        primer_contacto_dia=es_primer_contacto_hoy
                    )
                    
                    if es_primer_contacto_hoy:
                        saludo = random.choice(["Hola", "¡Hola!", "¡Qué tal", "Saludos", "Buen día"])
                        nom_ia = zombie.nombre_ia.strip() if zombie.nombre_ia else ""
                        cabecera = f"{saludo} {nom_ia} 👋\n\n" if nom_ia else "¡Hola! 👋\n\n"
                    else:
                        cabecera = ""
                        
                    mensaje_completo = f"{cabecera}{cuerpo_ia}"
                    
                    if enviar_mensaje_whatsapp(zombie.telefono, mensaje_completo, prod_zombie['url_imagen'], session=obrero['sesion']):
                        with engine.begin() as conn_save:
                            conn_save.execute(text("""
                                INSERT INTO mensajes (id_cliente, telefono, tipo, contenido, fecha, leido, session_name, origen_bot) 
                                VALUES (:idc, :t, 'SALIENTE_BOT', :c, (NOW() AT TIME ZONE 'America/Lima'), TRUE, :sess, :origen)
                            """), {"idc": zombie.id_cliente, "t": zombie.telefono, "c": mensaje_completo, "sess": obrero['sesion'],
                                   "origen": origen_zombie(nivel_actual)})
                        
                        log_mkt(f"🧟✅ Disparo ZOMBIE (Lvl {nivel_actual}) exitoso a {zombie.telefono} ({obrero['sesion']}).")
                        log_mkt("⏳ Esperando 60 segundos antes de evaluar al siguiente cliente para evitar spam...")
                        time.sleep(60)
    except Exception as e:
        log_mkt(f"🔥 Error catastrófico: {e}")

if __name__ == "__main__":
    ejecutar_francotirador()