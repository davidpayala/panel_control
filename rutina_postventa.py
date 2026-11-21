import os
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz

# 1. CARGAR VARIABLES DE ENTORNO CON RUTA ABSOLUTA (Fundamental para Cron)
from dotenv import load_dotenv
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)

from sqlalchemy import text
from database import engine
from utils import enviar_mensaje_whatsapp

# --- FUNCIÓN DE REGISTRO EN SQL ---
def log_mkt(mensaje):
    """Guarda el log en la Base de Datos e imprime en consola"""
    mensaje_limpio = str(mensaje).lstrip('\n')
    print(mensaje_limpio, flush=True)
    if engine is not None:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO logs_postventa (fecha, mensaje) VALUES (NOW(), :msg)"),
                    {"msg": mensaje_limpio}
                )
        except Exception as e:
            print(f"Error crítico escribiendo log en BD: {e}")

def calcular_dias_sin_domingos(fecha_inicio, fecha_fin):
    dias_totales = (fecha_fin.date() - fecha_inicio.date()).days
    if dias_totales <= 0:
        return 0
    
    dias_validos = 0
    for i in range(1, dias_totales + 1):
        dia_evaluado = fecha_inicio.date() + timedelta(days=i)
        if dia_evaluado.weekday() != 6: 
            dias_validos += 1
            
    return dias_validos

def enlaces_resena(id_cliente, maximo=2):
    """URLs (con la pestaña de opiniones abierta) de los productos de la última venta del cliente.
    Las reseñas generan las estrellas que Google muestra en los resultados de búsqueda."""
    try:
        with engine.connect() as conn:
            skus = [r[0] for r in conn.execute(text("""
                SELECT d.sku FROM DetalleVenta d
                WHERE d.id_venta = (
                    SELECT id_venta FROM Ventas
                    WHERE id_cliente = :id AND COALESCE(anulado, FALSE) = FALSE
                    ORDER BY fecha_venta DESC LIMIT 1)
                  AND COALESCE(d.sku, '') <> ''
                ORDER BY d.subtotal DESC NULLS LAST
            """), {"id": int(id_cliente)}).fetchall()]
            if not skus:
                return []
            mapa = dict(conn.execute(text("SELECT sku, permalink FROM woo_permalinks")).fetchall())
    except Exception as e:
        log_mkt(f"⚠️ No se pudieron armar enlaces de reseña para cliente {id_cliente}: {e}")
        return []

    enlaces = []
    for sku in skus:
        sku = sku.strip()
        url = mapa.get(sku)
        if not url and "-" in sku:                       # NL032C-0000 -> NL032C
            url = mapa.get(sku.rsplit("-", 1)[0])
        if url and url not in enlaces:
            enlaces.append(url)
        if len(enlaces) >= maximo:
            break
    return [u.split("#")[0] + "#reviews" for u in enlaces]


def ejecutar_agradecimientos_automaticos():
    if engine is None:
        log_mkt("❌ Error fatal: No se pudo conectar a la base de datos (engine es None).")
        return

    log_mkt("🚀 Iniciando revisión de clientes 'En Camino' excluyendo domingos...")
    
    query_etapa = text("SELECT id_etapa FROM EtapasCliente WHERE subgrupo ILIKE 'sin empezar' LIMIT 1")
    
    query_clientes = text("""
        WITH ultima_interaccion AS (
            SELECT DISTINCT ON (tc.id_cliente)
                tc.id_cliente,
                m.fecha as ultima_fecha,
                m.session_name
            FROM telefonoscliente tc
            JOIN mensajes m ON m.telefono = tc.telefono OR m.telefono = tc.lid
            WHERE tc.activo = TRUE
            ORDER BY tc.id_cliente, m.fecha DESC
        )
        SELECT 
            c.id_cliente, 
            COALESCE(
                (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                c.telefono
            ) as telefono,
            c.nombre_corto,
            c.nombre_ia, 
            c.estado,
            u.ultima_fecha,
            u.session_name,
            (SELECT d.tipo_envio FROM Direcciones d WHERE d.id_cliente = c.id_cliente AND d.activo = TRUE ORDER BY d.es_principal DESC, d.id_direccion DESC LIMIT 1) as tipo_envio
        FROM Clientes c
        JOIN EtapasCliente e ON c.id_etapa = e.id_etapa
        JOIN ultima_interaccion u ON c.id_cliente = u.id_cliente
        WHERE e.grupo ILIKE '%etapa 3%'
          AND c.activo = TRUE
    """)

    try:
        with engine.connect() as conn:
            res_etapa = conn.execute(query_etapa).fetchone()
            id_etapa_sin_empezar = res_etapa[0] if res_etapa else None
            df_clientes = pd.read_sql(query_clientes, conn)
            
        if df_clientes.empty:
            log_mkt("ℹ️ No hay clientes en estado 'En camino'.")
            return
            
    except Exception as e:
        log_mkt(f"❌ Error al consultar la base de datos: {e}")
        return

    tz_lima = pytz.timezone('America/Lima')
    ahora = datetime.now(tz_lima).replace(tzinfo=None)

    for _, row in df_clientes.iterrows():
        try:
            id_cliente = row['id_cliente']
            
            telefono = str(row['telefono']).strip()
            if pd.isna(row['telefono']) or telefono.lower() == 'nan' or not telefono:
                log_mkt(f"⚠️ Cliente {id_cliente} ignorado: No tiene un número de teléfono válido registrado.")
                continue
                
            sesion_detectada = str(row['session_name']).strip().lower() if pd.notna(row['session_name']) and str(row['session_name']).strip() else 'default'
            tipo_envio = str(row['tipo_envio']).upper() if pd.notna(row['tipo_envio']) else 'OTROS'
            
            ultima_fecha = pd.to_datetime(row['ultima_fecha'])
            if ultima_fecha.tzinfo is not None:
                ultima_fecha = ultima_fecha.tz_localize(None)

            dias_transcurridos = calcular_dias_sin_domingos(ultima_fecha, ahora)
            dias_objetivo = 7 if tipo_envio == 'AGENCIA' else 2

            if dias_transcurridos >= dias_objetivo:
                log_mkt(f"[{telefono}] Han pasado {dias_transcurridos} días hábiles (Objetivo: {dias_objetivo} por {tipo_envio}). Enviando agradecimiento vía '{sesion_detectada}'...")
                
                nombre_ia_val = str(row['nombre_ia']).strip() if pd.notna(row['nombre_ia']) else ""
                saludo = f"¡Hola {nombre_ia_val}! 👋\n\n" if nombre_ia_val else "¡Hola! 👋\n\n"
                    
                mensaje = (
                    f"{saludo}"
                    f"Esperamos que ya tengas tu pedido contigo y todo esté en perfectas condiciones. 📦✨\n"
                    f"Queríamos tomar un momento para decirte: *¡Muchas gracias por tu compra y por confiar en nosotros!*\n\n"
                    f"Si necesitas asistencia o tienes alguna duda, este chat siempre está abierto para ti."
                )

                # Pedido de reseña (genera estrellas en Google y confianza para nuevos clientes)
                enlaces = enlaces_resena(id_cliente)
                if enlaces:
                    mensaje += (
                        "\n\n⭐ *¿Nos regalas tu opinión?* Nos ayuda muchísimo y te toma 1 minuto:\n"
                        + "\n".join(enlaces)
                    )
                
                if enviar_mensaje_whatsapp(telefono, mensaje, session=sesion_detectada):
                    with engine.begin() as conn_upd:
                        conn_upd.execute(text("""
                            UPDATE Clientes 
                            SET estado = 'Sin empezar', id_etapa = :id_etapa 
                            WHERE id_cliente = :id
                        """), {"id_etapa": id_etapa_sin_empezar, "id": int(id_cliente)})
                        
                        conn_upd.execute(text("""
                            UPDATE Ventas 
                            SET pendiente_pago = '0' 
                            WHERE id_cliente = :id
                        """), {"id": int(id_cliente)})
                    
                    log_mkt(f"✅ Cliente {id_cliente} actualizado a 'Sin empezar' con deuda 0.")
                    time.sleep(30) 
                else:
                    log_mkt(f"⚠️ Falló el envío a {telefono}, se intentará de nuevo en la siguiente ronda.")

        except Exception as e:
            log_mkt(f"❌ Error procesando al cliente {id_cliente} ({telefono}): {e}")
            continue 

if __name__ == "__main__":
    ejecutar_agradecimientos_automaticos()