import os
import time
import pandas as pd
from datetime import datetime, timedelta

# 1. CARGAR VARIABLES DE ENTORNO ANTES DE IMPORTAR LA BD
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from database import engine
from utils import enviar_mensaje_whatsapp

def calcular_dias_sin_domingos(fecha_inicio, fecha_fin):
    """
    Calcula la diferencia en días entre dos fechas excluyendo los domingos.
    """
    dias_totales = (fecha_fin.date() - fecha_inicio.date()).days
    if dias_totales <= 0:
        return 0
    
    dias_validos = 0
    # Recorremos cada día transcurrido
    for i in range(1, dias_totales + 1):
        dia_evaluado = fecha_inicio.date() + timedelta(days=i)
        # weekday() devuelve 6 para los domingos
        if dia_evaluado.weekday() != 6: 
            dias_validos += 1
            
    return dias_validos

def ejecutar_agradecimientos_automaticos():
    if engine is None:
        print("❌ Error fatal: No se pudo conectar a la base de datos (engine es None).")
        return

    print("Iniciando revisión de clientes 'En Camino' excluyendo domingos...")
    
    query_etapa = text("SELECT id_etapa FROM EtapasCliente WHERE subgrupo ILIKE 'sin empezar' LIMIT 1")
    
    # Consulta dinámica leyendo la sesión desde los mensajes
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
            c.telefono, 
            c.nombre_corto,
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
        with engine.begin() as conn:
            res_etapa = conn.execute(query_etapa).fetchone()
            id_etapa_sin_empezar = res_etapa[0] if res_etapa else None

            df_clientes = pd.read_sql(query_clientes, conn)
            
            if df_clientes.empty:
                print("No hay clientes en estado 'En camino'.")
                return

            ahora = datetime.now()

            for _, row in df_clientes.iterrows():
                id_cliente = row['id_cliente']
                telefono = row['telefono']
                
                sesion_detectada = str(row['session_name']).strip().lower() if pd.notna(row['session_name']) and str(row['session_name']).strip() else 'default'
                tipo_envio = str(row['tipo_envio']).upper() if pd.notna(row['tipo_envio']) else 'OTROS'
                
                ultima_fecha = pd.to_datetime(row['ultima_fecha'])
                if ultima_fecha.tzinfo is not None:
                    ultima_fecha = ultima_fecha.tz_localize(None)

                # --- 🚀 NUEVA LÓGICA: DÍAS SIN DOMINGOS ---
                dias_transcurridos = calcular_dias_sin_domingos(ultima_fecha, ahora)

                dias_objetivo = 7 if tipo_envio == 'AGENCIA' else 2

                if dias_transcurridos >= dias_objetivo:
                    print(f"[{telefono}] Han pasado {dias_transcurridos} días hábiles (Objetivo: {dias_objetivo} por {tipo_envio}). Enviando agradecimiento vía '{sesion_detectada}'...")
                    
                    nombre = row['nombre_corto'] or "Estimado/a"
                    mensaje = (
                        f"¡Hola {nombre}! 👋\n\n"
                        f"Esperamos que ya tengas tu pedido contigo y todo esté en perfectas condiciones. 📦✨\n"
                        f"Queríamos tomar un momento para decirte: *¡Muchas gracias por tu compra y por confiar en nosotros!*\n\n"
                        f"Si necesitas asistencia o tienes alguna duda, este chat siempre está abierto para ti."
                    )
                    
                    if enviar_mensaje_whatsapp(telefono, mensaje, session=sesion_detectada):
                        update_cliente = text("""
                            UPDATE Clientes 
                            SET estado = 'Sin empezar', id_etapa = :id_etapa 
                            WHERE id_cliente = :id
                        """)
                        
                        update_venta = text("""
                            UPDATE Ventas 
                            SET pendiente_pago = '0' 
                            WHERE id_cliente = :id
                        """)
                        
                        conn.execute(update_cliente, {"id_etapa": id_etapa_sin_empezar, "id": int(id_cliente)})
                        conn.execute(update_venta, {"id": int(id_cliente)})
                        
                        print(f"✅ Cliente {id_cliente} actualizado a 'Sin empezar' con deuda 0.")
                        time.sleep(2) 

    except Exception as e:
        print(f"❌ Error en la rutina de post-venta: {e}")

if __name__ == "__main__":
    ejecutar_agradecimientos_automaticos()