import os
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz

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
    
    # Consulta extraída de la tabla Clientes
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

    # --- FASE 1: LECTURA RÁPIDA (Sin bloquear la BD) ---
    try:
        with engine.connect() as conn:
            res_etapa = conn.execute(query_etapa).fetchone()
            id_etapa_sin_empezar = res_etapa[0] if res_etapa else None

            df_clientes = pd.read_sql(query_clientes, conn)
            
        if df_clientes.empty:
            print("No hay clientes en estado 'En camino'.")
            return
            
    except Exception as e:
        print(f"❌ Error al consultar la base de datos: {e}")
        return

    # Usamos zona horaria de Lima para evitar desajustes con el servidor
    tz_lima = pytz.timezone('America/Lima')
    ahora = datetime.now(tz_lima).replace(tzinfo=None)

    # --- FASE 2: PROCESAMIENTO CLIENTE POR CLIENTE ---
    for _, row in df_clientes.iterrows():
        try:
            id_cliente = row['id_cliente']
            
            # Limpieza y validación anti-nan
            telefono = str(row['telefono']).strip()
            if pd.isna(row['telefono']) or telefono.lower() == 'nan' or not telefono:
                print(f"⚠️ Cliente {id_cliente} ignorado: No tiene un número de teléfono válido registrado.")
                continue
                
            # A partir de aquí tu código sigue igual...
            sesion_detectada = str(row['session_name']).strip().lower() if pd.notna(row['session_name']) and str(row['session_name']).strip() else 'default'
            tipo_envio = str(row['tipo_envio']).upper() if pd.notna(row['tipo_envio']) else 'OTROS'
            
            ultima_fecha = pd.to_datetime(row['ultima_fecha'])
            if ultima_fecha.tzinfo is not None:
                ultima_fecha = ultima_fecha.tz_localize(None)

            dias_transcurridos = calcular_dias_sin_domingos(ultima_fecha, ahora)
            dias_objetivo = 7 if tipo_envio == 'AGENCIA' else 2

            if dias_transcurridos >= dias_objetivo:
                print(f"[{telefono}] Han pasado {dias_transcurridos} días hábiles (Objetivo: {dias_objetivo} por {tipo_envio}). Enviando agradecimiento vía '{sesion_detectada}'...")
                
                nombre_ia_val = str(row['nombre_ia']).strip() if pd.notna(row['nombre_ia']) else ""
                saludo = f"¡Hola {nombre_ia_val}! 👋\n\n" if nombre_ia_val else "¡Hola! 👋\n\n"
                    
                mensaje = (
                    f"{saludo}"
                    f"Esperamos que ya tengas tu pedido contigo y todo esté en perfectas condiciones. 📦✨\n"
                    f"Queríamos tomar un momento para decirte: *¡Muchas gracias por tu compra y por confiar en nosotros!*\n\n"
                    f"Si necesitas asistencia o tienes alguna duda, este chat siempre está abierto para ti."
                )
                
                # API Call de WhatsApp (SIN bloquear base de datos)
                if enviar_mensaje_whatsapp(telefono, mensaje, session=sesion_detectada):
                    
                    # --- FASE 3: ESCRITURA RÁPIDA EN BD (Micro-transacción) ---
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
                    
                    print(f"✅ Cliente {id_cliente} actualizado a 'Sin empezar' con deuda 0.")
                    
                    # Mecanismo anti-spam SEGURO
                    print("⏳ Esperando 30 segundos por seguridad anti-spam...")
                    time.sleep(30) 
                else:
                    print(f"⚠️ Falló el envío a {telefono}, se intentará de nuevo en la siguiente ronda.")

        except Exception as e:
            print(f"❌ Error procesando al cliente {id_cliente} ({telefono}): {e}")
            continue # Permite que el sistema pase al siguiente cliente sin morir

if __name__ == "__main__":
    ejecutar_agradecimientos_automaticos()