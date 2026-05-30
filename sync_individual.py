import os
from dotenv import load_dotenv

load_dotenv()

from database import engine
from sqlalchemy import text
from utils import crear_en_google, normalizar_telefono_maestro

def sincronizar_cliente_especifico(id_cliente):
    with engine.connect() as conn:
        cliente = conn.execute(text("""
            SELECT c.id_cliente, c.nombre_corto, t.telefono 
            FROM clientes c 
            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
            WHERE c.id_cliente = :id_cliente
            LIMIT 1
        """), {"id_cliente": id_cliente}).fetchone()

    if not cliente:
        print(f"❌ No se encontró el cliente con ID {id_cliente}.")
        return

    print(f"🔍 Evaluando: {cliente.nombre_corto} - Tel: {cliente.telefono}")
    
    norm = normalizar_telefono_maestro(cliente.telefono)
    if not norm:
        print("❌ El número no tiene un formato válido para normalizar.")
        return

    exito = crear_en_google(cliente.nombre_corto, "", norm['google'])
    
    if exito:
        # Aquí estaba el error (cliente en lugar de cli)
        print(f"✅ Agregado correctamente a Google: {cliente.nombre_corto} ({norm['google']})")
        
        # Actualizamos la BD para dejar constancia
        with engine.begin() as conn_update:
            conn_update.execute(text("""
                UPDATE clientes SET google_id = 'Sincronizado Manual' 
                WHERE id_cliente = :id_cliente AND google_id IS NULL
            """), {"id_cliente": id_cliente})
    else:
        print(f"❌ Error al conectar con la API de Google para este contacto.")

if __name__ == "__main__":
    sincronizar_cliente_especifico(12361)