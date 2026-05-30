import os
from dotenv import load_dotenv

# Cargar variables de entorno PRIMERO
load_dotenv()

from database import engine
from sqlalchemy import text
from utils import get_google_service, crear_en_google, normalizar_telefono_maestro

def sincronizar_con_diagnostico():
    print("📥 1. Descargando agenda de Google...")
    service = get_google_service()
    if not service:
        print("❌ No se pudo conectar a Google.")
        return

    mapa_google = {}
    request = service.people().connections().list(
        resourceName='people/me',
        personFields='names,phoneNumbers',
        pageSize=1000
    )
    
    while request is not None:
        response = request.execute()
        for person in response.get('connections', []):
            resource_name = person.get('resourceName')
            for phone in person.get('phoneNumbers', []):
                val = phone.get('value', '')
                norm = normalizar_telefono_maestro(val)
                if norm:
                    mapa_google[norm['db']] = resource_name
                    mapa_google[norm['corto']] = resource_name
        request = service.people().connections().list_next(request, response)

    print(f"✅ Agenda descargada. Buscando casos en la Base de Datos...\n")

    with engine.connect() as conn:
        # Traemos TODOS los clientes sin google_id para evaluar por qué se saltan
        clientes = conn.execute(text("""
            SELECT c.nombre_corto, t.telefono, c.activo, t.es_principal
            FROM clientes c 
            JOIN telefonoscliente t ON c.id_cliente = t.id_cliente
            WHERE c.google_id IS NULL
        """)).fetchall()

    if not clientes:
        print("ℹ️ No hay clientes con google_id en NULL en la base de datos.")
        return

    for cli in clientes:
        # Diagnóstico de filtros de la base de datos
        if not cli.activo:
            print(f"⚠️ Omitido: '{cli.nombre_corto}' está INACTIVO en la BD.")
            continue
        if not cli.es_principal:
            print(f"⚠️ Omitido: El teléfono {cli.telefono} de '{cli.nombre_corto}' no está marcado como principal.")
            continue

        norm = normalizar_telefono_maestro(cli.telefono)
        if not norm:
            print(f"❌ Omitido: El teléfono '{cli.telefono}' de '{cli.nombre_corto}' no se pudo normalizar (formato inválido).")
            continue

        tel_db = norm['db']
        tel_corto = norm['corto']

        if tel_db in mapa_google or tel_corto in mapa_google:
            g_id = mapa_google.get(tel_db) or mapa_google.get(tel_corto)
            # Volvemos a enlazar por si acaso
            with engine.begin() as conn_update:
                conn_update.execute(text("""
                    UPDATE clientes SET google_id = :gid 
                    WHERE nombre_corto = :nom AND google_id IS NULL
                """), {"gid": g_id, "nom": cli.nombre_corto})
            # No se crea porque el número ya existe bajo otro contacto en Google
            print(f"🔄 Vinculado: '{cli.nombre_corto}' ya existía en Google bajo el número {tel_corto}.")
        else:
            # Creación limpia
            exito = crear_en_google(cli.nombre_corto, "", norm['google'])
            if exito:
                print(f"✅ Creado con éxito: '{cli.nombre_corto}' ({norm['google']})")
            else:
                print(f"❌ Error de API al intentar crear a '{cli.nombre_corto}'.")

if __name__ == "__main__":
    sincronizar_con_diagnostico()