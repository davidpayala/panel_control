import os
import sys
import time
import random
from dotenv import load_dotenv
from sqlalchemy import text

# 1. Cargar entorno y base de datos
ruta_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(ruta_env)
from database import engine

# 2. Importar tu función existente de WAHA
try:
    from utils import obtener_lid_de_waha
except ImportError:
    print("❌ Error: No se pudo importar obtener_lid_de_waha desde utils.py")
    sys.exit(1)

def ejecutar_sincronizacion_silenciosa():
    print("👻 Iniciando Sincronizador Fantasma de LIDs...")
    
    # Extraemos todos los teléfonos válidos que aún no tienen LID
    with engine.connect() as conn:
        query = text("""
            SELECT id_telefono, telefono 
            FROM telefonoscliente 
            WHERE lid IS NULL 
              AND telefono IS NOT NULL 
              AND activo = TRUE
            ORDER BY id_telefono DESC
        """)
        pendientes = conn.execute(query).fetchall()
        
    total = len(pendientes)
    print(f"📊 Se encontraron {total} contactos pendientes de sincronizar.")
    
    if total == 0:
        print("✅ Todo está actualizado. Saliendo...")
        return

    exitos = 0
    fallos = 0

    for index, registro in enumerate(pendientes, 1):
        id_tel = registro.id_telefono
        telefono = registro.telefono
        
        print(f"[{index}/{total}] 🔍 Consultando LID para: {telefono}...")
        
        try:
            # Consultamos a WAHA
            lid_api = obtener_lid_de_waha(telefono)
            
            if lid_api:
                # Guardamos silenciosamente en la BD
                with engine.begin() as conn_update:
                    conn_update.execute(text("UPDATE telefonoscliente SET lid = :lid WHERE id_telefono = :id"), 
                                        {"lid": lid_api, "id": id_tel})
                print(f"  ✅ ¡Éxito! LID guardado: {lid_api}")
                exitos += 1
            else:
                print(f"  ⚠️ WAHA no devolvió LID (Posible restricción de privacidad).")
                fallos += 1
                
        except Exception as e:
            print(f"  ❌ Error consultando {telefono}: {e}")
            fallos += 1
            
        # ==========================================================
        # 🛡️ ESCUDO ANTI-SPAM: Retraso Orgánico (25 a 60 segundos)
        # ==========================================================
        if index < total:
            retraso = random.randint(25, 60)
            print(f"  ⏳ Modo orgánico: Durmiendo {retraso} segundos para no alertar a Meta...\n")
            time.sleep(retraso)

    print("\n🏁 ======================================")
    print(f"🏁 SINCRONIZACIÓN FANTASMA TERMINADA")
    print(f"🏁 Éxitos: {exitos} | Fallos/No Encontrados: {fallos}")
    print("🏁 ======================================\n")

if __name__ == "__main__":
    ejecutar_sincronizacion_silenciosa()