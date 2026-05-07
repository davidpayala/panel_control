import os
from dotenv import load_dotenv
from woocommerce import API
from sqlalchemy import create_engine, text

# 1. Cargar las llaves de seguridad
load_dotenv()

def sync_inventario_con_visibilidad():
    print("⏳ Iniciando sincronización con reglas de stock en tránsito...")

    # 2. Conectar a PostgreSQL local
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
        with engine.connect() as conn:
            # NUEVO: Obtenemos también el stock_transito
            query = text("""
                SELECT sku, 
                       (COALESCE(stock_interno, 0) + COALESCE(stock_externo, 0)) AS stock_total,
                       COALESCE(stock_transito, 0) AS stock_transito
                FROM Variantes 
                WHERE sku IS NOT NULL AND sku != ''
            """)
            resultados = conn.execute(query).fetchall()
            
        # Guardamos tanto el stock físico como el de tránsito en un diccionario
        stock_local = {row.sku: {"total": row.stock_total, "transito": row.stock_transito} for row in resultados}
        print(f"📦 Datos locales: {len(stock_local)} SKUs procesados.")
    except Exception as e:
        print(f"🔥 Error en base de datos: {e}")
        return

    # 3. Conectar a WooCommerce
    wcapi = API(
        url=os.getenv("WOO_URL"),
        consumer_key=os.getenv("WOO_KEY"),
        consumer_secret=os.getenv("WOO_SECRET"),
        version="wc/v3",
        timeout=30
    )

    # 4. Mapear IDs de WooCommerce
    print("🌐 Mapeando productos en la web...")
    woo_productos = []
    pagina = 1
    while True:
        resp = wcapi.get("products", params={"per_page": 100, "page": pagina})
        items = resp.json()
        if not items: break
        woo_productos.extend(items)
        pagina += 1

    mapa_woo = {p["sku"]: p["id"] for p in woo_productos if p.get("sku")}

    # 5. Preparar actualización con Lógica de Visibilidad Mejorada
    paquete_actualizacion = []
    for sku, data in stock_local.items():
        if sku in mapa_woo:
            stock_real = data["total"]
            stock_camino = data["transito"]
            
            # NUEVA REGLA DAVID: 
            # Visible si hay stock físico (>0) O si hay stock en camino (>0)
            visibilidad = "visible" if (stock_real > 0 or stock_camino > 0) else "hidden"
            
            paquete_actualizacion.append({
                "id": mapa_woo[sku],
                "manage_stock": True,
                "stock_quantity": stock_real,      # El stock seguirá siendo 0 en la web
                "catalog_visibility": visibilidad  # Pero el producto estará visible
            })

    # 6. Envío por lotes (Batch)
    if not paquete_actualizacion:
        print("⚠️ No se encontraron coincidencias de SKU.")
        return

    print(f"🚀 Actualizando {len(paquete_actualizacion)} productos y su visibilidad...")
    lote_tamano = 100
    for i in range(0, len(paquete_actualizacion), lote_tamano):
        lote = paquete_actualizacion[i:i + lote_tamano]
        wcapi.post("products/batch", {"update": lote})
        print(f"✅ Lote {i//lote_tamano + 1} sincronizado.")

    print("🎉 ¡Sincronización terminada! Reglas de tránsito aplicadas.")

if __name__ == "__main__":
    sync_inventario_con_visibilidad()