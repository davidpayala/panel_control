import os
import time
from dotenv import load_dotenv
from woocommerce import API
from sqlalchemy import create_engine, text

# 1. Cargar las llaves de seguridad
load_dotenv()

def sincronizar_tienda_woo(nombre_tienda, wcapi, stock_local_tienda):
    """
    Motor 'Obrero' reutilizable: Descarga, mapea y actualiza una instancia específica de WooCommerce.
    """
    print(f"\n==============================================================")
    print(f"🌐 INICIANDO SINCRONIZACIÓN WEB PARA: {nombre_tienda.upper()}")
    print(f"==============================================================")

    if not stock_local_tienda:
        print(f"ℹ️ OMITIDO: No hay SKUs locales clasificados para {nombre_tienda}.")
        return

    # 1. Descargar y mapear el catálogo de esta web específica
    print(f"📥 Descargando y mapeando catálogo web de {nombre_tienda}...")
    mapa_simples = {}
    mapa_variaciones = {} 
    
    pagina = 1
    while True:
        resp = wcapi.get("products", params={"per_page": 100, "page": pagina})
        items = resp.json()
        if not items: 
            break
            
        for p in items:
            p_id = p["id"]
            p_sku = p.get("sku", "").strip()
            p_type = p.get("type")
            
            if p_type == "variable":
                if p_id not in mapa_variaciones:
                    mapa_variaciones[p_id] = {}
                v_pagina = 1
                while True:
                    v_resp = wcapi.get(f"products/{p_id}/variations", params={"per_page": 100, "page": v_pagina})
                    v_items = v_resp.json()
                    if not v_items: 
                        break
                    for v in v_items:
                        v_sku = v.get("sku", "").strip()
                        if v_sku:
                            mapa_variaciones[p_id][v_sku] = v["id"]
                    v_pagina += 1
            elif p_sku:
                mapa_simples[p_sku] = p_id
                
        pagina += 1

    print(f"🔗 Mapeo de {nombre_tienda} listo. Simples: {len(mapa_simples)}. Modelos variables: {len(mapa_variaciones)}.")

    # 2. Reporte de SKUs no encontrados
    woo_skus_totales = set(mapa_simples.keys())
    for variaciones in mapa_variaciones.values():
        woo_skus_totales.update(variaciones.keys())

    skus_no_encontrados = [sku for sku in stock_local_tienda.keys() if sku not in woo_skus_totales]
    if skus_no_encontrados:
        print(f"⚠️ AVISO: Hay {len(skus_no_encontrados)} SKUs en PostgreSQL que NO existen en la web de {nombre_tienda}.")
    else:
        print(f"✅ Todos los SKUs locales de {nombre_tienda} matchean con la web.")

    # 3. Procesar y empaquetar Productos Simples
    paquete_simples = []
    for sku, data in stock_local_tienda.items():
        if sku in mapa_simples:
            stock_real = data["total"]
            stock_camino = data["transito"]
            visibilidad = "visible" if (stock_real > 0 or stock_camino > 0) else "hidden"
            
            paquete_simples.append({
                "id": mapa_simples[sku],
                "manage_stock": True,
                "stock_quantity": stock_real,
                "catalog_visibility": visibilidad
            })

    if paquete_simples:
        print(f"🚀 Enviando {len(paquete_simples)} simples a {nombre_tienda}...")
        lote_tamano = 100
        for i in range(0, len(paquete_simples), lote_tamano):
            lote = paquete_simples[i:i + lote_tamano]
            wcapi.post("products/batch", {"update": lote})
            print(f"   ✅ Lote simples {i//lote_tamano + 1} de {nombre_tienda} completado.")

    # 4. Procesar y empaquetar Variaciones
    print(f"🚀 Enviando lotes de variaciones a {nombre_tienda}...")
    for parent_id, variaciones in mapa_variaciones.items():
        paquete_variaciones_padre = []
        for sku, var_id in variaciones.items():
            if sku in stock_local_tienda:
                paquete_variaciones_padre.append({
                    "id": var_id,
                    "manage_stock": True,
                    "stock_quantity": stock_local_tienda[sku]["total"]
                })
        
        if paquete_variaciones_padre:
            wcapi.post(f"products/{parent_id}/variations/batch", {"update": paquete_variaciones_padre})
            print(f"   ✅ Variaciones del producto padre #{parent_id} actualizadas.")

# ==============================================================================
# FUNCIÓN ORQUESTADORA PRINCIPAL
# ==============================================================================
def sync_inventario_completo():
    print("⏳ Extrayendo inventario maestro unificado de PostgreSQL...")
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
        with engine.connect() as conn:
            # Traemos la macro_categoria para saber a qué tienda pertenece cada SKU
            query = text("""
                SELECT v.sku, 
                       (COALESCE(v.stock_interno, 0) + COALESCE(v.stock_externo, 0)) AS stock_total,
                       COALESCE(v.stock_transito, 0) AS stock_transito,
                       COALESCE(p.macro_categoria, 'Lentes') AS macro_categoria
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.sku IS NOT NULL AND TRIM(v.sku) != ''
            """)
            resultados = conn.execute(query).fetchall()

        # Partición local en dos diccionarios independientes
        stock_lentes = {}
        stock_pelucas = {}

        for r in resultados:
            sku = r.sku.strip()
            item_data = {"total": r.stock_total, "transito": r.stock_transito}
            
            if r.macro_categoria == "Pelucas":
                stock_pelucas[sku] = item_data
            else:
                stock_lentes[sku] = item_data

        print(f"📦 Reparto local listo -> LENTES: {len(stock_lentes)} SKUs | PELUCAS: {len(stock_pelucas)} SKUs.")

    except Exception as e:
        print(f"🔥 Error crítico al conectar con PostgreSQL: {e}")
        return

    # --- DISPARO 1: TIENDA DE LENTES ---
    url_lentes = os.getenv("WOO_LENTES_URL")
    key_lentes = os.getenv("WOO_LENTES_KEY")
    sec_lentes = os.getenv("WOO_LENTES_SECRET")

    if url_lentes and key_lentes and sec_lentes:
        wcapi_lentes = API(url=url_lentes, consumer_key=key_lentes, consumer_secret=sec_lentes, version="wc/v3", timeout=60)
        sincronizar_tienda_woo("Lentes (kmlentes.pe)", wcapi_lentes, stock_lentes)
    else:
        print("\n⚠️ OMITIDO: No se encontraron credenciales de WOO_LENTES en el archivo .env")

    # --- DISPARO 2: TIENDA DE PELUCAS ---
    url_pelucas = os.getenv("WOO_PELUCAS_URL")
    key_pelucas = os.getenv("WOO_PELUCAS_KEY")
    sec_pelucas = os.getenv("WOO_PELUCAS_SECRET")

    if url_pelucas and key_pelucas and sec_pelucas:
        wcapi_pelucas = API(url=url_pelucas, consumer_key=key_pelucas, consumer_secret=sec_pelucas, version="wc/v3", timeout=60)
        sincronizar_tienda_woo("Pelucas (pelucat.pe)", wcapi_pelucas, stock_pelucas)
    else:
        print("\n⚠️ OMITIDO: No se encontraron credenciales de WOO_PELUCAS en el archivo .env")

    print("\n🎉 ¡Sincronización masiva de ambas tiendas completada con éxito!")

if __name__ == "__main__":
    sync_inventario_completo()