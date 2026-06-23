import os
import time
from datetime import datetime
from dotenv import load_dotenv
from woocommerce import API
from sqlalchemy import create_engine, text

# 1. Cargar las llaves de seguridad
load_dotenv()

def sincronizar_tienda_woo(engine, nombre_tienda, wcapi, stock_local_tienda):
    """
    Motor 'Obrero' con observabilidad: Mapea, concilia estrictamente por SKU y emite métricas.
    """
    hora_inicio = datetime.now()
    str_hora_inicio = hora_inicio.strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n==============================================================")
    print(f"🌐 [{str_hora_inicio}] INICIANDO SINCRONIZACIÓN PARA: {nombre_tienda.upper()}")
    print(f"==============================================================")

    if not stock_local_tienda:
        print(f"ℹ️ OMITIDO: No hay SKUs locales clasificados para {nombre_tienda}.")
        return

    # 1. Descargar y mapear el catálogo web basado 100% en SKUs
    print(f"📥 Descargando y mapeando catálogo web de {nombre_tienda}...")
    
    mapa_simples = {}         # {sku: product_id}
    mapa_variaciones = {}     # {parent_id: {sku: variation_id}}
    nombres_web = {}          # {sku: "Nombre Web (Solo para lectura humana)"}
    woo_skus_totales = set()  # Saco absoluto de todos los SKUs vivos en esta web

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
            p_title = p.get("name", "Producto sin título")
            
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
                        if v_sku: # IDENTIDAD BASADA ESTRICTAMENTE EN EL SKU
                            mapa_variaciones[p_id][v_sku] = v["id"]
                            woo_skus_totales.add(v_sku)
                            
                            opciones = [attr.get('option', '') for attr in v.get('attributes', [])]
                            nombres_web[v_sku] = f"{p_title} ({' '.join(opciones)})".strip()
                    v_pagina += 1
            elif p_sku: # Producto Simple con SKU
                mapa_simples[p_sku] = p_id
                woo_skus_totales.add(p_sku)
                nombres_web[p_sku] = p_title
                
        pagina += 1

    print(f"🔗 Mapeo de {nombre_tienda} listo. SKUs totales detectados en la Web: {len(woo_skus_totales)}.")

    # =========================================================================
    # 2. AUDITORÍA CRUZADA DE CONJUNTOS POR SKU (Puntos 3 y 4 del usuario)
    # =========================================================================
    # Saco absoluto de SKUs de la Base de Datos para esta tienda
    db_skus_totales = set(stock_local_tienda.keys())

    # RESTA MATEMÁTICA DE CONJUNTOS
    faltan_en_woo = db_skus_totales - woo_skus_totales  # Viven en Postgres, no están en WordPress
    faltan_en_db = woo_skus_totales - db_skus_totales   # Viven en WordPress, no están en Postgres

    with engine.begin() as conn:
        # Purgar los desajustes anteriores estrictamente de esta tienda
        conn.execute(text("DELETE FROM auditoria_skus_woo WHERE tienda = :t"), {"t": nombre_tienda})
        
        # Guardar Tipo A: Faltan en la Web
        for sku_err in faltan_en_woo:
            desc_db = stock_local_tienda[sku_err].get("nombre_ref", "Ítem en Postgres")
            conn.execute(text("""
                INSERT INTO auditoria_skus_woo (tienda, tipo_error, sku, detalle) 
                VALUES (:t, 'FALTA_EN_WOO', :s, :d)
            """), {"t": nombre_tienda, "s": sku_err, "d": desc_db})

        # Guardar Tipo B: Faltan en la Base de Datos
        for sku_err in faltan_en_db:
            desc_w = nombres_web.get(sku_err, "Ítem en WordPress")
            conn.execute(text("""
                INSERT INTO auditoria_skus_woo (tienda, tipo_error, sku, detalle) 
                VALUES (:t, 'FALTA_EN_DB', :s, :d)
            """), {"t": nombre_tienda, "s": sku_err, "d": desc_w})

    # Feedback en consola de los desajustes
    if faltan_en_woo: print(f"  ⚠️ [FALTA EN WOO]: {len(faltan_en_woo)} SKUs locales no existen en la tienda web.")
    if faltan_en_db:  print(f"  ⚠️ [FALTA EN BD]:  {len(faltan_en_db)} SKUs de la web no están registrados en PostgreSQL.")
    if not faltan_en_woo and not faltan_en_db:
        print("  ✅ CONCILIACIÓN PERFECTA: 100% de match de SKUs entre base de datos y catálogo web.")

    # =========================================================================
    # 3. ENVÍO DE LOTES (Usando el SKU como puente hacia el ID de Woo)
    # =========================================================================
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

    simples_enviados_ok = 0
    simples_objetivo = len(paquete_simples)

    if paquete_simples:
        print(f"🚀 Enviando {simples_objetivo} simples a {nombre_tienda}...")
        lote_tamano = 100
        for i in range(0, len(paquete_simples), lote_tamano):
            lote = paquete_simples[i:i + lote_tamano]
            try:
                wcapi.post("products/batch", {"update": lote})
                simples_enviados_ok += len(lote)
            except Exception as e:
                print(f"❌ Error en lote simples: {e}")

    # Variaciones
    lotes_variaciones = []
    for parent_id, variaciones in mapa_variaciones.items():
        paq_v = []
        for sku, var_id in variaciones.items():
            if sku in stock_local_tienda:
                paq_v.append({
                    "id": var_id,
                    "manage_stock": True,
                    "stock_quantity": stock_local_tienda[sku]["total"]
                })
        if paq_v: lotes_variaciones.append((parent_id, paq_v))

    vars_enviados_ok = 0
    vars_objetivo = sum(len(p) for _, p in lotes_variaciones)

    if lotes_variaciones:
        print(f"🚀 Enviando variaciones a {nombre_tienda}...")
        for parent_id, paq_v in lotes_variaciones:
            try:
                wcapi.post(f"products/{parent_id}/variations/batch", {"update": paq_v})
                vars_enviados_ok += len(paq_v)
            except Exception as e:
                print(f"❌ Error en variaciones del padre #{parent_id}: {e}")

    # =========================================================================
    # 4. CÁLCULO DE PORCENTAJES RESUMIDOS (Punto 2 del usuario)
    # =========================================================================
    pct_simples = (simples_enviados_ok / simples_objetivo * 100) if simples_objetivo > 0 else 100.0
    pct_vars = (vars_enviados_ok / vars_objetivo * 100) if vars_objetivo > 0 else 100.0
    
    duracion = datetime.now() - hora_inicio

    print(f"\n📊 RESUMEN DE TAREAS PARA {nombre_tienda.upper()}:")
    print(f"   • Lotes Simples:  {pct_simples:.0f}% completado ({simples_enviados_ok}/{simples_objetivo})")
    print(f"   • Variaciones:    {pct_vars:.0f}% completado ({vars_enviados_ok}/{vars_objetivo})")
    print(f"   • Tiempo tomado:  {str(duracion).split('.')[0]}")

# ==============================================================================
# FUNCIÓN ORQUESTADORA PRINCIPAL
# ==============================================================================
def sync_inventario_completo():
    print("⏳ Extrayendo inventario maestro unificado de PostgreSQL...")
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
        
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS auditoria_skus_woo (
                    id SERIAL PRIMARY KEY,
                    tienda VARCHAR(50),
                    tipo_error VARCHAR(50),
                    sku VARCHAR(100),
                    detalle VARCHAR(255),
                    fecha_deteccion TIMESTAMP DEFAULT NOW()
                );
            """))

        with engine.connect() as conn:
            query = text("""
                SELECT v.sku, 
                       (COALESCE(v.stock_interno, 0) + COALESCE(v.stock_externo, 0)) AS stock_total,
                       COALESCE(v.stock_transito, 0) AS stock_transito,
                       COALESCE(p.macro_categoria, 'Lentes') AS macro_categoria,
                       CONCAT(COALESCE(p.marca, ''), ' ', COALESCE(p.modelo, ''), ' ', COALESCE(p.nombre, '')) AS nombre_ref
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.sku IS NOT NULL AND TRIM(v.sku) != ''
            """)
            resultados = conn.execute(query).fetchall()

        stock_lentes, stock_pelucas = {}, {}

        for r in resultados:
            sku = r.sku.strip()
            item_data = {
                "total": r.stock_total, 
                "transito": r.stock_transito,
                "nombre_ref": r.nombre_ref.strip()
            }
            if r.macro_categoria == "Pelucas": stock_pelucas[sku] = item_data
            else: stock_lentes[sku] = item_data

        print(f"📦 Reparto local listo -> LENTES: {len(stock_lentes)} SKUs | PELUCAS: {len(stock_pelucas)} SKUs.")

    except Exception as e:
        print(f"🔥 Error crítico al conectar con PostgreSQL: {e}")
        return

    # --- DISPARO 1: LENTES ---
    url_lentes, key_lentes, sec_lentes = os.getenv("WOO_LENTES_URL"), os.getenv("WOO_LENTES_KEY"), os.getenv("WOO_LENTES_SECRET")
    if url_lentes and key_lentes and sec_lentes:
        wcapi_lentes = API(url=url_lentes, consumer_key=key_lentes, consumer_secret=sec_lentes, version="wc/v3", timeout=60)
        sincronizar_tienda_woo(engine, "Lentes (kmlentes.pe)", wcapi_lentes, stock_lentes)

    # --- DISPARO 2: PELUCAS ---
    url_pelucas, key_pelucas, sec_pelucas = os.getenv("WOO_PELUCAS_URL"), os.getenv("WOO_PELUCAS_KEY"), os.getenv("WOO_PELUCAS_SECRET")
    if url_pelucas and key_pelucas and sec_pelucas:
        wcapi_pelucas = API(url=url_pelucas, consumer_key=key_pelucas, consumer_secret=sec_pelucas, version="wc/v3", timeout=60)
        sincronizar_tienda_woo(engine, "Pelucas (pelucat.pe)", wcapi_pelucas, stock_pelucas)

    print("\n🎉 ¡Orquestación de inventario completada!")

if __name__ == "__main__":
    sync_inventario_completo()