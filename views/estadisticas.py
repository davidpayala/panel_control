import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE DetalleVenta ADD COLUMN IF NOT EXISTS macro_categoria VARCHAR(50)"))
        conn.execute(text("ALTER TABLE Productos ADD COLUMN IF NOT EXISTS macro_categoria VARCHAR(100)"))
except: pass

def render_estadisticas():
    if "ESTADISTICAS" not in st.session_state.get('modulos', []) and st.session_state.get('rol') != 'Admin':
        st.error("No tienes acceso a este módulo.")
        return

    st.title("📊 Panel de Estadísticas y Rendimiento")
    
    # ==========================================================================
    # 1. DATOS DE VENTAS 
    # ==========================================================================
    query_ventas = text("""
        SELECT 
            DATE_TRUNC('month', v.fecha_venta)::DATE AS "Mes",
            CASE 
                WHEN prod.macro_categoria ILIKE 'peluca%' OR var.sku ILIKE 'WB-%' OR var.sku ILIKE 'WIG-%' OR d.descripcion ILIKE '[Pelucas]%' THEN 'Pelucas'
                WHEN d.descripcion ILIKE '[Otros]%' OR d.sku IS NULL THEN 'Otros'
                ELSE 'Lentes'
            END AS "Macrocategoría",
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            SUM(d.cantidad) AS "Cantidad",
            SUM(d.subtotal) AS "Total"
        FROM ventas v
        JOIN detalleventa d ON v.id_venta = d.id_venta
        LEFT JOIN variantes var ON d.sku = var.sku
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        WHERE v.anulado = FALSE 
        GROUP BY 1, 2, 3
        ORDER BY 1 ASC;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query_ventas, conn)
    
    if not df.empty:
        df['Mes'] = pd.to_datetime(df['Mes'])
        
        st.subheader("📈 Resumen Mensual de Ventas")
        c_filt1, c_filt2 = st.columns(2)
        
        lista_macros = ["Todas"] + sorted(df['Macrocategoría'].unique().tolist())
        macro_sel = c_filt1.selectbox("📂 Línea de Negocio (Macro):", lista_macros, index=0)
        
        df_filt = df.copy() if macro_sel == "Todas" else df[df['Macrocategoría'] == macro_sel]
            
        lista_cats = ["Todas"] + sorted(df_filt['Categoría'].unique().tolist())
        cat_sel = c_filt2.selectbox("📑 Subcategoría:", lista_cats, index=0)
        
        if cat_sel != "Todas": df_filt = df_filt[df_filt['Categoría'] == cat_sel]
            
        df_mensual = df_filt.groupby('Mes', as_index=False)[['Cantidad', 'Total']].sum().sort_values('Mes')
        df_mensual['Mes_Grafico'] = df_mensual['Mes'].dt.strftime('%Y - %m')
        
        col_t1, col_t2 = st.columns([1, 2])
        with col_t1:
            df_tabla = df_mensual.copy()
            df_tabla['Mes'] = df_tabla['Mes'].dt.strftime('%Y - %m')
            st.dataframe(df_tabla.set_index('Mes')[['Cantidad', 'Total']], use_container_width=True)
            
        with col_t2:
            st.bar_chart(df_mensual, x='Mes_Grafico', y='Total')

        st.divider()

        st.subheader("💰 Desglose Detallado por Mes")
        df['Mes_Label'] = df['Mes'].dt.strftime('%Y - %m')
        meses_disponibles = sorted(df['Mes_Label'].unique().tolist(), reverse=True)
        
        if meses_disponibles:
            mes_seleccionado = st.selectbox("📅 Seleccionar mes para inspeccionar:", meses_disponibles)
            df_mes_actual = df[df['Mes_Label'] == mes_seleccionado]
            
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.markdown("**Comparativa por Línea Mayor**")
                df_mes_macro = df_mes_actual.groupby('Macrocategoría', as_index=False)[['Cantidad', 'Total']].sum()
                st.dataframe(df_mes_macro, use_container_width=True, hide_index=True)
                st.bar_chart(df_mes_macro.set_index('Macrocategoría')['Total'])
                
            with col_m2:
                st.markdown("**Desglose por Subcategoría**")
                df_mes_cat = df_mes_actual.groupby('Categoría', as_index=False)[['Cantidad', 'Total']].sum()
                st.dataframe(df_mes_cat, use_container_width=True, hide_index=True)
                st.bar_chart(df_mes_cat.set_index('Categoría')['Total'])
    else:
        st.info("No hay datos de ventas registradas.")

    st.divider()

    # ==========================================================================
    # 3. STOCK ACTUAL 
    # ==========================================================================
    st.subheader("📦 Almacén y Valorizado (Stock Actual)")
    query_stock = text("""
        SELECT 
            CASE 
                WHEN prod.macro_categoria ILIKE 'peluca%' OR var.sku ILIKE 'WB-%' OR var.sku ILIKE 'WIG-%' THEN 'Pelucas'
                ELSE 'Lentes'
            END AS "Macrocategoría",
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            SUM(var.stock_interno) AS "Stock"
        FROM variantes var
        JOIN productos prod ON var.id_producto = prod.id_producto
        GROUP BY 1, 2
    """)
    with engine.connect() as conn:
        df_stock = pd.read_sql(query_stock, conn)
    
    if not df_stock.empty:
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            st.markdown("**Volumen Físico por Línea Mayor**")
            df_stock_macro = df_stock.groupby('Macrocategoría', as_index=False)['Stock'].sum()
            st.dataframe(df_stock_macro, use_container_width=True, hide_index=True)
            st.bar_chart(df_stock_macro.set_index('Macrocategoría')['Stock'])

        with col_s2:
            st.markdown("**Distribución por Subcategoría**")
            macro_filtro_stk = st.selectbox("Filtrar Almacén por Línea Mayor:", ["Todas"] + sorted(df_stock['Macrocategoría'].unique().tolist()), key="stk_mac_fil")
            df_stk_cat = df_stock.groupby('Categoría', as_index=False)['Stock'].sum() if macro_filtro_stk == "Todas" else df_stock[df_stock['Macrocategoría'] == macro_filtro_stk].groupby('Categoría', as_index=False)['Stock'].sum()
            st.dataframe(df_stk_cat, use_container_width=True, hide_index=True)
            st.bar_chart(df_stk_cat.set_index('Categoría')['Stock'])

    # ==========================================================================
    # 4. EVOLUCIÓN HISTÓRICA DEL STOCK (¡DESACOPLADO POR LLAVE COMPUESTA!)
    # ==========================================================================
    st.subheader("📊 Evolución del Stock Histórico")
    
    query_mov = text("""
        SELECT 
            DATE_TRUNC('month', m.fecha)::DATE AS "Mes",
            CASE 
                WHEN prod.macro_categoria ILIKE 'peluca%' OR m.sku ILIKE 'WB-%' OR m.sku ILIKE 'WIG-%' THEN 'Pelucas'
                WHEN m.sku IS NULL THEN 'Otros'
                ELSE 'Lentes'
            END AS "Macrocategoría",
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            SUM(COALESCE(m.stock_nuevo, 0) - COALESCE(m.stock_anterior, 0)) AS "Neto"
        FROM movimientos m
        LEFT JOIN variantes var ON m.sku = var.sku
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        WHERE m.fecha IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1 ASC;
    """)
    with engine.connect() as conn:
        df_mov = pd.read_sql(query_mov, conn)

    if not df_mov.empty and not df_stock.empty:
        df_mov['Mes'] = pd.to_datetime(df_mov['Mes'])
        
        # 1. CREACIÓN DE LA LLAVE COMPUESTA ANTI-HOMONIMIA
        df_mov['Cat_Compuesta'] = df_mov['Macrocategoría'] + " -> " + df_mov['Categoría']
        df_stock['Cat_Compuesta'] = df_stock['Macrocategoría'] + " -> " + df_stock['Categoría']

        df_pivot = df_mov.pivot_table(index='Mes', columns='Cat_Compuesta', values='Neto', aggfunc='sum').fillna(0)
        stock_actual_serie = df_stock.set_index('Cat_Compuesta')['Stock']

        for cat in stock_actual_serie.index:
            if cat not in df_pivot.columns: df_pivot[cat] = 0.0

        movimientos_totales = df_pivot.sum()
        stock_base = stock_actual_serie.fillna(0) - movimientos_totales.fillna(0)
        
        df_acumulado = df_pivot.cumsum() + stock_base
        df_acumulado.index = df_acumulado.index.strftime('%Y - %m')
        
        col_e1, col_e2 = st.columns([1, 2])
        macro_evo_sel = col_e1.selectbox("📂 Aislar Curva por Línea Mayor:", ["Todas", "Lentes", "Pelucas", "Otros"], key="evo_mac_sel")
        
        todas_compuestas = sorted(df_acumulado.columns.tolist())

        if macro_evo_sel != "Todas":
            opciones_grafico = [c for c in todas_compuestas if c.startswith(f"{macro_evo_sel} -> ")]
        else:
            opciones_grafico = todas_compuestas
            
        default_grafico = [c for c in opciones_grafico if not c.startswith("Otros -> ")]
        if not default_grafico and opciones_grafico: default_grafico = opciones_grafico

        # 2. SELECTOR MÚLTIPLE INTELIGENTE CON FORMATEADOR VISUAL
        categorias_seleccionadas = col_e2.multiselect(
            "📑 Subcategorías a graficar en el tiempo:",
            options=opciones_grafico,
            default=default_grafico,
            format_func=lambda x: f"[{x.split(' -> ')[0]}] {x.split(' -> ')[1]}" if macro_evo_sel == "Todas" else x.split(" -> ")[1]
        )
        
        if categorias_seleccionadas:
            st.line_chart(df_acumulado[categorias_seleccionadas])
            with st.expander("🔍 Ver tabla detallada de la evolución del stock"):
                # Limpiamos los títulos de la tabla para que sea legible en Excel
                df_render_tabla = df_acumulado[categorias_seleccionadas].copy()
                df_render_tabla.columns = [c.replace(" -> ", " -> ") for c in df_render_tabla.columns]
                st.dataframe(df_render_tabla, use_container_width=True)
        else:
            st.warning("⚠️ Selecciona al menos una subcategoría.")
    else:
        st.info("Aún no hay suficientes movimientos de inventario registrados.")