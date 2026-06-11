import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

def render_estadisticas():
    if "ESTADISTICAS" not in st.session_state.get('modulos', []) and st.session_state.get('rol') != 'Admin':
        st.error("No tienes acceso a este módulo.")
        return

    st.title("📊 Panel de Estadísticas")
    
    # --------------------------------------------------------
    # 1. DATOS DE VENTAS
    # --------------------------------------------------------
    query_ventas = text("""
        SELECT 
            DATE_TRUNC('month', v.fecha_venta)::DATE AS "Mes",
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            SUM(d.cantidad) AS "Cantidad",
            SUM(d.subtotal) AS "Total"
        FROM ventas v
        JOIN detalleventa d ON v.id_venta = d.id_venta
        LEFT JOIN variantes var ON d.sku = var.sku
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        WHERE v.anulado = FALSE 
        GROUP BY 1, 2
        ORDER BY 1 ASC;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query_ventas, conn)
    
    if not df.empty:
        df['Mes'] = pd.to_datetime(df['Mes'])
        
        # --- 1. RESUMEN MENSUAL GENERAL CON FILTRO ---
        st.subheader("📈 Resumen Mensual General")
        
        # Filtro de categoría
        lista_categorias = ["Todas"] + sorted(df['Categoría'].unique().tolist())
        cat_seleccionada = st.selectbox("Filtrar por Categoría:", lista_categorias, index=0)
        
        if cat_seleccionada == "Todas":
            df_filtrado = df.copy()
        else:
            df_filtrado = df[df['Categoría'] == cat_seleccionada]
            
        # Agrupamos manteniendo el Mes original para ordenar cronológicamente
        df_mensual = df_filtrado.groupby('Mes', as_index=False)[['Cantidad', 'Total']].sum()
        df_mensual = df_mensual.sort_values('Mes')
        
        # SOLUCIÓN EJE X (Tu idea): Usamos YYYY - MM para que el orden alfabético sea igual al cronológico
        df_mensual['Mes_Grafico'] = df_mensual['Mes'].dt.strftime('%Y - %m')
        
        col_t1, col_t2 = st.columns([1, 2])
        
        with col_t1:
            df_tabla = df_mensual.copy()
            # La tabla también usará el nuevo formato para mantener la coherencia visual
            df_tabla['Mes'] = df_tabla['Mes'].dt.strftime('%Y - %m')
            st.dataframe(df_tabla.set_index('Mes')[['Cantidad', 'Total']], use_container_width=True)
            
        with col_t2:
            st.bar_chart(df_mensual, x='Mes_Grafico', y='Total')

        st.divider()

        # --- 2. DESGLOSE POR CATEGORÍA ---
        st.subheader("💰 Ventas Detalladas por Mes")
        
        df['Mes_Label'] = df['Mes'].dt.strftime('%Y - %m')
        meses_disponibles = df_mensual['Mes_Grafico'].unique()
        
        if len(meses_disponibles) > 0:
            mes_seleccionado = st.selectbox("Seleccionar mes para ver detalle:", meses_disponibles)
            
            df_detalle = df[df['Mes_Label'] == mes_seleccionado]
            st.dataframe(df_detalle[['Categoría', 'Cantidad', 'Total']], use_container_width=True, hide_index=True)
            
            st.bar_chart(df_detalle.set_index('Categoría')['Total'])
            
    else:
        st.info("No hay datos de ventas para mostrar.")

    st.divider()

    # --------------------------------------------------------
    # 3. STOCK ACTUAL
    # --------------------------------------------------------
    st.subheader("📦 Stock Actual por Categoría")
    query_stock = text("""
        SELECT COALESCE(prod.categoria, 'Otros') AS "Categoría", SUM(var.stock_interno) AS "Stock"
        FROM variantes var
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        GROUP BY 1
    """)
    with engine.connect() as conn:
        df_stock = pd.read_sql(query_stock, conn)
    
    if not df_stock.empty:
        st.bar_chart(df_stock.set_index('Categoría')['Stock'])

# --------------------------------------------------------
    # 4. EVOLUCIÓN HISTÓRICA DEL STOCK 
    # --------------------------------------------------------
    st.subheader("📊 Evolución del Stock Histórico")
    
    query_mov = text("""
        SELECT 
            DATE_TRUNC('month', m.fecha)::DATE AS "Mes",
            COALESCE(prod.categoria, 'Otros') AS "Categoría",
            SUM(COALESCE(m.stock_nuevo, 0) - COALESCE(m.stock_anterior, 0)) AS "Neto"
        FROM movimientos m
        LEFT JOIN variantes var ON m.sku = var.sku
        LEFT JOIN productos prod ON var.id_producto = prod.id_producto
        GROUP BY 1, 2
        ORDER BY 1 ASC;
    """)
    
    with engine.connect() as conn:
        df_mov = pd.read_sql(query_mov, conn)

    if not df_mov.empty and not df_stock.empty:
        df_mov['Mes'] = pd.to_datetime(df_mov['Mes'])
        df_pivot = df_mov.pivot_table(index='Mes', columns='Categoría', values='Neto', aggfunc='sum').fillna(0)
        
        stock_actual_serie = df_stock.set_index('Categoría')['Stock']

        # Asegurarnos de que todas las categorías del stock actual estén en nuestro pivot
        for cat in stock_actual_serie.index:
            if cat not in df_pivot.columns:
                df_pivot[cat] = 0.0

        # Calculamos la base: Stock Inicial = Stock Actual - Todos los movimientos
        movimientos_totales = df_pivot.sum()
        stock_base = stock_actual_serie.fillna(0) - movimientos_totales.fillna(0)
        
        # Generamos la evolución real
        df_acumulado = df_pivot.cumsum() + stock_base
        
        # Formateo del Eje X a texto ordenado
        df_acumulado.index = df_acumulado.index.strftime('%Y - %m')
        
        # --- NUEVO: FILTRO MULTISELECCIÓN ---
        categorias_disponibles = df_acumulado.columns.tolist()
        
        # Por defecto seleccionamos todas las categorías MENOS "Otros"
        categorias_por_defecto = [cat for cat in categorias_disponibles if cat != 'Otros']
        
        categorias_seleccionadas = st.multiselect(
            "Filtrar categorías a mostrar en el gráfico:",
            options=categorias_disponibles,
            default=categorias_por_defecto
        )
        
        # Dibujamos el gráfico solo si hay al menos una categoría seleccionada
        if categorias_seleccionadas:
            st.line_chart(df_acumulado[categorias_seleccionadas])
            
            with st.expander("Ver tabla detallada de la evolución del stock"):
                st.dataframe(df_acumulado[categorias_seleccionadas], use_container_width=True)
        else:
            st.warning("⚠️ Selecciona al menos una categoría para visualizar el gráfico.")
    else:
        st.info("Aún no hay suficientes movimientos de inventario registrados para generar la evolución.")