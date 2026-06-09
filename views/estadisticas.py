import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

def render_estadisticas():
    if "ESTADISTICAS" not in st.session_state.get('modulos', []) and st.session_state.get('rol') != 'Admin':
        st.error("No tienes acceso a este módulo.")
        return

    st.title("📊 Panel de Estadísticas")
    
    # Consulta única para obtener todo el detalle
    query = text("""
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
        ORDER BY 1 DESC, 4 DESC;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    if not df.empty:
        # Formatear Mes
        df['Mes_Label'] = pd.to_datetime(df['Mes']).dt.strftime('%b %Y')
        
        # --- 1. RESUMEN MENSUAL GENERAL ---
        st.subheader("📈 Resumen Mensual General")
        
        # 1. Agrupamos por mes
        df_mensual = df.groupby('Mes')[['Cantidad', 'Total']].sum().sort_index(ascending=True)
        
        # 2. ASEGURAR QUE EL ÍNDICE SEA DATETIME ANTES DE USAR .dt o .strftime
        df_mensual.index = pd.to_datetime(df_mensual.index)
        
        # 3. Creamos la etiqueta de texto para visualizar
        df_visual = df_mensual.copy()
        df_visual.index = df_visual.index.strftime('%b %Y')
        
        col_t1, col_t2 = st.columns([1, 2])
        with col_t1:
            st.dataframe(df_visual, use_container_width=True)
            
        with col_t2:
            # 4. Usamos el DataFrame formateado para el gráfico
            # Como el índice es string, forzamos un orden lógico pasando el df ya ordenado
            st.bar_chart(df_visual['Total'])

        st.divider()

        # --- 2. DESGLOSE POR CATEGORÍA ---
        st.subheader("💰 Ventas Detalladas por Categoría")
        
        # Filtro de mes para el desglose
        meses = df['Mes_Label'].unique()
        mes_seleccionado = st.selectbox("Seleccionar mes para ver detalle:", meses)
        
        df_detalle = df[df['Mes_Label'] == mes_seleccionado]
        st.dataframe(df_detalle[['Categoría', 'Cantidad', 'Total']], use_container_width=True, hide_index=True)
        
        # Gráfico de categorías para ese mes
        st.bar_chart(df_detalle.set_index('Categoría')['Total'])
        
    else:
        st.info("No hay datos de ventas para mostrar.")

    # 3. Stock Actual
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