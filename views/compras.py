import streamlit as st
import pandas as pd
import time
import io
from datetime import datetime, date
from sqlalchemy import text
from database import engine

def render_compras():
    st.subheader("🚢 Gestión de Importaciones y Reposición")

    tab_asistente, tab_pedir, tab_recepcionar = st.tabs([
        "💡 Asistente de Compras (IA)", 
        "✈️ Registrar Compra (AliExpress)", 
        "📦 Recepcionar Mercadería (Llegó)"
    ])

    # -------------------------------------------------------------------------
    # A) ASISTENTE INTELIGENTE
    # -------------------------------------------------------------------------
    with tab_asistente:
        # 1. CONTROLES
        with st.container(border=True):
            c_filtros, c_acciones = st.columns([3, 1])
            with c_filtros:
                st.markdown("**Configuración del Reporte**")
                col_f1, col_f2, col_f3 = st.columns(3)
                
                umbral_stock = col_f1.slider("Alerta Stock bajo (<):", 0, 50, 5)
                solo_con_externo = col_f2.checkbox("Stock en Proveedor", value=True)
                
                filtro_ali = col_f3.radio(
                    "Filtro AliExpress:", 
                    ["Todos", "Con Link", "Sin Link"], 
                    index=0,
                    horizontal=True
                )
            
            with c_acciones:
                st.write("")
                if st.button("🔄 Actualizar Tabla", type="primary", width='stretch'):
                    st.rerun()

        # 2. DEFINIR AÑOS
        year_actual = datetime.now().year 
        y1, y2, y3 = year_actual, year_actual - 1, year_actual - 2 

        def get_hist_sql(year):
            return f"COALESCE(h.v{year}, 0)" if year <= 2025 else "0"

        # 3. CONSULTA HÍBRIDA
        with engine.connect() as conn:
            try:
                hist_y3, hist_y2, hist_y1 = get_hist_sql(y3), get_hist_sql(y2), get_hist_sql(y1)

                query_hybrid = text(f"""
                    WITH VentasSQL AS (
                        SELECT 
                            d.sku,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y3 THEN d.cantidad ELSE 0 END) as sql_y3,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y2 THEN d.cantidad ELSE 0 END) as sql_y2,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y1 THEN d.cantidad ELSE 0 END) as sql_y1
                        FROM DetalleVenta d
                        JOIN Ventas v ON d.id_venta = v.id_venta
                        GROUP BY d.sku
                    )
                    SELECT 
                        v.sku, 
                        p.marca || ' ' || p.modelo || ' - ' || COALESCE(p.nombre, '') || ' (' || v.medida || ')' as nombre,
                        v.stock_interno,
                        v.stock_externo,
                        COALESCE(v.stock_transito, 0) as stock_transito,
                        p.importacion,
                        p.url_compra,
                        ({hist_y3} + COALESCE(live.sql_y3, 0)) as venta_year_3,
                        ({hist_y2} + COALESCE(live.sql_y2, 0)) as venta_year_2,
                        ({hist_y1} + COALESCE(live.sql_y1, 0)) as venta_year_1
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    LEFT JOIN HistorialAnual h ON v.sku = h.sku
                    LEFT JOIN VentasSQL live ON v.sku = live.sku
                    WHERE (v.stock_interno + COALESCE(v.stock_transito, 0)) <= :umbral
                """)
                
                df_reco = pd.read_sql(query_hybrid, conn, params={"umbral": umbral_stock, "y1": y1, "y2": y2, "y3": y3})
                
                if not df_reco.empty:
                    df_reco['demanda_historica'] = df_reco['venta_year_1'] + df_reco['venta_year_2'] + df_reco['venta_year_3']
                    df_reco['sugerencia_compra'] = df_reco['demanda_historica'] - (df_reco['stock_interno'] + df_reco['stock_transito'])
                    df_reco['sugerencia_compra'] = df_reco['sugerencia_compra'].clip(lower=0)

            except Exception as e:
                st.error(f"⚠️ Error en consulta: {e}")
                df_reco = pd.DataFrame()

        # 4. FILTROS
        if not df_reco.empty:
            df_reco['sku'] = df_reco['sku'].astype(str).str.strip()
            
            # Filtro 1: Stock Externo
            if solo_con_externo:
                df_reco = df_reco[df_reco['stock_externo'] > 0]
            
            # Filtro 2: Lógica AliExpress (Apuntando a url_compra)
            if filtro_ali == "Con Link":
                df_reco = df_reco[df_reco['url_compra'].notna() & (df_reco['url_compra'] != '')]
            elif filtro_ali == "Sin Link":
                df_reco = df_reco[df_reco['url_compra'].isna() | (df_reco['url_compra'] == '')]

            patron_medida = r'-\d{4}$'
            es_medida = df_reco['sku'].str.contains(patron_medida, regex=True, na=False)
            es_base = df_reco['sku'].str.endswith('-0000', na=False)
            df_reco = df_reco[~es_medida | es_base]
            
            df_reco = df_reco.sort_values(by='sugerencia_compra', ascending=False)

        # 5. VISUALIZACIÓN
        st.divider()
        col_res_txt, col_res_btn = st.columns([3, 1])
        with col_res_txt:
            st.markdown(f"### 📋 Sugerencias de Compra ({len(df_reco)} items)")
            if filtro_ali == "Sin Link":
                st.caption("Mostrando productos que **NO** tienen enlace de importación configurado.")

        with col_res_btn:
            if not df_reco.empty:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_reco.to_excel(writer, index=False, sheet_name='SugerenciaCompra')
                st.download_button("📥 Descargar Excel", data=buffer.getvalue(), file_name=f"Compras_{date.today()}.xlsx", width='stretch')

        st.dataframe(
            df_reco,
            column_config={
                "sku": "SKU",
                "nombre": st.column_config.TextColumn("Producto", width="large"),
                "importacion": None, # Ocultamos la columna de texto de importacion
                "url_compra": st.column_config.LinkColumn("Link Ali", display_text="Ver Link"), # Mostramos el enlace real
                "stock_interno": st.column_config.NumberColumn("En Mano", format="%d"),
                "stock_transito": st.column_config.NumberColumn("En Camino", format="%d"),
                "stock_externo": None,
                "venta_year_3": st.column_config.NumberColumn(str(y3), format="%d"), # Cabecera dinámica
                "venta_year_2": st.column_config.NumberColumn(str(y2), format="%d"), # Cabecera dinámica
                "venta_year_1": st.column_config.NumberColumn(str(y1), format="%d"), # Cabecera dinámica
                "sugerencia_compra": st.column_config.NumberColumn("⚠️ Sugerido", format="%d"),
                "demanda_historica": st.column_config.ProgressColumn("Demanda Hist.", format="%d", min_value=0, max_value=int(df_reco['demanda_historica'].max()) if not df_reco.empty else 10),
            },
            hide_index=True,
            width='stretch'
        )
    # -------------------------------------------------------------------------
    # B) REGISTRAR PEDIDO
    # -------------------------------------------------------------------------
    with tab_pedir:
        st.info("✈️ Registra aquí tus pagos en AliExpress para sumarlos a 'En Camino'.")
        sku_pedido_raw = st.text_input("SKU a Importar:", key="sku_pedir")
        sku_pedido = sku_pedido_raw.strip() if sku_pedido_raw else ""
        
        if sku_pedido:
            with engine.connect() as conn:
                res = pd.read_sql(text("SELECT sku, stock_transito FROM Variantes WHERE sku = :s"), conn, params={"s": sku_pedido})
            
            if not res.empty:
                curr_transito = int(res.iloc[0]['stock_transito'] or 0)
                st.success(f"Producto encontrado. En camino actual: **{curr_transito}**")
                
                with st.form("form_pedido_ali"):
                    cant_pedido = st.number_input("Cantidad Comprada:", min_value=1, step=1)
                    nota_pedido = st.text_input("Nota / ID Pedido:", help="Ej: Pedido #81234")
                    fecha_pedido = st.date_input("Fecha de Compra:", value=date.today())
                    
                    if st.form_submit_button("✈️ Registrar 'En Camino'", width='stretch'):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                conn.execute(text("UPDATE Variantes SET stock_transito = stock_transito + :c WHERE sku=:s"), 
                                                        {"c": cant_pedido, "s": sku_pedido})
                                
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, fecha)
                                    VALUES (:s, 'PEDIDO_IMPORT', :c, :ant, :nue, :nota, :fec)
                                """), {"s": sku_pedido, "c": cant_pedido, "ant": curr_transito, "nue": curr_transito + cant_pedido, "nota": nota_pedido, "fec": fecha_pedido})
                                
                                trans.commit()
                                st.success("✅ Registrado correctamente.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error: {e}")
            else:
                st.warning(f"⚠️ El SKU '{sku_pedido}' no existe.")

    # -------------------------------------------------------------------------
    # C) RECEPCIONAR MERCADERÍA
    # -------------------------------------------------------------------------
    with tab_recepcionar:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    v.sku,
                    p.modelo || ' - ' || COALESCE(p.nombre, '') as nombre,
                    v.stock_transito as pendiente,
                    v.stock_interno as stock_actual,
                    v.ubicacion,
                    (SELECT nota FROM Movimientos m WHERE m.sku = v.sku AND m.tipo_movimiento = 'PEDIDO_IMPORT' ORDER BY m.fecha DESC LIMIT 1) as ultima_nota,
                    (SELECT date(fecha) FROM Movimientos m WHERE m.sku = v.sku AND m.tipo_movimiento = 'PEDIDO_IMPORT' ORDER BY m.fecha DESC LIMIT 1) as ultima_fecha
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.stock_transito > 0
                ORDER BY ultima_fecha ASC, ultima_nota ASC
            """)
            df_transito = pd.read_sql(query, conn)

        if not df_transito.empty:
            # --- SECCIÓN: ACCIÓN POR GRUPO ---
            st.markdown("### ⚡ Recepción por Grupo")
            notas_unicas = df_transito["ultima_nota"].unique().tolist()
            col_sel, col_btn = st.columns([2, 1])
            
            nota_seleccionada = col_sel.selectbox("Seleccionar Pedido por Nota:", ["---"] + notas_unicas)
            
            if nota_seleccionada != "---":
                items_grupo = df_transito[df_transito["ultima_nota"] == nota_seleccionada]
                if col_btn.button(f"✅ Aceptar todo el pedido ({len(items_grupo)} items)", type="primary"):
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            for _, row in items_grupo.iterrows():
                                conn.execute(text("""
                                    UPDATE Variantes 
                                    SET stock_interno = stock_interno + stock_transito, stock_transito = 0 
                                    WHERE sku = :s
                                """), {"s": row['sku']})
                                
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                    VALUES (:s, 'RECEPCION_IMPORT', :c, :ant, :nue, :nota)
                                """), {
                                    "s": row['sku'], "c": row['pendiente'], "ant": row['stock_actual'],
                                    "nue": row['stock_actual'] + row['pendiente'], "nota": f"Ingreso Grupal: {nota_seleccionada}"
                                })
                            trans.commit()
                            st.success(f"✅ Pedido '{nota_seleccionada}' ingresado al inventario.")
                            time.sleep(1.5)
                            st.rerun()
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error: {e}")

            st.divider()

            # --- SECCIÓN: TABLA INDIVIDUAL ---
            st.markdown("### 📋 Detalle de Productos en Camino")
            df_transito["✅ Llegó?"] = False
            df_transito["Cant. Recibida"] = df_transito["pendiente"]
            
            df_editor = df_transito[[
                "✅ Llegó?", "sku", "ultima_fecha", "ultima_nota", "nombre", "Cant. Recibida", "pendiente", "stock_actual", "ubicacion"
            ]]

            cambios = st.data_editor(
                df_editor,
                column_config={
                    "✅ Llegó?": st.column_config.CheckboxColumn("Recibir"),
                    "sku": st.column_config.TextColumn("SKU", disabled=True),
                    "ultima_fecha": st.column_config.DateColumn("Fecha Pedido", disabled=True, format="DD/MM"),
                    "ultima_nota": st.column_config.TextColumn("Nota", disabled=True),
                    "nombre": st.column_config.TextColumn("Producto", disabled=True),
                    "Cant. Recibida": st.column_config.NumberColumn("Recibido", min_value=0), # Modificado a 0
                    "pendiente": st.column_config.NumberColumn("Esperado", disabled=True),
                    "stock_actual": st.column_config.NumberColumn("Stock Hoy", disabled=True),
                    "ubicacion": st.column_config.TextColumn("Ubicación")
                },
                hide_index=True,
                use_container_width=True,
                key="editor_recepcion"
            )

            # Lógica para procesar selección manual...
            filas_ok = cambios[cambios["✅ Llegó?"] == True]
            if not filas_ok.empty:
                if st.button(f"📥 Procesar {len(filas_ok)} items marcados"):
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            for _, row in filas_ok.iterrows():
                                n_stk = row['stock_actual'] + row['Cant. Recibida']
                                
                                # Si pone 0, cancelamos el saldo pendiente
                                if row['Cant. Recibida'] == 0:
                                    n_trans = 0
                                    nota_mov = f"Cancelado/No llegó - Ref: {row['ultima_nota']}"
                                else:
                                    n_trans = max(0, row['pendiente'] - row['Cant. Recibida'])
                                    nota_mov = f"Manual - Ref: {row['ultima_nota']}"
                                
                                conn.execute(text("UPDATE Variantes SET stock_interno=:nm, stock_transito=:nt, ubicacion=:u WHERE sku=:s"),
                                            {"nm": n_stk, "nt": n_trans, "u": row['ubicacion'], "s": row['sku']})
                                conn.execute(text("INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota) VALUES (:s,'RECEPCION_IMPORT',:c,:ant,:nue,:n)"),
                                            {"s": row['sku'], "c": row['Cant. Recibida'], "ant": row['stock_actual'], "nue": n_stk, "n": nota_mov})
                            
                            trans.commit()
                            st.success("✅ Items actualizados.")
                            time.sleep(1.2)
                            st.rerun()
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error: {e}")
        else:
            st.info("🎉 No hay mercadería pendiente de llegada.")
            