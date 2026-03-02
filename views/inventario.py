import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine

def render_inventario():
    st.subheader("🔎 Gestión de Inventario e Importación")

    tab_gestion, tab_importar = st.tabs(["📊 Gestión de Inventario", "📦 Importar Stock Externo"])

    # ==============================================================================
    # PESTAÑA 1: GESTIÓN DE INVENTARIO (CÓDIGO ORIGINAL)
    # ==============================================================================
    with tab_gestion:
        col_search, col_btn = st.columns([4, 1])
        with col_search:
            filtro_inv = st.text_input("🔍 Buscar:", placeholder="Escribe SKU, Marca, Modelo o Ubicación...")
        with col_btn:
            st.write("") 
            if st.button("🔄 Recargar Tabla"):
                if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
                st.rerun()

        if 'df_inventario' not in st.session_state:
            with engine.connect() as conn:
                q_inv = """
                    SELECT 
                        v.sku, 
                        v.id_producto,
                        p.categoria,
                        p.marca, 
                        p.modelo, 
                        p.nombre,
                        p.color_principal, 
                        p.diametro, 
                        v.medida,
                        v.stock_interno,
                        v.stock_externo,
                        v.stock_transito,
                        v.ubicacion,
                        p.importacion,
                        p.url_compra,
                        p.url_imagen
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    ORDER BY p.marca, p.modelo, v.sku ASC
                """
                st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

        df_calc = st.session_state.df_inventario.copy()

        df_calc['nombre_completo'] = (
            df_calc['marca'].fillna('') + " " + 
            df_calc['modelo'].fillna('') + " - " + 
            df_calc['nombre'].fillna('') + " (" +
            df_calc['sku'].fillna('') + ")"
        ).str.strip()

        def formatear_detalles(row):
            partes = []
            if row['color_principal']: partes.append(str(row['color_principal']))
            if row['diametro']: partes.append(f"Dia:{row['diametro']}")
            if row['medida']: partes.append(f"Med:{row['medida']}")
            return " | ".join(partes)

        df_calc['detalles_info'] = df_calc.apply(formatear_detalles, axis=1)

        if filtro_inv:
            f = filtro_inv.lower()
            df_calc = df_calc[
                df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
                df_calc['sku'].str.lower().str.contains(f, na=False) |
                df_calc['ubicacion'].str.lower().str.contains(f, na=False) |
                df_calc['importacion'].str.lower().str.contains(f, na=False)
            ]

        df_final = df_calc[[
            'url_imagen', 
            'sku', 
            'id_producto', 
            'categoria', 
            'nombre_completo', 
            'detalles_info', 
            'stock_interno', 
            'stock_externo',
            'stock_transito',
            'ubicacion',
            'importacion',
            'url_compra'
        ]]

        st.caption("📝 Editables: **En Tránsito**, **Ubicación**, **Importación** y **URL**.")

        cambios_inv = st.data_editor(
            df_final,
            key="editor_inventario_v3",
            column_config={
                "url_imagen": st.column_config.ImageColumn("Foto 📸", width="small", help="Clic para ver en grande"),
                "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
                "id_producto": None, 
                "categoria": st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                "nombre_completo": st.column_config.TextColumn("Producto", disabled=True, width="large"),
                "detalles_info": st.column_config.TextColumn("Detalles", disabled=True, width="medium"),
                "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
                "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
                "stock_transito": st.column_config.NumberColumn("En Camino 🚚", help="Stock pedido al proveedor", min_value=0, step=1, format="%d", width="small"),
                "ubicacion": st.column_config.TextColumn("Ubicación 📍", width="small"),
                "importacion": st.column_config.SelectboxColumn("Importar De ✈️", width="small", options=["Aliexpress", "Alibaba", "Proveedor Nacional", "Otro"], required=False),
                "url_compra": st.column_config.LinkColumn("Link Compra 🔗", width="medium", display_text="Ver Enlace", validate="^https://.*", required=False)
            },
            hide_index=True,
            width='stretch',
            num_rows="fixed" 
        )

        edited_rows = st.session_state["editor_inventario_v3"].get("edited_rows")

        if edited_rows:
            st.info(f"💾 Tienes cambios pendientes en {len(edited_rows)} filas...")
            if st.button("Confirmar Cambios"):
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        count_ubi = 0
                        count_prod = 0
                        count_transito = 0 
                        
                        for idx, updates in edited_rows.items():
                            row_original = df_final.iloc[idx]
                            sku_target = row_original['sku']
                            id_prod_target = int(row_original['id_producto']) 
                            
                            if 'ubicacion' in updates:
                                conn.execute(text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"), {"u": updates['ubicacion'], "s": sku_target})
                                count_ubi += 1

                            if 'stock_transito' in updates:
                                conn.execute(text("UPDATE Variantes SET stock_transito = :st WHERE sku = :s"), {"st": updates['stock_transito'], "s": sku_target})
                                count_transito += 1
                            
                            if 'importacion' in updates or 'url_compra' in updates:
                                nuevo_imp = updates.get('importacion', row_original['importacion'])
                                nueva_url = updates.get('url_compra', row_original['url_compra'])
                                conn.execute(text("UPDATE Productos SET importacion = :imp, url_compra = :url WHERE id_producto = :idp"), 
                                                        {"imp": nuevo_imp, "url": nueva_url, "idp": id_prod_target})
                                count_prod += 1
                        
                        trans.commit()
                        st.success(f"✅ Guardado: {count_ubi} Ubicaciones, {count_transito} Stocks en tránsito y {count_prod} Datos de Importación.")
                        del st.session_state['df_inventario'] 
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error al guardar: {e}")

    # ==============================================================================
    # PESTAÑA 2: IMPORTAR STOCK EXTERNO (NUEVO)
    # ==============================================================================
    with tab_importar:
        st.markdown("### 📥 Actualización Masiva de Stock de Proveedor")
        st.info("Sube un archivo CSV con exactamente dos columnas: `sku` y `stock`. Este proceso reemplazará el valor del **stock_externo**.")

        archivo_csv = st.file_uploader("Selecciona el archivo CSV", type=["csv"])

        if archivo_csv:
            try:
                df_proveedor = pd.read_csv(archivo_csv)
                df_proveedor.columns = df_proveedor.columns.str.strip().str.lower()

                if 'sku' not in df_proveedor.columns or 'stock' not in df_proveedor.columns:
                    st.error("❌ El archivo CSV debe contener exactamente las columnas 'sku' y 'stock'.")
                else:
                    st.dataframe(df_proveedor.head(), use_container_width=True)

                    if st.button("🚀 Procesar y Actualizar Stock Externo", type="primary"):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                contador = 0
                                for _, row in df_proveedor.iterrows():
                                    sku_csv = str(row['sku']).strip()
                                    try:
                                        stock_csv = int(row['stock'])
                                    except ValueError:
                                        continue 

                                    conn.execute(text("""
                                        UPDATE Variantes 
                                        SET stock_externo = :stk 
                                        WHERE sku = :sku
                                    """), {"stk": stock_csv, "sku": sku_csv})
                                    contador += 1
                                
                                trans.commit()
                                st.balloons()
                                st.success(f"✅ Se ha actualizado el stock externo de {contador} variantes correctamente.")
                                
                                if 'df_inventario' in st.session_state:
                                    del st.session_state['df_inventario']
                                time.sleep(2)
                                st.rerun()
                                
                            except Exception as e:
                                trans.rollback()
                                st.error(f"❌ Ocurrió un error en la base de datos: {e}")

            except Exception as e:
                st.error(f"❌ Error al leer el archivo CSV: {e}")