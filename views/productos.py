import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
import utils

# =========================================================================
# AUTO-CREACIÓN DE TABLA MAESTRA PREVENTIVA
# =========================================================================
try:
    with engine.begin() as conn_init:
        conn_init.execute(text("""
            CREATE TABLE IF NOT EXISTS Subcategorias_Sistema (
                id SERIAL PRIMARY KEY,
                macro_categoria VARCHAR(50) NOT NULL,
                subcategoria VARCHAR(100) NOT NULL,
                UNIQUE(macro_categoria, subcategoria)
            )
        """))
        conn_init.execute(text("""
            INSERT INTO Subcategorias_Sistema (macro_categoria, subcategoria) VALUES 
            ('Lentes', 'Estilo Natural'),
            ('Lentes', 'Estilo Fantasía'),
            ('Lentes', 'Accesorios'),
            ('Pelucas', 'Peluca Natural'),
            ('Pelucas', 'Peluca Fantasía'),
            ('Pelucas', 'Accesorios Pelucas')
            ON CONFLICT DO NOTHING
        """))
except Exception:
    pass


def vista_productos():
    st.title("📦 Gestión de Productos e Inventario")
    
    tab_gestion, tab_importar = st.tabs(["📊 Gestión de Inventario Avanzada", "📦 Importar Stock Externo (CSV)"])

    # ==============================================================================
    # --- PESTAÑA 1: GESTIÓN DE INVENTARIO ---
    # ==============================================================================
    with tab_gestion:
        if 'df_inventario' not in st.session_state:
            with engine.connect() as conn:
                q_inv = """
                    SELECT 
                        v.sku, v.id_producto, 
                        CASE 
                            WHEN p.macro_categoria ILIKE 'peluca%' OR v.sku ILIKE 'WB-%' OR v.sku ILIKE 'WIG-%' THEN 'Pelucas'
                            ELSE 'Lentes'
                        END AS macro_categoria, 
                        p.categoria, p.marca, p.modelo, p.nombre,
                        p.color_principal, p.diametro, v.medida, v.stock_interno,
                        v.stock_externo, v.stock_transito, v.ubicacion, p.importacion,
                        p.url_compra, 
                        COALESCE(NULLIF(TRIM(v.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS url_imagen
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    ORDER BY macro_categoria DESC, p.marca, p.modelo, v.sku ASC
                """
                st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

        df_calc = st.session_state.df_inventario.copy()
        df_calc['nombre_completo'] = df_calc.apply(utils.generar_nombre_inteligente, axis=1)

        # REFACTOR 2: Columna visual delgada (EXCLUSIVAMENTE EMOJI)
        mapa_linea_corta = {'Pelucas': '💇‍♀️', 'Lentes': '👓'}
        df_calc['linea_corta'] = df_calc['macro_categoria'].map(mapa_linea_corta).fillna('📦')

        def _clean_flt(val):
            try: return float(''.join(c for c in str(val) if c.isdigit() or c=='.'))
            except: return 0.0

        def formatear_detalles(row):
            partes = []
            macro = str(row.get('macro_categoria', '')).strip()

            if macro == 'Pelucas':
                if pd.notna(row['color_principal']) and str(row['color_principal']).strip().lower() != 'nan':
                    partes.append(f"🎨 {row['color_principal']}")
                
                largo = str(row['medida']).strip() if pd.notna(row['medida']) and str(row['medida']).strip() not in ['', 'nan', '0.0'] else str(row['diametro'])
                if largo and largo.lower() != 'nan' and _clean_flt(largo) > 0:
                    txt_largo = largo if 'cm' in largo.lower() else f"{largo}cm"
                    partes.append(f"📏 Largo: {txt_largo}")
            else:
                if pd.notna(row['color_principal']) and str(row['color_principal']).strip().lower() != 'nan':
                    partes.append(str(row['color_principal']))
                if pd.notna(row['diametro']) and _clean_flt(row['diametro']) > 0:
                    partes.append(f"Dia:{row['diametro']}")
                if pd.notna(row['medida']) and str(row['medida']).strip().lower() not in ['', 'nan']:
                    partes.append(f"Med:{row['medida']}")

            return " | ".join(partes) if partes else "Estandar"

        df_calc['detalles_info'] = df_calc.apply(formatear_detalles, axis=1)

        with st.container(border=True):
            st.markdown("##### 🔍 Filtros Avanzados de Búsqueda")
            c_mac, c_cat, c_stk, c_txt, c_btn = st.columns([1.5, 1.5, 1.5, 2.5, 1])
            
            with c_btn:
                st.write("") 
                if st.button("🔄 Recargar BD", use_container_width=True, type="primary"):
                    if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
                    st.rerun()

            lineas_disp = ["Todas"] + sorted(df_calc['macro_categoria'].unique().tolist())
            filtro_macro = c_mac.selectbox("📂 Línea Mayor:", lineas_disp)
            if filtro_macro != "Todas":
                df_calc = df_calc[df_calc['macro_categoria'] == filtro_macro]

            cats_disp = ["Todas"] + sorted(df_calc['categoria'].dropna().unique().tolist())
            filtro_cat = c_cat.selectbox("📑 Subcategoría:", cats_disp)
            if filtro_cat != "Todas":
                df_calc = df_calc[df_calc['categoria'] == filtro_cat]

            filtro_stk = c_stk.selectbox("📦 Estado Almacén:", ["Todos", "Con Stock (>0)", "Sin Stock (0)", "En Camino (>0)"])
            if filtro_stk == "Con Stock (>0)": df_calc = df_calc[df_calc['stock_interno'] > 0]
            elif filtro_stk == "Sin Stock (0)": df_calc = df_calc[df_calc['stock_interno'] <= 0]
            elif filtro_stk == "En Camino (>0)": df_calc = df_calc[df_calc['stock_transito'] > 0]

            filtro_txt = c_txt.text_input("🔎 Búsqueda Libre:", placeholder="SKU, Marca, Modelo o Ubicación...")
            if filtro_txt:
                f = filtro_txt.lower()
                df_calc = df_calc[
                    df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
                    df_calc['sku'].str.lower().str.contains(f, na=False) |
                    df_calc['ubicacion'].str.lower().str.contains(f, na=False) |
                    df_calc['importacion'].str.lower().str.contains(f, na=False)
                ]

        df_final = df_calc[[
            'url_imagen', 'sku', 'id_producto', 'linea_corta', 'categoria', 'nombre_completo', 
            'detalles_info', 'stock_interno', 'stock_externo', 'stock_transito',
            'ubicacion', 'importacion', 'url_compra'
        ]]

        st.caption(f"Mostrando **{len(df_final)} variantes**. 📝 Editables: **Subcategoría**, **En Tránsito**, **Ubicación**, **Importación** y **URL**.")

        # REFACTOR 1 y 3: Traer opciones estrictas desde PostgreSQL según la Línea Mayor filtrada
        with engine.connect() as conn_sub:
            if filtro_macro != "Todas":
                res_subs = conn_sub.execute(text("SELECT subcategoria FROM Subcategorias_Sistema WHERE macro_categoria = :mac ORDER BY subcategoria"), {"mac": filtro_macro}).fetchall()
            else:
                res_subs = conn_sub.execute(text("SELECT subcategoria FROM Subcategorias_Sistema ORDER BY subcategoria")).fetchall()
        
        opciones_subcategorias = [r[0] for r in res_subs]
        if not opciones_subcategorias:
            opciones_subcategorias = sorted(list(set(df_calc['categoria'].dropna().unique().tolist() + ["Estilo Natural", "Estilo Fantasía", "Accesorios", "Peluca Natural", "Peluca Fantasía"])))

        cambios_inv = st.data_editor(
            df_final,
            key="editor_inventario_v3",
            column_config={
                "url_imagen": st.column_config.ImageColumn("Foto 📸", width="small"),
                "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
                "id_producto": None, 
                "linea_corta": st.column_config.TextColumn("Línea", disabled=True, width="small"),
                "categoria": st.column_config.SelectboxColumn("Subcat.", options=opciones_subcategorias, width="medium", required=True),
                "nombre_completo": st.column_config.TextColumn("Producto", disabled=True, width="large"),
                "detalles_info": st.column_config.TextColumn("Detalles", disabled=True, width="medium"),
                "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
                "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
                "stock_transito": st.column_config.NumberColumn("En Camino 🚚", min_value=0, step=1, format="%d", width="small"),
                "ubicacion": st.column_config.TextColumn("Ubicación 📍", width="small"),
                "importacion": st.column_config.SelectboxColumn("Importar De ✈️", width="small", options=["Aliexpress", "Alibaba", "Proveedor Nacional", "Otro"]),
                "url_compra": st.column_config.LinkColumn("Link Compra 🔗", width="medium", display_text="Ver Enlace")
            },
            hide_index=True,
            width='stretch',
            num_rows="fixed" 
        )

        edited_rows = st.session_state["editor_inventario_v3"].get("edited_rows")

        if edited_rows:
            st.info(f"💾 Tienes cambios pendientes en {len(edited_rows)} filas...")
            if st.button("Confirmar Cambios en Inventario", type="primary"):
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        count_ubi, count_prod, count_transito = 0, 0, 0 
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
                            
                            if 'importacion' in updates or 'url_compra' in updates or 'categoria' in updates:
                                nuevo_imp = updates.get('importacion', row_original['importacion'])
                                nueva_url = updates.get('url_compra', row_original['url_compra'])
                                nueva_cat = updates.get('categoria', row_original['categoria'])
                                
                                imp_db = None if pd.isna(nuevo_imp) else str(nuevo_imp)
                                url_db = None if pd.isna(nueva_url) else str(nueva_url)
                                cat_db = None if pd.isna(nueva_cat) else str(nueva_cat)

                                conn.execute(text("""
                                    UPDATE Productos 
                                    SET importacion = :imp, url_compra = :url, categoria = :cat 
                                    WHERE id_producto = :idp
                                """), {"imp": imp_db, "url": url_db, "cat": cat_db, "idp": id_prod_target})
                                count_prod += 1
                        
                        trans.commit()
                        st.success(f"✅ Guardado con éxito: {count_ubi} Ubicaciones, {count_transito} Stocks en Camino y {count_prod} Datos de Producto.")
                        del st.session_state['df_inventario'] 
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error al guardar: {e}")

    # ==============================================================================
    # --- PESTAÑA 1B: IMPORTAR STOCK EXTERNO (CSV) ---
    # ==============================================================================
    with tab_importar:
        st.markdown("### 📥 Actualización Masiva de Stock de Proveedor")
        st.info("Sube un archivo CSV con exactamente dos columnas: `sku` y `stock`.")
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
                                    try: stock_csv = int(row['stock'])
                                    except ValueError: continue 

                                    conn.execute(text("UPDATE Variantes SET stock_externo = :stk WHERE sku = :sku"), {"stk": stock_csv, "sku": sku_csv})
                                    contador += 1
                                trans.commit()
                                st.balloons()
                                st.success(f"✅ Se ha actualizado el stock externo de {contador} variantes.")
                                if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
                                time.sleep(2)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"❌ Error en base de datos: {e}")
            except Exception as e:
                st.error(f"❌ Error al leer archivo CSV: {e}")