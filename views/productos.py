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
    
    tab_gestion, tab_importar, tab_ubicaciones = st.tabs(["📊 Gestión de Inventario Avanzada", "📦 Importar Stock Externo (CSV)", "📍 Distribuir Ubicaciones"])
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
                        (SELECT COALESCE(SUM(cantidad), 0) FROM Stock_Ubicaciones su WHERE su.sku = v.sku) as stock_asignado,
                        (SELECT STRING_AGG(u.nombre || ' (' || su.cantidad || ' un.)', ' | ') 
                         FROM Stock_Ubicaciones su 
                         JOIN Ubicaciones_Estandar u ON su.id_ubicacion = u.id_ubicacion 
                         WHERE su.sku = v.sku AND su.cantidad > 0) as ubicacion_nueva,
                        v.stock_externo, v.stock_transito, v.ubicacion as ubicacion_antigua, p.importacion,
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
        df_calc['ubicacion_nueva'] = df_calc['ubicacion_nueva'].fillna('-')

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

            filtro_stk = c_stk.selectbox("📦 Estado Almacén:", [
                "Todos", 
                "Con Stock (>0)", 
                "Sin Stock (0)", 
                "En Camino (>0)",
                "⚠️ Stock Sobrante (Pendiente Asignar)"
            ])
            if filtro_stk == "Con Stock (>0)": df_calc = df_calc[df_calc['stock_interno'] > 0]
            elif filtro_stk == "Sin Stock (0)": df_calc = df_calc[df_calc['stock_interno'] <= 0]
            elif filtro_stk == "En Camino (>0)": df_calc = df_calc[df_calc['stock_transito'] > 0]
            elif filtro_stk == "⚠️ Stock Sobrante (Pendiente Asignar)":
                # LÓGICA DE AUDITORÍA: Stock físico es mayor a lo que has guardado en los estantes
                df_calc = df_calc[(df_calc['stock_interno'] > 0) & (df_calc['stock_interno'] > df_calc['stock_asignado'])]

            filtro_txt = c_txt.text_input("🔎 Búsqueda Libre:", placeholder="SKU, Marca, Modelo o Ubicación...")
            if filtro_txt:
                f = filtro_txt.lower()
                df_calc = df_calc[
                    df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
                    df_calc['sku'].str.lower().str.contains(f, na=False) |
                    df_calc['ubicacion_antigua'].str.lower().str.contains(f, na=False) |
                    df_calc['ubicacion_nueva'].str.lower().str.contains(f, na=False) |
                    df_calc['importacion'].str.lower().str.contains(f, na=False)
                ]

        df_final = df_calc[[
            'url_imagen', 'sku', 'id_producto', 'linea_corta', 'categoria', 'nombre_completo', 
            'detalles_info', 'stock_interno', 'stock_externo', 'stock_transito',
            'ubicacion_antigua', 'ubicacion_nueva', 'importacion', 'url_compra'
        ]]

        st.caption(f"Mostrando **{len(df_final)} variantes**. 📝 Editables: **Subcategoría**, **En Tránsito**, **Ubi. Antigua**, **Importación** y **URL**.")

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
                "ubicacion_antigua": st.column_config.TextColumn("Ubi. Ant. 📍", width="small"),
                "ubicacion_nueva": st.column_config.TextColumn("Ubi. Nueva 🗄️", disabled=True, width="medium", help="Se edita desde la pestaña de Ubicaciones"),
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
                            
                            if 'ubicacion_antigua' in updates:
                                conn.execute(text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"), {"u": updates['ubicacion_antigua'], "s": sku_target})
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
    # ==============================================================================
    # --- NUEVA PESTAÑA 3: CONTROL DE UBICACIONES Y AUDITORÍA ---
    # ==============================================================================
    with tab_ubicaciones:
        try:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE Stock_Ubicaciones ADD COLUMN IF NOT EXISTS fecha_actualizacion TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP"))
        except Exception:
            pass

        st.subheader("📍 Control de Stock, Estantes y Auditoría")
        
        subtab_dist, subtab_audit = st.tabs(["📦 Distribuir y Asignar Stock", "🔍 Inspeccionar Estantes y Corregir"])

        # ---------------------------------------------------------
        # SUB-PESTAÑA 1: ASIGNAR Y DISTRIBUIR STOCK
        # ---------------------------------------------------------
        with subtab_dist:
            col_admin, col_dist = st.columns([1, 2])
            
            # 1. ADMINISTRADOR DE ESTANTES
            with col_admin:
                st.markdown("#### 1️⃣ Tus Estantes")
                nueva_ubi = st.text_input("Crear nueva ubicación:", placeholder="Ej: 09A")
                if st.button("➕ Añadir Estante", use_container_width=True):
                    if nueva_ubi.strip():
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("INSERT INTO Ubicaciones_Estandar (nombre) VALUES (:n)"), {"n": nueva_ubi.strip().upper()})
                            st.success("Estante creado.")
                            st.rerun()
                        except Exception:
                            st.error("El estante ya existe o hubo un error.")
                
                st.divider()
                with engine.connect() as conn:
                    df_ubis = pd.read_sql("SELECT nombre FROM Ubicaciones_Estandar ORDER BY nombre", conn)
                st.dataframe(df_ubis, use_container_width=True, hide_index=True)

            # 2. REPARTIDOR DE STOCK POR SKU
            with col_dist:
                st.markdown("#### 2️⃣ Distribuir Stock de un Producto")
                
                with engine.connect() as conn:
                    skus_disp = pd.read_sql("SELECT sku, stock_interno FROM Variantes WHERE stock_interno > 0 ORDER BY sku", conn)
                
                if not skus_disp.empty:
                    sku_sel = st.selectbox("Selecciona un SKU con stock en almacén:", skus_disp['sku'].tolist())
                    stock_total_sku = int(skus_disp[skus_disp['sku'] == sku_sel]['stock_interno'].values[0])
                    
                    with engine.connect() as conn:
                        df_reparto = pd.read_sql(text("""
                            SELECT su.id_ubicacion, u.nombre as "Estante", su.cantidad as "Unidades", 
                                   TO_CHAR(su.fecha_actualizacion, 'DD/MM/YYYY HH24:MI') as "Última Revisión"
                            FROM Stock_Ubicaciones su
                            JOIN Ubicaciones_Estandar u ON su.id_ubicacion = u.id_ubicacion
                            WHERE su.sku = :sku AND su.cantidad > 0
                            ORDER BY u.nombre
                        """), conn, params={"sku": sku_sel})
                    
                    ya_asignado = int(df_reparto['Unidades'].sum()) if not df_reparto.empty else 0
                    disponible_para_asignar = stock_total_sku - ya_asignado
                    
                    cm1, cm2, cm3 = st.columns(3)
                    cm1.metric("📦 Stock Total Sistema", f"{stock_total_sku} un.")
                    cm2.metric("📍 Ya en Estantes", f"{ya_asignado} un.")
                    cm3.metric("⚠️ Pendiente Asignar", f"{disponible_para_asignar} un.", delta=disponible_para_asignar if disponible_para_asignar > 0 else None)
                    
                    st.divider()

                    # =========================================================
                    # FORMULARIO 1: AÑADIR A ESTANTE
                    # =========================================================
                    if disponible_para_asignar <= 0:
                        if disponible_para_asignar < 0:
                            st.error(f"⚠️ ¡Alerta! Hay {abs(disponible_para_asignar)} unidades excedentes asignadas por error. Corrige abajo 👇")
                        else:
                            st.success("✅ Todo el stock de este producto ya está distribuido correctamente en los estantes.")
                    else:
                        st.markdown("##### ➕ Asignar a nuevo estante o sumar stock:")
                        with st.form("form_asignar_ubi"):
                            c_ub, c_cant = st.columns(2)
                            ubi_asignar = c_ub.selectbox("Asignar al estante:", df_ubis['nombre'].tolist())
                            
                            max_add_safe = max(1, int(disponible_para_asignar))
                            cant_asignar = c_cant.number_input(
                                "Cantidad a sumar:", 
                                min_value=1, 
                                max_value=max_add_safe, 
                                value=1,
                                help=f"Solo puedes asignar hasta {disponible_para_asignar} unidades más."
                            )
                            
                            if st.form_submit_button("💾 Guardar Ubicación", type="primary"):
                                if cant_asignar > disponible_para_asignar:
                                    st.error(f"❌ No puedes asignar {cant_asignar} porque solo quedan {disponible_para_asignar} unidades libres.")
                                else:
                                    try:
                                        with engine.begin() as conn:
                                            id_ubi = conn.execute(text("SELECT id_ubicacion FROM Ubicaciones_Estandar WHERE nombre = :n"), {"n": ubi_asignar}).scalar()
                                            conn.execute(text("""
                                                INSERT INTO Stock_Ubicaciones (sku, id_ubicacion, cantidad, fecha_actualizacion) 
                                                VALUES (:sku, :idu, :cant, CURRENT_TIMESTAMP)
                                                ON CONFLICT (sku, id_ubicacion) 
                                                DO UPDATE SET 
                                                    cantidad = Stock_Ubicaciones.cantidad + :cant,
                                                    fecha_actualizacion = CURRENT_TIMESTAMP
                                            """), {"sku": sku_sel, "idu": id_ubi, "cant": cant_asignar})
                                        st.success(f"Se sumaron {cant_asignar} unidades al estante {ubi_asignar}.")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                    
                    st.markdown("##### 📦 Ubicaciones registradas para este SKU:")
                    if not df_reparto.empty:
                        st.dataframe(df_reparto[['Estante', 'Unidades', 'Última Revisión']], hide_index=True, use_container_width=True)
                        
                        # =========================================================
                        # HERRAMIENTA DE CORRECCIÓN (BLINDADA CONTRA CRASHES)
                        # =========================================================
                        with st.expander("🛠️ Corregir o Eliminar una ubicación para este SKU", expanded=(disponible_para_asignar < 0)):
                            c_edit1, c_edit2 = st.columns([2, 1])
                            estante_a_modificar = c_edit1.selectbox("Selecciona el estante a corregir:", df_reparto['Estante'].tolist(), key="mod_est")
                            
                            cant_actual_estante = int(df_reparto[df_reparto['Estante'] == estante_a_modificar]['Unidades'].values[0])
                            max_posible_cambio = int(stock_total_sku - (ya_asignado - cant_actual_estante))
                            
                            # BLINDAJE: Garantizamos que max_value sea al menos el número actual para evitar que Streamlit colapse
                            max_edit_seguro = max(max_posible_cambio, cant_actual_estante, 0)
                            val_default_seguro = min(cant_actual_estante, max_edit_seguro)
                            
                            nueva_cant_estante = c_edit2.number_input(
                                "Nueva cantidad exacta:", 
                                min_value=0, 
                                max_value=max_edit_seguro, 
                                value=val_default_seguro,
                                help="Pon el número real o usa el botón rojo para eliminar."
                            )
                            
                            b_col1, b_col2 = st.columns(2)
                            if b_col1.button("💾 Sobreescribir Cantidad", type="secondary", use_container_width=True):
                                id_ubi_mod = df_reparto[df_reparto['Estante'] == estante_a_modificar]['id_ubicacion'].values[0]
                                with engine.begin() as conn:
                                    if nueva_cant_estante == 0:
                                        conn.execute(text("DELETE FROM Stock_Ubicaciones WHERE sku = :sku AND id_ubicacion = :idu"), {"sku": sku_sel, "idu": int(id_ubi_mod)})
                                    else:
                                        conn.execute(text("""
                                            UPDATE Stock_Ubicaciones SET cantidad = :c, fecha_actualizacion = CURRENT_TIMESTAMP
                                            WHERE sku = :sku AND id_ubicacion = :idu
                                        """), {"c": nueva_cant_estante, "sku": sku_sel, "idu": int(id_ubi_mod)})
                                st.success(f"Estante {estante_a_modificar} actualizado a {nueva_cant_estante} un.")
                                st.rerun()

                            if b_col2.button("🗑️ Eliminar del Estante", type="primary", use_container_width=True):
                                id_ubi_mod = df_reparto[df_reparto['Estante'] == estante_a_modificar]['id_ubicacion'].values[0]
                                with engine.begin() as conn:
                                    conn.execute(text("DELETE FROM Stock_Ubicaciones WHERE sku = :sku AND id_ubicacion = :idu"), {"sku": sku_sel, "idu": int(id_ubi_mod)})
                                st.success(f"SKU retirado completamente del estante {estante_a_modificar}.")
                                st.rerun()
                    else:
                        st.caption("Aún no se ha asignado este SKU a ningún estante.")

        # ---------------------------------------------------------
        # SUB-PESTAÑA 2: AUDITORÍA POR ESTANTE (BLINDADA)
        # ---------------------------------------------------------
        with subtab_audit:
            st.markdown("#### 🔍 Auditoría y Corrección Rápida por Ubicación")
            
            with engine.connect() as conn:
                df_ubis_tot = pd.read_sql(text("""
                    SELECT u.id_ubicacion, u.nombre, COALESCE(SUM(su.cantidad), 0) as total_unidades, COUNT(CASE WHEN su.cantidad > 0 THEN su.sku END) as total_skus
                    FROM Ubicaciones_Estandar u
                    LEFT JOIN Stock_Ubicaciones su ON u.id_ubicacion = su.id_ubicacion AND su.cantidad > 0
                    GROUP BY u.id_ubicacion, u.nombre
                    ORDER BY u.nombre
                """), conn)
            
            if df_ubis_tot.empty:
                st.info("No hay estantes creados en el sistema.")
            else:
                ubi_inspeccionar = st.selectbox("🗄️ Selecciona el Estante a auditar:", df_ubis_tot['nombre'].tolist(), key="audit_sel")
                info_u = df_ubis_tot[df_ubis_tot['nombre'] == ubi_inspeccionar].iloc[0]
                id_estante_actual = int(info_u['id_ubicacion'])
                
                c_m1, c_m2 = st.columns(2)
                c_m1.metric("📦 Total de Unidades", f"{int(info_u['total_unidades'])} un.")
                c_m2.metric("🏷️ SKUs Diferentes", f"{int(info_u['total_skus'])} productos")
                
                st.divider()
                
                with engine.connect() as conn:
                    df_contenido = pd.read_sql(text("""
                        SELECT 
                            su.sku as "SKU",
                            COALESCE(p.modelo || ' ' || p.nombre || ' (' || v.medida || ')', v.nombre_variante, 'Sin descripción') as "Producto",
                            su.cantidad as "Unidades",
                            v.stock_interno as "Stock Global",
                            TO_CHAR(su.fecha_actualizacion, 'DD/MM/YYYY HH24:MI:SS') as "Último Conteo"
                        FROM Stock_Ubicaciones su
                        JOIN Variantes v ON su.sku = v.sku
                        JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE su.id_ubicacion = :idu AND su.cantidad > 0
                        ORDER BY su.fecha_actualizacion DESC, su.sku ASC
                    """), conn, params={"idu": id_estante_actual})
                
                if df_contenido.empty:
                    st.warning(f"El estante **{ubi_inspeccionar}** está completamente vacío en este momento.")
                else:
                    st.dataframe(df_contenido[['SKU', 'Producto', 'Unidades', 'Stock Global', 'Último Conteo']], use_container_width=True, hide_index=True)
                    
                    # =========================================================
                    # CORRECCIÓN DIRECTA BLINDADA
                    # =========================================================
                    st.markdown("##### 🛠️ Corregir o retirar un ítem de este estante:")
                    with st.container(border=True):
                        c_a1, c_a2 = st.columns([2, 1])
                        sku_a_corregir = c_a1.selectbox("Selecciona producto a corregir:", df_contenido['SKU'].tolist(), key="audit_sku_sel")
                        
                        datos_sku_sel = df_contenido[df_contenido['SKU'] == sku_a_corregir].iloc[0]
                        cant_en_est = int(datos_sku_sel['Unidades'])
                        stock_global = int(datos_sku_sel['Stock Global'])
                        
                        # BLINDAJE: Evita que explote si cant_en_est es mayor a stock_global por un error previo
                        max_audit_seguro = max(stock_global, cant_en_est, 0)
                        val_audit_seguro = min(cant_en_est, max_audit_seguro)
                        
                        nueva_cant_audit = c_a2.number_input(
                            f"Cantidad real en {ubi_inspeccionar}:", 
                            min_value=0, 
                            max_value=max_audit_seguro, 
                            value=val_audit_seguro,
                            help=f"El stock global en sistema es de {stock_global} un."
                        )
                        
                        b_aud1, b_aud2 = st.columns(2)
                        if b_aud1.button("💾 Actualizar este Estante", type="secondary", use_container_width=True):
                            with engine.begin() as conn:
                                if nueva_cant_audit == 0:
                                    conn.execute(text("DELETE FROM Stock_Ubicaciones WHERE sku = :sku AND id_ubicacion = :idu"), {"sku": sku_a_corregir, "idu": id_estante_actual})
                                else:
                                    conn.execute(text("""
                                        UPDATE Stock_Ubicaciones SET cantidad = :c, fecha_actualizacion = CURRENT_TIMESTAMP
                                        WHERE sku = :sku AND id_ubicacion = :idu
                                    """), {"c": nueva_cant_audit, "sku": sku_a_corregir, "idu": id_estante_actual})
                            st.success(f"¡Corregido! Ahora hay {nueva_cant_audit} un. de {sku_a_corregir} en {ubi_inspeccionar}.")
                            st.rerun()

                        if b_aud2.button("🗑️ Retirar Producto del Estante", type="primary", use_container_width=True):
                            with engine.begin() as conn:
                                conn.execute(text("DELETE FROM Stock_Ubicaciones WHERE sku = :sku AND id_ubicacion = :idu"), {"sku": sku_a_corregir, "idu": id_estante_actual})
                            st.success(f"Producto {sku_a_corregir} retirado del estante {ubi_inspeccionar}.")
                            st.rerun()

                    st.download_button(
                        label=f"📥 Descargar Lista de Estante {ubi_inspeccionar} (CSV)",
                        data=df_contenido[['SKU', 'Producto', 'Unidades', 'Último Conteo']].to_csv(index=False).encode('utf-8'),
                        file_name=f"Inventario_Estante_{ubi_inspeccionar}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )