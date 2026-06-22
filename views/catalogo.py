import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine

def render_catalogo():
    st.subheader("🔧 Administración de Productos y Variantes")

    # --- 1. BARRA LATERAL: BUSCADOR RÁPIDO (Mejorado con Macro-Categoría) ---
    with st.expander("🔎 Verificador Rápido de SKU / Nombre", expanded=False):
        check_str = st.text_input("Escribe para buscar coincidencias:", placeholder="Ej: NL01")
        if check_str:
            with engine.connect() as conn:
                q_check = text("""
                    SELECT v.sku, p.macro_categoria as "Línea", p.categoria as "Subcategoría", p.modelo, p.nombre as color, v.medida 
                    FROM Variantes v 
                    JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku ILIKE :s OR p.nombre ILIKE :s OR p.modelo ILIKE :s
                    LIMIT 10
                """)
                df_check = pd.read_sql(q_check, conn, params={"s": f"%{check_str}%"})
            if not df_check.empty:
                st.dataframe(df_check, hide_index=True)
            else:
                st.caption("✅ No se encontraron coincidencias.")

    st.divider()

    # TABS PARA ORGANIZAR MEJOR
    tab_gestion, tab_marketing = st.tabs(["🛠️ Gestión de Inventario", "📢 Marketing & Feed Meta"])

    # ==============================================================================
    # TAB 1: GESTIÓN DE INVENTARIO
    # ==============================================================================
    with tab_gestion:
        modo_catalogo = st.radio("Acción:", ["🌱 Crear Nuevo", "✏️ Editar / Renombrar"], horizontal=True)

        COLORES_OFICIALES = ["", "Amarillo", "Azul", "Blanco", "Chocolate", "Dorado", "Gris", "Marrón", "Miel", "Morado", "Multicolor", "Naranja", "Negro", "Rojo", "Rosado", "Turquesa", "Verde"]

        # --- MODO 1: CREAR NUEVO ---
        if modo_catalogo == "🌱 Crear Nuevo":
            tipo_creacion = st.selectbox("Tipo de Creación:", 
                                            ["Medida Nueva (Hijo) para Producto Existente", 
                                             "Producto Nuevo (Marca/Color Nuevo)"])
            
            # A) NUEVA MEDIDA (Hijo)
            if "Medida Nueva" in tipo_creacion:
                with engine.connect() as conn:
                    df_prods = pd.read_sql(text("SELECT id_producto, macro_categoria, categoria, marca, modelo, nombre FROM Productos ORDER BY macro_categoria, marca, modelo, nombre"), conn)
                
                if not df_prods.empty:
                    # Formato visual limpio mostrando a qué Línea pertenece el producto padre
                    opciones_prod = df_prods.apply(lambda x: f"[{x['macro_categoria']}] {x['marca']} {x['modelo']} - {x['nombre']} (ID: {x['id_producto']})", axis=1).to_dict()
                    idx_prod = st.selectbox("Selecciona el Producto Padre:", options=list(opciones_prod.keys()), format_func=lambda x: opciones_prod[x])
                    id_producto_real = df_prods.iloc[idx_prod]['id_producto']
                    
                    with st.form("form_add_variante"):
                        st.caption(f"Agregando variante a: **{df_prods.iloc[idx_prod]['nombre']}**")
                        c1, c2 = st.columns(2)
                        sku_new = c1.text_input("Nuevo SKU (Único):").strip()
                        medida_new = c2.text_input("Medida / Graduación / Talla:", value="0.00")

                        c3, c4 = st.columns(2)
                        stock_ini = c3.number_input("Stock Inicial:", min_value=0)
                        precio_new = c4.number_input("Precio Venta:", min_value=0.0)
                        ubi_new = st.text_input("Ubicación en Almacén:")

                        if st.form_submit_button("Guardar Variante"):
                            # --- NUEVA VALIDACIÓN AMIGABLE DE SKU ---
                            if not sku_new or not sku_new.strip():
                                st.error("⚠️ El campo 'Nuevo SKU' es completamente obligatorio para registrar la variante.")
                            else:
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text("""
                                            INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                            VALUES (:sku, :idp, '', :med, :si, :pre, :ubi)
                                        """), {
                                            "sku": sku_new.strip(), "idp": int(id_producto_real), 
                                            "med": medida_new, "si": stock_ini, "pre": precio_new, "ubi": ubi_new
                                        })
                                        conn.commit()
                                    st.success(f"SKU {sku_new} creado exitosamente.")
                                except Exception as e:
                                    st.error(f"Error al guardar: {e}")

            # B) PRODUCTO NUEVO FULL (Con Jerarquía Macro -> Sub)
            else:
                with st.form("form_new_full"):
                    st.markdown("**1. Definir Jerarquía y Producto**")
                    
                    # --- REFACTOR 1: Selector Doble Dependiente ---
                    col_mac, col_cat, col_col = st.columns(3)
                    macro_sel = col_mac.selectbox("Línea de Negocio (Macro):", ["Lentes", "Pelucas"])
                    
                    if macro_sel == "Lentes":
                        opciones_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios"] # Accesorios de Lentes
                    else:
                        opciones_sub = ["Peluca Natural", "Peluca Fantasía", "Accesorios Pelucas"]

                    cat_sel = col_cat.selectbox("Subcategoría:", opciones_sub)
                    color_prin = col_col.selectbox("Color Filtro (Base):", COLORES_OFICIALES)

                    c1, c2, c3 = st.columns(3)
                    marca = c1.text_input("Marca:", placeholder="Ej: Freshlady, Pelucat")
                    modelo = c2.text_input("Modelo:", placeholder="Ej: Sharingan, Bob")
                    nombre_prod = c3.text_input("Nombre Tono/Estilo:", placeholder="Ej: Gris Intenso")

                    c_dia, c_url1 = st.columns(2)
                    diametro = c_dia.number_input("Diámetro / Largo (mm/cm):", min_value=0.0, step=0.1, format="%.1f")
                    url_img = c_url1.text_input("URL Imagen (Foto):")
                    url_buy = st.text_input("URL Compra (Importación):")

                    st.markdown("**2. Crear Primera Variante (Ej: Plano / Estándar)**")
                    c4, c5, c6 = st.columns(3)
                    sku_1 = c4.text_input("SKU Variante:")
                    medida_1 = c5.text_input("Medida / Talla:", value="0.00")
                    prec_1 = c6.number_input("Precio Venta normal", 0.0)
                    ubi_1 = st.text_input("Ubicación física")

                    if st.form_submit_button("Crear Producto Completo"):
                        # --- NUEVA VALIDACIÓN DE CAMPOS TRONCALES ---
                        if not marca.strip() or not nombre_prod.strip() or not sku_1.strip():
                            st.error("⚠️ Los campos **Marca**, **Nombre Tono/Estilo** y **SKU Variante** son obligatorios. No puedes dejarlos vacíos.")
                        else:
                            try:
                                with engine.connect() as conn:
                                    trans = conn.begin()
                                    res_p = conn.execute(text("""
                                        INSERT INTO Productos (marca, modelo, nombre, macro_categoria, categoria, color_principal, diametro, url_imagen, url_compra) 
                                        VALUES (:m, :mod, :nom, :macro, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto
                                    """), {
                                        "m": marca.strip(), "mod": modelo.strip(), "nom": nombre_prod.strip(), 
                                        "macro": macro_sel, "cat": cat_sel, "col": color_prin, "dia": str(diametro), 
                                        "uimg": url_img, "ubuy": url_buy
                                    })
                                    new_id = res_p.fetchone()[0]

                                    conn.execute(text("""
                                        INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                        VALUES (:sku, :idp, '', :med, 0, :pr, :ub)
                                    """), {
                                        "sku": sku_1.strip(), "idp": new_id, "med": medida_1,
                                        "pr": prec_1, "ub": ubi_1
                                    })
                                    trans.commit()
                                st.success(f"Producto '{nombre_prod}' creado con éxito bajo el SKU '{sku_1}'.")
                            except Exception as e:
                                st.error(f"Error en base de datos: {e}")

        # --- MODO 2: EDITAR / RENOMBRAR / RECLASIFICAR ---
        else:
            st.markdown("#### ✏️ Modificar Producto y Reclasificar")
            sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
            
            if sku_edit:
                with engine.connect() as conn:
                    # --- REFACTOR 3: Traemos macro_categoria en la consulta ---
                    query_full = text("""
                        SELECT v.*, p.marca, p.modelo, p.nombre as nombre_prod, p.macro_categoria, p.categoria, p.diametro, p.color_principal, p.url_imagen, p.url_compra
                        FROM Variantes v 
                        JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE v.sku = :sku
                    """)
                    df_data = pd.read_sql(query_full, conn, params={"sku": sku_edit})
                
                if not df_data.empty:
                    curr = df_data.iloc[0]
                    
                    col_img, col_form = st.columns([1, 3])
                    with col_img:
                        if curr['url_imagen']: st.image(curr['url_imagen'], caption="Foto Actual", use_column_width=True)
                        else: st.info("Sin imagen")

                    with col_form:
                        st.info(f"Editando: **{curr['marca']} {curr['modelo']}** - Color: **{curr['nombre_prod']}**")
                        
                        with st.form("form_edit_sku"):
                            st.markdown("📦 **Jerarquía y Datos Generales**")
                            
                            # --- REFACTOR 4: Permitir re-etiquetar Macro y Subcategoría ---
                            c_mac, c_cat = st.columns(2)
                            macro_act = curr['macro_categoria'] if curr['macro_categoria'] else "Lentes"
                            new_macro = c_mac.selectbox("Línea de Negocio (Macro):", ["Lentes", "Pelucas"], index=["Lentes", "Pelucas"].index(macro_act))

                            if new_macro == "Lentes":
                                opts_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios"]
                            else:
                                opts_sub = ["Peluca Natural", "Peluca Fantasía", "Accesorios Pelucas"]

                            cat_act = curr['categoria'] if curr['categoria'] in opts_sub else opts_sub[0]
                            new_cat = c_cat.selectbox("Subcategoría:", opts_sub, index=opts_sub.index(cat_act))

                            c_p1, c_p2, c_p3 = st.columns(3)
                            new_marca = c_p1.text_input("Marca:", value=curr['marca'])
                            new_modelo = c_p2.text_input("Modelo:", value=curr['modelo'])
                            new_nombre_prod = c_p3.text_input("Nombre (Color/Tono):", value=curr['nombre_prod'])
                            
                            c_p4, c_p5 = st.columns(2)
                            idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                            new_color_prin = c_p4.selectbox("Color Filtro:", COLORES_OFICIALES, index=idx_col)
                            val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                            new_diametro = c_p5.number_input("Diámetro / Largo:", value=val_dia, step=0.1, format="%.1f")

                            new_url_img = st.text_input("URL Imagen:", value=curr['url_imagen'] if curr['url_imagen'] else "")
                            new_url_buy = st.text_input("URL Compra:", value=curr['url_compra'] if curr['url_compra'] else "")

                            st.divider()

                            st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                            col_a, col_b = st.columns(2)
                            new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                            new_medida = col_b.text_input("Medida / Talla:", value=curr['medida'] if curr['medida'] else "0.00")
                            
                            col_e, col_f = st.columns(2)
                            new_precio = col_e.number_input("Precio Normal:", value=float(curr['precio']))
                            val_reb_str = str(curr['precio_rebajado']) if (curr['precio_rebajado'] and float(curr['precio_rebajado']) > 0) else ""
                            new_precio_reb_txt = col_f.text_input("Precio Rebajado:", value=val_reb_str)

                            if st.form_submit_button("💾 Guardar Cambios"):
                                final_rebajado = None
                                if new_precio_reb_txt.strip(): 
                                    try: final_rebajado = float(new_precio_reb_txt)
                                    except: pass

                                try:
                                    with engine.connect() as conn:
                                        trans = conn.begin()
                                        conn.execute(text("""
                                            UPDATE Variantes 
                                            SET sku=:n_sku, medida=:n_med, precio=:n_pre, precio_rebajado=:n_prer
                                            WHERE sku=:old_sku
                                        """), {
                                            "n_sku": new_sku_val, "n_med": new_medida, "n_pre": new_precio, "n_prer": final_rebajado, "old_sku": curr['sku']
                                        })
                                        # Actualizamos el producto padre incluyendo macro_categoria y categoria
                                        conn.execute(text("""
                                            UPDATE Productos 
                                            SET marca=:mar, modelo=:mod, nombre=:nom, macro_categoria=:macro, categoria=:cat, color_principal=:col, diametro=:dia, url_imagen=:uimg, url_compra=:ubuy
                                            WHERE id_producto=:idp
                                        """), {
                                            "mar": new_marca, "mod": new_modelo, "nom": new_nombre_prod, "macro": new_macro, "cat": new_cat,
                                            "col": new_color_prin, "dia": str(new_diametro), "uimg": new_url_img, "ubuy": new_url_buy, "idp": int(curr['id_producto'])
                                        })
                                        trans.commit()
                                    st.success("✅ ¡Producto y jerarquía actualizados correctamente!")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
            else:
                st.warning("SKU no encontrado.")

        # --- HERRAMIENTA EXTRA: SEPARAR VARIANTE ---
        st.divider()
        with st.expander("✂️ Separar Variante (Mover a Producto Nuevo)", expanded=False):
            sku_to_split = st.text_input("Ingresa el SKU a separar:", placeholder="Ej: NL-ERROR-01")
            
            if sku_to_split:
                with engine.connect() as conn:
                    # Traemos también macro_categoria
                    q_split = text("""
                        SELECT v.sku, v.id_producto, p.marca, p.modelo, p.nombre, 
                               p.macro_categoria, p.categoria, p.color_principal, p.diametro, p.url_imagen, p.url_compra
                        FROM Variantes v
                        JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE v.sku = :s
                    """)
                    res_split = pd.read_sql(q_split, conn, params={"s": sku_to_split})
                
                if not res_split.empty:
                    curr = res_split.iloc[0]
                    st.info(f"El SKU **{curr['sku']}** actualmente pertenece a: **[{curr['macro_categoria']}] {curr['marca']} {curr['modelo']} - {curr['nombre']}**")
                    
                    with st.form("form_split_product"):
                        st.write("Define los datos del **NUEVO** producto contenedor:")
                        c1, c2, c3 = st.columns(3)
                        n_marca = c1.text_input("Nueva Marca", value=curr['marca'])
                        n_modelo = c2.text_input("Nuevo Modelo", value=curr['modelo'])
                        n_nombre = c3.text_input("Nuevo Nombre (Color)", value=curr['nombre']) 
                        
                        st.caption("Nota: Se copiarán automáticamente la Línea Mayor, Subcategoría, Imagen y URL de Compra del producto original.")

                        if st.form_submit_button("🚀 Separar y Crear Producto"):
                            if not n_marca or not n_modelo or not n_nombre:
                                st.error("Debes llenar Marca, Modelo y Nombre.")
                            else:
                                try:
                                    with engine.connect() as conn:
                                        trans = conn.begin()
                                        res_insert = conn.execute(text("""
                                            INSERT INTO Productos (marca, modelo, nombre, macro_categoria, categoria, color_principal, diametro, url_imagen, url_compra)
                                            VALUES (:m, :mod, :nom, :macro, :cat, :col, :dia, :uimg, :ubuy)
                                            RETURNING id_producto
                                        """), {
                                            "m": n_marca, "mod": n_modelo, "nom": n_nombre, "macro": curr['macro_categoria'],
                                            "cat": curr['categoria'], "col": curr['color_principal'], "dia": curr['diametro'],
                                            "uimg": curr['url_imagen'], "ubuy": curr['url_compra']
                                        })
                                        new_id_producto = res_insert.fetchone()[0]
                                        
                                        conn.execute(text("UPDATE Variantes SET id_producto = :new_id WHERE sku = :sku"), 
                                                     {"new_id": new_id_producto, "sku": curr['sku']})
                                        trans.commit()
                                    st.success(f"✅ Éxito: {curr['sku']} movido a un nuevo producto padre.")
                                    time.sleep(2)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error al separar: {e}")
                else:
                    st.warning("SKU no encontrado.")

    # ==============================================================================
    # TAB 2: MARKETING & FEED META
    # ==============================================================================
    with tab_marketing:
        st.header("📢 Configuración para Meta Ads")
        st.info("Genera el archivo CSV compatible con Facebook e Instagram Shop, con enrutamiento automático de dominios.")

        with st.expander("⚙️ Configuración de Descripciones Base", expanded=True):
            desc_default = st.text_input("Descripción genérica adjunta:", value="Producto garantizado de alta calidad.")

        if st.button("🚀 GENERAR FEED PRO", type="primary"):
            with st.spinner("Generando catálogo unificado..."):
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT 
                                v.sku, p.macro_categoria, p.categoria, p.marca, p.modelo, p.nombre, p.url_imagen, 
                                v.precio, (v.stock_interno + v.stock_externo) as stock_total
                            FROM Variantes v
                            JOIN Productos p ON v.id_producto = p.id_producto
                            WHERE p.url_imagen IS NOT NULL AND p.url_imagen != ''
                        """)
                        df_raw = pd.read_sql(query, conn)

                    if df_raw.empty:
                        st.error("⚠️ No hay productos con imagen guardada.")
                    else:
                        df_feed = pd.DataFrame()
                        df_feed['id'] = df_raw['sku']
                        
                        df_feed['title'] = (
                            df_raw['marca'].fillna('') + " " + 
                            df_raw['modelo'].fillna('') + " - " + 
                            df_raw['nombre'].fillna('') + " (" +
                            df_raw['sku'].fillna('') + ")"
                        ).str.strip()

                        df_feed['description'] = desc_default + " " + df_feed['title']
                        df_feed['availability'] = df_raw['stock_total'].apply(lambda x: 'in_stock' if x > 0 else 'out_of_stock')
                        df_feed['condition'] = 'new'
                        df_feed['price'] = df_raw['precio'].astype(str) + ' PEN'
                        
                        # --- REFACTOR 5: Enrutamiento Dinámico de URLs de Tienda ---
                        df_feed['link'] = df_raw.apply(
                            lambda r: f"https://pelucat.pe/producto/{r['sku']}" if r['macro_categoria'] == 'Pelucas' else f"https://kmlentes.pe/producto/{r['sku']}", 
                            axis=1
                        )

                        df_feed['image_link'] = df_raw['url_imagen']
                        df_feed['brand'] = df_raw['marca'].fillna('K&M')
                        df_feed['custom_label_0'] = df_raw['macro_categoria'].fillna('Lentes')
                        df_feed['product_type'] = df_raw['categoria'].fillna('General')

                        st.success(f"✅ Feed comercial generado con {len(df_feed)} variantes.")
                        st.dataframe(df_feed.head(3), use_container_width=True)
                        
                        csv = df_feed.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 DESCARGAR FEED META (CSV)", data=csv, file_name="feed_meta_unificado.csv", mime="text/csv")

                except Exception as e:
                    st.error(f"Error generando feed: {e}")