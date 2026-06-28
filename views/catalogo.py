import json
import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
import utils

def render_catalogo():
    st.subheader("🔧 Administración de Productos y Variantes")

    # --- 1. BARRA LATERAL: BUSCADOR RÁPIDO ---
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
        modo_catalogo = st.radio(
            "Acción:", 
            ["🌱 Crear Nuevo", "✏️ Editar / Renombrar", "🌳 Árbol de Jerarquía (Padre e Hijos)"], 
            horizontal=True
        )

        COLORES_OFICIALES = ["", "Amarillo", "Azul", "Blanco", "Chocolate", "Dorado", "Gris", "Marrón", "Miel", "Morado", "Multicolor", "Naranja", "Negro", "Rojo", "Rosado", "Turquesa", "Verde"]

        # ------------------------------------------------------------------------------
        # MODO 1: CREAR NUEVO
        # ------------------------------------------------------------------------------
        if modo_catalogo == "🌱 Crear Nuevo":
            tipo_creacion = st.selectbox("Tipo de Creación:", 
                                            ["Medida Nueva (Hijo) para Producto Existente", 
                                             "Producto Nuevo (Marca/Color Nuevo)"])
            
            if "Medida Nueva" in tipo_creacion:
                with engine.connect() as conn:
                    df_prods = pd.read_sql(text("SELECT id_producto, macro_categoria, categoria, marca, modelo, nombre FROM Productos ORDER BY macro_categoria, marca, modelo, nombre"), conn)
                
                if not df_prods.empty:
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
                            if not sku_new or not sku_new.strip():
                                st.error("⚠️ El campo 'Nuevo SKU' es obligatorio.")
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

            else:
                with st.form("form_new_full"):
                    st.markdown("**1. Definir Jerarquía y Producto**")
                    col_mac, col_cat, col_col = st.columns(3)
                    macro_sel = col_mac.selectbox("Línea de Negocio (Macro):", ["Lentes", "Pelucas"])
                    
                    # --- CORRECCIÓN DE SUB-CATEGORÍAS EN CREACIÓN ---
                    if macro_sel == "Lentes": 
                        opciones_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios"]
                    else: 
                        opciones_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios Pelucas"]

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
                        if not marca.strip() or not nombre_prod.strip() or not sku_1.strip():
                            st.error("⚠️ Marca, Nombre y SKU son obligatorios.")
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
                                    """), {"sku": sku_1.strip(), "idp": new_id, "med": medida_1, "pr": prec_1, "ub": ubi_1})
                                    trans.commit()
                                st.success(f"Producto '{nombre_prod}' creado con éxito bajo el SKU '{sku_1}'.")
                            except Exception as e: st.error(f"Error: {e}")

        # ------------------------------------------------------------------------------
        # MODO 2: EDITAR / RENOMBRAR / RECLASIFICAR (¡PERFECTAMENTE UNIFICADO!)
        # ------------------------------------------------------------------------------
        elif modo_catalogo == "✏️ Editar / Renombrar":
            st.markdown("#### ✏️ Modificar Producto y Reclasificar")
            sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
            
            if sku_edit:
                with engine.connect() as conn:
                    # BLINDAJE SQL: Resuelve macro_categoria con CASE WHEN preventivo
                    q_edit = text("""
                        SELECT 
                            v.sku, v.id_producto, v.medida, v.precio, v.precio_rebajado, 
                            v.stock_interno, v.stock_externo, v.stock_transito, v.ubicacion,
                            v.url_imagen as url_imagen_variante,
                            p.marca, p.modelo, p.nombre as nombre_prod, 
                            CASE 
                                WHEN p.macro_categoria ILIKE 'peluca%' OR v.sku ILIKE 'WB-%' OR v.sku ILIKE 'WIG-%' THEN 'Pelucas'
                                ELSE 'Lentes'
                            END AS macro_categoria, 
                            p.categoria, p.diametro, p.color_principal, p.url_compra,
                            p.url_imagen as url_imagen_padre
                        FROM Variantes v 
                        JOIN Productos p ON v.id_producto = p.id_producto 
                        WHERE v.sku = :sku
                    """)
                    df_data = pd.read_sql(q_edit, conn, params={"sku": sku_edit})
                
                if not df_data.empty:
                    curr = df_data.iloc[0]
                    
                    foto_v = str(curr['url_imagen_variante']).strip() if pd.notna(curr['url_imagen_variante']) else ""
                    foto_p = str(curr['url_imagen_padre']).strip() if pd.notna(curr['url_imagen_padre']) else ""
                    foto_mostrar = foto_v if foto_v and foto_v != 'nan' else (foto_p if foto_p and foto_p != 'nan' else "")

                    col_img, col_form = st.columns([1, 3])
                    with col_img:
                        if foto_mostrar: 
                            st.image(foto_mostrar, caption=f"Foto {'Variante' if foto_v and foto_v!='nan' else 'Portada'}", use_column_width=True)
                        else: 
                            st.info("Sin foto")

                    with col_form:
                        with st.form("form_edit_sku"):
                            st.markdown("📦 **Jerarquía y Datos Generales**")
                            c_mac, c_cat = st.columns(2)
                            macro_act = curr['macro_categoria'] if curr['macro_categoria'] else "Lentes"
                            new_macro = c_mac.selectbox("Línea (Macro):", ["Lentes", "Pelucas"], index=["Lentes", "Pelucas"].index(macro_act))

                            # --- CORRECCIÓN DE SUB-CATEGORÍAS EN EDICIÓN ---
                            if new_macro == "Lentes": 
                                opts_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios"]
                            else: 
                                opts_sub = ["Estilo Natural", "Estilo Fantasía", "Accesorios Pelucas"]

                            cat_act = curr['categoria'] if curr['categoria'] in opts_sub else opts_sub[0]
                            new_cat = c_cat.selectbox("Subcategoría:", opts_sub, index=opts_sub.index(cat_act))

                            c_p1, c_p2, c_p3 = st.columns(3)
                            new_marca = c_p1.text_input("Marca:", value=curr['marca'])
                            new_modelo = c_p2.text_input("Modelo:", value=curr['modelo'])
                            new_nombre_prod = c_p3.text_input("Nombre (Color):", value=curr['nombre_prod'])
                            
                            c_p4, c_p5 = st.columns(2)
                            idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                            new_color_prin = c_p4.selectbox("Color Filtro:", COLORES_OFICIALES, index=idx_col)
                            val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                            new_diametro = c_p5.number_input("Diámetro/Largo:", value=val_dia, step=0.1, format="%.1f")

                            new_url_buy = st.text_input("URL Compra (Proveedor):", value=curr['url_compra'] or "")

                            st.divider()
                            st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                            col_a, col_b = st.columns(2)
                            new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                            new_medida = col_b.text_input("Medida/Talla:", value=curr['medida'] or "0.00")
                            
                            col_e, col_f = st.columns(2)
                            new_precio = col_e.number_input("Precio Normal:", value=float(curr['precio']))
                            val_reb_str = str(curr['precio_rebajado']) if (curr['precio_rebajado'] and float(curr['precio_rebajado']) > 0) else ""
                            new_precio_reb_txt = col_f.text_input("Precio Rebajado:", value=val_reb_str)

                            st.divider()
                            st.markdown("📸 **Control Granular de Fotografías**")
                            new_url_img_p = st.text_input("URL Foto Portada (Padre):", value=foto_p if foto_p!='nan' else "", help="Foto representativa de la familia")
                            new_url_img_v = st.text_input("URL Foto de esta Variante (Hijo):", value=foto_v if foto_v!='nan' else "", help="Foto específica de este SKU/Color")

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
                                            SET sku=:n_sku, medida=:n_med, precio=:n_pre, precio_rebajado=:n_prer, url_imagen=:uimg_v 
                                            WHERE sku=:old_sku
                                        """), {
                                            "n_sku": new_sku_val, "n_med": new_medida, "n_pre": new_precio, 
                                            "n_prer": final_rebajado, "uimg_v": new_url_img_v.strip() if new_url_img_v.strip() else None, 
                                            "old_sku": curr['sku']
                                        })
                                        conn.execute(text("""
                                            UPDATE Productos 
                                            SET marca=:mar, modelo=:mod, nombre=:nom, macro_categoria=:macro, categoria=:cat, 
                                                color_principal=:col, diametro=:dia, url_imagen=:uimg_p, url_compra=:ubuy 
                                            WHERE id_producto=:idp
                                        """), {
                                            "mar": new_marca, "mod": new_modelo, "nom": new_nombre_prod, "macro": new_macro, 
                                            "cat": new_cat, "col": new_color_prin, "dia": str(new_diametro), 
                                            "uimg_p": new_url_img_p.strip() if new_url_img_p.strip() else None, 
                                            "ubuy": new_url_buy, "idp": int(curr['id_producto'])
                                        })
                                        trans.commit()
                                    st.success("✅ ¡Actualizado correctamente!")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e: st.error(f"Error: {e}")
            else: st.warning("SKU no encontrado.")

            st.divider()
            with st.expander("✂️ Separar Variante (Mover a Producto Nuevo)", expanded=False):
                sku_to_split = st.text_input("Ingresa el SKU a separar:", placeholder="Ej: NL-ERROR-01")
                if sku_to_split:
                    with engine.connect() as conn:
                        res_split = pd.read_sql(text("SELECT v.sku, v.id_producto, p.marca, p.modelo, p.nombre, p.macro_categoria, p.categoria, p.color_principal, p.diametro, p.url_imagen, p.url_compra FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto WHERE v.sku = :s"), conn, params={"s": sku_to_split})
                    
                    if not res_split.empty:
                        curr = res_split.iloc[0]
                        with st.form("form_split_product"):
                            st.write("Define los datos del **NUEVO** producto contenedor:")
                            c1, c2, c3 = st.columns(3)
                            n_marca = c1.text_input("Nueva Marca", value=curr['marca'])
                            n_modelo = c2.text_input("Nuevo Modelo", value=curr['modelo'])
                            n_nombre = c3.text_input("Nuevo Nombre (Color)", value=curr['nombre']) 
                            
                            if st.form_submit_button("🚀 Separar y Crear Producto"):
                                if not n_marca or not n_modelo or not n_nombre: st.error("Llenar Marca, Modelo y Nombre.")
                                else:
                                    try:
                                        with engine.connect() as conn:
                                            trans = conn.begin()
                                            res_insert = conn.execute(text("INSERT INTO Productos (marca, modelo, nombre, macro_categoria, categoria, color_principal, diametro, url_imagen, url_compra) VALUES (:m, :mod, :nom, :macro, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto"), 
                                                {"m": n_marca, "mod": n_modelo, "nom": n_nombre, "macro": curr['macro_categoria'], "cat": curr['categoria'], "col": curr['color_principal'], "dia": curr['diametro'], "uimg": curr['url_imagen'], "ubuy": curr['url_compra']})
                                            new_id_producto = res_insert.fetchone()[0]
                                            conn.execute(text("UPDATE Variantes SET id_producto = :new_id WHERE sku = :sku"), {"new_id": new_id_producto, "sku": curr['sku']})
                                            trans.commit()
                                        st.success(f"✅ Éxito: {curr['sku']} movido a un nuevo producto padre.")
                                        time.sleep(2)
                                        st.rerun()
                                    except Exception as e: st.error(f"Error al separar: {e}")

        # ------------------------------------------------------------------------------
        # MODO 3: EXPLORADOR CON BORRADO MÚLTIPLE Y MEMORIA DE ESTADO
        # ------------------------------------------------------------------------------
        else:
            st.markdown("#### 🌳 Explorador de Jerarquía (Padre e Hijos)")
            st.info("Selecciona un ítem raíz para inspeccionar sus existencias o aplicar borrado masivo sin perder la vista.")

            if 'cat_padre_inspect_id' not in st.session_state:
                st.session_state['cat_padre_inspect_id'] = None
            
            with engine.connect() as conn:
                df_padres = pd.read_sql(text("""
                    SELECT p.id_producto, COALESCE(p.macro_categoria, 'Lentes') as macro_categoria, p.categoria, p.marca, p.modelo, p.nombre, 
                           COUNT(v.sku) as total_variantes
                    FROM Productos p
                    LEFT JOIN Variantes v ON p.id_producto = v.id_producto
                    GROUP BY p.id_producto
                    ORDER BY p.macro_categoria, p.marca, p.modelo, p.nombre
                """), conn)
                
            if not df_padres.empty:
                df_padres['label'] = df_padres.apply(
                    lambda r: f"[{r['macro_categoria']}] {r['marca']} {r['modelo']} - {r['nombre']}  ({r['total_variantes']} SKUs)  [ID: {r['id_producto']}]", 
                    axis=1
                )
                mapa_padres = dict(zip(df_padres['label'], df_padres['id_producto']))
                lista_titulos = list(mapa_padres.keys())

                idx_memoria = 0
                if st.session_state['cat_padre_inspect_id'] is not None:
                    for i, titulo in enumerate(lista_titulos):
                        if mapa_padres[titulo] == st.session_state['cat_padre_inspect_id']:
                            idx_memoria = i
                            break

                sel_padre_label = st.selectbox("Selecciona el Producto Padre a inspeccionar:", options=lista_titulos, index=idx_memoria)
                id_padre_sel = mapa_padres[sel_padre_label]
                st.session_state['cat_padre_inspect_id'] = id_padre_sel 
                
                with engine.connect() as conn:
                    p_info = conn.execute(text("SELECT * FROM Productos WHERE id_producto = :id"), {"id": int(id_padre_sel)}).fetchone()
                    df_hijos = pd.read_sql(text("""
                        SELECT sku, medida, precio, precio_rebajado, stock_interno, stock_externo, stock_transito, ubicacion 
                        FROM Variantes WHERE id_producto = :id ORDER BY sku ASC
                    """), conn, params={"id": int(id_padre_sel)})

                c_inf1, c_inf2 = st.columns([1, 3])
                with c_inf1:
                    if p_info.url_imagen: st.image(p_info.url_imagen, use_column_width=True)
                    else: st.caption("Sin foto")
                with c_inf2:
                    st.markdown(f"**Línea:** `{p_info.macro_categoria}` | **Subcat:** `{p_info.categoria}` | **Marca:** `{p_info.marca}`\n\n**Modelo y Color:** `{p_info.modelo} - {p_info.nombre}`")
                
                st.markdown("##### 🏷️ Desglose de Variantes Asociadas")
                if not df_hijos.empty:
                    st.dataframe(df_hijos, use_container_width=True, hide_index=True)
                    
                    st.divider()
                    st.markdown("⚠️ **Herramientas de Destrucción Masiva**")
                    col_del_v, col_del_p = st.columns(2)
                    
                    with col_del_v:
                        with st.container(border=True):
                            st.markdown("**🗑️ Borrar Variantes (Multi-selección)**")
                            skus_a_borrar = st.multiselect("Selecciona uno o varios SKUs para desintegrar:", options=df_hijos['sku'].tolist(), placeholder="Puedes elegir varios...")
                            if st.button(f"💥 Borrar {len(skus_a_borrar)} SKU(s) marcados", type="primary", disabled=len(skus_a_borrar)==0):
                                try:
                                    with engine.begin() as tx:
                                        for s_target in skus_a_borrar: tx.execute(text("DELETE FROM Variantes WHERE sku = :s"), {"s": s_target})
                                    st.success(f"¡Se eliminaron {len(skus_a_borrar)} variantes correctamente!")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    if "foreign key" in str(e).lower() or "integrityerror" in str(e).lower():
                                        st.error("❌ Rechazado: Al menos uno de los SKUs seleccionados ya está amarrado a boletas de venta pasadas.")
                                    else: st.error(f"Error SQL: {e}")

                    with col_del_p:
                        with st.container(border=True):
                            st.markdown("**🚨 Destrucción Total (Raíz + Variantes)**")
                            st.caption("Eliminará permanentemente al padre y a todas sus variantes.")
                            seguro_padre = st.checkbox(f"Confirmo desintegrar el ítem raíz ID {id_padre_sel}")
                            if st.button("💥 Desintegrar Raíz Completa", type="primary", disabled=not seguro_padre):
                                try:
                                    with engine.begin() as tx:
                                        tx.execute(text("DELETE FROM Variantes WHERE id_producto = :id"), {"id": int(id_padre_sel)})
                                        tx.execute(text("DELETE FROM Productos WHERE id_producto = :id"), {"id": int(id_padre_sel)})
                                    st.session_state['cat_padre_inspect_id'] = None
                                    st.success("¡Producto desintegrado!")
                                    time.sleep(1.5)
                                    st.rerun()
                                except Exception as e:
                                    if "foreign key" in str(e).lower() or "integrityerror" in str(e).lower():
                                        st.error("❌ Rechazado: Alguna de las variantes ya está amarrada a ventas históricas.")
                                    else: st.error(f"Error SQL: {e}")
                else:
                    st.warning("⚠️ Este producto raíz es un huérfano (no tiene variantes hijas).")
                    if st.button("🗑️ Eliminar Raíz Huérfana", type="primary"):
                        with engine.begin() as tx: tx.execute(text("DELETE FROM Productos WHERE id_producto = :id"), {"id": int(id_padre_sel)})
                        st.session_state['cat_padre_inspect_id'] = None
                        st.success("Raíz eliminada.")
                        time.sleep(1)
                        st.rerun()

    # ==============================================================================
    # TAB 2: MARKETING & FEED META 
    # ==============================================================================
    with tab_marketing:
        st.header("📢 Configuración para Meta Ads")
        st.info("Genera el archivo CSV compatible con Facebook e Instagram Shop.")

        with st.expander("⚙️ Configuración de Descripciones Base", expanded=True):
            desc_default = st.text_input("Descripción genérica adjunta:", value="Producto garantizado de alta calidad.")

        if st.button("🚀 GENERAR FEED PRO", type="primary"):
            with st.spinner("Generando catálogo unificado..."):
                try:
                    with engine.connect() as conn:
                        q_feed = text("""
                            SELECT 
                                v.sku, p.macro_categoria, p.categoria, p.marca, p.modelo, p.nombre, 
                                COALESCE(NULLIF(TRIM(v.url_imagen), ''), NULLIF(TRIM(p.url_imagen), '')) AS url_imagen, 
                                v.precio, (v.stock_interno + v.stock_externo) as stock_total 
                            FROM Variantes v 
                            JOIN Productos p ON v.id_producto = p.id_producto 
                            WHERE (v.url_imagen IS NOT NULL AND v.url_imagen != '') 
                               OR (p.url_imagen IS NOT NULL AND p.url_imagen != '')
                        """)
                        df_raw = pd.read_sql(q_feed, conn)

                    if df_raw.empty: st.error("⚠️ No hay productos con imagen guardada.")
                    else:
                        df_feed = pd.DataFrame()
                        df_feed['id'] = df_raw['sku']
                        df_feed['title'] = (df_raw['marca'].fillna('') + " " + df_raw['modelo'].fillna('') + " - " + df_raw['nombre'].fillna('') + " (" + df_raw['sku'].fillna('') + ")").str.strip()
                        df_feed['description'] = desc_default + " " + df_feed['title']
                        df_feed['availability'] = df_raw['stock_total'].apply(lambda x: 'in_stock' if x > 0 else 'out_of_stock')
                        df_feed['condition'] = 'new'
                        df_feed['price'] = df_raw['precio'].astype(str) + ' PEN'
                        df_feed['link'] = df_raw.apply(lambda r: f"https://pelucat.pe/producto/{r['sku']}" if r['macro_categoria'] == 'Pelucas' else f"https://kmlentes.pe/producto/{r['sku']}", axis=1)
                        df_feed['image_link'] = df_raw['url_imagen']
                        df_feed['brand'] = df_raw['marca'].fillna('K&M')
                        df_feed['custom_label_0'] = df_raw['macro_categoria'].fillna('Lentes')
                        df_feed['product_type'] = df_raw['categoria'].fillna('General')

                        st.success(f"✅ Feed generado con {len(df_feed)} variantes.")
                        st.dataframe(df_feed.head(3), use_container_width=True)
                        csv = df_feed.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 DESCARGAR FEED META (CSV)", data=csv, file_name="feed_meta_unificado.csv", mime="text/csv")

                except Exception as e: st.error(f"Error generando feed: {e}")