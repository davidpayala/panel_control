st.subheader("🔧 Administración de Productos y Variantes")

# --- BARRA LATERAL: BUSCADOR RÁPIDO ---
with st.expander("🔎 Verificador Rápido de SKU / Nombre", expanded=False):
    check_str = st.text_input("Escribe para buscar coincidencias:", placeholder="Ej: NL01")
    if check_str:
        with engine.connect() as conn:
            q_check = text("""
                SELECT v.sku, p.modelo, p.nombre as color, v.medida 
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

modo_catalogo = st.radio("Acción:", ["🌱 Crear Nuevo", "✏️ Editar / Renombrar"], horizontal=True)

# LISTA OFICIAL DE COLORES
COLORES_OFICIALES = ["", "Amarillo", "Azul", "Blanco", "Chocolate", "Dorado", "Gris", "Marrón", "Miel", "Morado", "Multicolor", "Naranja", "Negro", "Rojo", "Rosado", "Turquesa", "Verde"]

# ------------------------------------------------------------------
# MODO 1: CREAR NUEVO (ESTA PARTE SE MANTIENE IGUAL)
# ------------------------------------------------------------------
if modo_catalogo == "🌱 Crear Nuevo":
    tipo_creacion = st.selectbox("Tipo de Creación:", 
                                    ["Medida Nueva (Hijo) para Producto Existente", 
                                    "Producto Nuevo (Marca/Color Nuevo)"])
    
    # A) NUEVA MEDIDA
    if "Medida Nueva" in tipo_creacion:
        with engine.connect() as conn:
            df_prods = pd.read_sql(text("SELECT id_producto, marca, modelo, nombre FROM Productos ORDER BY marca, modelo, nombre"), conn)
        
        if not df_prods.empty:
            opciones_prod = df_prods.apply(lambda x: f"{x['marca']} {x['modelo']} - {x['nombre']} (ID: {x['id_producto']})", axis=1).to_dict()
            idx_prod = st.selectbox("Selecciona el Producto (Modelo y Color):", options=opciones_prod.keys(), format_func=lambda x: opciones_prod[x])
            id_producto_real = df_prods.iloc[idx_prod]['id_producto']
            
            with st.form("form_add_variante"):
                st.caption(f"Agregando medida a: **{df_prods.iloc[idx_prod]['nombre']}**")
                c1, c2 = st.columns(2)
                sku_new = c1.text_input("Nuevo SKU (Único):").strip()
                medida_new = c2.text_input("Medida / Graduación:", value="0.00")

                c3, c4 = st.columns(2)
                stock_ini = c3.number_input("Stock Inicial:", min_value=0)
                precio_new = c4.number_input("Precio Venta:", min_value=0.0)
                
                ubi_new = st.text_input("Ubicación:")

                if st.form_submit_button("Guardar Medida"):
                    try:
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                VALUES (:sku, :idp, '', :med, :si, :pre, :ubi)
                            """), {
                                "sku": sku_new, "idp": int(id_producto_real), 
                                "med": medida_new, "si": stock_ini, "pre": precio_new, "ubi": ubi_new
                            })
                            conn.commit()
                        st.success(f"SKU {sku_new} creado exitosamente.")
                    except Exception as e:
                        st.error(f"Error: {e}")

    # B) PRODUCTO NUEVO
    else:
        with st.form("form_new_full"):
            st.markdown("**1. Definir Producto (Visual)**")
            c1, c2, c3 = st.columns(3)
            marca = c1.text_input("Marca:")
            modelo = c2.text_input("Modelo:")
            nombre_prod = c3.text_input("Nombre (Color):", placeholder="Ej: Gris, Azul...")
            
            c_cat, c_col = st.columns(2)
            categ = c_cat.selectbox("Categoría:", ["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"])
            color_prin = c_col.selectbox("Color Filtro (Base):", COLORES_OFICIALES)

            c_dia, c_url1 = st.columns(2)
            diametro = c_dia.number_input("Diámetro (mm):", min_value=0.0, step=0.1, format="%.1f")
            url_img = c_url1.text_input("URL Imagen (Foto):")
            url_buy = st.text_input("URL Compra (Importación):")

            st.markdown("**2. Crear Primera Medida (Ej: Plano)**")
            c4, c5, c6 = st.columns(3)
            sku_1 = c4.text_input("SKU Variante:")
            medida_1 = c5.text_input("Medida:", value="0.00")
            prec_1 = c6.number_input("Precio Venta", 0.0)
            
            ubi_1 = st.text_input("Ubicación")

            if st.form_submit_button("Crear Producto Completo"):
                try:
                    with engine.connect() as conn:
                        trans = conn.begin()
                        res_p = conn.execute(text("""
                            INSERT INTO Productos (marca, modelo, nombre, categoria, color_principal, diametro, url_imagen, url_compra) 
                            VALUES (:m, :mod, :nom, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto
                        """), {
                            "m": marca, "mod": modelo, "nom": nombre_prod, "cat": categ, "col": color_prin, 
                            "dia": str(diametro), "uimg": url_img, "ubuy": url_buy
                        })
                        new_id = res_p.fetchone()[0]

                        conn.execute(text("""
                            INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                            VALUES (:sku, :idp, '', :med, 0, :pr, :ub)
                        """), {
                            "sku": sku_1, "idp": new_id, "med": medida_1,
                            "pr": prec_1, "ub": ubi_1
                        })
                        trans.commit()
                    st.success(f"Producto '{nombre_prod}' creado con éxito.")
                except Exception as e:
                    st.error(f"Error: {e}")

# ------------------------------------------------------------------
# MODO 2: EDITAR / RENOMBRAR (AQUÍ ESTÁ EL CAMBIO)
# ------------------------------------------------------------------
else:
    st.markdown("#### ✏️ Modificar Producto")
    
    sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
    
    if sku_edit:
        with engine.connect() as conn:
            query_full = text("""
                SELECT v.*, p.marca, p.modelo, p.nombre as nombre_prod, p.categoria, p.diametro, p.color_principal, p.url_imagen, p.url_compra
                FROM Variantes v 
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.sku = :sku
            """)
            df_data = pd.read_sql(query_full, conn, params={"sku": sku_edit})
        
        if not df_data.empty:
            curr = df_data.iloc[0]
            
            col_img, col_form = st.columns([1, 3])
            
            with col_img:
                if curr['url_imagen']:
                    st.image(curr['url_imagen'], caption="Foto Actual", width='stretch')
                else:
                    st.info("Sin imagen")

            with col_form:
                st.info(f"Editando: **{curr['marca']} {curr['modelo']}** - Color: **{curr['nombre_prod']}**")
                
                with st.form("form_edit_sku"):
                    # 1. PRODUCTO
                    st.markdown("📦 **Datos Generales (Producto)**")
                    
                    c_p1, c_p2, c_p3 = st.columns(3)
                    new_marca = c_p1.text_input("Marca:", value=curr['marca'])
                    new_modelo = c_p2.text_input("Modelo:", value=curr['modelo'])
                    new_nombre_prod = c_p3.text_input("Nombre (Color):", value=curr['nombre_prod'])
                    
                    c_p4, c_p5 = st.columns(2)
                    idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                    new_color_prin = c_p4.selectbox("Color Filtro:", COLORES_OFICIALES, index=idx_col)
                    val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                    new_diametro = c_p5.number_input("Diámetro:", value=val_dia, step=0.1, format="%.1f")

                    new_url_img = st.text_input("URL Imagen:", value=curr['url_imagen'] if curr['url_imagen'] else "")
                    new_url_buy = st.text_input("URL Compra:", value=curr['url_compra'] if curr['url_compra'] else "")

                    st.divider()

                    # 2. VARIANTE (SKU y Medidas)
                    st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                    col_a, col_b = st.columns(2)
                    new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                    new_medida = col_b.text_input("Medida:", value=curr['medida'] if curr['medida'] else "0.00")
                    
                    col_e, col_f = st.columns(2)
                    new_precio = col_e.number_input("Precio Normal:", value=float(curr['precio']))
                    
                    # --- CAMBIO AQUÍ: TEXT INPUT PARA PERMITIR VACÍO ---
                    # Si hay precio rebajado y es > 0, lo mostramos. Si es None o 0, mostramos vacío.
                    val_reb_str = str(curr['precio_rebajado']) if (curr['precio_rebajado'] and float(curr['precio_rebajado']) > 0) else ""
                    new_precio_reb_txt = col_f.text_input("Precio Rebajado (Vacío = Sin Oferta):", value=val_reb_str)

                    if st.form_submit_button("💾 Guardar Cambios"):
                        # Lógica para convertir el texto a Float o Null
                        final_rebajado = None
                        if new_precio_reb_txt.strip(): # Si no está vacío
                            try:
                                final_rebajado = float(new_precio_reb_txt)
                            except:
                                st.error("El precio rebajado debe ser un número (o dejarlo vacío).")
                                st.stop()

                        try:
                            with engine.connect() as conn:
                                trans = conn.begin()
                                
                                # A) Actualizar Variante
                                conn.execute(text("""
                                    UPDATE Variantes 
                                    SET sku=:n_sku, medida=:n_med, precio=:n_pre, precio_rebajado=:n_prer
                                    WHERE sku=:old_sku
                                """), {
                                    "n_sku": new_sku_val, "n_med": new_medida,
                                    "n_pre": new_precio, "n_prer": final_rebajado, # Pasamos el valor procesado
                                    "old_sku": curr['sku']
                                })

                                # B) Actualizar Producto
                                conn.execute(text("""
                                    UPDATE Productos 
                                    SET marca=:mar, modelo=:mod, nombre=:nom, color_principal=:col, diametro=:dia,
                                        url_imagen=:uimg, url_compra=:ubuy
                                    WHERE id_producto=:idp
                                """), {
                                    "mar": new_marca, "mod": new_modelo, "nom": new_nombre_prod,
                                    "col": new_color_prin, "dia": str(new_diametro), 
                                    "uimg": new_url_img, "ubuy": new_url_buy,
                                    "idp": int(curr['id_producto'])
                                })
                                
                                trans.commit()
                            
                            st.success("✅ ¡Actualizado correctamente!")
                            time.sleep(1.5)
                            st.rerun()

                        except Exception as e:
                            st.error(f"Error: {e}")
    else:
        st.warning("SKU no encontrado.")


# ------------------------------------------------------------------
# HERRAMIENTA EXTRA: SEPARAR VARIANTE (SPLIT)
# ------------------------------------------------------------------
st.divider()
with st.expander("✂️ Herramienta: Separar Variante (Convertir en Producto Independiente)", expanded=False):
    st.info("Usa esto cuando una variante (SKU) está metida dentro de un Producto incorrecto y quieres que tenga su propio Producto Padre separado.")
    
    sku_to_split = st.text_input("Ingresa el SKU a separar:", placeholder="Ej: NL-ERROR-01")
    
    if sku_to_split:
        with engine.connect() as conn:
            # 1. Buscamos la info actual
            q_split = text("""
                SELECT v.sku, v.id_producto, p.marca, p.modelo, p.nombre, p.categoria, p.color_principal, p.diametro, p.url_imagen, p.url_compra
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                WHERE v.sku = :s
            """)
            res_split = pd.read_sql(q_split, conn, params={"s": sku_to_split})
        
        if not res_split.empty:
            curr = res_split.iloc[0]
            
            st.markdown(f"La variante **{curr['sku']}** actualmente pertenece a: **{curr['marca']} {curr['modelo']} - {curr['nombre']}** (ID: {curr['id_producto']})")
            st.write("👇 **Configura el NUEVO Producto Padre para esta variante:**")
            
            with st.form("form_split_product"):
                c1, c2, c3 = st.columns(3)
                # Pre-llenamos con los datos del padre anterior para facilitar, pero dejamos editar
                n_marca = c1.text_input("Nueva Marca", value=curr['marca'])
                n_modelo = c2.text_input("Nuevo Modelo", value=curr['modelo'])
                n_nombre = c3.text_input("Nuevo Nombre (Color)", value=curr['nombre']) # Aquí es donde seguramente cambiarás el nombre
                
                c4, c5 = st.columns(2)
                n_cat = c4.selectbox("Categoría", ["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"], index=["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"].index(curr['categoria']) if curr['categoria'] in ["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"] else 0)
                
                idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                n_color = c5.selectbox("Color Principal", COLORES_OFICIALES, index=idx_col)
                
                c6, c7, c8 = st.columns(3)
                val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                n_diam = c6.number_input("Diámetro", value=val_dia)
                n_img = c7.text_input("URL Imagen", value=curr['url_imagen'] or "")
                n_buy = c8.text_input("URL Compra", value=curr['url_compra'] or "")
                
                st.caption("Al confirmar, se creará un producto nuevo y este SKU se moverá ahí.")
                
                if st.form_submit_button("🚀 Separar y Crear Nuevo Producto"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            
                            # A) Crear el NUEVO Producto Padre
                            res_new_prod = conn.execute(text("""
                                INSERT INTO Productos (marca, modelo, nombre, categoria, color_principal, diametro, url_imagen, url_compra)
                                VALUES (:ma, :mo, :no, :ca, :co, :di, :ui, :uc)
                                RETURNING id_producto
                            """), {
                                "ma": n_marca, "mo": n_modelo, "no": n_nombre, "ca": n_cat,
                                "co": n_color, "di": str(n_diam), "ui": n_img, "uc": n_buy
                            })
                            new_id_prod = res_new_prod.fetchone()[0]
                            
                            # B) Mover la Variante al nuevo Padre
                            conn.execute(text("""
                                UPDATE Variantes 
                                SET id_producto = :new_id 
                                WHERE sku = :sku
                            """), {"new_id": new_id_prod, "sku": sku_to_split})
                            
                            trans.commit()
                            
                        st.success(f"✅ ¡Listo! La variante {sku_to_split} ahora es independiente en su propio producto (ID: {new_id_prod}).")
                        time.sleep(2)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error al separar: {e}")
        else:
            st.warning("No se encontró ese SKU.")            
# --- Poner esto en un botón en tu App ---
if st.button("📢 Generar Feed para Facebook"):
try:
    total = generar_feed_facebook()
    st.success(f"✅ Feed generado con {total} productos.")
    st.info("Tu URL para Facebook es: https://panelcontrol-production.up.railway.app/app/static/feed_facebook.csv") 
    # (Ojo: Tendrás que configurar tu servidor para que sirva este archivo)
except Exception as e:
    st.error(f"Error: {e}")