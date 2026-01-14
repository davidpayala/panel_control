    # Importamos random aquí por seguridad por si falta arriba
    import random 

    # --- CABECERA ---
    col_modo, col_titulo = st.columns([1, 3])
    with col_modo:
        modo_operacion = st.radio("Modo:", ["💰 Venta", "📉 Salida / Merma"], horizontal=True)
    with col_titulo:
        if modo_operacion == "💰 Venta":
            st.subheader("🛒 Punto de Venta (Ingresos)")
        else:
            st.subheader("📉 Registro de Salidas (Mermas / Uso Interno)")

    st.divider()

    col_izq, col_der = st.columns([1, 1])

    # ------------------------------------------------------------------
    # COLUMNA IZQUIERDA: BUSCADOR
    # ------------------------------------------------------------------
    with col_izq:
        st.caption("1. Buscar Productos")
        tipo_producto = st.radio("Origen:", ["Inventario (SQL)", "Manual/Extra"], horizontal=True, label_visibility="collapsed")
        
        if tipo_producto == "Inventario (SQL)":
            sku_input = st.text_input("Escanear/Escribir SKU:", placeholder="Ej: CL-01...", key="sku_pos")
            if sku_input:
                with engine.connect() as conn:
                    res = pd.read_sql(text("""
                        SELECT v.sku, p.modelo, p.nombre as color, v.medida, v.stock_interno, v.precio, v.ubicacion 
                        FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE v.sku = :sku
                    """), conn, params={"sku": sku_input})
                
                if not res.empty:
                    prod = res.iloc[0]
                    nombre_full = f"{prod['modelo']} {prod['color']} ({prod['medida']})"
                    
                    if prod['stock_interno'] <= 0:
                        st.error(f"❌ Sin Stock ({prod['stock_interno']})")
                    else:
                        st.success(f"✅ Stock: {prod['stock_interno']} | 📍 {prod['ubicacion']}")

                    st.markdown(f"**{nombre_full}**")
                    
                    c1, c2 = st.columns(2)
                    cantidad = c1.number_input("Cant.", min_value=1, value=1)
                    precio_sugerido = float(prod['precio']) if modo_operacion == "💰 Venta" else 0.0
                    precio_final = c2.number_input("Precio Unit.", value=precio_sugerido, disabled=(modo_operacion != "💰 Venta"))
                    
                    if st.button("➕ Agregar"):
                        agregar_al_carrito(prod['sku'], nombre_full, cantidad, precio_final, True, prod['stock_interno'])
                else:
                    st.warning("SKU no encontrado.")
        
        else: 
            st.info("Item Manual (Servicios, etc.)")
            desc_manual = st.text_input("Descripción:")
            c1, c2 = st.columns(2)
            cant_manual = c1.number_input("Cant.", min_value=1, value=1, key="cm")
            precio_manual = c2.number_input("Precio", value=0.0, key="pm", disabled=(modo_operacion != "💰 Venta"))
            if st.button("➕ Agregar Manual"):
                if desc_manual: agregar_al_carrito(None, desc_manual, cant_manual, precio_manual, False)

    # ------------------------------------------------------------------
    # COLUMNA DERECHA: PROCESAR
    # ------------------------------------------------------------------
    with col_der:
        st.caption("2. Confirmación")
        
        if len(st.session_state.carrito) > 0:
            df_cart = pd.DataFrame(st.session_state.carrito)
            st.dataframe(df_cart[['descripcion', 'cantidad', 'subtotal']], hide_index=True, use_container_width=True)
            
            suma_subtotal = float(df_cart['subtotal'].sum())
            
            st.divider()

            # ==========================================================
            # MODO A: VENTA
            # ==========================================================
            if modo_operacion == "💰 Venta":
                st.markdown(f"**Subtotal Items:** S/ {suma_subtotal:.2f}")

                # 1. CLIENTE
                with engine.connect() as conn:
                    cli_df = pd.read_sql(text("SELECT id_cliente, nombre_corto FROM Clientes WHERE activo = TRUE ORDER BY nombre_corto"), conn)
                lista_cli = {row['nombre_corto']: row['id_cliente'] for i, row in cli_df.iterrows()}
                
                if not lista_cli:
                    st.error("No hay clientes. Crea uno en la pestaña Clientes.")
                    st.stop()

                nombre_cli = st.selectbox("Cliente:", options=list(lista_cli.keys()))
                id_cliente = lista_cli[nombre_cli]

                # 2. TIPO DE ENVÍO
                col_e1, col_e2 = st.columns(2)
                tipo_envio = col_e1.selectbox("Método Envío", ["Gratis", "🚚 Envío Lima", "Express (Moto)", "Agencia (Pago Destino)", "Agencia (Pagado)"])
                costo_envio = col_e2.number_input("Costo Envío", value=0.0)

                # 3. LÓGICA DE DIRECCIÓN
                es_agencia = "Agencia" in tipo_envio
                es_envio_lima = tipo_envio == "🚚 Envío Lima" or tipo_envio == "Express (Moto)"
                
                if es_agencia: cat_direccion = "AGENCIA"
                elif es_envio_lima: cat_direccion = "MOTO"
                else: cat_direccion = "OTROS"

                # Buscamos direcciones guardadas
                with engine.connect() as conn:
                    q_dir = text("""
                        SELECT * FROM Direcciones 
                        WHERE id_cliente = :id AND tipo_envio = :tipo AND activo = TRUE 
                        ORDER BY id_direccion DESC
                    """)
                    df_dirs = pd.read_sql(q_dir, conn, params={"id": id_cliente, "tipo": cat_direccion})

                usar_guardada = False
                datos_nuevos = {} 
                texto_direccion_final = ""
                
                opciones_visuales = {}
                if not df_dirs.empty:
                    for idx, row in df_dirs.iterrows():
                        if es_agencia:
                            lbl = f"🏢 {row['agencia_nombre']} - {row['sede_entrega']}"
                        else:
                            lbl = f"🏠 {row['direccion_texto']} ({row['distrito']})"
                        if row['observacion']: lbl += f" | 👁️ {row['observacion'][:20]}..."
                        opciones_visuales[lbl] = row

                KEY_NUEVA = "➕ Usar una Nueva Dirección..."
                lista_desplegable = list(opciones_visuales.keys()) + [KEY_NUEVA]
                
                st.markdown("📍 **Datos de Entrega:**")
                seleccion_dir = st.selectbox("Elige destino:", options=lista_desplegable, label_visibility="collapsed")
                
                if seleccion_dir != KEY_NUEVA:
                    usar_guardada = True
                    dir_data = opciones_visuales[seleccion_dir]
                    if es_agencia:
                        texto_direccion_final = f"{dir_data['agencia_nombre']} - {dir_data['sede_entrega']} [{dir_data['dni_receptor']}]"
                        st.info(f"📦 Destino: **{texto_direccion_final}**")
                    else:
                        texto_direccion_final = f"{dir_data['direccion_texto']} - {dir_data['distrito']}"
                        st.info(f"🏠 Destino: **{texto_direccion_final}**")
                        st.caption(f"📝 {dir_data['observacion']}")
                else:
                    st.warning("📝 Registro de Nuevos Datos:")
                    with st.container(border=True):
                        c_nom, c_tel = st.columns(2)
                        recibe = c_nom.text_input("Nombre Recibe:", value=nombre_cli)
                        telf = c_tel.text_input("Teléfono:", key="telf_new")
                        
                        if es_envio_lima:
                            direcc = st.text_input("Dirección Exacta:")
                            c_dist, c_ref = st.columns(2)
                            dist = c_dist.text_input("Distrito:")
                            ref = c_ref.text_input("Referencia:")
                            gps = st.text_input("📍 GPS (Link Google Maps):")
                            obs_extra = st.text_input("Observación:")
                            obs_full = f"REF: {ref} | GPS: {gps} | {obs_extra}"
                            datos_nuevos = {"tipo": "MOTO", "nom": recibe, "tel": telf, "dir": direcc, "dist": dist, "obs": obs_full, "dni": "", "age": "", "sede": ""}
                            texto_direccion_final = f"{direcc} - {dist} (Ref: {ref})"
                        
                        elif es_agencia:
                            c_dni, c_age = st.columns(2)
                            dni = c_dni.text_input("DNI:")
                            agencia = c_age.text_input("Agencia:", value="Shalom")
                            sede = st.text_input("Sede:")
                            obs_new = st.text_input("Obs:")
                            datos_nuevos = {"tipo": "AGENCIA", "nom": recibe, "tel": telf, "dni": dni, "age": agencia, "sede": sede, "obs": obs_new, "dir": "", "dist": ""}
                            texto_direccion_final = f"{agencia} - {sede}"
                        
                        else:
                            obs_new = st.text_input("Observación / Lugar:")
                            datos_nuevos = {"tipo": "OTROS", "nom": recibe, "tel": telf, "obs": obs_new, "dir": "", "dist": "", "dni": "", "age": "", "sede": ""}
                            texto_direccion_final = "Entrega Directa / Otro"

                # 4. CLAVE AGENCIA
                clave_agencia = None
                if es_agencia:
                    if 'clave_temp' not in st.session_state: 
                        st.session_state['clave_temp'] = str(random.randint(1000, 9999))
                    
                    col_k1, col_k2 = st.columns([1,2])
                    clave_agencia = col_k1.text_input("Clave", value=st.session_state['clave_temp'])
                    col_k2.info("🔐 Clave Entrega")

                total_final = suma_subtotal + costo_envio
                
                st.divider()
                c_tot1, c_tot2 = st.columns([2, 1])
                c_tot1.markdown(f"### 💰 Monto a Cobrar: S/ {total_final:.2f}")
                nota_venta = c_tot2.text_input("Nota Interna:", placeholder="Opcional")

                if st.button("✅ REGISTRAR VENTA", type="primary", use_container_width=True):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            if not usar_guardada and datos_nuevos:
                                conn.execute(text("""
                                    INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, dni_receptor, agencia_nombre, sede_entrega, observacion, activo)
                                    VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :dni, :age, :sede, :obs, TRUE)
                                """), {"id": id_cliente, **datos_nuevos})

                            nota_full = f"{nota_venta} | Envío: {texto_direccion_final}"
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad)
                                VALUES (:idc, :tipo, :costo, :total, :nota, :clave) RETURNING id_venta
                            """), {"idc": id_cliente, "tipo": tipo_envio, "costo": costo_envio, "total": total_final, "nota": nota_full, "clave": clave_agencia})
                            id_venta = res_v.fetchone()[0]

                            for item in st.session_state.carrito:
                                conn.execute(text("""
                                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                    VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv)
                                """), {"idv": id_venta, "sku": item['sku'], "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario']})
                                
                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                     {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    if nuevo_s <= 0: 
                                        conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, id_cliente) 
                                        VALUES (:sku, 'VENTA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota, :idc)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": f"Venta #{id_venta}", "idc": id_cliente})
                            
                            trans.commit()
                        st.balloons()
                        st.success(f"¡Venta #{id_venta} registrada!")
                        st.session_state.carrito = []
                        if 'clave_temp' in st.session_state: del st.session_state['clave_temp']
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

            # ==========================================================
            # MODO B: SALIDA (Merma)
            # ==========================================================
            else:
                st.warning("⚠️ Estás registrando una salida de stock (Sin cobro).")
                motivo_salida = st.selectbox("Motivo:", ["Merma / Dañado", "Regalo / Marketing", "Uso Personal", "Ajuste Inventario"])
                detalle_motivo = st.text_input("Detalle (Opcional):", placeholder="Ej: Se rompió una luna...")
                
                if st.button("📉 CONFIRMAR SALIDA", type="primary"):
                     # ... (Tu lógica de salida) ...
                     pass 
        else:
            st.info("El carrito está vacío.")
            
        if st.button("🗑️ Limpiar Todo", key="btn_limpiar_carrito"):
            st.session_state.carrito = []
            st.rerun()