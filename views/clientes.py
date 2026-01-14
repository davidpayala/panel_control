    st.subheader("👥 Gestión de Clientes")

    # --- SECCIÓN 1: CREAR NUEVO CLIENTE (Se mantiene igual) ---
    with st.expander("➕ Nuevo Cliente (Sincronizado)", expanded=True):
        with st.form("form_nuevo_cliente"):
            col1, col2 = st.columns(2)
            with col1:
                # Campos Base + Google
                nombre_real = st.text_input("Nombre (Google y Base)")
                apellido_real = st.text_input("Apellido (Google y Base)")
                telefono = st.text_input("Teléfono Principal (Google y Base)")
            with col2:
                # Campos Solo Base
                nombre_corto = st.text_input("Nombre Corto (Alias/Rápido)")
                medio = st.selectbox("Medio de Contacto", ["WhatsApp", "Instagram", "Facebook", "TikTok", "Recomendado", "Web"])
                codigo = st.text_input("Código Principal (DNI/RUC/Otro)")
                estado_ini = st.selectbox("Estado Inicial", ["Interesado en venta", "Responder duda", "Proveedor nacional"])
            
            btn_crear = st.form_submit_button("💾 Guardar y Sincronizar", type="primary")

            if btn_crear:
                if not telefono or not nombre_corto:
                    st.error("El Teléfono y el Nombre Corto son obligatorios.")
                else:
                    # 1. VERIFICAR DUPLICADOS (Base de Datos)
                    with engine.connect() as conn:
                        existe_db = conn.execute(text("SELECT COUNT(*) FROM Clientes WHERE telefono = :t"), {"t": telefono}).scalar()
                    
                    # 2. VERIFICAR DUPLICADOS (Google)
                    existe_google = buscar_contacto_google(telefono)

                    if existe_db > 0:
                        st.error("⚠️ Este teléfono ya existe en la Base de Datos Local.")
                    else:
                        # LÓGICA CORREGIDA:
                        # Si existe en Google, usamos ese ID. Si no, lo creamos.
                        if existe_google:
                            google_id = existe_google['resourceName']
                            st.info(f"☁️ Cliente encontrado en Google (ID: {google_id}). Vinculando a la base de datos...")
                            
                            # Opcional: Si quieres que los datos de Google se actualicen con lo que acabas de escribir:
                            # actualizar_en_google(google_id, nombre_real, apellido_real, telefono)
                        else:
                            # 3. CREAR EN GOOGLE (Solo si no existía)
                            google_id = crear_en_google(nombre_real, apellido_real, telefono)
                        
                        # 4. CREAR EN BASE DE DATOS (Siempre se ejecuta)
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                conn.execute(text("""
                                    INSERT INTO Clientes (
                                        nombre_corto, nombre, apellido, telefono, medio_contacto, 
                                        codigo_contacto, estado, fecha_seguimiento, google_id, activo
                                    ) VALUES (
                                        :nc, :nom, :ape, :tel, :medio, :cod, :est, CURRENT_DATE, :gid, TRUE
                                    )
                                """), {
                                    "nc": nombre_corto, "nom": nombre_real, "ape": apellido_real,
                                    "tel": telefono, "medio": medio, "cod": codigo,
                                    "est": estado_ini, "gid": google_id
                                })
                                trans.commit()
                                st.success(f"✅ Cliente registrado exitosamente (Sincronizado).")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error DB: {e}")

    st.divider()

    # --- SECCIÓN 2: BUSCADOR Y EDICIÓN RÁPIDA (MODIFICADA ⭐) ---
    st.subheader("🔍 Buscar y Editar Clientes")
    
    col_search, col_btn = st.columns([3, 1])
    with col_search:
        busqueda = st.text_input("Escribe el nombre o teléfono del cliente:", placeholder="Ej: Maria, 999...")

    # Lista completa de estados para el desplegable
    OPCIONES_ESTADO = [
        "Sin empezar", "Responder duda", "Interesado en venta", 
        "Proveedor nacional", "Proveedor internacional", 
        "Venta motorizado", "Venta agencia", "Venta express moto",
        "En camino moto", "En camino agencia", "Contraentrega agencia",
        "Pendiente agradecer", "Problema post"
    ]

    df_resultados = pd.DataFrame()
    
    if busqueda:
        with engine.connect() as conn:
            # AHORA INCLUIMOS 'estado' EN LA CONSULTA
            query = text("""
                SELECT id_cliente, nombre_corto, estado, nombre, apellido, telefono, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b) AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 20
            """)
            df_resultados = pd.read_sql(query, conn, params={"b": f"%{busqueda}%"})
    else:
        st.info("👆 Escribe arriba para buscar.")

    if not df_resultados.empty:
        st.caption(f"Se encontraron {len(df_resultados)} resultados.")
        
        cambios = st.data_editor(
            df_resultados,
            key="editor_busqueda",
            column_config={
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "google_id": None, # Oculto
                
                # AHORA SON EDITABLES:
                "nombre_corto": st.column_config.TextColumn("Nombre Corto (Alias)", required=True),
                "estado": st.column_config.SelectboxColumn("Estado Actual", options=OPCIONES_ESTADO, width="medium", required=True),
                
                # Datos Personales
                "nombre": st.column_config.TextColumn("Nombre (Google)", required=True),
                "apellido": st.column_config.TextColumn("Apellido (Google)", required=True),
                "telefono": st.column_config.TextColumn("Teléfono", required=True)
            },
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Guardar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for idx, row in cambios.iterrows():
                        # 1. Actualizamos la Base de Datos (Ahora incluye nombre_corto y estado)
                        conn.execute(text("""
                            UPDATE Clientes 
                            SET nombre=:n, apellido=:a, telefono=:t, nombre_corto=:nc, estado=:est
                            WHERE id_cliente=:id
                        """), {
                            "n": row['nombre'], "a": row['apellido'], 
                            "t": row['telefono'], "nc": row['nombre_corto'], 
                            "est": row['estado'], "id": row['id_cliente']
                        })
                        
                        # 2. Sincronizamos con Google (Solo datos personales, Google no tiene "estado" ni "alias")
                        if row['google_id']:
                            actualizar_en_google(row['google_id'], row['nombre'], row['apellido'], row['telefono'])
                            
                    trans.commit()
                    st.success("✅ Datos actualizados correctamente.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al guardar: {e}")
    
    elif busqueda:
        st.warning("No se encontraron clientes con esos datos.")

# ==============================================================================
# HERRAMIENTA DE FUSIÓN DE CLIENTES
# ==============================================================================
st.divider()
st.subheader("🧬 Fusión de Clientes Duplicados")
st.info("Utiliza esta herramienta cuando una persona tenga dos registros (ej. dos números). Se moverá todo el historial al 'Cliente Principal' y se guardará el número antiguo.")

col_dup, col_orig = st.columns(2)

# --- 1. SELECCIONAR EL DUPLICADO (EL QUE SE VA A BORRAR) ---
with col_dup:
    st.markdown("### ❌ 1. Cliente a ELIMINAR")
    search_dup = st.text_input("Buscar duplicado (Nombre/Telf):", key="search_dup")
    
    id_duplicado = None
    info_duplicado = None
    
    if search_dup:
        with engine.connect() as conn:
            # Buscamos clientes activos
            res = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono, nombre, apellido FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo = TRUE LIMIT 5"), conn, params={"s": f"%{search_dup}%"})
        
        if not res.empty:
            # Usamos un selectbox para elegir el ID exacto
            opts_dup = res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID: {x['id_cliente']}", axis=1).tolist()
            sel_dup_str = st.selectbox("Selecciona:", opts_dup, key="sel_dup")
            id_duplicado = int(sel_dup_str.split("ID: ")[1])
            
            # Guardamos datos para mostrar confirmación
            row_dup = res[res['id_cliente'] == id_duplicado].iloc[0]
            info_duplicado = f"**{row_dup['nombre_corto']}**\nTelf: {row_dup['telefono']}"
            st.warning(f"⚠️ Este cliente será DESACTIVADO.")
        else:
            st.caption("No encontrado.")

# --- 2. SELECCIONAR EL ORIGINAL (EL QUE SE QUEDA) ---
with col_orig:
    st.markdown("### ✅ 2. Cliente PRINCIPAL")
    search_orig = st.text_input("Buscar principal (Nombre/Telf):", key="search_orig")
    
    id_original = None
    
    if search_orig:
        with engine.connect() as conn:
            res2 = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo = TRUE LIMIT 5"), conn, params={"s": f"%{search_orig}%"})
        
        if not res2.empty:
            opts_orig = res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID: {x['id_cliente']}", axis=1).tolist()
            sel_orig_str = st.selectbox("Selecciona:", opts_orig, key="sel_orig")
            id_original = int(sel_orig_str.split("ID: ")[1])
            
            st.success(f"✅ Este cliente recibirá el historial.")
        else:
            st.caption("No encontrado.")

# --- 3. BOTÓN DE FUSIÓN (LÓGICA BLINDADA) ---
st.divider()

if id_duplicado and id_original:
    if id_duplicado == id_original:
        st.error("⛔ ¡No puedes fusionar al cliente consigo mismo! Selecciona dos distintos.")
    else:
        st.markdown(f"### 🔄 Confirmar Fusión")
        st.write(f"Vas a pasar todo de {info_duplicado} hacia el ID **{id_original}**.")
        
        if st.button("🚀 FUSIONAR AHORA", type="primary"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # 1. Obtener el teléfono del duplicado para no perderlo
                    old_phone = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente = :id"), {"id": id_duplicado}).scalar()
                    
                    # 2. Mover VENTAS
                    conn.execute(text("UPDATE Ventas SET id_cliente = :new_id WHERE id_cliente = :old_id"), {"new_id": id_original, "old_id": id_duplicado})
                    
                    # 3. Mover DIRECCIONES
                    conn.execute(text("UPDATE Direcciones SET id_cliente = :new_id WHERE id_cliente = :old_id"), {"new_id": id_original, "old_id": id_duplicado})
                    
                    # 4. Actualizar el Principal (Guardamos el teléfono viejo como secundario)
                    # Solo si el campo secundario está vacío, para no sobrescribir algo importante
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET telefono_secundario = :old_tel 
                        WHERE id_cliente = :new_id AND (telefono_secundario IS NULL OR telefono_secundario = '')
                    """), {"old_tel": old_phone, "new_id": id_original})
                    
                    # 5. Desactivar el duplicado (Soft Delete)
                    conn.execute(text("UPDATE Clientes SET activo = FALSE, nombre_corto = nombre_corto || ' (FUSIONADO)' WHERE id_cliente = :old_id"), {"old_id": id_duplicado})
                    
                    trans.commit()
                    st.balloons()
                    st.success("✨ ¡Fusión Completada! Historial unificado.")
                    time.sleep(2)
                    st.rerun()
                    
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error en la fusión: {e}")