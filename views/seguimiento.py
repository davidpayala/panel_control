# CSS para ajustar altura de filas y ver los saltos de línea
st.markdown("""
    <style>
        div[data-testid="stDataEditor"] td {
            white-space: pre-wrap !important;
            vertical-align: top !important;
        }
    </style>
""", unsafe_allow_html=True)

st.subheader("🎯 Tablero de Seguimiento Logístico")

# --- 1. CONFIGURACIÓN ---
ETAPAS = {
    "ETAPA_0": ["Sin empezar"],
    "ETAPA_1": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
    "ETAPA_2": ["Venta motorizado", "Venta agencia", "Venta express moto"],
    "ETAPA_3": ["En camino moto", "En camino agencia", "Contraentrega agencia"],
    "ETAPA_4": ["Pendiente agradecer", "Problema post"]
}
TODOS_LOS_ESTADOS = [e for lista in ETAPAS.values() for e in lista]

# --- 2. CONSULTA SQL ---
with engine.connect() as conn:
    query_seg = text("""
        SELECT 
            c.id_cliente, c.nombre_corto, c.telefono, c.estado, c.fecha_seguimiento, 
            
            -- Datos de Venta
            v.id_venta, v.total_venta, v.clave_seguridad, 
            v.fecha_venta, 
            v.pendiente_pago,
            (SELECT STRING_AGG(d.cantidad || 'x ' || d.descripcion, ', ') 
                FROM DetalleVenta d WHERE d.id_venta = v.id_venta) as resumen_items,

            -- Datos de Dirección
            dir.id_direccion, dir.nombre_receptor, dir.telefono_receptor, 
            dir.direccion_texto, dir.distrito, 
            dir.referencia, dir.gps, dir.observacion,
            dir.dni_receptor, dir.agencia_nombre, dir.sede_entrega

        FROM Clientes c
        LEFT JOIN LATERAL (
            SELECT * FROM Ventas v2 WHERE v2.id_cliente = c.id_cliente ORDER BY v2.id_venta DESC LIMIT 1
        ) v ON TRUE
        LEFT JOIN LATERAL (
            SELECT * FROM Direcciones d2 WHERE d2.id_cliente = c.id_cliente ORDER BY d2.id_direccion DESC LIMIT 1
        ) dir ON TRUE
        WHERE c.activo = TRUE 
        ORDER BY c.fecha_seguimiento ASC
    """)
    df_seg = pd.read_sql(query_seg, conn)

# --- 3. FUNCIÓN DE GUARDADO ---
def guardar_edicion_rapida(df_editado, tipo_tabla):
    try:
        with engine.connect() as conn:
            for index, row in df_editado.iterrows():
                # A) Actualizar Estado y FECHA DE SEGUIMIENTO
                conn.execute(text("UPDATE Clientes SET estado = :est, fecha_seguimiento = :fec WHERE id_cliente = :id"), 
                                {"est": row['estado'], "fec": row['fecha_seguimiento'], "id": row['id_cliente']})
                
                # B) Actualizar Pendiente de Pago (Si hay venta asociada)
                if pd.notnull(row['id_venta']):
                    conn.execute(text("UPDATE Ventas SET pendiente_pago = :pen WHERE id_venta = :idv"),
                                    {"pen": row['pendiente_pago'], "idv": row['id_venta']})
                    
                conn.commit()
        st.toast("✅ Cambios guardados correctamente", icon="💾")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Error: {e}")

# --- 4. RENDERIZADO ---
if not df_seg.empty:
    # Filtros
    df_moto = df_seg[df_seg['estado'].isin(["Venta motorizado", "Venta express moto"])].copy()
    df_agencia = df_seg[df_seg['estado'] == "Venta agencia"].copy()
    df_ruta = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_3"])].copy()
    # Resto de etapas
    df_e1 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_1"])].copy()
    df_e4 = df_seg[df_seg['estado'].isin(ETAPAS["ETAPA_4"])].copy()

    # Métricas
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🛵 Moto / Express", len(df_moto), border=True)
    c2.metric("🏢 Agencia", len(df_agencia), border=True)
    c3.metric("🚚 En Ruta", len(df_ruta))
    c4.metric("💬 Conversación", len(df_e1))
    
    st.divider()
    st.markdown("### 🔥 Zona Operativa: Por Despachar")
    st.info("💡 Marca la casilla '👉' para gestionar la dirección.")
    
    tab_moto, tab_agencia = st.tabs(["🛵 MOTORIZADO", "🏢 AGENCIA"])

    # --- FORMATOS VISUALES ---
    def formatear_entrega_moto(row):
        return (f"👤 {row['nombre_receptor']}\n"
                f"📞 {row['telefono_receptor']}\n"
                f"📍 {row['direccion_texto']} ({row['distrito']})\n"
                f"🏠 Ref: {row['referencia']}\n"
                f"🗺️ GPS: {row['gps']}\n"
                f"📝 Obs: {row['observacion']}")

    def formatear_entrega_agencia(row):
        return (f"👤 {row['nombre_receptor']}\n"
                f"🆔 DNI: {row['dni_receptor']}\n"
                f"📞 {row['telefono_receptor']}\n"
                f"🏢 {row['agencia_nombre']} - {row['sede_entrega']}\n"
                f"🔐 Clave: {row['clave_seguridad']}")

    def formatear_venta_resumen(row):
        if pd.isnull(row['id_venta']): return ""
        fecha_str = row['fecha_venta'].strftime('%d/%m %H:%M') if pd.notnull(row['fecha_venta']) else "--"
        total = float(row['total_venta']) if pd.notnull(row['total_venta']) else 0.0
        return (f"📅 {fecha_str}\n"
                f"🛒 {row['resumen_items']}\n"
                f"💰 Total Venta: S/ {total:.2f}")

    # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA MOTO <<<<<<<<<<<<<<<<<<<<<<<<<
    with tab_moto:
        if not df_moto.empty:
            df_moto["datos_entrega"] = df_moto.apply(formatear_entrega_moto, axis=1)
            df_moto["resumen_venta"] = df_moto.apply(formatear_venta_resumen, axis=1)
            
            df_view = df_moto.copy()
            df_view.insert(0, "Seleccionar", False)

            cols_show = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                            "resumen_venta", "datos_entrega", "pendiente_pago"]
            
            cfg = {
                "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium"),
                "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "telefono": st.column_config.TextColumn("📞 Telf. Cliente", disabled=True),
                "resumen_venta": st.column_config.TextColumn("🧾 Resumen Venta", width="medium", disabled=True),
                "datos_entrega": st.column_config.TextColumn("📦 Datos de Entrega", width="large", disabled=True),
                "pendiente_pago": st.column_config.NumberColumn("❗ A Cobrar", format="S/ %.2f"),
                "id_cliente": None
            }

            event_moto = st.data_editor(
                df_view[cols_show], 
                key="ed_moto", column_config=cfg, 
                hide_index=True, use_container_width=True
            )
            
            c_btn1, c_btn2 = st.columns([1, 1])
            
            if c_btn1.button("💾 Guardar Cambios", key="btn_save_moto"): 
                df_save = df_moto.loc[event_moto.index].copy()
                df_save['estado'] = event_moto['estado']
                df_save['fecha_seguimiento'] = event_moto['fecha_seguimiento']
                df_save['pendiente_pago'] = event_moto['pendiente_pago']
                guardar_edicion_rapida(df_save, "MOTO")

            if c_btn2.button("📋 Generar Lista de Ruta (Texto)", key="btn_gen_ruta"):
                texto_ruta = ""
                count = 1
                df_rut = df_moto.loc[event_moto.index]
                for idx, row in df_rut.iterrows():
                    monto = float(row['pendiente_pago']) if pd.notnull(row['pendiente_pago']) else 0.0
                    texto_ruta += f"*Pedido {count}*\n"
                    texto_ruta += f"*Recibe:* {row['nombre_receptor'] or ''}\n"
                    texto_ruta += f"*Dirección:* {row['direccion_texto'] or ''}\n"
                    texto_ruta += f"*Referencia:* {row['referencia'] or ''}\n"
                    texto_ruta += f"*GPS:* {row['gps'] or ''}\n"
                    texto_ruta += f"*Distrito:* {row['distrito'] or ''}\n"
                    texto_ruta += f"*Teléfono:* {row['telefono_receptor'] or ''}\n"
                    texto_ruta += f"*Observación:* {row['observacion'] or ''}\n"
                    texto_ruta += f"*Monto a cobrar:* S/ {monto:.2f}\n"
                    texto_ruta += "----------------------------------\n"
                    count += 1
                st.code(texto_ruta, language="text")
                st.toast("Lista generada arriba.", icon="📋")

            # GESTIÓN DIRECCIÓN MOTO
            filas_sel = event_moto[event_moto["Seleccionar"] == True]
            if not filas_sel.empty:
                row_full = df_moto.loc[filas_sel.index[0]]
                st.divider()
                st.markdown(f"#### 📍 Gestionar Dirección: **{row_full['nombre_corto']}**")
                with st.container(border=True):
                    with engine.connect() as conn:
                        hist_dirs = pd.read_sql(text("SELECT id_direccion, direccion_texto, distrito, referencia FROM Direcciones WHERE id_cliente = :id AND tipo_envio = 'MOTO' ORDER BY id_direccion DESC"), conn, params={"id": int(row_full['id_cliente'])})
                    
                    opts = {"🆕 Nueva / Editar Actual...": -1}
                    for i, r in hist_dirs.iterrows(): opts[f"{r['direccion_texto']} ({r['distrito']})"] = r['id_direccion']
                    sel_id = st.selectbox("Cargar Datos:", list(opts.keys()))
                    
                    with st.form("form_moto"):
                        if opts[sel_id] == -1:
                            d_nom, d_tel, d_dir, d_dist, d_ref, d_gps, d_obs = row_full['nombre_receptor'], row_full['telefono_receptor'], row_full['direccion_texto'], row_full['distrito'], row_full['referencia'], row_full['gps'], row_full['observacion']
                        else:
                            with engine.connect() as conn:
                                dd = conn.execute(text("SELECT * FROM Direcciones WHERE id_direccion=:id"), {"id": opts[sel_id]}).fetchone()
                                d_nom, d_tel, d_dir, d_dist, d_ref, d_gps, d_obs = dd.nombre_receptor, dd.telefono_receptor, dd.direccion_texto, dd.distrito, dd.referencia, dd.gps, dd.observacion

                        c1, c2 = st.columns(2)
                        n_nom, n_tel = c1.text_input("Recibe", d_nom), c2.text_input("Teléfono", d_tel)
                        n_dir = st.text_input("Dirección", d_dir)
                        c3, c4 = st.columns(2)
                        n_dist, n_ref = c3.text_input("Distrito", d_dist), c4.text_input("Ref", d_ref)
                        n_gps, n_obs = st.text_input("GPS", d_gps), st.text_input("Obs", d_obs)

                        if st.form_submit_button("✅ Guardar Dirección"):
                            with engine.connect() as conn:
                                conn.execute(text("INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, direccion_texto, distrito, referencia, gps, observacion, activo) VALUES (:id, 'MOTO', :n, :t, :d, :di, :r, :g, :o, TRUE)"), 
                                                {"id": int(row_full['id_cliente']), "n": n_nom, "t": n_tel, "d": n_dir, "di": n_dist, "r": n_ref, "g": n_gps, "o": n_obs})
                                conn.commit()
                            st.rerun()
        else:
            st.info("Nada en moto.")

    # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA AGENCIA <<<<<<<<<<<<<<<<<<<<<<<<<
    with tab_agencia:
        if not df_agencia.empty:
            df_agencia["datos_entrega"] = df_agencia.apply(formatear_entrega_agencia, axis=1)
            df_agencia["resumen_venta"] = df_agencia.apply(formatear_venta_resumen, axis=1)
            
            df_view_a = df_agencia.copy()
            df_view_a.insert(0, "Seleccionar", False)
            
            cols_show_a = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                            "resumen_venta", "datos_entrega", "pendiente_pago"]
            
            cfg_a = {
                "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium"),
                "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "telefono": st.column_config.TextColumn("📞 Telf. Cliente", disabled=True),
                "resumen_venta": st.column_config.TextColumn("🧾 Resumen", width="medium", disabled=True),
                "datos_entrega": st.column_config.TextColumn("📦 Datos Envío", width="large", disabled=True),
                "pendiente_pago": st.column_config.NumberColumn("❗ A Cobrar", format="S/ %.2f"),
                "id_cliente": None
            }

            event_agencia = st.data_editor(
                df_view_a[cols_show_a], key="ed_age", column_config=cfg_a, 
                hide_index=True, use_container_width=True
            )
            
            if st.button("💾 Guardar Cambios", key="btn_save_age"): 
                df_save_a = df_agencia.loc[event_agencia.index].copy()
                df_save_a['estado'] = event_agencia['estado']
                df_save_a['fecha_seguimiento'] = event_agencia['fecha_seguimiento'] 
                df_save_a['pendiente_pago'] = event_agencia['pendiente_pago']
                guardar_edicion_rapida(df_save_a, "AGENCIA")

            # GESTIÓN AGENCIA
            filas_sel_a = event_agencia[event_agencia["Seleccionar"] == True]
            if not filas_sel_a.empty:
                row_full_a = df_agencia.loc[filas_sel_a.index[0]]
                st.divider()
                st.markdown(f"#### 🏢 Gestionar Agencia: **{row_full_a['nombre_corto']}**")
                with st.form("form_age"):
                    c1, c2, c3 = st.columns(3)
                    n_nom, n_dni, n_tel = c1.text_input("Recibe", row_full_a['nombre_receptor']), c2.text_input("DNI", row_full_a['dni_receptor']), c3.text_input("Telf", row_full_a['telefono_receptor'])
                    c4, c5 = st.columns(2)
                    n_age, n_sede = c4.selectbox("Agencia", ["Shalom", "Olva", "Marvisur"]), c5.text_input("Sede", row_full_a['sede_entrega'])
                    if st.form_submit_button("✅ Guardar Agencia"):
                            with engine.connect() as conn:
                                conn.execute(text("INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, dni_receptor, telefono_receptor, agencia_nombre, sede_entrega, activo) VALUES (:id, 'AGENCIA', :n, :d, :t, :a, :s, TRUE)"),
                                                {"id": int(row_full_a['id_cliente']), "n": n_nom, "d": n_dni, "t": n_tel, "a": n_age, "s": n_sede})
                                conn.commit()
                            st.rerun()
        else:
            st.info("Nada en agencia.")

    st.divider()
    st.markdown("### 🚚 Zona Logística: En Ruta")
    
    if not df_ruta.empty:
        cols_ruta = ["id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", "resumen_items"]
        
        cfg_ruta = {
            "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
            "fecha_seguimiento": st.column_config.DateColumn("Fecha Seg.", format="DD/MM/YYYY"),
            "id_cliente": None
        }
        
        edit_ruta = st.data_editor(
            df_ruta[cols_ruta], 
            key="ed_ruta", 
            column_config=cfg_ruta, 
            hide_index=True, 
            use_container_width=True
        )
        
        if st.button("💾 Actualizar Ruta", key="btn_save_ruta"):
            # --- CORRECCIÓN AQUÍ ---
            # 1. Recuperamos la data completa original (que sí tiene id_venta) usando el índice
            df_save_ruta = df_ruta.loc[edit_ruta.index].copy()
            
            # 2. Sobrescribimos solo las columnas que permitimos editar
            df_save_ruta['estado'] = edit_ruta['estado']
            df_save_ruta['fecha_seguimiento'] = edit_ruta['fecha_seguimiento']
            
            # 3. Ahora sí guardamos (df_save_ruta tiene id_venta oculto, así que no fallará)
            guardar_edicion_rapida(df_save_ruta, "RUTA")
    else:
        st.info("Nada en ruta.")

    # ==================================================================
    # 📂 BANDEJAS DE GESTIÓN
    # ==================================================================
    st.divider()
    st.markdown("### 📂 Bandejas de Gestión")

    # --- ETAPA 1 (Restaurada, aquí se había colado el duplicado) ---
    with st.expander(f"💬 Conversación / Cotizando ({len(df_e1)})"):
        if not df_e1.empty:
            cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
            cfg_e1 = {
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "resumen_items": st.column_config.TextColumn("Historial / Interés", width="large"),
                "fecha_seguimiento": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                "id_cliente": None
            }
            event_e1 = st.data_editor(df_e1[cols_e1], key="ed_e1", column_config=cfg_e1, hide_index=True, use_container_width=True)
            if st.button("💾 Guardar (Conversación)", key="btn_save_e1"):
                    df_save_e1 = df_e1.loc[event_e1.index].copy()
                    df_save_e1['estado'] = event_e1['estado']
                    df_save_e1['fecha_seguimiento'] = event_e1['fecha_seguimiento']
                    guardar_edicion_rapida(df_save_e1, "GENERICO")
        else:
            st.info("Bandeja vacía.")

    # --- ETAPA 4 ---
    with st.expander(f"✨ Post-Venta ({len(df_e4)})"):
            if not df_e4.empty:
            cols_e4 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
            cfg_e4 = {
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "resumen_items": st.column_config.TextColumn("Compra Anterior", width="large"),
                "fecha_seguimiento": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                "id_cliente": None
            }
            event_e4 = st.data_editor(df_e4[cols_e4], key="ed_e4", column_config=cfg_e4, hide_index=True, use_container_width=True)
            if st.button("💾 Guardar (Post-Venta)", key="btn_save_e4"):
                    df_save_e4 = df_e4.loc[event_e4.index].copy()
                    df_save_e4['estado'] = event_e4['estado']
                    df_save_e4['fecha_seguimiento'] = event_e4['fecha_seguimiento']
                    guardar_edicion_rapida(df_save_e4, "GENERICO")
            else:
            st.info("Bandeja vacía.")