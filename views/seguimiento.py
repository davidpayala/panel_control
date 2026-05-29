import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
from datetime import datetime, timedelta

# Lista de respaldo por seguridad si la base de datos está vacía
ESTADOS_FALLBACK = [
    "Sin empezar", "Responder duda", "Interesado en venta", "Proveedor nacional", 
    "Proveedor internacional", "Venta motorizado", "Venta agencia", "Venta express moto", 
    "En camino moto", "En camino agencia", "Contraentrega agencia", "Pendiente agradecer", "Problema post"
]

def render_seguimiento():
    # CSS para ajustar altura de filas
    st.markdown("""
        <style>
            div[data-testid="stDataEditor"] td {
                white-space: pre-wrap !important;
                vertical-align: top !important;
            }
        </style>
    """, unsafe_allow_html=True)

    c_titulo, c_refresh = st.columns([4, 1])
    c_titulo.subheader("🎯 Tablero de Seguimiento Logístico")
    
    # BOTÓN MANUAL DE RECARGA
    if c_refresh.button("🔄 Recargar Datos"):
        if 'df_seguimiento_cache' in st.session_state:
            del st.session_state['df_seguimiento_cache']
        st.rerun()

    # --- 1. CARGA DINÁMICA DE ETAPAS ---
    try:
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("SELECT id_etapa, grupo, subgrupo FROM EtapasCliente WHERE activo = TRUE"), conn)
        
        if not df_etapas.empty:
            todos_los_estados = df_etapas['subgrupo'].tolist()
            mapa_subgrupo_id = dict(zip(df_etapas['subgrupo'], df_etapas['id_etapa']))
            
            # Agrupar por bloques dinámicos mapeando las hileras de la DB
            etapa_1_etapas = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 1']['subgrupo'].tolist()
            etapa_2_etapas = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 2']['subgrupo'].tolist()
            etapa_3_etapas = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 3']['subgrupo'].tolist()
            etapa_4_etapas = df_etapas[df_etapas['grupo'].str.lower() == 'etapa 4']['subgrupo'].tolist()
        else:
            todos_los_estados = ESTADOS_FALLBACK
            mapa_subgrupo_id = {}
            etapa_1_etapas, etapa_2_etapas, etapa_3_etapas, etapa_4_etapas = [], [], [], []
    except Exception as e:
        todos_los_estados = ESTADOS_FALLBACK
        mapa_subgrupo_id = {}
        etapa_1_etapas, etapa_2_etapas, etapa_3_etapas, etapa_4_etapas = [], [], [], []

    # --- 2. CARGA DE DATOS CONTROLADA ---
    if 'df_seguimiento_cache' not in st.session_state:
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
                    dir.referencia, dir.gps_link, dir.observacion,
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
            df_loaded = pd.read_sql(query_seg, conn)
            st.session_state['df_seguimiento_cache'] = df_loaded
    
    df_seg = st.session_state['df_seguimiento_cache']

    # --- 3. FUNCIÓN DE GUARDADO (ACTUALIZA TEXTO E ID_ETAPA) ---
    def guardar_edicion_rapida(df_editado, tipo_tabla):
        try:
            with engine.begin() as conn:
                for index, row in df_editado.iterrows():
                    id_etapa_val = mapa_subgrupo_id.get(row['estado'])
                    
                    # Conversión explícita a tipos de Python nativos
                    id_cliente_nativo = int(row['id_cliente'])
                    id_etapa_nativo = int(id_etapa_val) if pd.notnull(id_etapa_val) else None
                    
                    # Actualizar Estado e id_etapa
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET estado = :est, id_etapa = :id_etapa, fecha_seguimiento = :fec 
                        WHERE id_cliente = :id
                    """), {
                        "est": row['estado'], 
                        "id_etapa": id_etapa_nativo,
                        "fec": row['fecha_seguimiento'], 
                        "id": id_cliente_nativo
                    })
                    
                    # Actualizar Pendiente de Pago
                    if pd.notnull(row['id_venta']):
                        id_venta_nativo = int(row['id_venta'])
                        pendiente_nativo = float(row['pendiente_pago']) if pd.notnull(row['pendiente_pago']) else 0.0
                        conn.execute(text("UPDATE Ventas SET pendiente_pago = :pen WHERE id_venta = :idv"),
                                        {"pen": pendiente_nativo, "idv": id_venta_nativo})
            
            if 'df_seguimiento_cache' in st.session_state:
                del st.session_state['df_seguimiento_cache']
                
            st.toast("✅ Cambios guardados correctamente", icon="💾")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    def guardar_datos_envio_completo(id_direccion, id_cliente, datos):
        try:
            with engine.begin() as conn:
                if id_direccion and id_direccion > 0:
                    conn.execute(text("""
                        UPDATE Direcciones SET 
                            nombre_receptor = :nom,
                            telefono_receptor = :tel,
                            distrito = :dist,
                            direccion_texto = :dir,
                            referencia = :ref,
                            gps_link = :gps,
                            observacion = :obs
                        WHERE id_direccion = :id_dir
                    """), {
                        "nom": datos['nombre_receptor'],
                        "tel": datos['telefono_receptor'],
                        "dist": datos['distrito'],
                        "dir": datos['direccion_texto'],
                        "ref": datos['referencia'],
                        "gps": datos['gps_link'],
                        "obs": datos['observacion'],
                        "id_dir": int(id_direccion)
                    })
                
                if datos.get('nuevo_estado'):
                    id_etapa_val = mapa_subgrupo_id.get(datos['nuevo_estado'])
                    id_etapa_nativo = int(id_etapa_val) if pd.notnull(id_etapa_val) else None
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET estado = :e, id_etapa = :id_etapa, fecha_seguimiento = NOW() 
                        WHERE id_cliente = :id
                    """), {"e": datos['nuevo_estado'], "id_etapa": id_etapa_nativo, "id": int(id_cliente)})
                                
            st.toast("✅ Datos de envío actualizados correctamente.", icon="💾")
            if 'df_seguimiento_cache' in st.session_state:
                del st.session_state['df_seguimiento_cache']
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error guardando formulario: {e}")

    # --- 4. RENDERIZADO Y FILTRADO INTELIGENTE ---
    if not df_seg.empty:
        # Segmentación de la Etapa 2 basado en el texto del subgrupo
        df_etapa2 = df_seg[df_seg['estado'].isin(etapa_2_etapas)]
        df_moto = df_etapa2[~df_etapa2['estado'].str.lower().str.contains('agencia', na=False)].copy()
        df_agencia = df_etapa2[df_etapa2['estado'].str.lower().str.contains('agencia', na=False)].copy()
        
        df_ruta = df_seg[df_seg['estado'].isin(etapa_3_etapas)].copy()
        df_e1 = df_seg[df_seg['estado'].isin(etapa_1_etapas)].copy()
        df_e4 = df_seg[df_seg['estado'].isin(etapa_4_etapas)].copy()

        # Métricas
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🛵 Moto / Interno", len(df_moto))
        c2.metric("🏢 Agencia", len(df_agencia))
        c3.metric("🚚 En Ruta", len(df_ruta))
        c4.metric("💬 Conversación", len(df_e1))
        
        st.divider()
        
        tab_moto, tab_agencia = st.tabs(["🛵 MOTORIZADO / INTERNO", "🏢 AGENCIA"])

        # --- FORMATOS VISUALES ---
        def formatear_entrega_moto(row):
            return (f"👤 {row['nombre_receptor']}\n"
                    f"📞 {row['telefono_receptor']}\n"
                    f"📍 {row['direccion_texto']} ({row['distrito']})\n"
                    f"🏠 Ref: {row['referencia']}\n"
                    f"🗺️ GPS: {row['gps_link']}\n"
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
                    f"💰 Total: S/ {total:.2f}")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA MOTO <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_moto:
            if not df_moto.empty:
                df_moto["datos_entrega"] = df_moto.apply(formatear_entrega_moto, axis=1)
                df_moto["resumen_venta"] = df_moto.apply(formatear_venta_resumen, axis=1)
                
                df_view = df_moto.copy()
                df_view.insert(0, "Seleccionar", False)

                cols_show = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                                "resumen_venta", "datos_entrega", "pendiente_pago"]
                
                event_moto = st.data_editor(
                    df_view[cols_show], 
                    key="ed_moto", 
                    column_config={
                        "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                        "estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados, width="medium"),
                        "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                        "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                        "telefono": st.column_config.TextColumn("📞 Telf.", disabled=True),
                        "resumen_venta": st.column_config.TextColumn("🧾 Venta", width="medium", disabled=True),
                        "datos_entrega": st.column_config.TextColumn("📦 Entrega", width="large", disabled=True),
                        "pendiente_pago": st.column_config.NumberColumn("❗ Cobrar", format="S/ %.2f"),
                        "id_cliente": None
                    },
                    hide_index=True, use_container_width=True
                )
                
                c_btn1, c_btn2 = st.columns([1, 1])
                
                if c_btn1.button("💾 Guardar Cambios Moto", type="primary"): 
                    df_save = df_moto.loc[event_moto.index].copy()
                    df_save['estado'] = event_moto['estado']
                    df_save['fecha_seguimiento'] = event_moto['fecha_seguimiento']
                    df_save['pendiente_pago'] = event_moto['pendiente_pago']
                    guardar_edicion_rapida(df_save, "MOTO")

                if c_btn2.button("📋 Generar Lista Ruta"):
                    fecha_manana = (datetime.now() - timedelta(hours=5) + timedelta(days=1)).strftime("%d/%m/%Y")
                    texto_ruta = f"*Fecha {fecha_manana}*\n----------------\n\n"
                    count = 1
                    df_rut = df_moto.loc[event_moto.index]
                    for idx, row in df_rut.iterrows():
                        try:
                            monto = float(row['pendiente_pago']) if row['pendiente_pago'] else 0.0
                        except:
                            monto = 0.0
                        str_cobro = "Pagó todo" if monto <= 0 else f"S/ {monto:.2f}"
                        
                        texto_ruta += f"*Pedido {count}*\n"
                        texto_ruta += f"*Recibe:* {row['nombre_receptor'] or ''}\n"
                        texto_ruta += f"*Dirección:* {row['direccion_texto'] or ''}\n"
                        texto_ruta += f"*Ref:* {row['referencia'] or ''}\n"
                        texto_ruta += f"*Distrito:* {row['distrito'] or ''}\n"
                        texto_ruta += f"*GPS:* {row['gps_link'] or ''}\n"
                        texto_ruta += f"*Telf:* {row['telefono_receptor'] or ''}\n"
                        texto_ruta += f"*Cobrar:* {str_cobro}\n"
                        texto_ruta += f"*Observación:* {row['observacion'] or ''}\n"
                        texto_ruta += "----------------------------------\n"
                        count += 1
                    st.code(texto_ruta)

                # GESTIÓN DIRECCIÓN MOTO
                filas_sel = event_moto[event_moto["Seleccionar"] == True]
                if not filas_sel.empty:
                    row_full = df_moto.loc[filas_sel.index[0]]
                    st.divider()
                    st.info(f"📍 Editando dirección de: **{row_full['nombre_corto']}**")
                    
                    with st.form("form_moto_dir"):
                        c1, c2, c3 = st.columns(3)
                        n_nom = c1.text_input("Recibe", row_full['nombre_receptor'])
                        n_tel = c2.text_input("Teléfono", row_full['telefono_receptor'])
                        n_dist = c3.text_input("Distrito", row_full['distrito'])
                        
                        st.caption("📍 Ubicación")
                        n_dir = st.text_input("Dirección Exacta", row_full['direccion_texto'])
                        
                        c4, c5, c6 = st.columns(3)
                        n_ref = c4.text_input("Referencia", row_full['referencia'])
                        n_gps = c5.text_input("Link GPS", row_full['gps_link'])
                        n_obs = c6.text_input("Observaciones", row_full['observacion']) 
                        
                        st.markdown("---")
                        col_st, col_btn = st.columns([2, 1])
                        
                        idx_estado = todos_los_estados.index(row_full['estado']) if row_full['estado'] in todos_los_estados else 0
                        nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado)
                        
                        if col_btn.form_submit_button("💾 Guardar Guía Completa", type="primary"):
                            datos_form = {
                                "nombre_receptor": n_nom, 
                                "telefono_receptor": n_tel,
                                "distrito": n_dist, 
                                "direccion_texto": n_dir,
                                "referencia": n_ref, 
                                "gps_link": n_gps,
                                "observacion": n_obs,
                                "nuevo_estado": nuevo_estado
                            }
                            guardar_datos_envio_completo(int(row_full['id_direccion']), row_full['id_cliente'], datos_form)
            else:
                st.caption("No hay pedidos para motorizado.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA AGENCIA <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_agencia:
            if not df_agencia.empty:
                df_agencia["datos_entrega"] = df_agencia.apply(formatear_entrega_agencia, axis=1)
                df_agencia["resumen_venta"] = df_agencia.apply(formatear_venta_resumen, axis=1)
                
                df_view_a = df_agencia.copy()
                df_view_a.insert(0, "Seleccionar", False)
                
                cols_show_a = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                                "resumen_venta", "datos_entrega", "pendiente_pago"]
                
                event_agencia = st.data_editor(
                    df_view_a[cols_show_a], key="ed_age", 
                    column_config={
                        "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                        "estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados, width="medium"),
                        "fecha_seguimiento": st.column_config.DateColumn("📅 Fecha", format="DD/MM/YYYY", width="medium"),
                        "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                        "telefono": st.column_config.TextColumn("Telf.", disabled=True),
                        "resumen_venta": st.column_config.TextColumn("Resumen", width="medium", disabled=True),
                        "datos_entrega": st.column_config.TextColumn("Envío", width="large", disabled=True),
                        "pendiente_pago": st.column_config.NumberColumn("Cobrar", format="S/ %.2f"),
                        "id_cliente": None
                    }, 
                    hide_index=True, use_container_width=True
                )
                
                if st.button("💾 Guardar Cambios Agencia", type="primary"): 
                    df_save_a = df_agencia.loc[event_agencia.index].copy()
                    df_save_a['estado'] = event_agencia['estado']
                    df_save_a['fecha_seguimiento'] = event_agencia['fecha_seguimiento'] 
                    df_save_a['pendiente_pago'] = event_agencia['pendiente_pago']
                    guardar_edicion_rapida(df_save_a, "AGENCIA")

                # GESTIÓN AGENCIA INDIVIDUAL
                filas_sel_a = event_agencia[event_agencia["Seleccionar"] == True]
                if not filas_sel_a.empty:
                    row_full_a = df_agencia.loc[filas_sel_a.index[0]]
                    st.divider()
                    st.info(f"🏢 Editando agencia de: **{row_full_a['nombre_corto']}**")
                    with st.form("form_age"):
                        c1, c2, c3 = st.columns(3)
                        n_nom = c1.text_input("Recibe", row_full_a['nombre_receptor'])
                        n_dni = c2.text_input("DNI", row_full_a['dni_receptor'])
                        n_tel = c3.text_input("Telf", row_full_a['telefono_receptor'])
                        
                        c4, c5 = st.columns(2)
                        n_age = c4.selectbox("Agencia", ["Shalom", "Olva", "Marvisur"])
                        n_sede = c5.text_input("Sede", row_full_a['sede_entrega'])
                        
                        if st.form_submit_button("Actualizar Agencia"):
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    UPDATE Direcciones SET 
                                        nombre_receptor=:n, dni_receptor=:d, telefono_receptor=:t, 
                                        agencia_nombre=:a, sede_entrega=:s 
                                    WHERE id_direccion = :id_dir
                                """), {
                                    "n": n_nom, "d": n_dni, "t": n_tel, "a": n_age, "s": n_sede,
                                    "id_dir": int(row_full_a['id_direccion'])
                                })
                            if 'df_seguimiento_cache' in st.session_state:
                                del st.session_state['df_seguimiento_cache']
                            st.success("Datos actualizados.")
                            time.sleep(0.5)
                            st.rerun()
            else:
                st.caption("No hay envíos por agencia.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> SECCIÓN EN RUTA <<<<<<<<<<<<<<<<<<<<<<<<<
        st.divider()
        st.markdown("### 🚚 En Ruta")
        
        if not df_ruta.empty:
            cols_ruta = ["id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", "resumen_items"]
            edit_ruta = st.data_editor(
                df_ruta[cols_ruta], 
                key="ed_ruta", 
                column_config={
                    "estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados),
                    "fecha_seguimiento": st.column_config.DateColumn("Fecha Seg.", format="DD/MM/YYYY"),
                    "id_cliente": None
                }, 
                hide_index=True, use_container_width=True
            )
            
            if st.button("💾 Actualizar Ruta"):
                df_save_ruta = df_ruta.loc[edit_ruta.index].copy()
                df_save_ruta['estado'] = edit_ruta['estado']
                df_save_ruta['fecha_seguimiento'] = edit_ruta['fecha_seguimiento']
                guardar_edicion_rapida(df_save_ruta, "RUTA")
        else:
            st.caption("Nada en ruta.")

        # --- OTRAS BANDEJAS EXPANDIBLES ---
        st.divider()
        with st.expander(f"💬 Conversación / Cotizando ({len(df_e1)})"):
            if not df_e1.empty:
                cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
                event_e1 = st.data_editor(df_e1[cols_e1], key="ed_e1", 
                                          column_config={"estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados), "id_cliente": None}, 
                                          hide_index=True, use_container_width=True)
                if st.button("💾 Guardar Conversación"):
                    df_save_e1 = df_e1.loc[event_e1.index].copy()
                    df_save_e1['estado'] = event_e1['estado']
                    df_save_e1['fecha_seguimiento'] = event_e1['fecha_seguimiento']
                    guardar_edicion_rapida(df_save_e1, "E1")
            else:
                st.caption("Vacío.")

        with st.expander(f"✨ Post-Venta ({len(df_e4)})"):
            if not df_e4.empty:
                cols_e4 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
                event_e4 = st.data_editor(df_e4[cols_e4], key="ed_e4", 
                                          column_config={"estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados), "id_cliente": None},
                                          hide_index=True, use_container_width=True)
                if st.button("💾 Guardar Post-Venta"):
                    df_save_e4 = df_e4.loc[event_e4.index].copy()
                    df_save_e4['estado'] = event_e4['estado']
                    df_save_e4['fecha_seguimiento'] = event_e4['fecha_seguimiento']
                    guardar_edicion_rapida(df_save_e4, "E4")
            else:
                st.caption("Vacío.")
    else:
        st.info("No se encontraron clientes.")