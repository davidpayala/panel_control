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
                    dir.id_direccion, dir.tipo_envio, dir.nombre_receptor, dir.telefono_receptor, 
                    dir.direccion_texto, dir.distrito, 
                    dir.referencia, dir.gps_link, dir.observacion,
                    dir.dni_receptor, dir.agencia_nombre, dir.sede_entrega

                FROM Clientes c
                LEFT JOIN LATERAL (
                    SELECT * FROM Ventas v2 WHERE v2.id_cliente = c.id_cliente ORDER BY v2.id_venta DESC LIMIT 1
                ) v ON TRUE
                LEFT JOIN LATERAL (
                    SELECT * FROM Direcciones d2 
                    WHERE d2.id_cliente = c.id_cliente AND d2.activo = TRUE 
                    ORDER BY d2.es_principal DESC, d2.id_direccion DESC LIMIT 1
                ) dir ON TRUE
                WHERE c.activo = TRUE 
                ORDER BY c.fecha_seguimiento ASC
            """)
            df_loaded = pd.read_sql(query_seg, conn)
            st.session_state['df_seguimiento_cache'] = df_loaded
    
    df_seg = st.session_state['df_seguimiento_cache']

    # --- PARCHE DE SEGURIDAD PARA CACHÉ DESACTUALIZADO ---
    if 'tipo_envio' not in df_seg.columns:
        del st.session_state['df_seguimiento_cache']
        st.rerun()

    # --- 3. FUNCIONES DE GUARDADO ---
    def guardar_edicion_rapida(df_editado, tipo_tabla):
        try:
            with engine.begin() as conn:
                for index, row in df_editado.iterrows():
                    id_etapa_val = mapa_subgrupo_id.get(row['estado'])
                    id_cliente_nativo = int(row['id_cliente'])
                    id_etapa_nativo = int(id_etapa_val) if pd.notnull(id_etapa_val) else None
                    
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET estado = :est, id_etapa = :id_etapa, fecha_seguimiento = :fec 
                        WHERE id_cliente = :id
                    """), {
                        "est": row['estado'], "id_etapa": id_etapa_nativo,
                        "fec": row['fecha_seguimiento'], "id": id_cliente_nativo
                    })
                    
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
                if pd.notna(id_direccion) and str(id_direccion).strip().lower() not in ['', 'nan', 'none'] and int(id_direccion) > 0:
                    conn.execute(text("""
                        UPDATE Direcciones SET 
                            nombre_receptor = :nom, telefono_receptor = :tel, distrito = :dist,
                            direccion_texto = :dir, referencia = :ref, gps_link = :gps,
                            observacion = :obs, dni_receptor = :dni, agencia_nombre = :anom, 
                            sede_entrega = :sede, tipo_envio = :tipo
                        WHERE id_direccion = :id_dir
                    """), {
                        "nom": datos['nombre_receptor'], "tel": datos['telefono_receptor'], "dist": datos.get('distrito'),
                        "dir": datos.get('direccion_texto'), "ref": datos.get('referencia'), "gps": datos.get('gps_link'),
                        "obs": datos['observacion'], "dni": datos.get('dni_receptor'), "anom": datos.get('agencia_nombre'),
                        "sede": datos.get('sede_entrega'), "tipo": datos['tipo_envio'], "id_dir": int(id_direccion)
                    })
                else:
                    conn.execute(text("""
                        INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                                                 direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                                                 sede_entrega, observacion, activo, es_principal)
                        VALUES (:idc, :tipo, :nom, :tel, :dist, :dir, :ref, :gps, :dni, :anom, :sede, :obs, TRUE, TRUE)
                    """), {
                        "idc": int(id_cliente), "tipo": datos['tipo_envio'], "nom": datos['nombre_receptor'], "tel": datos['telefono_receptor'],
                        "dist": datos.get('distrito'), "dir": datos.get('direccion_texto'), "ref": datos.get('referencia'),
                        "gps": datos.get('gps_link'), "dni": datos.get('dni_receptor'), "anom": datos.get('agencia_nombre'), "sede": datos.get('sede_entrega'), "obs": datos['observacion']
                    })
                
                if datos.get('nuevo_estado'):
                    id_etapa_val = mapa_subgrupo_id.get(datos['nuevo_estado'])
                    id_etapa_nativo = int(id_etapa_val) if pd.notnull(id_etapa_val) else None
                    conn.execute(text("""
                        UPDATE Clientes 
                        SET estado = :e, id_etapa = :id_etapa, fecha_seguimiento = NOW() 
                        WHERE id_cliente = :id
                    """), {"e": datos['nuevo_estado'], "id_etapa": id_etapa_nativo, "id": int(id_cliente)})
                                
            st.toast("✅ Datos de envío registrados correctamente.", icon="💾")
            if 'df_seguimiento_cache' in st.session_state:
                del st.session_state['df_seguimiento_cache']
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error guardando formulario: {e}")

    # --- 4. RENDERIZADO Y FILTRADO INTELIGENTE ---
    if not df_seg.empty:
        df_etapa2 = df_seg[df_seg['estado'].isin(etapa_2_etapas)]
        
        # Separación Inicial: Con dirección vs Sin dirección registrada
        df_por_registrar = df_etapa2[pd.isna(df_etapa2['id_direccion'])].copy()
        df_con_direccion = df_etapa2[pd.notna(df_etapa2['id_direccion'])].copy()
        
        # Segmentación Etapa 2 por columna tipo_envio
        df_moto = df_con_direccion[df_con_direccion['tipo_envio'] == 'MOTO'].copy()
        df_agencia = df_con_direccion[df_con_direccion['tipo_envio'] == 'AGENCIA'].copy()
        df_otros = df_con_direccion[df_con_direccion['tipo_envio'] == 'OTROS'].copy()
        
        # Segmentación Etapa 3 (En Ruta) por columna tipo_envio
        df_ruta_general = df_seg[df_seg['estado'].isin(etapa_3_etapas)]
        df_ruta_moto = df_ruta_general[df_ruta_general['tipo_envio'] == 'MOTO'].copy()
        df_ruta_agencia = df_ruta_general[df_ruta_general['tipo_envio'] == 'AGENCIA'].copy()
        df_ruta_otros = df_ruta_general[df_ruta_general['tipo_envio'] == 'OTROS'].copy()
        
        df_e1 = df_seg[df_seg['estado'].isin(etapa_1_etapas)].copy()
        df_e4 = df_seg[df_seg['estado'].isin(etapa_4_etapas)].copy()

        # Métricas (5 Columnas)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🛵 Moto / Interno", len(df_moto))
        c2.metric("🏢 Agencia", len(df_agencia))
        c3.metric("📦 Otros", len(df_otros))
        c4.metric("📝 Por Registrar", len(df_por_registrar))
        c5.metric("🚚 En Ruta Total", len(df_ruta_general))
        
        st.divider()
        
        tab_moto, tab_agencia, tab_otros, tab_por_registrar = st.tabs([
            "🛵 MOTORIZADO / INTERNO", "🏢 AGENCIA", "📦 OTROS", "📝 POR REGISTRAR"
        ])

        # --- FORMATOS VISUALES ---
        def formatear_entrega_moto(row):
            return (f"👤 {row['nombre_receptor'] or ''}\n"
                    f"📞 {row['telefono_receptor'] or ''}\n"
                    f"📍 {row['direccion_texto'] or ''} ({row['distrito'] or ''})\n"
                    f"🏠 Ref: {row['referencia'] or ''}\n"
                    f"🗺️ GPS: {row['gps_link'] or ''}\n"
                    f"📝 Obs: {row['observacion'] or ''}")

        def formatear_entrega_agencia(row):
            return (f"👤 {row['nombre_receptor'] or ''}\n"
                    f"🆔 DNI: {row['dni_receptor'] or ''}\n"
                    f"📞 {row['telefono_receptor'] or ''}\n"
                    f"🏢 {row['agencia_nombre'] or ''} - {row['sede_entrega'] or ''}\n"
                    f"🔐 Clave: {row['clave_seguridad'] or ''}")

        def formatear_entrega_otros(row):
            return (f"👤 {row['nombre_receptor'] or ''}\n"
                    f"📞 {row['telefono_receptor'] or ''}\n"
                    f"📝 Obs: {row['observacion'] or ''}")

        def formatear_venta_resumen(row):
            if pd.isnull(row['id_venta']): return ""
            fecha_str = row['fecha_venta'].strftime('%d/%m %H:%M') if pd.notnull(row['fecha_venta']) else "--"
            total = float(row['total_venta']) if pd.notnull(row['total_venta']) else 0.0
            return (f"📅 {fecha_str}\n"
                    f"🛒 {row['resumen_items']}\n"
                    f"💰 Total: S/ {total:.2f}")

        cols_show = ["Seleccionar", "id_cliente", "estado", "fecha_seguimiento", "nombre_corto", "telefono", 
                     "resumen_venta", "datos_entrega", "pendiente_pago"]

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA MOTO <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_moto:
            if not df_moto.empty:
                df_moto["datos_entrega"] = df_moto.apply(formatear_entrega_moto, axis=1)
                df_moto["resumen_venta"] = df_moto.apply(formatear_venta_resumen, axis=1)
                df_view = df_moto.copy()
                df_view.insert(0, "Seleccionar", False)

                event_moto = st.data_editor(
                    df_view[cols_show], key="ed_moto", 
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
                    }, hide_index=True, use_container_width=True
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
                        monto = float(row['pendiente_pago']) if row['pendiente_pago'] else 0.0
                        str_cobro = "Pagó todo" if monto <= 0 else f"S/ {monto:.2f}"
                        texto_ruta += f"*Pedido {count}*\n*Recibe:* {row['nombre_receptor'] or ''}\n*Dirección:* {row['direccion_texto'] or ''}\n*Ref:* {row['referencia'] or ''}\n*Distrito:* {row['distrito'] or ''}\n*GPS:* {row['gps_link'] or ''}\n*Telf:* {row['telefono_receptor'] or ''}\n*Cobrar:* {str_cobro}\n*Observación:* {row['observacion'] or ''}\n----------------------------------\n"
                        count += 1
                    st.code(texto_ruta)

                filas_sel = event_moto[event_moto["Seleccionar"] == True]
                if not filas_sel.empty:
                    row_full = df_moto.loc[filas_sel.index[0]]
                    st.divider()
                    with st.form("form_moto_dir"):
                        c1, c2, c3 = st.columns(3)
                        n_nom = c1.text_input("Recibe", row_full['nombre_receptor'])
                        n_tel = c2.text_input("Teléfono", row_full['telefono_receptor'])
                        n_dist = c3.text_input("Distrito", row_full['distrito'])
                        n_dir = st.text_input("Dirección Exacta", row_full['direccion_texto'])
                        c4, c5, c6 = st.columns(3)
                        n_ref = c4.text_input("Referencia", row_full['referencia'])
                        n_gps = c5.text_input("Link GPS", row_full['gps_link'])
                        n_obs = c6.text_input("Observaciones", row_full['observacion']) 
                        col_st, col_btn = st.columns([2, 1])
                        idx_estado = todos_los_estados.index(row_full['estado']) if row_full['estado'] in todos_los_estados else 0
                        nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_moto_ind")
                        if col_btn.form_submit_button("💾 Guardar Guía Moto", type="primary"):
                            datos_form = {"nombre_receptor": n_nom, "telefono_receptor": n_tel, "distrito": n_dist, "direccion_texto": n_dir, "referencia": n_ref, "gps_link": n_gps, "observacion": n_obs, "tipo_envio": "MOTO", "nuevo_estado": nuevo_estado}
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
                
                event_agencia = st.data_editor(
                    df_view_a[cols_show], key="ed_age", 
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
                    }, hide_index=True, use_container_width=True
                )
                
                if st.button("💾 Guardar Cambios Agencia", type="primary"): 
                    df_save_a = df_agencia.loc[event_agencia.index].copy()
                    df_save_a['estado'] = event_agencia['estado']
                    df_save_a['fecha_seguimiento'] = event_agencia['fecha_seguimiento'] 
                    df_save_a['pendiente_pago'] = event_agencia['pendiente_pago']
                    guardar_edicion_rapida(df_save_a, "AGENCIA")

                filas_sel_a = event_agencia[event_agencia["Seleccionar"] == True]
                if not filas_sel_a.empty:
                    row_full_a = df_agencia.loc[filas_sel_a.index[0]]
                    st.divider()
                    with st.form("form_age_ind"):
                        c1, c2, c3 = st.columns(3)
                        n_nom = c1.text_input("Recibe", row_full_a['nombre_receptor'])
                        n_dni = c2.text_input("DNI", row_full_a['dni_receptor'])
                        n_tel = c3.text_input("Telf", row_full_a['telefono_receptor'])
                        c4, c5 = st.columns(2)
                        agencias_opciones = ["Shalom", "Olva", "Marvisur"]
                        age_act = row_full_a['agencia_nombre'] if row_full_a['agencia_nombre'] in agencias_opciones else "Shalom"
                        n_age = c4.selectbox("Agencia", agencias_opciones, index=agencias_opciones.index(age_act))
                        n_sede = c5.text_input("Sede", row_full_a['sede_entrega'])
                        n_obs = st.text_input("Observación Extra", row_full_a['observacion'])
                        col_st, col_btn = st.columns([2, 1])
                        idx_estado = todos_los_estados.index(row_full_a['estado']) if row_full_a['estado'] in todos_los_estados else 0
                        nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_age_ind")
                        if col_btn.form_submit_button("Actualizar Agencia"):
                            datos_form = {"nombre_receptor": n_nom, "dni_receptor": n_dni, "telefono_receptor": n_tel, "agencia_nombre": n_age, "sede_entrega": n_sede, "observacion": n_obs, "tipo_envio": "AGENCIA", "nuevo_estado": nuevo_estado}
                            guardar_datos_envio_completo(int(row_full_a['id_direccion']), row_full_a['id_cliente'], datos_form)
            else:
                st.caption("No hay envíos por agencia.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA OTROS <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_otros:
            if not df_otros.empty:
                df_otros["datos_entrega"] = df_otros.apply(formatear_entrega_otros, axis=1)
                df_otros["resumen_venta"] = df_otros.apply(formatear_venta_resumen, axis=1)
                df_view_o = df_otros.copy()
                df_view_o.insert(0, "Seleccionar", False)

                event_otros = st.data_editor(
                    df_view_o[cols_show], key="ed_otros",
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
                    }, hide_index=True, use_container_width=True
                )

                if st.button("💾 Guardar Cambios Otros", type="primary"):
                    df_save_o = df_otros.loc[event_otros.index].copy()
                    df_save_o['estado'] = event_otros['estado']
                    df_save_o['fecha_seguimiento'] = event_otros['fecha_seguimiento']
                    df_save_o['pendiente_pago'] = event_otros['pendiente_pago']
                    guardar_edicion_rapida(df_save_o, "OTROS")

                filas_sel_o = event_otros[event_otros["Seleccionar"] == True]
                if not filas_sel_o.empty:
                    row_full_o = df_otros.loc[filas_sel_o.index[0]]
                    st.divider()
                    with st.form("form_otros_ind"):
                        c1, c2 = st.columns(2)
                        n_nom = c1.text_input("Recibe", row_full_o['nombre_receptor'])
                        n_tel = c2.text_input("Teléfono", row_full_o['telefono_receptor'])
                        n_obs = st.text_area("Observación / Lugar", row_full_o['observacion'])
                        col_st, col_btn = st.columns([2, 1])
                        idx_estado = todos_los_estados.index(row_full_o['estado']) if row_full_o['estado'] in todos_los_estados else 0
                        nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_otros_ind")
                        if col_btn.form_submit_button("💾 Guardar Guía Otros", type="primary"):
                            datos_form = {"nombre_receptor": n_nom, "telefono_receptor": n_tel, "observacion": n_obs, "tipo_envio": "OTROS", "nuevo_estado": nuevo_estado}
                            guardar_datos_envio_completo(int(row_full_o['id_direccion']), row_full_o['id_cliente'], datos_form)
            else:
                st.caption("No hay pedidos en Otros.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> PESTAÑA POR REGISTRAR <<<<<<<<<<<<<<<<<<<<<<<<<
        with tab_por_registrar:
            if not df_por_registrar.empty:
                df_por_registrar["resumen_venta"] = df_por_registrar.apply(formatear_venta_resumen, axis=1)
                df_por_registrar["datos_entrega"] = "⚠️ Sin dirección registrada en el sistema"
                df_view_pr = df_por_registrar.copy()
                df_view_pr.insert(0, "Seleccionar", False)

                event_pr = st.data_editor(
                    df_view_pr[cols_show], key="ed_pr",
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
                    }, hide_index=True, use_container_width=True
                )

                if st.button("💾 Guardar Cambios Rápidos Pendientes", type="primary"):
                    df_save_pr = df_por_registrar.loc[event_pr.index].copy()
                    df_save_pr['estado'] = event_pr['estado']
                    df_save_pr['fecha_seguimiento'] = event_pr['fecha_seguimiento']
                    df_save_pr['pendiente_pago'] = event_pr['pendiente_pago']
                    guardar_edicion_rapida(df_save_pr, "PR")

                filas_sel_pr = event_pr[event_pr["Seleccionar"] == True]
                if not filas_sel_pr.empty:
                    row_full_pr = df_por_registrar.loc[filas_sel_pr.index[0]]
                    st.divider()
                    st.warning(f"📝 Registrar Primera Dirección para: **{row_full_pr['nombre_corto']}**")
                    
                    tipo_nuevo_ui = st.selectbox("Tipo de Envío a Registrar", ["Motorizado", "Agencia", "Otros"], key="tipo_envio_pr_sb")
                    mapa_ui_to_db = {"Motorizado": "MOTO", "Agencia": "AGENCIA", "Otros": "OTROS"}
                    tipo_nuevo_db = mapa_ui_to_db[tipo_nuevo_ui]

                    with st.form("form_pr_dir_ind"):
                        c1, c2 = st.columns(2)
                        n_nom = c1.text_input("Nombre Receptor", value=row_full_pr['nombre_corto'])
                        n_tel = c2.text_input("Telf. Receptor", value=row_full_pr['telefono'])
                        
                        n_dist, n_dir, n_ref, n_gps = "", "", "", ""
                        n_dni, n_age, n_sede = "", "", ""
                        
                        if tipo_nuevo_db == "MOTO":
                            d1, d2 = st.columns(2)
                            n_dist = d1.text_input("Distrito")
                            n_dir = d2.text_input("Dirección Exacta")
                            n_ref = st.text_input("Referencia")
                            n_gps = st.text_input("Link GPS")
                        elif tipo_nuevo_db == "AGENCIA":
                            d1, d2, d3 = st.columns(3)
                            n_dni = d1.text_input("DNI Receptor")
                            n_age = d2.text_input("Nombre Agencia (Shalom, Olva...)")
                            n_sede = d3.text_input("Sede Entrega")
                            
                        n_obs = st.text_area("Observación")
                        col_st, col_btn = st.columns([2, 1])
                        idx_estado = todos_los_estados.index(row_full_pr['estado']) if row_full_pr['estado'] in todos_los_estados else 0
                        nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_pr_ind")
                        
                        if col_btn.form_submit_button("💾 Guardar y Registrar Dirección", type="primary"):
                            datos_form = {"nombre_receptor": n_nom, "telefono_receptor": n_tel, "distrito": n_dist, "direccion_texto": n_dir, "referencia": n_ref, "gps_link": n_gps, "dni_receptor": n_dni, "agencia_nombre": n_age, "sede_entrega": n_sede, "observacion": n_obs, "tipo_envio": tipo_nuevo_db, "nuevo_estado": nuevo_estado}
                            guardar_datos_envio_completo(None, row_full_pr['id_cliente'], datos_form)
            else:
                st.caption("No hay clientes de Etapa 2 pendientes de dirección.")

        # >>>>>>>>>>>>>>>>>>>>>>>>> SECCIÓN EN RUTA (ETAPA 3 SEGMENTADA) <<<<<<<<<<<<<<<<<<<<<<<<<
        st.divider()
        st.markdown("### 🚚 En Ruta Logística")
        
        if not df_ruta_general.empty:
            tab_r_moto, tab_r_agencia, tab_r_otros = st.tabs([
                "🛵 EN RUTA MOTORIZADO", "🏢 EN RUTA AGENCIA", "📦 EN RUTA OTROS"
            ])
            
            with tab_r_moto:
                if not df_ruta_moto.empty:
                    df_ruta_moto["datos_entrega"] = df_ruta_moto.apply(formatear_entrega_moto, axis=1)
                    df_ruta_moto["resumen_venta"] = df_ruta_moto.apply(formatear_venta_resumen, axis=1)
                    df_view_rm = df_ruta_moto.copy()
                    df_view_rm.insert(0, "Seleccionar", False)
                    
                    event_rm = st.data_editor(
                        df_view_rm[cols_show], key="ed_r_moto",
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
                        }, hide_index=True, use_container_width=True
                    )
                    
                    if st.button("💾 Guardar Ruta Moto", type="primary"):
                        df_save_rm = df_ruta_moto.loc[event_rm.index].copy()
                        df_save_rm['estado'] = event_rm['estado']
                        df_save_rm['fecha_seguimiento'] = event_rm['fecha_seguimiento']
                        df_save_rm['pendiente_pago'] = event_rm['pendiente_pago']
                        guardar_edicion_rapida(df_save_rm, "R_MOTO")
                        
                    filas_sel_rm = event_rm[event_rm["Seleccionar"] == True]
                    if not filas_sel_rm.empty:
                        row_full_rm = df_ruta_moto.loc[filas_sel_rm.index[0]]
                        st.divider()
                        with st.form("form_r_moto_dir"):
                            c1, c2, c3 = st.columns(3)
                            n_nom = c1.text_input("Recibe", row_full_rm['nombre_receptor'])
                            n_tel = c2.text_input("Teléfono", row_full_rm['telefono_receptor'])
                            n_dist = c3.text_input("Distrito", row_full_rm['distrito'])
                            n_dir = st.text_input("Dirección Exacta", row_full_rm['direccion_texto'])
                            c4, c5, c6 = st.columns(3)
                            n_ref = c4.text_input("Referencia", row_full_rm['referencia'])
                            n_gps = c5.text_input("Link GPS", row_full_rm['gps_link'])
                            n_obs = c6.text_input("Observaciones", row_full_rm['observacion']) 
                            col_st, col_btn = st.columns([2, 1])
                            idx_estado = todos_los_estados.index(row_full_rm['estado']) if row_full_rm['estado'] in todos_los_estados else 0
                            nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_rm_ind")
                            if col_btn.form_submit_button("💾 Actualizar Envió Moto", type="primary"):
                                datos_form = {"nombre_receptor": n_nom, "telefono_receptor": n_tel, "distrito": n_dist, "direccion_texto": n_dir, "referencia": n_ref, "gps_link": n_gps, "observacion": n_obs, "tipo_envio": "MOTO", "nuevo_estado": nuevo_estado}
                                guardar_datos_envio_completo(int(row_full_rm['id_direccion']), row_full_rm['id_cliente'], datos_form)
                else:
                    st.caption("No hay envíos motorizados en ruta.")
                    
            with tab_r_agencia:
                if not df_ruta_agencia.empty:
                    df_ruta_agencia["datos_entrega"] = df_ruta_agencia.apply(formatear_entrega_agencia, axis=1)
                    df_ruta_agencia["resumen_venta"] = df_ruta_agencia.apply(formatear_venta_resumen, axis=1)
                    df_view_ra = df_ruta_agencia.copy()
                    df_view_ra.insert(0, "Seleccionar", False)
                    
                    event_ra = st.data_editor(
                        df_view_ra[cols_show], key="ed_r_age",
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
                        }, hide_index=True, use_container_width=True
                    )
                    
                    if st.button("💾 Guardar Ruta Agencia", type="primary"):
                        df_save_ra = df_ruta_agencia.loc[event_ra.index].copy()
                        df_save_ra['estado'] = event_ra['estado']
                        df_save_ra['fecha_seguimiento'] = event_ra['fecha_seguimiento']
                        df_save_ra['pendiente_pago'] = event_ra['pendiente_pago']
                        guardar_edicion_rapida(df_save_ra, "R_AGENCIA")
                        
                    filas_sel_ra = event_ra[event_ra["Seleccionar"] == True]
                    if not filas_sel_ra.empty:
                        row_full_ra = df_ruta_agencia.loc[filas_sel_ra.index[0]]
                        st.divider()
                        with st.form("form_r_age_ind"):
                            c1, c2, c3 = st.columns(3)
                            n_nom = c1.text_input("Recibe", row_full_ra['nombre_receptor'])
                            n_dni = c2.text_input("DNI", row_full_ra['dni_receptor'])
                            n_tel = c3.text_input("Telf", row_full_ra['telefono_receptor'])
                            c4, c5 = st.columns(2)
                            agencias_opciones = ["Shalom", "Olva", "Marvisur"]
                            age_act = row_full_ra['agencia_nombre'] if row_full_ra['agencia_nombre'] in agencias_opciones else "Shalom"
                            n_age = c4.selectbox("Agencia", agencias_opciones, index=agencias_opciones.index(age_act))
                            n_sede = c5.text_input("Sede", row_full_ra['sede_entrega'])
                            n_obs = st.text_input("Observación Extra", row_full_ra['observacion'])
                            col_st, col_btn = st.columns([2, 1])
                            idx_estado = todos_los_estados.index(row_full_ra['estado']) if row_full_ra['estado'] in todos_los_estados else 0
                            nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_ra_ind")
                            if col_btn.form_submit_button("Actualizar Envío Agencia"):
                                datos_form = {"nombre_receptor": n_nom, "dni_receptor": n_dni, "telefono_receptor": n_tel, "agencia_nombre": n_age, "sede_entrega": n_sede, "observacion": n_obs, "tipo_envio": "AGENCIA", "nuevo_estado": nuevo_estado}
                                guardar_datos_envio_completo(int(row_full_ra['id_direccion']), row_full_ra['id_cliente'], datos_form)
                else:
                    st.caption("No hay envíos en agencia en ruta.")
                    
            with tab_r_otros:
                if not df_ruta_otros.empty:
                    df_ruta_otros["datos_entrega"] = df_ruta_otros.apply(formatear_entrega_otros, axis=1)
                    df_ruta_otros["resumen_venta"] = df_ruta_otros.apply(formatear_venta_resumen, axis=1)
                    df_view_ro = df_ruta_otros.copy()
                    df_view_ro.insert(0, "Seleccionar", False)
                    
                    event_ro = st.data_editor(
                        df_view_ro[cols_show], key="ed_r_otros",
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
                        }, hide_index=True, use_container_width=True
                    )
                    
                    if st.button("💾 Guardar Ruta Otros", type="primary"):
                        df_save_ro = df_ruta_otros.loc[event_ro.index].copy()
                        df_save_ro['estado'] = event_ro['estado']
                        df_save_ro['fecha_seguimiento'] = event_ro['fecha_seguimiento']
                        df_save_ro['pendiente_pago'] = event_ro['pendiente_pago']
                        guardar_edicion_rapida(df_save_ro, "R_OTROS")
                        
                    filas_sel_ro = event_ro[event_ro["Seleccionar"] == True]
                    if not filas_sel_ro.empty:
                        row_full_ro = df_ruta_otros.loc[filas_sel_ro.index[0]]
                        st.divider()
                        with st.form("form_r_otros_ind"):
                            c1, c2 = st.columns(2)
                            n_nom = c1.text_input("Recibe", row_full_ro['nombre_receptor'])
                            n_tel = c2.text_input("Teléfono", row_full_ro['telefono_receptor'])
                            n_obs = st.text_area("Observación / Lugar", row_full_o['observacion'])
                            col_st, col_btn = st.columns([2, 1])
                            idx_estado = todos_los_estados.index(row_full_ro['estado']) if row_full_ro['estado'] in todos_los_estados else 0
                            nuevo_estado = col_st.selectbox("Mover a Estado:", todos_los_estados, index=idx_estado, key="est_ro_ind")
                            if col_btn.form_submit_button("💾 Guardar Guía Otros", type="primary"):
                                datos_form = {"nombre_receptor": n_nom, "telefono_receptor": n_tel, "observacion": n_obs, "tipo_envio": "OTROS", "nuevo_estado": nuevo_estado}
                                guardar_datos_envio_completo(int(row_full_ro['id_direccion']), row_full_ro['id_cliente'], datos_form)
                else:
                    st.caption("No hay envíos en Otros en ruta.")
        else:
            st.caption("Nada en ruta por despachar.")

        # --- OTRAS BANDEJAS EXPANDIBLES ---
        st.divider()
        with st.expander(f"💬 Conversación / Cotizando ({len(df_e1)})"):
            if not df_e1.empty:
                cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
                event_e1 = st.data_editor(df_e1[cols_e1], key="ed_e1", column_config={"estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados), "id_cliente": None}, hide_index=True, use_container_width=True)
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
                event_e4 = st.data_editor(df_e4[cols_e4], key="ed_e4", column_config={"estado": st.column_config.SelectboxColumn("Estado", options=todos_los_estados), "id_cliente": None}, hide_index=True, use_container_width=True)
                if st.button("💾 Guardar Post-Venta"):
                    df_save_e4 = df_e4.loc[event_e4.index].copy()
                    df_save_e4['estado'] = event_e4['estado']
                    df_save_e4['fecha_seguimiento'] = event_e4['fecha_seguimiento']
                    guardar_edicion_rapida(df_save_e4, "E4")
            else:
                st.caption("Vacío.")
    else:
        st.info("No se encontraron clientes.")