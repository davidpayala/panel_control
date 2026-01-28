import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine

def guardar_edicion_rapida(df_modificado, etapa_key):
    """Guarda cambios de Estado y Dirección Completa"""
    if df_modificado.empty: return
    
    try:
        count = 0
        with engine.begin() as conn:
            for index, row in df_modificado.iterrows():
                # 1. Actualizar ESTADO del Cliente
                if 'estado' in row and row['estado']:
                    conn.execute(text("UPDATE Clientes SET estado = :e, fecha_seguimiento = NOW() WHERE id_cliente = :id"), 
                                 {"e": row['estado'], "id": row['id_cliente']})
                
                # 2. Actualizar DIRECCIÓN (Si es una fila con dirección)
                if 'id_direccion' in row and pd.notna(row['id_direccion']) and row['id_direccion'] > 0:
                    conn.execute(text("""
                        UPDATE Direcciones SET 
                            activo = :act,
                            tipo_envio = :tipo,
                            distrito = :dist,
                            direccion_texto = :dir,
                            gps_link = :gps,
                            referencia = :ref,
                            observacion = :obs
                        WHERE id_direccion = :id_dir
                    """), {
                        "act": row.get('dir_activo', True), # El Check
                        "tipo": row.get('tipo_envio', 'DOMICILIO'),
                        "dist": row.get('distrito', ''),
                        "dir": row.get('direccion_texto', ''),
                        "gps": row.get('gps_link', ''),
                        "ref": row.get('referencia', ''),
                        "obs": row.get('observacion', ''),
                        "id_dir": row['id_direccion']
                    })
                count += 1
                
        st.toast(f"✅ {count} registros actualizados.", icon="💾")
        if 'df_seguimiento_cache' in st.session_state:
            del st.session_state['df_seguimiento_cache']
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Error guardando {etapa_key}: {e}")

def render_seguimiento():
    # Estilos para celdas más amplias
    st.markdown("""
        <style>
            div[data-testid="stDataEditor"] td {
                white-space: pre-wrap !important;
                vertical-align: top !important;
                font-size: 13px;
                min-width: 100px;
            }
        </style>
    """, unsafe_allow_html=True)

    c_titulo, c_refresh = st.columns([4, 1])
    c_titulo.subheader("🎯 Tablero de Seguimiento Logístico")
    
    if c_refresh.button("🔄 Recargar Datos"):
        if 'df_seguimiento_cache' in st.session_state:
            del st.session_state['df_seguimiento_cache']
        st.rerun()

    # --- 1. CONFIGURACIÓN ---
    ETAPAS = {
        "ETAPA_0": ["Sin empezar"],
        "ETAPA_1": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
        "ETAPA_2": ["Venta motorizado", "Venta agencia", "Venta express moto"],
        "ETAPA_3": ["En camino moto", "En camino agencia", "Contraentrega agencia"],
        "ETAPA_4": ["Pendiente agradecer", "Problema post", "Venta cerrada", "Post-venta"]
    }
    TODOS_LOS_ESTADOS = [e for lista in ETAPAS.values() for e in lista]
    OPCIONES_ENVIO = ["DOMICILIO", "MOTO", "AGENCIA SHALOM", "AGENCIA OLVA", "OTRA AGENCIA"]

    # --- 2. CARGA DE DATOS ---
    if 'df_seguimiento_cache' in st.session_state:
        df = st.session_state['df_seguimiento_cache']
    else:
        # Nota: Quitamos "AND d.activo = TRUE" para ver todas y poder activar/desactivar con el check
        query = """
            SELECT 
                c.id_cliente, c.nombre_corto, c.telefono, c.estado, c.fecha_seguimiento,
                d.id_direccion, d.activo as dir_activo, d.tipo_envio, d.distrito, d.direccion_texto,
                d.gps_link, d.referencia, d.observacion,
                (SELECT STRING_AGG(CONCAT('S/ ', total_venta, ' (', COALESCE(nota, '-'), ')'), ', ') 
                 FROM Ventas v WHERE v.id_cliente = c.id_cliente AND v.fecha_venta > (NOW() - INTERVAL '30 days')) as resumen_items
            FROM Clientes c
            LEFT JOIN Direcciones d ON c.id_cliente = d.id_cliente
            WHERE c.activo = TRUE
            ORDER BY c.fecha_seguimiento DESC
        """
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            # Limpieza de nulos
            cols_txt = ['gps_link', 'referencia', 'observacion', 'direccion_texto', 'distrito', 'tipo_envio']
            for col in cols_txt:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str)
            
            # Asegurar booleanos
            if 'dir_activo' in df.columns:
                df['dir_activo'] = df['dir_activo'].fillna(False).astype(bool)

            st.session_state['df_seguimiento_cache'] = df
        except Exception as e:
            st.error(f"Error SQL: {e}")
            return

    # Filtros
    df_e0 = df[df['estado'].isin(ETAPAS["ETAPA_0"])]
    df_e1 = df[df['estado'].isin(ETAPAS["ETAPA_1"])]
    df_e2 = df[df['estado'].isin(ETAPAS["ETAPA_2"])]
    df_e3 = df[df['estado'].isin(ETAPAS["ETAPA_3"])]
    df_e4 = df[df['estado'].isin(ETAPAS["ETAPA_4"])]

    # --- 3. TABLAS ---

    # ETAPA 0
    with st.expander(f"❄️ Sin Empezar ({len(df_e0)})"):
        st.dataframe(df_e0[['nombre_corto', 'telefono']], hide_index=True)

    # ETAPA 1
    with st.expander(f"💬 En Conversación ({len(df_e1)})", expanded=True):
        if not df_e1.empty:
            cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
            event_e1 = st.data_editor(
                df_e1[cols_e1], 
                key="ed_e1", 
                column_config={
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                    "id_cliente": None,
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "resumen_items": st.column_config.TextColumn("Compras", disabled=True)
                },
                hide_index=True, use_container_width=True
            )
            if st.button("💾 Guardar Conversación"):
                df_save = df_e1.loc[event_e1.index].copy()
                df_save['estado'] = event_e1['estado']
                guardar_edicion_rapida(df_save, "E1")
        else:
            st.caption("Vacío")

    # ETAPA 2: DESPACHOS (Aquí está la edición completa que pediste)
    st.markdown("---")
    st.subheader(f"📦 Listos para Despachar ({len(df_e2)})")
    
    if not df_e2.empty:
        # Incluimos: Activo (Check), Tipo Envio, Distrito, Dirección, GPS, Ref, Obs
        cols_e2 = ["id_cliente", "id_direccion", "estado", "nombre_corto", 
                   "dir_activo", "tipo_envio", "distrito", "direccion_texto", 
                   "gps_link", "referencia", "observacion", "resumen_items"]
        
        event_e2 = st.data_editor(
            df_e2[cols_e2], 
            key="ed_e2", 
            column_config={
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                "id_cliente": None, "id_direccion": None,
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                
                # --- SECCIÓN DIRECCIONES ---
                "dir_activo": st.column_config.CheckboxColumn("Usar?", width="small"),
                "tipo_envio": st.column_config.SelectboxColumn("Tipo", options=OPCIONES_ENVIO, width="medium"),
                "distrito": st.column_config.TextColumn("Distrito", width="small"),
                "direccion_texto": st.column_config.TextColumn("Dirección", width="medium"),
                
                # --- TUS CAMPOS PEDIDOS ---
                "gps_link": st.column_config.TextColumn("📍 GPS", width="medium"),
                "referencia": st.column_config.TextColumn("🏠 Ref", width="medium"),
                "observacion": st.column_config.TextColumn("📝 Obs", width="medium"),
                
                "resumen_items": st.column_config.TextColumn("Nota Venta", disabled=True)
            },
            hide_index=True, use_container_width=True
        )
        
        if st.button("💾 Guardar Despachos"):
            df_save = df_e2.loc[event_e2.index].copy()
            # Mapeamos todo lo editable
            for col in ["estado", "dir_activo", "tipo_envio", "distrito", "direccion_texto", "gps_link", "referencia", "observacion"]:
                df_save[col] = event_e2[col]
            guardar_edicion_rapida(df_save, "E2")
    else:
        st.info("Sin despachos.")

    # ETAPA 3: EN RUTA
    st.markdown("---")
    st.subheader(f"🚀 En Camino ({len(df_e3)})")
    
    if not df_e3.empty:
        cols_e3 = ["id_cliente", "id_direccion", "estado", "nombre_corto", "dir_activo", "gps_link", "observacion"]
        
        event_e3 = st.data_editor(
            df_e3[cols_e3], 
            key="ed_e3", 
            column_config={
                "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                "id_cliente": None, "id_direccion": None,
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "dir_activo": st.column_config.CheckboxColumn("Ok?", disabled=True),
                "gps_link": st.column_config.TextColumn("GPS"),
                "observacion": st.column_config.TextColumn("Notas Entrega")
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Guardar Rutas"):
            df_save = df_e3.loc[event_e3.index].copy()
            df_save['estado'] = event_e3['estado']
            df_save['gps_link'] = event_e3['gps_link']
            df_save['observacion'] = event_e3['observacion']
            guardar_edicion_rapida(df_save, "E3")
    else:
        st.caption("Nada en ruta.")

    # ETAPA 4: POST VENTA
    with st.expander(f"✨ Post-Venta ({len(df_e4)})"):
        if not df_e4.empty:
            cols_e4 = ["id_cliente", "estado", "nombre_corto"]
            event_e4 = st.data_editor(
                df_e4[cols_e4], 
                key="ed_e4", 
                column_config={"estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS), "id_cliente": None},
                hide_index=True, use_container_width=True
            )
            if st.button("💾 Guardar Post-Venta"):
                df_save = df_e4.loc[event_e4.index].copy()
                df_save['estado'] = event_e4['estado']
                guardar_edicion_rapida(df_save, "E4")