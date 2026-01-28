import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine

def guardar_edicion_rapida(df_modificado, etapa_key):
    """Guarda cambios de Estado y Dirección (GPS, Ref, Obs)"""
    if df_modificado.empty: return
    
    try:
        count = 0
        with engine.begin() as conn:
            for index, row in df_modificado.iterrows():
                # 1. Actualizar ESTADO del Cliente
                if 'estado' in row and row['estado']:
                    conn.execute(text("UPDATE Clientes SET estado = :e, fecha_seguimiento = NOW() WHERE id_cliente = :id"), 
                                 {"e": row['estado'], "id": row['id_cliente']})
                
                # 2. Actualizar DIRECCIÓN (Si existe dirección asociada)
                if 'id_direccion' in row and pd.notna(row['id_direccion']) and row['id_direccion'] > 0:
                    # CORRECCIÓN: 'observacion' en singular para coincidir con tu DB
                    conn.execute(text("""
                        UPDATE Direcciones SET 
                            gps_link = :gps,
                            referencia = :ref,
                            observacion = :obs, 
                            direccion_texto = :dir
                        WHERE id_direccion = :id_dir
                    """), {
                        "gps": row.get('gps_link', ''),
                        "ref": row.get('referencia', ''),
                        "obs": row.get('observacion', ''), # <-- CORREGIDO
                        "dir": row.get('direccion_texto', ''),
                        "id_dir": row['id_direccion']
                    })
                count += 1
                
        st.toast(f"✅ {count} registros actualizados (Estado + Dirección).")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Error guardando {etapa_key}: {e}")

def render_seguimiento():
    # CSS para ajustar altura de filas y hacerlas más legibles
    st.markdown("""
        <style>
            div[data-testid="stDataEditor"] td {
                white-space: pre-wrap !important;
                vertical-align: top !important;
                font-size: 13px;
            }
        </style>
    """, unsafe_allow_html=True)

    c_titulo, c_refresh = st.columns([4, 1])
    c_titulo.subheader("🎯 Tablero de Seguimiento Logístico")
    
    if c_refresh.button("🔄 Recargar Datos"):
        st.rerun()

    # --- 1. CONFIGURACIÓN DE ETAPAS ---
    ETAPAS = {
        "ETAPA_0": ["Sin empezar"],
        "ETAPA_1": ["Responder duda", "Interesado en venta", "Proveedor nacional", "Proveedor internacional"],
        "ETAPA_2": ["Venta motorizado", "Venta agencia", "Venta express moto"], 
        "ETAPA_3": ["En camino moto", "En camino agencia", "Contraentrega agencia"], 
        "ETAPA_4": ["Pendiente agradecer", "Problema post", "Venta cerrada", "Post-venta"]
    }
    TODOS_LOS_ESTADOS = [e for lista in ETAPAS.values() for e in lista]

    # --- 2. CARGA DE DATOS (JOIN CON DIRECCIONES) ---
    # CORRECCIÓN: 'd.observacion' en singular en el SELECT
    query = """
        SELECT 
            c.id_cliente, c.nombre_corto, c.telefono, c.estado, c.fecha_seguimiento,
            d.id_direccion, d.direccion_texto, d.distrito, d.tipo_envio,
            d.gps_link, d.referencia, d.observacion,
            (SELECT STRING_AGG(CONCAT(cantidad, ' x ', producto), ', ') 
             FROM Ventas v WHERE v.id_cliente = c.id_cliente AND v.fecha_venta > (NOW() - INTERVAL '30 days')) as resumen_items
        FROM Clientes c
        LEFT JOIN Direcciones d ON c.id_cliente = d.id_cliente AND d.activo = TRUE
        WHERE c.activo = TRUE
        ORDER BY c.fecha_seguimiento DESC
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    # Convertir columnas a string y manejar nulos
    cols_txt = ['gps_link', 'referencia', 'observacion', 'direccion_texto'] # <-- CORREGIDO
    for col in cols_txt:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)

    # Filtrado por etapas
    df_e0 = df[df['estado'].isin(ETAPAS["ETAPA_0"])]
    df_e1 = df[df['estado'].isin(ETAPAS["ETAPA_1"])]
    df_e2 = df[df['estado'].isin(ETAPAS["ETAPA_2"])] 
    df_e3 = df[df['estado'].isin(ETAPAS["ETAPA_3"])] 
    df_e4 = df[df['estado'].isin(ETAPAS["ETAPA_4"])]

    # --- 3. RENDERING DE TABLAS ---

    # >>> ETAPA 0: SIN EMPEZAR <<<
    with st.expander(f"❄️ Sin Empezar ({len(df_e0)})", expanded=False):
        st.dataframe(df_e0[['nombre_corto', 'telefono', 'fecha_seguimiento']], hide_index=True)

    # >>> ETAPA 1: CONVERSACIÓN <<<
    with st.expander(f"💬 En Conversación ({len(df_e1)})", expanded=True):
        if not df_e1.empty:
            cols_e1 = ["id_cliente", "estado", "nombre_corto", "telefono", "resumen_items", "fecha_seguimiento"]
            event_e1 = st.data_editor(
                df_e1[cols_e1], 
                key="ed_e1", 
                column_config={
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS, width="medium"),
                    "id_cliente": None, 
                    "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                    "resumen_items": st.column_config.TextColumn("Interés reciente", disabled=True)
                },
                hide_index=True, use_container_width=True
            )
            if st.button("💾 Guardar Cambios (Conversación)"):
                cambios = df_e1.loc[event_e1.index].copy()
                cambios['estado'] = event_e1['estado']
                guardar_edicion_rapida(cambios, "E1")
        else:
            st.info("No hay clientes en esta etapa.")

    # >>> ETAPA 2: LISTO PARA ENVÍO (MOTO / AGENCIA) <<<
    st.markdown("---")
    st.subheader(f"📦 Listos para Despachar ({len(df_e2)})")
    st.caption("Edita aquí GPS, Referencias y Observaciones para limpiar tus datos.")
    
    if not df_e2.empty:
        # CORRECCIÓN: Usando 'observacion' en singular
        cols_e2 = ["id_cliente", "id_direccion", "estado", "nombre_corto", "distrito", "direccion_texto", "referencia", "gps_link", "observacion", "resumen_items"]
        
        event_e2 = st.data_editor(
            df_e2[cols_e2], 
            key="ed_e2", 
            column_config={
                "estado": st.column_config.SelectboxColumn("Estado (Mover a En Camino)", options=TODOS_LOS_ESTADOS, width="medium"),
                "id_cliente": None,
                "id_direccion": None,
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True, width="medium"),
                "distrito": st.column_config.TextColumn("Distrito", disabled=True, width="small"),
                "direccion_texto": st.column_config.TextColumn("Dirección", width="medium"),
                
                "gps_link": st.column_config.TextColumn("📍 Link GPS", width="medium"),
                "referencia": st.column_config.TextColumn("🏠 Referencia", width="medium"),
                "observacion": st.column_config.TextColumn("🧹 Obs (Limpiar)", width="medium"), # <-- CORREGIDO
                
                "resumen_items": st.column_config.TextColumn("Pedido", disabled=True)
            },
            hide_index=True, use_container_width=True
        )
        
        if st.button("💾 Guardar Despachos (GPS/Ref Actualizados)"):
            cambios_e2 = df_e2.loc[event_e2.index].copy()
            # Capturar los cambios usando el nombre correcto
            cambios_e2['estado'] = event_e2['estado']
            cambios_e2['gps_link'] = event_e2['gps_link']
            cambios_e2['referencia'] = event_e2['referencia']
            cambios_e2['observacion'] = event_e2['observacion'] # <-- CORREGIDO
            cambios_e2['direccion_texto'] = event_e2['direccion_texto']
            
            guardar_edicion_rapida(cambios_e2, "E2")
    else:
        st.info("Bandeja de despachos vacía.")

    # >>> ETAPA 3: EN CAMINO <<<
    st.markdown("---")
    st.subheader(f"🚀 En Camino / Ruta ({len(df_e3)})")
    
    if not df_e3.empty:
        # CORRECCIÓN: Usando 'observacion' en singular
        cols_e3 = ["id_cliente", "id_direccion", "estado", "nombre_corto", "distrito", "direccion_texto", "gps_link", "observacion"]
        
        event_e3 = st.data_editor(
            df_e3[cols_e3], 
            key="ed_e3", 
            column_config={
                "estado": st.column_config.SelectboxColumn("Estado (Finalizar)", options=TODOS_LOS_ESTADOS),
                "id_cliente": None, "id_direccion": None,
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "gps_link": st.column_config.TextColumn("📍 GPS", width="small"),
                "observacion": st.column_config.TextColumn("Notas Entrega") # <-- CORREGIDO
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Guardar Rutas"):
            cambios_e3 = df_e3.loc[event_e3.index].copy()
            cambios_e3['estado'] = event_e3['estado']
            cambios_e3['gps_link'] = event_e3['gps_link']
            cambios_e3['observacion'] = event_e3['observacion'] # <-- CORREGIDO
            guardar_edicion_rapida(cambios_e3, "E3")
    else:
        st.caption("Ningún pedido en ruta actualmente.")

    # >>> ETAPA 4: POST-VENTA <<<
    with st.expander(f"✨ Post-Venta ({len(df_e4)})"):
        if not df_e4.empty:
            cols_e4 = ["id_cliente", "estado", "nombre_corto", "fecha_seguimiento"]
            event_e4 = st.data_editor(
                df_e4[cols_e4], 
                key="ed_e4", 
                column_config={
                    "estado": st.column_config.SelectboxColumn("Estado", options=TODOS_LOS_ESTADOS),
                    "id_cliente": None
                },
                hide_index=True, use_container_width=True
            )
            if st.button("💾 Guardar Post-Venta"):
                cambios_e4 = df_e4.loc[event_e4.index].copy()
                cambios_e4['estado'] = event_e4['estado']
                guardar_edicion_rapida(cambios_e4, "E4")