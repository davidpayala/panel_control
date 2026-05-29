import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

def render_opciones():
    # Creamos pestañas para mantener todo ordenado en la sección de Opciones
    tab1, tab2 = st.tabs(["📋 Estados de Clientes", "👥 Usuarios"])
    
    # -------------------------------------------------------------------------
    # PESTAÑA 1: GESTIÓN DE ETAPAS / ESTADOS
    # -------------------------------------------------------------------------
    with tab1:
        st.subheader("Gestión de Etapas y Estados")
        st.info("Aquí puedes editar los grupos y subgrupos. También puedes agregar nuevas filas al final de la tabla para crear nuevos estados.")
        
        # Cargar los datos actuales
        with engine.connect() as conn:
            df_etapas = pd.read_sql("""
                SELECT id_etapa, grupo, subgrupo, activo 
                FROM EtapasCliente 
                ORDER BY grupo, id_etapa
            """, conn)
            
        # Mostrar el editor de datos (permite editar, agregar y borrar filas)
        edited_df = st.data_editor(
            df_etapas,
            column_config={
                "id_etapa": st.column_config.NumberColumn("ID", disabled=True),
                "grupo": st.column_config.TextColumn("Grupo (Ej: Etapa 1)", required=True),
                "subgrupo": st.column_config.TextColumn("Subgrupo (Estado)", required=True),
                "activo": st.column_config.CheckboxColumn("Activo")
            },
            num_rows="dynamic",
            key="editor_etapas",
            use_container_width=True
        )
        
        if st.button("💾 Guardar Cambios de Etapas", type="primary"):
            try:
                with engine.begin() as conn:
                    # En lugar de comprobar uno por uno, una forma limpia es iterar el DataFrame actual
                    for index, row in edited_df.iterrows():
                        # Si es una fila nueva, no tiene id_etapa (será None o NaN)
                        if pd.isna(row['id_etapa']):
                            conn.execute(text("""
                                INSERT INTO EtapasCliente (grupo, subgrupo, activo) 
                                VALUES (:g, :s, :a)
                            """), {"g": row['grupo'], "s": row['subgrupo'], "a": row['activo']})
                        else:
                            # Si ya tiene ID, actualizamos sus valores
                            conn.execute(text("""
                                UPDATE EtapasCliente 
                                SET grupo = :g, subgrupo = :s, activo = :a 
                                WHERE id_etapa = :id
                            """), {
                                "g": row['grupo'], 
                                "s": row['subgrupo'], 
                                "a": row['activo'], 
                                "id": row['id_etapa']
                            })
                            
                st.success("✅ Estados y etapas guardados correctamente en la Base de Datos.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Error al guardar los estados: {e}")

    # -------------------------------------------------------------------------
    # PESTAÑA 2: USUARIOS
    # -------------------------------------------------------------------------
    with tab2:
        st.subheader("Gestión de Usuarios del Sistema")
        st.warning("El módulo de usuarios ha sido trasladado aquí. (Añade aquí tu código de administración de cuentas).")
        # Aquí puedes pegar o llamar a tu formulario de alta/baja de usuarios