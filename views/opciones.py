import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine

def render_opciones():
    # Creamos ahora 3 pestañas para mantener el control ordenado
    tab1, tab2, tab3 = st.tabs(["📋 Estados de Clientes", "📁 Jerarquía de Categorías", "👥 Usuarios"])

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
    # PESTAÑA 2: DEPURACIÓN Y GESTIÓN DE CATEGORÍAS (NUEVO)
    # -------------------------------------------------------------------------
    with tab2:
        st.subheader("🛠️ Depuración de 'Lentes de Contacto' y Estructura")
        
        # --- SUB-SECCIÓN A: DEPURADOR DE PRODUCTOS ERRÓNEOS ---
        with engine.connect() as conn:
            erroneos_df = pd.read_sql("""
                SELECT id_producto, marca, modelo, nombre, categoria 
                FROM Productos 
                WHERE categoria = 'Lentes de contacto' OR categoria IS NULL
            """, conn)
            
        if not erroneos_df.empty:
            st.warning(f"⚠️ Se han detectado **{len(erroneos_df)} productos** con la categoría obsoleta 'Lentes de contacto' o sin categoría.")
            
            with st.container(border=True):
                st.markdown("**Reclasificación Rápida de Producto:**")
                # Selector del producto a corregir
                opciones_prod = {row['id_producto']: f"[{row['marca']} {row['modelo']}] - {row['nombre']}" for _, row in erroneos_df.iterrows()}
                id_prod_sel = st.selectbox("Selecciona el producto a corregir:", options=list(opciones_prod.keys()), format_func=lambda x: opciones_prod[x])
                
                # Destino limpio
                nueva_subcat = st.selectbox("Asignar a Subcategoría Correcta:", ["Estilo Natural", "Estilo Fantasía", "Accesorios"])
                
                if st.button("🔄 Corregir Categoría de Producto", type="primary"):
                    with engine.begin() as conn_tx:
                        conn_tx.execute(text("""
                            UPDATE Productos 
                            SET macro_categoria = 'Lentes', categoria = :nueva 
                            WHERE id_producto = :id
                        """), {"nueva": nueva_subcat, "id": id_prod_sel})
                    st.toast(f"✅ Producto corregido a {nueva_subcat} exitosamente.")
                    time.sleep(1)
                    st.rerun()
        else:
            st.success("🎉 ¡Excelente! No quedan productos huérfanos con la etiqueta 'Lentes de contacto'.")

        st.divider()

        # --- SUB-SECCIÓN B: EDITOR GLOBAL DE CATEGORÍAS ---
        st.markdown("### 📂 Mantenimiento de Categorías del Sistema")
        st.info("Aquí puedes visualizar cómo están mapeadas las carpetas de tus productos en el Panel.")
        
        with engine.connect() as conn:
            df_cat_resumen = pd.read_sql("""
                SELECT macro_categoria as "Categoría Mayor", categoria as "Subcategoría", COUNT(id_producto) as "Cant. Productos"
                FROM Productos 
                GROUP BY macro_categoria, categoria
                ORDER BY macro_categoria, categoria
            """, conn)
            
        st.dataframe(df_cat_resumen, use_container_width=True, hide_index=True)
        
        # Formulario para añadir categorías futuras para Pelucas
        with st.expander("➕ Preparar Categorías para Línea de Pelucas"):
            st.caption("Nota: Al usar el asistente de carga masiva de Excel, estas configuraciones se indexarán automáticamente en la base de datos.")
            with st.form("nueva_categoria_form"):
                macro_input = st.selectbox("Categoría Mayor (Línea de Negocio):", ["Pelucas", "Lentes", "Accesorios"])
                sub_input = st.text_input("Nueva Subcategoría (Ej: Peluca Natural, Peluca Fantasía):")
                
                if st.form_submit_button("Registrar en Estructura"):
                    if not sub_input.strip():
                        st.error("El nombre de la subcategoría no puede estar vacío.")
                    else:
                        st.info(f"Estructura lista. Cuando importes tus pelucas del Excel, podrás usar la subcategoría '{sub_input}' bajo la línea '{macro_input}'.")

    # -------------------------------------------------------------------------
    # PESTAÑA 3: USUARIOS (Tu código existente)
    # -------------------------------------------------------------------------
    with tab3:
        st.subheader("Gestión de Usuarios del Sistema")
        st.warning("El módulo de usuarios ha sido trasladado aquí. (Añade aquí tu código de administración de cuentas).")
        # Aquí puedes pegar o llamar a tu formulario de alta/baja de usuarios
    
