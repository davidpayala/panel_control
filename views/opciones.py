import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine

# =========================================================================
# AUTO-CREACIÓN DE TABLA MAESTRA DE SUBCATEGORÍAS AL INICIAR
# =========================================================================
try:
    with engine.begin() as conn_init:
        conn_init.execute(text("""
            CREATE TABLE IF NOT EXISTS Subcategorias_Sistema (
                id SERIAL PRIMARY KEY,
                macro_categoria VARCHAR(50) NOT NULL,
                subcategoria VARCHAR(100) NOT NULL,
                UNIQUE(macro_categoria, subcategoria)
            )
        """))
        # Sembrar subcategorías base por defecto si está vacía
        conn_init.execute(text("""
            INSERT INTO Subcategorias_Sistema (macro_categoria, subcategoria) VALUES 
            ('Lentes', 'Estilo Natural'),
            ('Lentes', 'Estilo Fantasía'),
            ('Lentes', 'Accesorios'),
            ('Pelucas', 'Peluca Natural'),
            ('Pelucas', 'Peluca Fantasía'),
            ('Pelucas', 'Accesorios Pelucas')
            ON CONFLICT DO NOTHING
        """))
except Exception:
    pass


def render_opciones():
    tab1, tab2, tab3 = st.tabs(["📋 Estados de Clientes", "📁 Jerarquía de Categorías", "👥 Usuarios"])

    # =========================================================================
    # PESTAÑA 1: GESTIÓN DE ETAPAS / ESTADOS
    # =========================================================================
    with tab1:
        st.subheader("Gestión de Etapas y Estados")
        st.info("Aquí puedes editar los grupos y subgrupos. También puedes agregar nuevas filas al final de la tabla para crear nuevos estados.")
        
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("""
                SELECT id_etapa, grupo, subgrupo, activo 
                FROM EtapasCliente 
                ORDER BY grupo, id_etapa
            """), conn)
            
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
                    for index, row in edited_df.iterrows():
                        if pd.isna(row['id_etapa']):
                            conn.execute(text("""
                                INSERT INTO EtapasCliente (grupo, subgrupo, activo) 
                                VALUES (:g, :s, :a)
                            """), {"g": row['grupo'], "s": row['subgrupo'], "a": row['activo']})
                        else:
                            conn.execute(text("""
                                UPDATE EtapasCliente 
                                SET grupo = :g, subgrupo = :s, activo = :a 
                                WHERE id_etapa = :id
                            """), {
                                "g": row['grupo'], 
                                "s": row['subgrupo'], 
                                "a": row['activo'], 
                                "id": int(row['id_etapa'])
                            })
                            
                st.success("✅ Estados y etapas guardados correctamente en la Base de Datos.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Error al guardar los estados: {e}")

    # =========================================================================
    # PESTAÑA 2: DEPURACIÓN Y GESTIÓN DE CATEGORÍAS
    # =========================================================================
    with tab2:
        st.subheader("🛠️ Depuración de Categorías y Estructura")
        
        with engine.connect() as conn:
            erroneos_df = pd.read_sql(
                text("""
                    SELECT id_producto, marca, modelo, nombre, categoria 
                    FROM Productos 
                    WHERE categoria ILIKE :buscar OR categoria IS NULL
                """), 
                conn, 
                params={"buscar": "%contacto%"}
            )
            
        if not erroneos_df.empty:
            st.warning(f"⚠️ Se han detectado **{len(erroneos_df)} productos** con la categoría obsoleta o sin clasificar.")
            
            with st.container(border=True):
                st.markdown("**Reclasificación Rápida de Producto:**")
                opciones_prod = {row['id_producto']: f"[{row['marca']} {row['modelo']}] - {row['nombre']} ({row['categoria']})" for _, row in erroneos_df.iterrows()}
                id_prod_sel = st.selectbox("Selecciona el producto a corregir:", options=list(opciones_prod.keys()), format_func=lambda x: opciones_prod[x])
                
                with engine.connect() as conn_sub:
                    subs_lentes = [r[0] for r in conn_sub.execute(text("SELECT subcategoria FROM Subcategorias_Sistema WHERE macro_categoria = 'Lentes' ORDER BY subcategoria")).fetchall()]
                if not subs_lentes: subs_lentes = ["Estilo Natural", "Estilo Fantasía", "Accesorios"]

                nueva_subcat = st.selectbox("Asignar a Subcategoría Correcta:", subs_lentes)
                
                if st.button("🔄 Corregir Categoría de Producto", type="primary"):
                    with engine.begin() as conn_tx:
                        conn_tx.execute(text("""
                            UPDATE Productos 
                            SET macro_categoria = 'Lentes', categoria = :nueva 
                            WHERE id_producto = :id
                        """), {"nueva": nueva_subcat, "id": int(id_prod_sel)})
                    st.toast(f"✅ Producto corregido a {nueva_subcat} exitosamente.")
                    time.sleep(1)
                    st.rerun()
        else:
            st.success("🎉 ¡Excelente! No quedan productos con etiquetas obsoletas de contacto.")

        st.divider()

        st.markdown("### 📂 Subcategorías Oficiales del Sistema")
        st.info("Las subcategorías registradas aquí alimentan directamente las listas desplegables en la edición de Productos.")
        
        with engine.connect() as conn:
            df_maestras = pd.read_sql(text("SELECT macro_categoria AS \"Línea Mayor\", subcategoria AS \"Subcategoría Registrada\" FROM Subcategorias_Sistema ORDER BY macro_categoria, subcategoria"), conn)
        st.dataframe(df_maestras, use_container_width=True, hide_index=True)
        
        with st.expander("➕ Agregar Nueva Subcategoría al Sistema", expanded=True):
            with st.form("nueva_categoria_form"):
                macro_input = st.selectbox("Línea Mayor (Asociar a):", ["Pelucas", "Lentes", "Accesorios"])
                sub_input = st.text_input("Nombre de la Nueva Subcategoría (Ej: Cosplay Premium, Lace Front):")
                
                if st.form_submit_button("Registrar Subcategoría en BD"):
                    if not sub_input.strip():
                        st.error("El nombre de la subcategoría no puede estar vacío.")
                    else:
                        try:
                            with engine.begin() as conn_tx:
                                conn_tx.execute(text("""
                                    INSERT INTO Subcategorias_Sistema (macro_categoria, subcategoria)
                                    VALUES (:m, :s)
                                    ON CONFLICT DO NOTHING
                                """), {"m": macro_input, "s": sub_input.strip()})
                            st.success(f"✅ ¡Subcategoría '{sub_input.strip()}' agregada permanentemente a '{macro_input}'!")
                            time.sleep(1.2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error SQL: {e}")

        # --- SUB-SECCIÓN C: DESTRUCTOR DE FANTASMAS ---
        st.divider()
        st.markdown("### 👻 Destructor de Registros Fantasma (Vacíos)")
        with engine.connect() as conn:
            fantasmas_df = pd.read_sql(text("""
                SELECT p.id_producto, p.macro_categoria, p.categoria, p.marca, p.modelo, p.nombre, v.sku 
                FROM Productos p
                LEFT JOIN Variantes v ON p.id_producto = v.id_producto
                WHERE TRIM(COALESCE(p.nombre, '')) = '' OR (v.sku IS NOT NULL AND TRIM(v.sku) = '')
            """), conn)
            
        if not fantasmas_df.empty:
            st.error(f"🚨 **¡Alerta de Fantasma!** Se ha detectado {len(fantasmas_df)} registro(s) con datos en blanco:")
            st.dataframe(fantasmas_df, use_container_width=True)
            id_destruir = st.selectbox("Selecciona el ID del registro vacío a eliminar:", fantasmas_df['id_producto'].unique())
            if st.button("💥 Eliminar Registro Fantasma Definitivamente", type="primary"):
                with engine.begin() as tx:
                    tx.execute(text("DELETE FROM Variantes WHERE id_producto = :id"), {"id": int(id_destruir)})
                    tx.execute(text("DELETE FROM Productos WHERE id_producto = :id"), {"id": int(id_destruir)})
                st.success("¡Registro eliminado de la base de datos!")
                time.sleep(1.5)
                st.rerun()
        else:
            st.caption("No se detectaron registros fantasma en la base de datos.")

    # =========================================================================
    # PESTAÑA 3: USUARIOS
    # =========================================================================
    with tab3:
        st.subheader("👥 Gestión de Usuarios del Sistema")
        with engine.connect() as conn:
            df_usuarios = pd.read_sql(text("SELECT id, usuario, rol, modulos FROM Usuarios ORDER BY id"), conn)
            
        st.dataframe(df_usuarios, hide_index=True, use_container_width=True)
        st.info("Para modificar contraseñas o permisos de acceso, utiliza tu cliente SQL o pgAdmin conectado a la base de datos local.")