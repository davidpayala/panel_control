import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
import json

def vista_productos():
    st.title("📦 Gestión de Productos")
    
    # Creamos dos pestañas principales
    tab_stock, tab_grupos = st.tabs(["📊 Inventario (Stock)", "📁 Grupos de Marketing"])

# --- PESTAÑA 1: INVENTARIO ---
    with tab_stock:
        st.subheader("🔎 Gestión de Inventario e Importación")

        # PASO A: Traer los grupos para el selector
        with engine.connect() as conn:
            res_grupos = conn.execute(text("SELECT id_grupo, nombre_grupo FROM Grupos_Productos ORDER BY nombre_grupo")).fetchall()
            # Creamos un diccionario para mapear Nombre -> ID y viceversa
            mapa_grupos = {row.nombre_grupo: row.id_grupo for row in res_grupos}
            opciones_grupos = ["Sin Grupo"] + list(mapa_grupos.keys())

        tab_gestion, tab_importar = st.tabs(["📊 Gestión de Inventario", "📦 Importar Stock Externo"])

        # ==============================================================================
        # PESTAÑA 1A: GESTIÓN DE INVENTARIO
        # ==============================================================================
        with tab_gestion:
            col_search, col_btn = st.columns([4, 1])
            with col_search:
                filtro_inv = st.text_input("🔍 Buscar:", placeholder="Escribe SKU, Marca, Modelo o Ubicación...")
            with col_btn:
                st.write("") 
                if st.button("🔄 Recargar Tabla"):
                    if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
                    st.rerun()

            if 'df_inventario' not in st.session_state:
                with engine.connect() as conn:
                    q_inv = """
                        SELECT 
                            v.sku, v.id_producto, p.categoria, p.marca, p.modelo, p.nombre,
                            p.color_principal, p.diametro, v.medida, v.stock_interno,
                            v.stock_externo, v.stock_transito, v.ubicacion, p.importacion,
                            p.url_compra, p.url_imagen,
                            g.nombre_grupo as grupo_mkt -- Traemos el nombre del grupo
                        FROM Variantes v
                        JOIN Productos p ON v.id_producto = p.id_producto
                        LEFT JOIN Grupos_Productos g ON v.id_grupo = g.id_grupo -- Unión con grupos
                        ORDER BY p.marca, p.modelo, v.sku ASC
                    """
                    st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

            df_calc = st.session_state.df_inventario.copy()
            df_calc['grupo_mkt'] = df_calc['grupo_mkt'].fillna("Sin Grupo")
            df_calc['nombre_completo'] = (
                df_calc['marca'].fillna('') + " " + 
                df_calc['modelo'].fillna('') + " - " + 
                df_calc['nombre'].fillna('') + " (" +
                df_calc['sku'].fillna('') + ")"
            ).str.strip()

            def formatear_detalles(row):
                partes = []
                if row['color_principal']: partes.append(str(row['color_principal']))
                if row['diametro']: partes.append(f"Dia:{row['diametro']}")
                if row['medida']: partes.append(f"Med:{row['medida']}")
                return " | ".join(partes)

            df_calc['detalles_info'] = df_calc.apply(formatear_detalles, axis=1)

            if filtro_inv:
                f = filtro_inv.lower()
                df_calc = df_calc[
                    df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
                    df_calc['sku'].str.lower().str.contains(f, na=False) |
                    df_calc['ubicacion'].str.lower().str.contains(f, na=False) |
                    df_calc['importacion'].str.lower().str.contains(f, na=False)
                ]

            df_final = df_calc[[
                'url_imagen', 'sku', 'id_producto', 'categoria', 'grupo_mkt', 'nombre_completo', 
                'detalles_info', 'stock_interno', 'stock_externo', 'stock_transito',
                'ubicacion', 'importacion', 'url_compra'
            ]]

            st.caption("📝 Editables: **En Tránsito**, **Ubicación**, **Importación** y **URL**.")

            cambios_inv = st.data_editor(
                df_final,
                key="editor_inventario_v3",
                column_config={
                    "url_imagen": st.column_config.ImageColumn("Foto 📸", width="small", help="Clic para ver en grande"),
                    "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
                    "id_producto": None, 
                    "categoria": st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                    "grupo_mkt": st.column_config.SelectboxColumn(
                        "📁 Grupo Marketing", 
                        options=opciones_grupos, # Aquí salen tus grupos creados
                        width="medium"
                    ),
                    "nombre_completo": st.column_config.TextColumn("Producto", disabled=True, width="large"),
                    "detalles_info": st.column_config.TextColumn("Detalles", disabled=True, width="medium"),
                    "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
                    "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
                    "stock_transito": st.column_config.NumberColumn("En Camino 🚚", help="Stock pedido al proveedor", min_value=0, step=1, format="%d", width="small"),
                    "ubicacion": st.column_config.TextColumn("Ubicación 📍", width="small"),
                    "importacion": st.column_config.SelectboxColumn("Importar De ✈️", width="small", options=["Aliexpress", "Alibaba", "Proveedor Nacional", "Otro"], required=False),
                    "url_compra": st.column_config.LinkColumn("Link Compra 🔗", width="medium", display_text="Ver Enlace", validate="^https://.*", required=False)
                },
                hide_index=True,
                width='stretch',
                num_rows="fixed" 
            )

            edited_rows = st.session_state["editor_inventario_v3"].get("edited_rows")

            if edited_rows:
                st.info(f"💾 Tienes cambios pendientes en {len(edited_rows)} filas...")
                if st.button("Confirmar Cambios"):
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            count_ubi, count_prod, count_transito = 0, 0, 0 
                            
                            for idx, updates in edited_rows.items():
                                row_original = df_final.iloc[idx]
                                sku_target = row_original['sku']
                                id_prod_target = int(row_original['id_producto']) 
                                
                                if 'ubicacion' in updates:
                                    conn.execute(text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"), {"u": updates['ubicacion'], "s": sku_target})
                                    count_ubi += 1

                                if 'stock_transito' in updates:
                                    conn.execute(text("UPDATE Variantes SET stock_transito = :st WHERE sku = :s"), {"st": updates['stock_transito'], "s": sku_target})
                                    count_transito += 1
                                
                                if 'importacion' in updates or 'url_compra' in updates:
                                    nuevo_imp = updates.get('importacion', row_original['importacion'])
                                    nueva_url = updates.get('url_compra', row_original['url_compra'])
                                    conn.execute(text("UPDATE Productos SET importacion = :imp, url_compra = :url WHERE id_producto = :idp"), 
                                                 {"imp": nuevo_imp, "url": nueva_url, "idp": id_prod_target})
                                    count_prod += 1
                                # Lógica para vincular el grupo
                                if 'grupo_mkt' in updates:
                                    nombre_sel = updates['grupo_mkt']
                                    # Si elige 'Sin Grupo', ponemos NULL, si no, buscamos su ID
                                    id_g_db = mapa_grupos.get(nombre_sel) if nombre_sel != "Sin Grupo" else None
                                    
                                    conn.execute(text("UPDATE Variantes SET id_grupo = :idg WHERE sku = :s"), 
                                                 {"idg": id_g_db, "s": sku_target})
                            
                            trans.commit()
                            st.success(f"✅ Guardado: {count_ubi} Ubicaciones, {count_transito} Stocks y {count_prod} Importaciones.")
                            del st.session_state['df_inventario'] 
                            time.sleep(1.5)
                            st.rerun()
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error al guardar: {e}")

        # ==============================================================================
        # PESTAÑA 1B: IMPORTAR STOCK EXTERNO
        # ==============================================================================
        with tab_importar:
            st.markdown("### 📥 Actualización Masiva de Stock de Proveedor")
            st.info("Sube un archivo CSV con exactamente dos columnas: `sku` y `stock`.")
            archivo_csv = st.file_uploader("Selecciona el archivo CSV", type=["csv"])

            if archivo_csv:
                try:
                    df_proveedor = pd.read_csv(archivo_csv)
                    df_proveedor.columns = df_proveedor.columns.str.strip().str.lower()

                    if 'sku' not in df_proveedor.columns or 'stock' not in df_proveedor.columns:
                        st.error("❌ El archivo CSV debe contener exactamente las columnas 'sku' y 'stock'.")
                    else:
                        st.dataframe(df_proveedor.head(), use_container_width=True)

                        if st.button("🚀 Procesar y Actualizar Stock Externo", type="primary"):
                            with engine.connect() as conn:
                                trans = conn.begin()
                                try:
                                    contador = 0
                                    for _, row in df_proveedor.iterrows():
                                        sku_csv = str(row['sku']).strip()
                                        try:
                                            stock_csv = int(row['stock'])
                                        except ValueError:
                                            continue 

                                        conn.execute(text("""
                                            UPDATE Variantes SET stock_externo = :stk WHERE sku = :sku
                                        """), {"stk": stock_csv, "sku": sku_csv})
                                        contador += 1
                                    
                                    trans.commit()
                                    st.balloons()
                                    st.success(f"✅ Se ha actualizado el stock de {contador} variantes.")
                                    if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
                                    time.sleep(2)
                                    st.rerun()
                                    
                                except Exception as e:
                                    trans.rollback()
                                    st.error(f"❌ Error en la base de datos: {e}")
                except Exception as e:
                    st.error(f"❌ Error al leer el CSV: {e}")

    # --- PESTAÑA 2: GRUPOS DE MARKETING ---
    with tab_grupos:
        st.subheader("Configuración de Grupos para Mensajes")
        
        # 1. Formulario para crear nuevos grupos
        with st.expander("➕ Crear Nuevo Grupo de Productos", expanded=True):
            with st.form("nuevo_grupo"):
                nombre = st.text_input("Nombre del Grupo (Ej: FL002 - Sharingan)*")
                tipo = st.selectbox("Tipo de Lente", ["Natural", "Fantasía", "Accesorios"])
                marcas = st.multiselect("Marcas asociadas", ["FreshLady", "Meetone", "UYAAI"]) # TODO: Cargar desde BD
                modelo = st.text_input("Modelo base")
                contorno = st.checkbox("¿Tiene contorno?")
                enlace = st.text_input("Enlace de WordPress")
                descripcion = st.text_area("Texto para el mensaje de WhatsApp")
                imagenes = st.text_area("Enlaces de imágenes (uno por línea o separados por coma)")
                
                guardar_btn = st.form_submit_button("Guardar Grupo")

                if guardar_btn:
                    if not nombre.strip():
                        st.warning("⚠️ El nombre del grupo es obligatorio.")
                    else:
                        try:
                            # Preparamos los datos JSON
                            marcas_json = json.dumps(marcas)
                            # Limpiamos las imágenes por si pones enters o espacios
                            lista_imagenes = [img.strip() for img in imagenes.replace('\n', ',').split(',') if img.strip()]
                            imagenes_json = json.dumps(lista_imagenes)

                            # Guardado en Base de Datos
                            with engine.connect() as conn:
                                trans = conn.begin()
                                try:
                                    query_insert = text("""
                                        INSERT INTO Grupos_Productos 
                                        (nombre_grupo, tipo, marca, modelo, tiene_contorno, enlace_tienda, descripcion, imagenes)
                                        VALUES (:nom, :tip, CAST(:mar AS JSONB), :mod, :con, :enl, :des, CAST(:img AS JSONB))
                                    """)
                                    
                                    conn.execute(query_insert, {
                                        "nom": nombre, "tip": tipo, "mar": marcas_json, "mod": modelo,
                                        "con": contorno, "enl": enlace, "des": descripcion, "img": imagenes_json
                                    })
                                    trans.commit()
                                    st.success(f"✅ ¡Grupo '{nombre}' creado correctamente!")
                                    time.sleep(1.5)
                                    st.rerun()
                                except Exception as e:
                                    trans.rollback()
                                    st.error(f"❌ Error SQL: {e}")
                        except Exception as e:
                            st.error(f"❌ Error al procesar datos: {e}")

# 2. Visualización y Edición de Grupos existentes
        st.write("---")
        st.write("### 📂 Grupos Registrados")
        
        with engine.connect() as conn:
            query_grupos = text("SELECT * FROM Grupos_Productos ORDER BY id_grupo DESC")
            df_grupos = pd.read_sql(query_grupos, conn)

        if not df_grupos.empty:
            # Editor de datos
            editado_grupos = st.data_editor(
                df_grupos,
                key="editor_grupos_mkt_v2",
                column_config={
                    "id_grupo": st.column_config.TextColumn("ID", disabled=True),
                    "nombre_grupo": "Nombre del Grupo",
                    "tipo": st.column_config.SelectboxColumn("Tipo", options=["Natural", "Fantasía", "Accesorios"]),
                    "marca": "Marcas (JSON)",
                    "modelo": "Modelo",
                    "tiene_contorno": "Contorno",
                    "enlace_tienda": st.column_config.LinkColumn("Tienda 🔗"),
                    "descripcion": st.column_config.TextColumn("Descripción", width="large"),
                    "imagenes": "Imágenes (JSON)",
                    "fecha_creacion": None 
                },
                hide_index=True,
                use_container_width=True
            )
            
            # --- LÓGICA PARA GUARDAR CAMBIOS ---
            cambios_mkt = st.session_state["editor_grupos_mkt_v2"].get("edited_rows")
            
            if cambios_mkt:
                st.info(f"💾 Tienes cambios pendientes en {len(cambios_mkt)} grupos...")
                if st.button("Confirmar Cambios en Grupos", type="primary"):
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            for idx, updates in cambios_mkt.items():
                                id_target = int(df_grupos.iloc[idx]['id_grupo'])
                                
                                # Construimos el UPDATE dinámicamente según lo que cambió
                                for col, val in updates.items():
                                    if col in ['marca', 'imagenes']:
                                        # Para los campos JSON usamos CAST
                                        conn.execute(text(f"UPDATE Grupos_Productos SET {col} = CAST(:v AS JSONB) WHERE id_grupo = :id"), 
                                                     {"v": val, "id": id_target})
                                    else:
                                        # Para campos normales (texto, boolean)
                                        conn.execute(text(f"UPDATE Grupos_Productos SET {col} = :v WHERE id_grupo = :id"), 
                                                     {"v": val, "id": id_target})
                            
                            trans.commit()
                            st.success("✅ ¡Cambios guardados correctamente!")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            trans.rollback()
                            st.error(f"❌ Error al actualizar: {e}")

            # --- LÓGICA PARA BORRAR ---
            st.write("")
            with st.expander("🗑️ Zona de Peligro (Eliminar)"):
                col_del, col_espacio = st.columns([2, 3])
                with col_del:
                    id_borrar = st.number_input("ID del grupo a eliminar", min_value=0, step=1, value=0)
                    if st.button("🗑️ Eliminar permanentemente"):
                        if id_borrar > 0:
                            with engine.connect() as conn:
                                trans = conn.begin()
                                conn.execute(text("DELETE FROM Grupos_Productos WHERE id_grupo = :id"), {"id": id_borrar})
                                trans.commit()
                                st.success(f"Grupo {id_borrar} eliminado.")
                                time.sleep(1)
                                st.rerun()
        else:
            st.info("Aún no tienes grupos creados.")