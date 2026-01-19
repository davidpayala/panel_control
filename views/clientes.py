import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
from utils import (
    buscar_contacto_google, crear_en_google, actualizar_en_google, 
    normalizar_telefono_maestro
)

# ==============================================================================
# SUB-COMPONENTE: GESTIÓN DE DIRECCIONES
# ==============================================================================
def render_gestion_direcciones(id_cliente, nombre_cliente):
    """
    Muestra un panel para agregar, editar y desactivar direcciones de un cliente específico.
    """
    st.markdown(f"### 📍 Direcciones de: {nombre_cliente}")
    
    # 1. FORMULARIO PARA AGREGAR NUEVA
    with st.expander("➕ Agregar Nueva Dirección"):
        with st.form(f"form_add_dir_{id_cliente}"):
            c1, c2, c3 = st.columns(3)
            tipo = c1.selectbox("Tipo de Envío", ["DOMICILIO", "MOTO", "AGENCIA SHALOM", "AGENCIA OLVA", "OTRA AGENCIA"])
            distrito = c2.text_input("Distrito / Ciudad")
            referencia = c3.text_input("Referencia")
            
            dir_texto = st.text_input("Dirección Exacta (Calle/Av + Num) o Nombre de Agencia")
            
            c4, c5, c6 = st.columns(3)
            receptor = c4.text_input("Nombre Receptor (Si es otro)")
            dni_rec = c5.text_input("DNI Receptor")
            tel_rec = c6.text_input("Telf. Receptor")
            
            # Campos específicos de agencia
            agencia_detalles = ""
            if "AGENCIA" in tipo:
                agencia_detalles = st.text_input("Sede / Observación Agencia")

            if st.form_submit_button("Guardar Dirección"):
                if not dir_texto or not distrito:
                    st.error("La dirección y el distrito son obligatorios.")
                else:
                    with engine.connect() as conn:
                        try:
                            conn.execute(text("""
                                INSERT INTO Direcciones (
                                    id_cliente, tipo_envio, direccion_texto, distrito, referencia,
                                    nombre_receptor, dni_receptor, telefono_receptor, 
                                    agencia_nombre, sede_entrega, activo
                                ) VALUES (
                                    :id, :tipo, :dir, :dist, :ref,
                                    :nom, :dni, :tel,
                                    :agencia, :sede, TRUE
                                )
                            """), {
                                "id": id_cliente, "tipo": tipo, "dir": dir_texto, "dist": distrito, "ref": referencia,
                                "nom": receptor, "dni": dni_rec, "tel": tel_rec,
                                "agencia": tipo if "AGENCIA" in tipo else None, 
                                "sede": agencia_detalles
                            })
                            conn.commit()
                            st.success("✅ Dirección agregada.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

    # 2. EDITOR DE DIRECCIONES EXISTENTES
    with engine.connect() as conn:
        # Traemos solo las columnas útiles para editar
        query_dirs = text("""
            SELECT 
                id_direccion, activo, tipo_envio, direccion_texto, distrito, referencia, 
                nombre_receptor, dni_receptor, telefono_receptor, sede_entrega
            FROM Direcciones 
            WHERE id_cliente = :id
            ORDER BY id_direccion DESC
        """)
        df_dirs = pd.read_sql(query_dirs, conn, params={"id": id_cliente})

    if not df_dirs.empty:
        st.info("💡 Desmarca la casilla 'activo' para eliminar una dirección.")
        
        cambios_dir = st.data_editor(
            df_dirs,
            key=f"editor_dirs_{id_cliente}",
            column_config={
                "id_direccion": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "activo": st.column_config.CheckboxColumn("Activo?", help="Desmarca para borrar"),
                "tipo_envio": st.column_config.SelectboxColumn("Tipo", options=["DOMICILIO", "MOTO", "AGENCIA SHALOM", "AGENCIA OLVA", "OTRA AGENCIA"], required=True),
                "direccion_texto": st.column_config.TextColumn("Dirección", required=True, width="large"),
                "distrito": st.column_config.TextColumn("Distrito", required=True),
                "sede_entrega": st.column_config.TextColumn("Sede/Obs Agencia"),
            },
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Actualizar Direcciones", key=f"btn_upd_dir_{id_cliente}"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for idx, row in cambios_dir.iterrows():
                        conn.execute(text("""
                            UPDATE Direcciones 
                            SET activo=:act, tipo_envio=:tipo, direccion_texto=:dir, distrito=:dist, 
                                referencia=:ref, nombre_receptor=:nom, dni_receptor=:dni, 
                                telefono_receptor=:tel, sede_entrega=:sede
                            WHERE id_direccion=:id
                        """), {
                            "act": row['activo'], "tipo": row['tipo_envio'], "dir": row['direccion_texto'],
                            "dist": row['distrito'], "ref": row['referencia'], "nom": row['nombre_receptor'],
                            "dni": row['dni_receptor'], "tel": row['telefono_receptor'], 
                            "sede": row['sede_entrega'], "id": row['id_direccion']
                        })
                    trans.commit()
                    st.success("✅ Direcciones actualizadas.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al actualizar direcciones: {e}")
    else:
        st.caption("Este cliente no tiene direcciones registradas aún.")


# ==============================================================================
# VISTA PRINCIPAL
# ==============================================================================
def render_clientes():
    st.subheader("👥 Gestión de Clientes")

    # --- 1. CREAR NUEVO CLIENTE ---
    with st.expander("➕ Nuevo Cliente (Sincronizado)", expanded=False):
        st.info("💡 Si ingresas solo el teléfono, el sistema buscará los datos en Google Contacts.")
        with st.form("form_nuevo_cliente"):
            c1, c2 = st.columns(2)
            with c1:
                telefono_input = st.text_input("📱 Teléfono (Obligatorio)")
                nombre_real = st.text_input("Nombre (Google)")
                apellido_real = st.text_input("Apellido (Google)")
            with c2:
                nombre_corto = st.text_input("📝 Alias / Nombre Corto")
                medio = st.selectbox("Medio", ["WhatsApp", "Instagram", "Facebook", "TikTok", "Recomendado", "Web"])
                estado_ini = st.selectbox("Estado", ["Interesado en venta", "Responder duda", "Proveedor nacional"])
                codigo = st.text_input("Código (DNI/RUC)")

            if st.form_submit_button("💾 Guardar y Sincronizar", type="primary"):
                norm = normalizar_telefono_maestro(telefono_input)
                if not norm:
                    st.error("Número inválido.")
                else:
                    tel_db = norm['db']
                    # Validar duplicado
                    with engine.connect() as conn:
                        exists = conn.execute(text("SELECT COUNT(*) FROM Clientes WHERE telefono=:t"), {"t": tel_db}).scalar()
                    
                    if exists:
                        st.error("El cliente ya existe.")
                    else:
                        # Lógica Google
                        gid = None
                        datos_google = buscar_contacto_google(tel_db)
                        if datos_google and datos_google['encontrado']:
                            gid = datos_google['google_id']
                            if not nombre_real: nombre_real = datos_google['nombre']
                            if not apellido_real: apellido_real = datos_google['apellido']
                        elif nombre_real:
                            gid = crear_en_google(nombre_real, apellido_real, tel_db)

                        if not nombre_corto: 
                            nombre_corto = f"{nombre_real} {apellido_real}".strip() or "Cliente Nuevo"

                        # Insertar
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO Clientes (nombre_corto, nombre, apellido, telefono, medio_contacto, codigo_contacto, estado, google_id, activo, fecha_registro)
                                VALUES (:nc, :n, :a, :t, :m, :c, :e, :g, TRUE, NOW())
                            """), {"nc": nombre_corto, "n": nombre_real, "a": apellido_real, "t": tel_db, "m": medio, "c": codigo, "e": estado_ini, "g": gid})
                            conn.commit()
                        st.success(f"Cliente {nombre_corto} creado.")
                        time.sleep(1)
                        st.rerun()

    st.divider()

    # --- 2. BUSCADOR Y EDICIÓN ---
    st.subheader("🔍 Buscar y Editar")
    
    col_search, _ = st.columns([3, 1])
    busqueda = col_search.text_input("Buscar cliente:", placeholder="Nombre, alias o teléfono...")
    
    # Variable para saber qué cliente expandir direcciones
    selected_client_id = None 
    selected_client_name = None

    if busqueda:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT id_cliente, nombre_corto, estado, nombre, apellido, telefono, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b OR nombre ILIKE :b) AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 10
            """), conn, params={"b": f"%{busqueda}%"})

        if not df.empty:
            # A. EDITOR DE CLIENTES (LOTE)
            st.caption("Edita los datos del cliente aquí:")
            edited_df = st.data_editor(
                df, key="editor_clientes_main",
                column_config={
                    "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "google_id": None,
                    "nombre_corto": st.column_config.TextColumn("Alias", required=True),
                    "estado": st.column_config.SelectboxColumn("Estado", options=["Sin empezar", "Interesado en venta", "Venta cerrada", "Post-venta", "Proveedor nacional"], required=True),
                    "telefono": st.column_config.TextColumn("Teléfono", required=True)
                },
                hide_index=True, use_container_width=True
            )

            if st.button("💾 Guardar Cambios Clientes", type="primary"):
                # ... (Lógica de guardado igual que antes) ...
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        for _, row in edited_df.iterrows():
                            norm = normalizar_telefono_maestro(row['telefono'])
                            if norm:
                                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, telefono=:t, nombre_corto=:nc, estado=:e WHERE id_cliente=:id"),
                                    {"n": row['nombre'], "a": row['apellido'], "t": norm['db'], "nc": row['nombre_corto'], "e": row['estado'], "id": row['id_cliente']})
                                if row['google_id']:
                                    actualizar_en_google(row['google_id'], row['nombre'], row['apellido'], norm['db'])
                        trans.commit()
                        st.success("Datos guardados.")
                        time.sleep(1)
                        st.rerun()
                    except: trans.rollback()

            st.divider()
            
            # B. SELECCIONAR CLIENTE PARA DIRECCIONES
            st.markdown("#### 🚚 Gestionar Direcciones")
            # Creamos un selectbox para elegir a quién editarle las direcciones
            opciones_clientes = df.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID:{x['id_cliente']}", axis=1).tolist()
            cliente_seleccionado = st.selectbox("Selecciona un cliente para ver sus direcciones:", opciones_clientes)
            
            if cliente_seleccionado:
                selected_client_id = int(cliente_seleccionado.split("ID:")[1])
                selected_client_name = cliente_seleccionado.split(" (")[0]
                
                # C. RENDERIZAR GESTOR DE DIRECCIONES
                render_gestion_direcciones(selected_client_id, selected_client_name)

        else:
            st.info("No se encontraron clientes.")

    # ==============================================================================
    # 3. FUSIÓN DE DUPLICADOS (Sin cambios, tu lógica era buena)
    # ==============================================================================
    st.divider()
    st.subheader("🧬 Fusión de Clientes Duplicados")
    
    with st.expander("Abrir herramienta de fusión"):
        col_dup, col_orig = st.columns(2)
        
        # ... (Tu código de fusión se mantiene igual abajo, es funcional) ...
        # Solo asegúrate de copiar la lógica de fusión que ya tenías
        
        # 1. CLIENTE A ELIMINAR
        with col_dup:
            st.markdown("### ❌ A Eliminar")
            search_dup = st.text_input("Buscar duplicado:", key="search_dup")
            id_duplicado = None
            info_duplicado = None
            if search_dup:
                with engine.connect() as conn:
                    res = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_dup}%"})
                if not res.empty:
                    opts_dup = res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID:{x['id_cliente']}", axis=1).tolist()
                    sel_dup = st.selectbox("Sel. Duplicado:", opts_dup)
                    id_duplicado = int(sel_dup.split("ID:")[1])
                    info_duplicado = sel_dup

        # 2. CLIENTE PRINCIPAL
        with col_orig:
            st.markdown("### ✅ Principal")
            search_orig = st.text_input("Buscar principal:", key="search_orig")
            id_original = None
            if search_orig:
                with engine.connect() as conn:
                    res2 = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_orig}%"})
                if not res2.empty:
                    opts_orig = res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']}) - ID:{x['id_cliente']}", axis=1).tolist()
                    sel_orig = st.selectbox("Sel. Principal:", opts_orig)
                    id_original = int(sel_orig.split("ID:")[1])

        # 3. ACCIÓN
        if id_duplicado and id_original:
            if id_duplicado == id_original:
                st.error("Son el mismo cliente.")
            elif st.button("🚀 FUSIONAR"):
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        # Recuperar teléfono viejo
                        old_tel = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente=:id"), {"id": id_duplicado}).scalar()
                        # Mover Ventas y Direcciones
                        conn.execute(text("UPDATE Ventas SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        conn.execute(text("UPDATE Direcciones SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        # Mover Mensajes (NUEVO: Importante no perder chats)
                        conn.execute(text("UPDATE mensajes SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                        # Guardar teléfono viejo en secundario
                        conn.execute(text("UPDATE Clientes SET telefono_secundario=:tel WHERE id_cliente=:id AND (telefono_secundario IS NULL OR telefono_secundario='')"), {"tel":old_tel, "id":id_original})
                        # Desactivar
                        conn.execute(text("UPDATE Clientes SET activo=FALSE, nombre_corto=nombre_corto||' (FUSIONADO)' WHERE id_cliente=:id"), {"id":id_duplicado})
                        trans.commit()
                        st.success("Fusión completada.")
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error: {e}")