import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
from utils import (
    buscar_contacto_google, crear_en_google, actualizar_en_google, 
    normalizar_telefono_maestro, generar_nombre_ia
)

OPCIONES_TAGS = [
    "🚫 SPAM", "⚠️ Problemático", "💎 VIP / Recurrente",
    "✅ Compró", "👀 Prospecto", "❓ Preguntón",
    "📉 Pide Rebaja", "📦 Mayorista", "📦 Proveedor" 
]

# ==============================================================================
# SUB-COMPONENTE: GESTIÓN DE DIRECCIONES
# ==============================================================================
def render_gestion_direcciones(id_cliente, nombre_cliente):
    st.markdown(f"### 📍 Direcciones de: {nombre_cliente}")
    
    # 1. AGREGAR NUEVA
    with st.expander("➕ Agregar Nueva Dirección"):
        with st.form(f"form_add_dir_{id_cliente}"):
            c1, c2, c3 = st.columns(3)
            tipo = c1.selectbox("Tipo de Envío", ["DOMICILIO", "MOTO", "AGENCIA SHALOM", "AGENCIA OLVA", "OTRA AGENCIA"])
            distrito = c2.text_input("Distrito / Ciudad")
            referencia = c3.text_input("Referencia")
            dir_texto = st.text_input("Dirección Exacta o Nombre de Agencia")
            
            c4, c5, c6 = st.columns(3)
            receptor = c4.text_input("Receptor (Opcional)")
            dni_rec = c5.text_input("DNI Receptor")
            tel_rec = c6.text_input("Telf. Receptor")
            
            agencia_detalles = ""
            if "AGENCIA" in tipo:
                agencia_detalles = st.text_input("Sede / Observación Agencia")

            if st.form_submit_button("Guardar Dirección"):
                if not dir_texto or not distrito:
                    st.error("Dirección y distrito obligatorios.")
                else:
                    try:
                        # CORRECCIÓN: Usamos engine.begin() para evitar conflictos de transacción
                        with engine.begin() as conn:
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
                        st.success("✅ Dirección agregada.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # 2. EDITAR EXISTENTES
    # Lectura simple (no necesita transacción)
    with engine.connect() as conn:
        df_dirs = pd.read_sql(text("SELECT id_direccion, activo, tipo_envio, direccion_texto, distrito, referencia, nombre_receptor, dni_receptor, telefono_receptor, sede_entrega FROM Direcciones WHERE id_cliente = :id ORDER BY id_direccion DESC"), conn, params={"id": id_cliente})

    if not df_dirs.empty:
        cambios_dir = st.data_editor(
            df_dirs,
            key=f"editor_dirs_{id_cliente}",
            column_config={
                "id_direccion": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "activo": st.column_config.CheckboxColumn("Activo?", help="Desmarca para borrar"),
                "tipo_envio": st.column_config.SelectboxColumn("Tipo", options=["DOMICILIO", "MOTO", "AGENCIA SHALOM", "AGENCIA OLVA", "OTRA AGENCIA"], required=True),
                "direccion_texto": st.column_config.TextColumn("Dirección", required=True, width="large"),
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Actualizar Direcciones", key=f"btn_upd_dir_{id_cliente}"):
            try:
                # CORRECCIÓN: Transacción segura
                with engine.begin() as conn:
                    for idx, row in cambios_dir.iterrows():
                        conn.execute(text("""
                            UPDATE Direcciones SET activo=:act, tipo_envio=:tipo, direccion_texto=:dir, distrito=:dist, referencia=:ref, nombre_receptor=:nom, dni_receptor=:dni, telefono_receptor=:tel, sede_entrega=:sede WHERE id_direccion=:id
                        """), {
                            "act": row['activo'], "tipo": row['tipo_envio'], "dir": row['direccion_texto'],
                            "dist": row['distrito'], "ref": row['referencia'], "nom": row['nombre_receptor'],
                            "dni": row['dni_receptor'], "tel": row['telefono_receptor'], "sede": row['sede_entrega'], "id": row['id_direccion']
                        })
                st.success("Actualizado.")
                time.sleep(0.5)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
    else:
        st.caption("Sin direcciones.")

# ==============================================================================
# VISTA PRINCIPAL
# ==============================================================================
def render_clientes():
    st.subheader("👥 Gestión de Clientes")

    # --- 1. CREAR NUEVO CLIENTE ---
    with st.expander("➕ Nuevo Cliente", expanded=False):
        with st.form("form_nuevo_cliente"):
            c1, c2 = st.columns(2)
            with c1:
                telefono_input = st.text_input("📱 Teléfono (Obligatorio)")
                nombre_real = st.text_input("Nombre (Google)")
                apellido_real = st.text_input("Apellido (Google)")
            with c2:
                nombre_corto = st.text_input("📝 Alias / Nombre Corto")
                tags_nuevos = st.multiselect("🏷️ Etiquetas", OPCIONES_TAGS)
                estado_ini = st.selectbox("Estado", ["Interesado en venta", "Responder duda", "Proveedor nacional"])

            if st.form_submit_button("💾 Guardar y Sincronizar", type="primary"):
                norm = normalizar_telefono_maestro(telefono_input)
                if not norm:
                    st.error("Número inválido.")
                else:
                    tel_db = norm['db']
                    with engine.connect() as conn:
                        exists = conn.execute(text("SELECT COUNT(*) FROM Clientes WHERE telefono=:t"), {"t": tel_db}).scalar()
                    
                    if exists:
                        st.error("Cliente ya existe.")
                    else:
                        gid = None
                        datos_google = buscar_contacto_google(tel_db)
                        if datos_google and datos_google['encontrado']:
                            gid = datos_google['google_id']
                            if not nombre_real: nombre_real = datos_google['nombre']
                            if not apellido_real: apellido_real = datos_google['apellido']
                        elif nombre_real:
                            gid = crear_en_google(nombre_real, apellido_real, tel_db)

                        if not nombre_corto: nombre_corto = f"{nombre_real} {apellido_real}".strip() or "Cliente Nuevo"
                        
                        # CALCULO NOMBRE IA
                        nombre_ia_calc = generar_nombre_ia(nombre_corto, nombre_real)
                        tags_str = ",".join(tags_nuevos)

                        try:
                            # CORRECCIÓN: Transacción segura
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO Clientes (nombre_corto, nombre, apellido, telefono, etiquetas, estado, google_id, nombre_ia, activo, fecha_registro)
                                    VALUES (:nc, :n, :a, :t, :tag, :e, :g, :nia, TRUE, NOW())
                                """), {"nc": nombre_corto, "n": nombre_real, "a": apellido_real, "t": tel_db, "tag": tags_str, "e": estado_ini, "g": gid, "nia": nombre_ia_calc})
                            st.success(f"Cliente {nombre_corto} creado.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al guardar: {e}")

    st.divider()

    # --- 2. HERRAMIENTAS Y BUSCADOR ---
    st.subheader("🔍 Buscar y Editar")
    
    with st.expander("⚡ Herramientas Masivas"):
        if st.button("🪄 Generar Nombres IA para TODOS (Backfill)"):
            with st.spinner("Analizando nombres..."):
                # PASO 1: LEER (Solo lectura)
                with engine.connect() as conn:
                    clientes = pd.read_sql(text("SELECT id_cliente, nombre_corto, nombre FROM Clientes WHERE activo=TRUE"), conn)
                
                # PASO 2: ESCRIBIR (Transacción segura)
                count = 0
                try:
                    with engine.begin() as conn:
                        for _, row in clientes.iterrows():
                            nuevo_ia = generar_nombre_ia(row['nombre_corto'], row['nombre'])
                            conn.execute(text("UPDATE Clientes SET nombre_ia = :nia WHERE id_cliente = :id"), 
                                         {"nia": nuevo_ia, "id": row['id_cliente']})
                            count += 1
                    st.success(f"✅ Procesados {count} clientes. Nombres IA actualizados.")
                except Exception as e:
                    st.error(f"Error masivo: {e}")

    # BUSCADOR
    col_search, _ = st.columns([3, 1])
    busqueda = col_search.text_input("Buscar:", placeholder="Nombre, teléfono o ETIQUETA...")
    
    if busqueda:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT id_cliente, nombre_corto, nombre_ia, estado, nombre, apellido, telefono, etiquetas, google_id 
                FROM Clientes 
                WHERE (nombre_corto ILIKE :b OR telefono ILIKE :b OR nombre ILIKE :b OR etiquetas ILIKE :b OR nombre_ia ILIKE :b) 
                AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 50
            """), conn, params={"b": f"%{busqueda}%"})

        if not df.empty:
            st.caption("Edita 'Nombre IA' si el algoritmo se equivocó:")
            
            df['etiquetas_list'] = df['etiquetas'].apply(lambda x: x.split(',') if x else [])

            edited_df = st.data_editor(
                df, key="editor_clientes_main",
                column_config={
                    "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "google_id": None, "etiquetas": None,
                    "nombre_corto": st.column_config.TextColumn("Alias Original", disabled=True),
                    "nombre_ia": st.column_config.TextColumn("🤖 Nombre IA", required=False),
                    "etiquetas_list": st.column_config.ListColumn("🏷️ Etiquetas", width="medium"), 
                    "estado": st.column_config.SelectboxColumn("Estado", options=["Sin empezar", "Interesado en venta", "Venta cerrada", "Post-venta", "Proveedor nacional"], required=True),
                    "telefono": st.column_config.TextColumn("Teléfono", disabled=True)
                },
                hide_index=True, use_container_width=True
            )

            if st.button("💾 Guardar Cambios Masivos", type="primary"):
                try:
                    # CORRECCIÓN: Transacción segura
                    with engine.begin() as conn:
                        for _, row in edited_df.iterrows():
                            tags_final = ",".join(row['etiquetas_list']) if isinstance(row['etiquetas_list'], list) else ""
                            
                            conn.execute(text("""
                                UPDATE Clientes SET nombre_ia=:nia, estado=:e, etiquetas=:tag 
                                WHERE id_cliente=:id
                            """), {"nia": row['nombre_ia'], "e": row['estado'], "tag": tags_final, "id": row['id_cliente']})
                    
                    st.success("Datos guardados.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

            # SELECCIÓN PARA DIRECCIONES
            st.divider()
            st.markdown("#### 🚚 Gestionar Direcciones")
            opciones_clientes = df.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1).tolist()
            cliente_seleccionado = st.selectbox("Selecciona cliente:", opciones_clientes)
            
            if cliente_seleccionado:
                row_sel = df[df.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1) == cliente_seleccionado].iloc[0]
                render_gestion_direcciones(int(row_sel['id_cliente']), row_sel['nombre_corto'])

        else:
            st.info("No se encontraron clientes.")
    
    # 3. FUSIÓN DE DUPLICADOS (FIXED CHATS)
    st.divider()
    st.subheader("🧬 Fusión de Clientes Duplicados")
    
    with st.expander("Abrir herramienta de fusión"):
        col_dup, col_orig = st.columns(2)
        
        with col_dup:
            st.markdown("### ❌ A Eliminar")
            search_dup = st.text_input("Buscar duplicado:", key="search_dup")
            id_duplicado = None
            if search_dup:
                with engine.connect() as conn:
                    res = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_dup}%"})
                if not res.empty:
                    opts_dup = res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1).tolist()
                    sel_dup = st.selectbox("Sel. Duplicado:", opts_dup)
                    if sel_dup:
                        id_duplicado = int(res[res.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1) == sel_dup].iloc[0]['id_cliente'])

        with col_orig:
            st.markdown("### ✅ Principal")
            search_orig = st.text_input("Buscar principal:", key="search_orig")
            id_original = None
            if search_orig:
                with engine.connect() as conn:
                    res2 = pd.read_sql(text("SELECT id_cliente, nombre_corto, telefono FROM Clientes WHERE (nombre_corto ILIKE :s OR telefono ILIKE :s) AND activo=TRUE LIMIT 5"), conn, params={"s":f"%{search_orig}%"})
                if not res2.empty:
                    opts_orig = res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1).tolist()
                    sel_orig = st.selectbox("Sel. Principal:", opts_orig)
                    if sel_orig:
                        id_original = int(res2[res2.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1) == sel_orig].iloc[0]['id_cliente'])

        if id_duplicado and id_original and st.button("🚀 FUSIONAR"):
            if id_duplicado == id_original:
                st.error("Son el mismo cliente.")
            else:
                try:
                    # CORRECCIÓN: Transacción segura
                    with engine.begin() as conn:
                        # Obtener teléfonos
                        dup_data = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente=:id"), {"id": id_duplicado}).fetchone()
                        orig_data = conn.execute(text("SELECT telefono FROM Clientes WHERE id_cliente=:id"), {"id": id_original}).fetchone()
                        
                        if dup_data and orig_data:
                            tel_old = dup_data.telefono
                            tel_new = orig_data.telefono
                            
                            # 1. Mover CHATS (Clave)
                            conn.execute(text("UPDATE mensajes SET telefono=:nt, id_cliente=:ni WHERE telefono=:ot"), {"nt": tel_new, "ni": id_original, "ot": tel_old})
                            
                            # 2. Mover Ventas y Direcciones
                            conn.execute(text("UPDATE Ventas SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                            conn.execute(text("UPDATE Direcciones SET id_cliente=:new WHERE id_cliente=:old"), {"new":id_original, "old":id_duplicado})
                            
                            # 3. Desactivar duplicado
                            conn.execute(text("UPDATE Clientes SET activo=FALSE, nombre_corto=nombre_corto||' (FUSIONADO)' WHERE id_cliente=:old"), {"old": id_duplicado})
                    
                    st.success("✅ Fusión completada.")
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error en fusión: {e}")