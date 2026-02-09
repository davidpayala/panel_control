import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia
import time

# ==============================================================================
# FUNCIÓN DE GESTIÓN DE DIRECCIONES (Agregada aquí para solucionar el error)
# ==============================================================================
def render_gestion_direcciones(id_cliente, nombre_cliente):
    st.markdown(f"##### 🏠 Direcciones de: **{nombre_cliente}**")
    
    # 1. Listar Direcciones Existentes
    with engine.connect() as conn:
        direcciones = pd.read_sql(text("""
            SELECT id_direccion, direccion_texto, distrito, referencia, gps_link, activo 
            FROM Direcciones 
            WHERE id_cliente = :id AND activo = TRUE 
            ORDER BY id_direccion DESC
        """), conn, params={"id": id_cliente})

    if not direcciones.empty:
        for _, dir_row in direcciones.iterrows():
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f"📍 **{dir_row['distrito']}** - {dir_row['direccion_texto']}")
                    if dir_row['referencia']:
                        st.caption(f"Referencia: {dir_row['referencia']}")
                    if dir_row['gps_link']:
                        st.markdown(f"[🔗 Ver Mapa]({dir_row['gps_link']})")
                with c2:
                    if st.button("🗑️", key=f"del_dir_{dir_row['id_direccion']}"):
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE Direcciones SET activo=FALSE WHERE id_direccion=:id"), 
                                         {"id": dir_row['id_direccion']})
                        st.toast("Dirección eliminada")
                        time.sleep(0.5)
                        st.rerun()
    else:
        st.info("No hay direcciones registradas.")

    # 2. Formulario Nueva Dirección
    with st.form(key=f"form_dir_{id_cliente}", clear_on_submit=True):
        st.markdown("**➕ Nueva Dirección**")
        c_a, c_b = st.columns(2)
        with c_a:
            new_dist = st.selectbox("Distrito", ["Lima", "Miraflores", "San Isidro", "Surco", "San Borja", "Callao", "Olivos", "SMP", "Otro"], key=f"dist_{id_cliente}")
            new_gps = st.text_input("Link GPS / Google Maps", key=f"gps_{id_cliente}")
        with c_b:
            new_dir = st.text_input("Dirección exacta", key=f"dir_{id_cliente}")
            new_ref = st.text_input("Referencia", key=f"ref_{id_cliente}")
        
        if st.form_submit_button("Guardar Dirección"):
            if new_dir:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO Direcciones (id_cliente, direccion_texto, distrito, referencia, gps_link, activo)
                            VALUES (:idc, :dt, :dis, :ref, :gps, TRUE)
                        """), {
                            "idc": id_cliente, "dt": new_dir, "dis": new_dist, 
                            "ref": new_ref, "gps": new_gps
                        })
                    st.success("Dirección agregada.")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("La dirección es obligatoria.")

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
                tags_nuevos = st.multiselect("🏷️ Etiquetas", ["VIP", "Mayorista", "Recurrente", "Nuevo", "Problemático"]) 
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
                        # Lógica de creación automática
                        if not nombre_real and not apellido_real:
                            datos_google = buscar_contacto_google(tel_db)
                            if datos_google and datos_google['encontrado']:
                                gid = datos_google['google_id']
                                nombre_real = datos_google['nombre']
                                apellido_real = datos_google['apellido']
                        
                        # Si aún no tenemos ID pero tenemos nombres, creamos en Google
                        if not gid and nombre_real:
                            gid = crear_en_google(nombre_real, apellido_real, tel_db)

                        if not nombre_corto: nombre_corto = f"{nombre_real} {apellido_real}".strip() or "Cliente Nuevo"
                        nombre_ia_calc = generar_nombre_ia(nombre_corto, nombre_real)
                        tags_str = ",".join(tags_nuevos)

                        try:
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

    # --- 2. BUSCADOR Y EDITOR MASIVO ---
    st.subheader("🔍 Buscar y Editar")
    
    col_search, _ = st.columns([3, 1])
    busqueda = col_search.text_input("Buscar:", placeholder="Nombre, teléfono o ETIQUETA...")
    
    if busqueda:
        # Limpieza de input para búsqueda numérica inteligente
        busqueda_limpia = "".join(filter(str.isdigit, busqueda))
        term_telefono = f"%{busqueda_limpia}%" if busqueda_limpia else f"%{busqueda}%"
        term_general = f"%{busqueda}%"

        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT id_cliente, nombre_corto, nombre_ia, estado, nombre, apellido, telefono, etiquetas, google_id 
                FROM Clientes 
                WHERE (
                    nombre_corto ILIKE :b_gen 
                    OR nombre ILIKE :b_gen 
                    OR etiquetas ILIKE :b_gen 
                    OR nombre_ia ILIKE :b_gen
                    OR telefono ILIKE :b_tel
                ) 
                AND activo = TRUE 
                ORDER BY nombre_corto ASC LIMIT 50
            """), conn, params={"b_gen": term_general, "b_tel": term_telefono})

        if not df.empty:
            # --- EDITOR MASIVO (Solo campos rápidos) ---
            st.caption("Edición rápida (Alias, Estado, Etiquetas):")
            df['etiquetas_list'] = df['etiquetas'].apply(lambda x: x.split(',') if x else [])

            edited_df = st.data_editor(
                df, key="editor_clientes_main",
                column_config={
                    "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "google_id": None, "etiquetas": None, "nombre": None, "apellido": None, 
                    "nombre_corto": st.column_config.TextColumn("Alias Original", required=True, width="medium"),
                    "nombre_ia": st.column_config.TextColumn("🤖 Nombre IA", required=False),
                    "etiquetas_list": st.column_config.ListColumn("🏷️ Etiquetas", width="medium"), 
                    "estado": st.column_config.SelectboxColumn("Estado", options=["Sin empezar", "Interesado en venta", "Venta cerrada", "Post-venta", "Proveedor nacional"], required=True),
                    "telefono": st.column_config.TextColumn("Teléfono", disabled=True)
                },
                hide_index=True, use_container_width=True
            )

            if st.button("💾 Guardar Cambios Masivos", type="primary"):
                try:
                    with engine.begin() as conn:
                        for _, row in edited_df.iterrows():
                            tags_final = ",".join(row['etiquetas_list']) if isinstance(row['etiquetas_list'], list) else ""
                            conn.execute(text("""
                                UPDATE Clientes 
                                SET nombre_corto=:nc, nombre_ia=:nia, estado=:e, etiquetas=:tag 
                                WHERE id_cliente=:id
                            """), {
                                "nc": row['nombre_corto'], "nia": row['nombre_ia'], 
                                "e": row['estado'], "tag": tags_final, "id": row['id_cliente']
                            })
                    st.success("Datos guardados.")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

            # --- GESTIÓN INDIVIDUAL (DIRECCIONES Y GOOGLE) ---
            st.divider()
            st.markdown("#### 👤 Gestión Individual")
            
            # Selector de cliente
            opciones_clientes = df.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1).tolist()
            cliente_seleccionado = st.selectbox("Selecciona cliente a gestionar:", opciones_clientes)
            
            if cliente_seleccionado:
                row_sel = df[df.apply(lambda x: f"{x['nombre_corto']} ({x['telefono']})", axis=1) == cliente_seleccionado].iloc[0]
                id_cli_sel = int(row_sel['id_cliente'])
                tel_cli_sel = row_sel['telefono']
                
                # Pestañas para organizar mejor
                tab_dir, tab_datos = st.tabs(["🏠 Direcciones", "👤 Datos Personales / Google"])
                
                with tab_dir:
                    # Llamamos a la función definida al inicio del archivo
                    render_gestion_direcciones(id_cli_sel, row_sel['nombre_corto'])

                with tab_datos:
                    st.markdown(f"**Gestión de Datos Personales para:** {row_sel['nombre_corto']}")
                    
                    # Verificamos estado actual
                    gid_actual = row_sel['google_id']
                    nombre_actual = row_sel['nombre']
                    apellido_actual = row_sel['apellido']

                    c_info, c_acc = st.columns([2, 1])
                    
                    with c_info:
                        st.text_input("Nombre (Google)", value=nombre_actual if nombre_actual else "", disabled=True)
                        st.text_input("Apellido (Google)", value=apellido_actual if apellido_actual else "", disabled=True)
                        st.text_input("Google ID", value=gid_actual if gid_actual else "No vinculado", disabled=True)
                    
                    with c_acc:
                        st.write("Acciones:")
                        if gid_actual:
                            st.success("✅ Vinculado con Google")
                            if st.button("🔄 Refrescar desde Google"):
                                datos = buscar_contacto_google(tel_cli_sel)
                                if datos and datos['encontrado']:
                                    with engine.begin() as conn:
                                        conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a WHERE id_cliente=:id"),
                                                     {"n": datos['nombre'], "a": datos['apellido'], "id": id_cli_sel})
                                    st.toast("Datos refrescados")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.warning("No se encontró al refrescar.")
                        else:
                            st.warning("⚠️ No vinculado")
                            
                            # 1. BUSCAR EN GOOGLE
                            if st.button("🔍 Buscar en Google Contacts"):
                                with st.spinner("Buscando..."):
                                    datos = buscar_contacto_google(tel_cli_sel)
                                    if datos and datos['encontrado']:
                                        # ENCONTRADO -> Actualizamos
                                        with engine.begin() as conn:
                                            conn.execute(text("""
                                                UPDATE Clientes 
                                                SET google_id=:gid, nombre=:n, apellido=:a 
                                                WHERE id_cliente=:id
                                            """), {"gid": datos['google_id'], "n": datos['nombre'], "a": datos['apellido'], "id": id_cli_sel})
                                        st.success("✅ ¡Encontrado y vinculado!")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error("❌ No encontrado en Google Contacts.")
                                        st.session_state['crear_google_mode'] = True

                            # 2. CREAR EN GOOGLE (Si no se encuentra)
                            if st.session_state.get('crear_google_mode', False):
                                st.markdown("---")
                                st.caption("Crear nuevo contacto en Google:")
                                new_nom = st.text_input("Nuevo Nombre", key="new_n_g")
                                new_ape = st.text_input("Nuevo Apellido", key="new_a_g")
                                
                                if st.button("💾 Crear en Google y Vincular"):
                                    if new_nom:
                                        with st.spinner("Creando en Google..."):
                                            nuevo_gid = crear_en_google(new_nom, new_ape, tel_cli_sel)
                                            if nuevo_gid:
                                                with engine.begin() as conn:
                                                    conn.execute(text("""
                                                        UPDATE Clientes 
                                                        SET google_id=:gid, nombre=:n, apellido=:a 
                                                        WHERE id_cliente=:id
                                                    """), {"gid": nuevo_gid, "n": new_nom, "a": new_ape, "id": id_cli_sel})
                                                st.success("✅ Creado y vinculado.")
                                                st.session_state['crear_google_mode'] = False
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.error("Error al crear en Google API.")
                                    else:
                                        st.warning("El nombre es obligatorio.")

        else:
            st.info("No se encontraron clientes.")