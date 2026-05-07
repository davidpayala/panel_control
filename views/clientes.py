import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia
import time

ESTADOS_CLIENTE = [
    "Sin empezar", "Responder duda", "Interesado en venta", 
    "Proveedor nacional", "Proveedor internacional", 
    "Venta motorizado", "Venta agencia", "Venta express moto", 
    "En camino moto", "En camino agencia", "Contraentrega agencia", 
    "Pendiente agradecer", "Problema post"
]

# ==============================================================================
# HERRAMIENTA DE FUSIÓN DE CLIENTES
# ==============================================================================
def render_herramienta_fusion():
    with st.expander("🔄 Fusionar Clientes Duplicados (Herramienta)", expanded=False):
        st.info("Utiliza esto para unir dos registros. Se migrarán chats, ventas y direcciones al cliente destino.")
        
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("SELECT id_cliente, telefono, nombre_corto, whatsapp_internal_id FROM Clientes WHERE activo=TRUE ORDER BY nombre_corto"), conn)
                opciones = df.apply(lambda x: f"{x['nombre_corto']} | {x['telefono']} (ID: {x['id_cliente']})", axis=1).tolist()
                mapa_ids = dict(zip(opciones, df['id_cliente']))
                mapa_tels = dict(zip(opciones, df['telefono']))
                mapa_wids = dict(zip(opciones, df['whatsapp_internal_id']))

                c1, c2 = st.columns(2)
                with c1: sel_keep = st.selectbox("✅ Cliente a CONSERVAR (Destino)", opciones, key="fusion_keep")
                with c2: sel_del = st.selectbox("❌ Cliente a ELIMINAR (Origen)", opciones, key="fusion_del")

                if sel_keep and sel_del:
                    id_keep = mapa_ids[sel_keep]
                    id_del = mapa_ids[sel_del]
                    tel_keep = mapa_tels[sel_keep]
                    tel_del = mapa_tels[sel_del]
                    wid_keep = mapa_wids[sel_keep]
                    wid_del = mapa_wids[sel_del]

                    if id_keep == id_del:
                        st.error("Debes seleccionar dos clientes diferentes.")
                    else:
                        st.warning(f"⚠️ Al fusionar, **{sel_del}** desaparecerá y todos sus datos pasarán a **{sel_keep}**.")
                        if st.button("🚀 Confirmar Fusión"):
                            with st.spinner("Fusionando historiales..."):
                                try:
                                    with engine.begin() as tx:
                                        tx.execute(text("UPDATE mensajes SET telefono = :tel_new WHERE telefono = :tel_old"), {"tel_new": tel_keep, "tel_old": tel_del})
                                        tx.execute(text("UPDATE Ventas SET id_cliente = :id_new WHERE id_cliente = :id_old"), {"id_new": id_keep, "id_old": id_del})
                                        tx.execute(text("UPDATE Direcciones SET id_cliente = :id_new WHERE id_cliente = :id_old"), {"id_new": id_keep, "id_old": id_del})
                                        if wid_del:
                                            tx.execute(text("UPDATE Clientes SET whatsapp_internal_id=:wid WHERE id_cliente=:id"), {"wid": wid_del, "id": id_keep})
                                        tx.execute(text("UPDATE Clientes SET activo=FALSE WHERE id_cliente = :id"), {"id": id_del})
                                    st.success(f"¡Fusión completada!")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e: st.error(f"Error: {e}")
        except: pass

# ==============================================================================
# RENDERIZADO PRINCIPAL
# ==============================================================================
def render_clientes():
    st.title("👤 Gestión de Clientes")
    render_herramienta_fusion()
    
    # --- 1. SECCIÓN: CREAR NUEVO CLIENTE ---
    with st.expander("➕ Registrar Nuevo Cliente", expanded=False):
        with st.form("form_nuevo_cliente"):
            c1, c2, c3 = st.columns([2, 2, 2])
            nuevo_tel = c1.text_input("Teléfono (Obligatorio)")
            nuevo_alias = c2.text_input("Alias / Nombre Corto")
            nuevo_estado = c3.selectbox("Estado Inicial", options=ESTADOS_CLIENTE, index=0)
            
            nuevas_etiquetas = st.text_input("Etiquetas (Separadas por coma)")
            vincular_google = st.checkbox("🔍 Intentar vincular con Google Contactos automáticamente", value=True)
            
            if st.form_submit_button("💾 Crear Cliente", type="primary"):
                # Normalización
                norm = normalizar_telefono_maestro(nuevo_tel)
                if not norm:
                    st.error("Número de teléfono inválido.")
                else:
                    tel_db = norm['db']
                    # Verificar si ya existe
                    with engine.connect() as conn:
                        existe = conn.execute(text("SELECT id_cliente FROM Clientes WHERE telefono=:t AND activo=TRUE"), {"t": tel_db}).fetchone()
                    
                    if existe:
                        st.warning(f"El cliente con teléfono {tel_db} ya existe (ID: {existe[0]}).")
                    else:
                        # Lógica de Google
                        g_id, g_nom, g_ape = None, None, None
                        if vincular_google:
                            res_g = buscar_contacto_google(tel_db)
                            if res_g and res_g.get('encontrado'):
                                g_id = res_g['google_id']
                                g_nom = res_g['nombre']
                                g_ape = res_g['apellido']
                                if not nuevo_alias: nuevo_alias = f"{g_nom} {g_ape}".strip()

                        if not nuevo_alias: nuevo_alias = "Cliente Nuevo"
                        nombre_ia = generar_nombre_ia(nuevo_alias, g_nom or "")

                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO Clientes (nombre_corto, telefono, estado, etiquetas, google_id, nombre, apellido, nombre_ia, activo, fecha_registro)
                                    VALUES (:nc, :t, :e, :et, :gid, :n, :a, :nia, TRUE, NOW())
                                """), {
                                    "nc": nuevo_alias, "t": tel_db, "e": nuevo_estado, 
                                    "et": nuevas_etiquetas, "gid": g_id, "n": g_nom, "a": g_ape, "nia": nombre_ia
                                })
                            st.success(f"✅ Cliente '{nuevo_alias}' registrado correctamente.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al insertar: {e}")

    st.divider()

    # --- 2. BUSCADOR Y EDITOR MASIVO ---
    st.subheader("🔍 Buscador y Editor Masivo")
    busqueda = st.text_input("Buscar cliente...", placeholder="Nombre, Teléfono o Etiquetas")
    
    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
    term_tel = f"%{busqueda_limpia}%" if busqueda_limpia else f"%{busqueda}%"
    term_gen = f"%{busqueda}%"

    query = "SELECT * FROM Clientes WHERE activo = TRUE"
    params = {}
    if busqueda:
        query += " AND (nombre_corto ILIKE :g OR telefono ILIKE :t OR nombre ILIKE :g OR etiquetas ILIKE :g)"
        params = {"g": term_gen, "t": term_tel}
    query += " ORDER BY id_cliente DESC LIMIT 50"

    with engine.connect() as conn: 
        df = pd.read_sql(text(query), conn, params=params)

    if not df.empty:
        df_view = df.copy()
        df_view.insert(0, "Seleccionar", False)
        
        edited_df = st.data_editor(
            df_view,
            key="ed_clientes_main",
            column_config={
                "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "nombre_corto": st.column_config.TextColumn("Alias (Editable)", width="medium"),
                "estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS_CLIENTE, width="medium"),
                "telefono": st.column_config.TextColumn("Teléfono", disabled=True),
                "nombre": st.column_config.TextColumn("Nombre Google", disabled=True),
                "apellido": st.column_config.TextColumn("Apellido Google", disabled=True),
                "etiquetas": st.column_config.TextColumn("Etiquetas", width="medium"),
                "google_id": None, "whatsapp_internal_id": None, "activo": None, "fecha_registro": None, "nombre_ia": None, "fecha_seguimiento": None
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Guardar Cambios Rápidos"):
            with engine.begin() as conn:
                for idx, row in edited_df.iterrows():
                    conn.execute(text("UPDATE Clientes SET nombre_corto=:nc, etiquetas=:e, estado=:est WHERE id_cliente=:id"),
                                 {"nc": row['nombre_corto'], "e": row['etiquetas'], "est": row['estado'], "id": row['id_cliente']})
            st.success("Cambios guardados.")
            time.sleep(1)
            st.rerun()

        # Lógica de Selección Individual
        filas_sel = edited_df[edited_df["Seleccionar"] == True]
        if not filas_sel.empty:
            row_full = df.loc[filas_sel.index[0]]
            id_cli_sel = int(row_full['id_cliente'])
            
            st.divider()
            st.subheader(f"⚙️ Gestión Individual: {row_full['nombre_corto']}")
            
            tab_datos, tab_dir = st.tabs(["👤 Datos Personales / Google", "🏠 Direcciones"])

            with tab_datos:
                with st.form(f"form_cliente_{id_cli_sel}"):
                    c1, c2, c3 = st.columns([2, 2, 2])
                    new_nombre = c1.text_input("Alias Original", value=row_full['nombre_corto'] or "")
                    telefono_actual_db = row_full['telefono']
                    new_telefono = c2.text_input("Teléfono", value=row_full['telefono'] or "")
                    curr_est = row_full['estado']
                    new_estado = c3.selectbox("Estado", options=ESTADOS_CLIENTE, index=ESTADOS_CLIENTE.index(curr_est) if curr_est in ESTADOS_CLIENTE else 0)
                    
                    if row_full['whatsapp_internal_id']:
                        st.info(f"🆔 ID WSP Vinculado: `{row_full['whatsapp_internal_id']}` (No se modificará)")
                    
                    st.caption("Datos de Google (Bloqueados, requiere sincronización)")
                    c4, c5 = st.columns(2)
                    new_nombre_real = c4.text_input("Nombre Real", value=row_full['nombre'] or "", disabled=True)
                    new_apellido = c5.text_input("Apellido", value=row_full['apellido'] or "", disabled=True)
                    new_etiquetas = st.text_area("Etiquetas / Notas", value=row_full['etiquetas'] or "")
                    
                    if st.form_submit_button("💾 Guardar Cambios Individuales"):
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE Clientes 
                                SET nombre_corto=:nc, etiquetas=:e, telefono=:t, estado=:est
                                WHERE id_cliente=:id
                            """), {"nc": new_nombre, "e": new_etiquetas, "t": new_telefono, "est": new_estado, "id": id_cli_sel})
                            
                            if telefono_actual_db != new_telefono:
                                conn.execute(text("UPDATE mensajes SET telefono = :new_tel WHERE telefono = :old_tel"), 
                                             {"new_tel": new_telefono, "old_tel": telefono_actual_db})
                                st.toast(f"Historial migrado a {new_telefono}")
                        st.success("Cambios aplicados.")
                        time.sleep(1)
                        st.rerun()

                # Botones de Sincronización Google
                col_g1, col_g2 = st.columns(2)
                with col_g1:
                    if st.button("🔍 Buscar/Refrescar Google"):
                        res = buscar_contacto_google(row_full['telefono'])
                        if res and res.get('encontrado'):
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE Clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"), 
                                            {"n": res['nombre'], "a": res['apellido'], "gid": res['google_id'], "id": id_cli_sel})
                            st.success("Vinculado con éxito.")
                            st.rerun()
                        else:
                            st.error("No encontrado en Google.")

                with col_g2:
                    if not row_full['google_id']:
                        if st.button("➕ Crear en Google Ahora"):
                            partes = (row_full['nombre_corto'] or "Cliente").split(" ", 1)
                            nom = partes[0]
                            ape = partes[1] if len(partes) > 1 else ""
                            gid = crear_en_google(nom, ape, row_full['telefono'])
                            if gid:
                                with engine.begin() as conn:
                                    conn.execute(text("UPDATE Clientes SET google_id=:gid, nombre=:n, apellido=:a WHERE id_cliente=:id"), 
                                                 {"gid": gid, "n": nom, "a": ape, "id": id_cli_sel})
                                st.success("Creado en Google.")
                                st.rerun()

            with tab_dir:
                # --- GESTIÓN DE DIRECCIONES (Mantenida de tu código anterior) ---
                st.markdown("#### Lista de Direcciones")
                with engine.connect() as conn:
                    dirs = pd.read_sql(text("SELECT * FROM Direcciones WHERE id_cliente=:id AND activo=TRUE ORDER BY id_direccion DESC"), conn, params={"id": id_cli_sel})
                
                if not dirs.empty:
                    dirs_view = dirs.copy()
                    dirs_view.insert(0, "Editar", False)
                    ed_dirs = st.data_editor(
                        dirs_view[["Editar", "id_direccion", "distrito", "direccion_texto", "referencia"]],
                        key="ed_dirs",
                        column_config={"Editar": st.column_config.CheckboxColumn("✏️", width="small"), "id_direccion": None},
                        hide_index=True, use_container_width=True
                    )
                    dir_sel = ed_dirs[ed_dirs["Editar"] == True]
                    if not dir_sel.empty:
                        r_dir = dirs.loc[dir_sel.index[0]]
                        with st.form("form_edit_dir"):
                            d1, d2, d3 = st.columns(3)
                            e_nom = d1.text_input("Recibe", r_dir.get('nombre_receptor', ''))
                            e_tel = d2.text_input("Telf. Receptor", r_dir.get('telefono_receptor', ''))
                            e_dist = d3.text_input("Distrito", r_dir['distrito'])
                            e_dir = st.text_input("Dirección", r_dir['direccion_texto'])
                            e_ref = st.text_input("Referencia", r_dir['referencia'] or "")
                            if st.form_submit_button("💾 Guardar Dirección"):
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        UPDATE Direcciones SET nombre_receptor=:n, telefono_receptor=:t, distrito=:dis, direccion_texto=:dt, referencia=:r
                                        WHERE id_direccion=:id
                                    """), {"n":e_nom, "t":e_tel, "dis":e_dist, "dt":e_dir, "r":e_ref, "id": int(r_dir['id_direccion'])})
                                st.success("Dirección actualizada.")
                                st.rerun()
                
                with st.expander("➕ Agregar Nueva Dirección"):
                    with st.form("form_new_dir"):
                        n1, n2, n3 = st.columns(3)
                        nn_nom = n1.text_input("Recibe")
                        nn_tel = n2.text_input("Telf. Receptor")
                        nn_dist = n3.text_input("Distrito")
                        nn_dir = st.text_input("Dirección Exacta")
                        nn_ref = st.text_input("Referencia")
                        if st.form_submit_button("Crear Dirección"):
                            if nn_dir:
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO Direcciones (id_cliente, nombre_receptor, telefono_receptor, distrito, direccion_texto, referencia, activo)
                                        VALUES (:idc, :n, :t, :dis, :dt, :r, TRUE)
                                    """), {"idc": id_cli_sel, "n":nn_nom, "t":nn_tel, "dis":nn_dist, "dt":nn_dir, "r":nn_ref})
                                st.success("Dirección creada.")
                                st.rerun()
    else:
        st.info("No se encontraron clientes.")