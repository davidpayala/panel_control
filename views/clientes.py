import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia
import time

# Lista de respaldo en caso de que la tabla esté vacía o falle la conexión
ESTADOS_CLIENTE_FALLBACK = [
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
    with st.expander("🔄 Fusionar Clientes Duplicados", expanded=False):
        st.info("💡 **¿Cómo funciona?** Esta herramienta une dos registros independientes. El número de teléfono del cliente que elimines se guardará **automáticamente como un teléfono adicional / secundario** del cliente que decidas conservar.")
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT c.id_cliente, c.nombre_corto, c.whatsapp_internal_id,
                           (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE LIMIT 1) as tel_prin
                    FROM clientes c WHERE c.activo=TRUE ORDER BY c.nombre_corto
                """), conn)
                
                if not df.empty:
                    opciones = df.apply(lambda x: f"{x['nombre_corto']} | {x['tel_prin']} (ID: {x['id_cliente']})", axis=1).tolist()
                    mapa_ids = dict(zip(opciones, df['id_cliente']))
                    mapa_wids = dict(zip(opciones, df['whatsapp_internal_id']))

                    c1, c2 = st.columns(2)
                    sel_keep = st.selectbox("✅ Cliente a CONSERVAR (Destino)", opciones, key="fusion_keep")
                    sel_del = st.selectbox("❌ Cliente a ELIMINAR (Origen con teléfono secundario)", opciones, key="fusion_del")

                    if sel_keep and sel_del:
                        id_keep = mapa_ids[sel_keep]
                        id_del = mapa_ids[sel_del]
                        wid_keep = mapa_wids[sel_keep]
                        wid_del = mapa_wids[sel_del]

                        if id_keep == id_del:
                            st.error("Debes seleccionar dos clientes diferentes.")
                        else:
                            st.warning(f"⚠️ Al fusionar, **{sel_del}** se desactivará y su número pasará a ser un **teléfono adicional** de **{sel_keep}**.")
                            if st.button("🚀 Confirmar Fusión"):
                                with st.spinner("Fusionando..."):
                                    try:
                                        with engine.begin() as tx:
                                            tx.execute(text("UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE ventas SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE direcciones SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            
                                            if wid_del and not wid_keep:
                                                tx.execute(text("UPDATE clientes SET whatsapp_internal_id=:wid WHERE id_cliente=:id"), {"wid": wid_del, "id": id_keep})
                                            
                                            tx.execute(text("UPDATE clientes SET activo=FALSE WHERE id_cliente = :id"), {"id": id_del})
                                        st.success("¡Fusión completada con éxito!")
                                        time.sleep(1)
                                        st.rerun()
                                    except Exception as e: st.error(f"Error: {e}")
        except Exception as e: st.error(f"Error cargando herramienta: {e}")

# ==============================================================================
# RENDERIZADO PRINCIPAL
# ==============================================================================
def render_clientes():
    # --- CARGA DINÁMICA DE ESTADOS DESDE LA BASE DE DATOS ---
    try:
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("SELECT id_etapa, subgrupo FROM EtapasCliente WHERE activo = TRUE ORDER BY grupo, id_etapa"), conn)
        if not df_etapas.empty:
            estados_opciones = df_etapas['subgrupo'].tolist()
            mapa_subgrupo_id = dict(zip(df_etapas['subgrupo'], df_etapas['id_etapa']))
        else:
            estados_opciones = ESTADOS_CLIENTE_FALLBACK
            mapa_subgrupo_id = {}
    except Exception as e:
        estados_opciones = ESTADOS_CLIENTE_FALLBACK
        mapa_subgrupo_id = {}

    st.title("👤 Gestión de Clientes")
    render_herramienta_fusion()

    # --- CREAR NUEVO CLIENTE ---
    with st.expander("➕ Registrar Nuevo Cliente", expanded=False):
        with st.form("form_nuevo_cliente"):
            c1, c2, c3 = st.columns([2, 2, 2])
            nuevo_tel = c1.text_input("Teléfono Principal (Obligatorio)")
            nuevo_alias = c2.text_input("Alias / Nombre Corto")
            nuevo_estado = c3.selectbox("Estado Inicial", options=estados_opciones, index=0)

            nuevas_etiquetas = st.text_input("Etiquetas (Separadas por coma)")
            vincular_google = st.checkbox("🔍 Intentar vincular con Google Contactos", value=True)

            if st.form_submit_button("💾 Crear Cliente", type="primary"):
                norm = normalizar_telefono_maestro(nuevo_tel)
                if not norm:
                    st.error("Número de teléfono inválido.")
                else:
                    tel_db = norm['db']
                    with engine.connect() as conn:
                        existe = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono=:t AND activo=TRUE"), {"t": tel_db}).fetchone()

                    if existe:
                        st.warning(f"El teléfono {tel_db} ya pertenece al cliente ID {existe[0]}.")
                    else:
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

                        id_etapa_val = mapa_subgrupo_id.get(nuevo_estado)

                        try:
                            with engine.begin() as conn:
                                res = conn.execute(text("""
                                    INSERT INTO clientes (nombre_corto, estado, id_etapa, etiquetas, google_id, nombre, apellido, nombre_ia, telefono, activo, fecha_registro)
                                    VALUES (:nc, :e, :id_etapa, :et, :gid, :n, :a, :nia, :t, TRUE, NOW())
                                    RETURNING id_cliente
                                """), {"nc": nuevo_alias, "e": nuevo_estado, "id_etapa": id_etapa_val, "et": nuevas_etiquetas, "gid": g_id, "n": g_nom, "a": g_ape, "nia": nombre_ia, "t": tel_db})
                                nuevo_id = res.fetchone()[0]

                                conn.execute(text("""
                                    INSERT INTO telefonoscliente (id_cliente, telefono, es_principal)
                                    VALUES (:id, :t, TRUE)
                                """), {"id": nuevo_id, "t": tel_db})

                            st.success(f"✅ Cliente registrado con ID {nuevo_id}.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al insertar: {e}")

    st.divider()

    # --- BUSCADOR Y EDITOR ---
    st.subheader("🔍 Buscador y Editor Masivo")
    busqueda = st.text_input("Buscar cliente...", placeholder="Nombre, Teléfono o Etiquetas")

    busqueda_limpia = "".join(filter(str.isdigit, busqueda))
    term_tel = f"%{busqueda_limpia}%" if busqueda_limpia else f"%{busqueda}%"
    term_gen = f"%{busqueda}%"

    query = """
        SELECT c.id_cliente, c.nombre_corto, c.estado, c.nombre, c.apellido, c.etiquetas, c.google_id, c.whatsapp_internal_id,
               (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE LIMIT 1) as tel_principal,
               (SELECT STRING_AGG(telefono, ' | ') FROM telefonoscliente WHERE id_cliente = c.id_cliente AND activo = TRUE) as todos_telefonos
        FROM clientes c
        WHERE c.activo = TRUE
    """
    params = {}
    if busqueda:
        query += """ AND (
            c.nombre_corto ILIKE :g OR c.nombre ILIKE :g OR c.etiquetas ILIKE :g
            OR EXISTS (SELECT 1 FROM telefonoscliente t WHERE t.id_cliente = c.id_cliente AND t.telefono ILIKE :t AND t.activo = TRUE)
        )"""
        params = {"g": term_gen, "t": term_tel}
    query += " ORDER BY c.id_cliente DESC LIMIT 50"

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
                "nombre_corto": st.column_config.TextColumn("Alias", width="medium"),
                "estado": st.column_config.SelectboxColumn("Estado", options=estados_opciones, width="medium"),
                "tel_principal": st.column_config.TextColumn("Telf. Principal", disabled=True),
                "todos_telefonos": st.column_config.TextColumn("Todos los Teléfonos", disabled=True, width="large"),
                "nombre": None, "apellido": None, "google_id": None, "whatsapp_internal_id": None, "etiquetas": None
            },
            hide_index=True, use_container_width=True
        )

        if st.button("💾 Guardar Cambios Rápidos"):
            with engine.begin() as conn:
                for idx, row in edited_df.iterrows():
                    id_etapa_val = mapa_subgrupo_id.get(row['estado'])
                    conn.execute(text("UPDATE clientes SET nombre_corto=:nc, estado=:est, id_etapa=:id_etapa WHERE id_cliente=:id"),
                                 {"nc": row['nombre_corto'], "est": row['estado'], "id_etapa": id_etapa_val, "id": row['id_cliente']})
            st.success("Cambios guardados.")
            time.sleep(1)
            st.rerun()

        # --- GESTIÓN INDIVIDUAL ---
        filas_sel = edited_df[edited_df["Seleccionar"] == True]
        if not filas_sel.empty:
            row_full = df.loc[filas_sel.index[0]]
            id_cli_sel = int(row_full['id_cliente'])

            st.divider()
            st.subheader(f"⚙️ Gestión Individual: {row_full['nombre_corto']}")

            tab_datos, tab_tel, tab_dir = st.tabs(["👤 Datos Personales", "📞 Teléfonos", "🏠 Direcciones"])

            with tab_datos:
                with st.form(f"form_cli_{id_cli_sel}"):
                    c1, c2 = st.columns(2)
                    new_nombre = c1.text_input("Alias Original", value=row_full['nombre_corto'] or "")
                    curr_est = row_full['estado']
                    new_estado = c2.selectbox("Estado", options=estados_opciones, index=estados_opciones.index(curr_est) if curr_est in estados_opciones else 0)

                    st.caption("Google Contacts (Bloqueado, usa sincronización)")
                    c3, c4 = st.columns(2)
                    c3.text_input("Nombre Real", value=row_full['nombre'] or "", disabled=True)
                    c4.text_input("Apellido", value=row_full['apellido'] or "", disabled=True)
                    new_etiquetas = st.text_area("Etiquetas / Notas", value=row_full['etiquetas'] or "")

                    if st.form_submit_button("💾 Guardar Datos"):
                        id_etapa_val = mapa_subgrupo_id.get(new_estado)
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE clientes SET nombre_corto=:nc, etiquetas=:e, estado=:est, id_etapa=:id_etapa WHERE id_cliente=:id
                            """), {"nc": new_nombre, "e": new_etiquetas, "est": new_estado, "id_etapa": id_etapa_val, "id": id_cli_sel})
                        st.success("Guardado.")
                        time.sleep(1)
                        st.rerun()

                if st.button("🔍 Sincronizar Google (Usa Tel. Principal)"):
                    if row_full['tel_principal']:
                        res = buscar_contacto_google(row_full['tel_principal'])
                        if res and res.get('encontrado'):
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE clientes SET nombre=:n, apellido=:a, google_id=:gid WHERE id_cliente=:id"),
                                            {"n": res['nombre'], "a": res['apellido'], "gid": res['google_id'], "id": id_cli_sel})
                            st.success("Sincronizado con éxito.")
                            time.sleep(1)
                            st.rerun()
                        else: st.error("No encontrado en Google.")
                    else: st.warning("No tiene un teléfono asignado para buscar.")

            with tab_tel:
                st.markdown("##### Números Asociados")
                with engine.connect() as conn:
                    tels = pd.read_sql(text("SELECT * FROM telefonoscliente WHERE id_cliente=:id AND activo=TRUE ORDER BY es_principal DESC"), conn, params={"id": id_cli_sel})

                for _, t_row in tels.iterrows():
                    col_t1, col_t2, col_t3 = st.columns([3, 2, 1])
                    es_prin = "⭐ Principal" if t_row['es_principal'] else "Secundario"
                    col_t1.markdown(f"**{t_row['telefono']}** ({es_prin})")

                    if not t_row['es_principal']:
                        if col_t2.button("Hacer Principal", key=f"p_{t_row['id_telefono']}"):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE telefonoscliente SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                tx.execute(text("UPDATE telefonoscliente SET es_principal=TRUE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                                tx.execute(text("UPDATE clientes SET telefono=:t WHERE id_cliente=:id"), {"t": t_row['telefono'], "id": id_cli_sel})
                            st.rerun()
                    
                    if len(tels) > 1:
                        if col_t3.button("🗑️", key=f"d_{t_row['id_telefono']}"):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                            st.rerun()

                with st.form(f"add_tel_{id_cli_sel}", clear_on_submit=True):
                    st.write("➕ Agregar Número")
                    new_tel = st.text_input("Teléfono (Ej: +51 999...)")
                    if st.form_submit_button("Añadir"):
                        norm_t = normalizar_telefono_maestro(new_tel)
                        if norm_t:
                            with engine.connect() as conn:
                                ex = conn.execute(text("""
                                    SELECT c.id_cliente, c.nombre_corto 
                                    FROM telefonoscliente t
                                    JOIN clientes c ON t.id_cliente = c.id_cliente
                                    WHERE t.telefono = :t AND t.activo = TRUE AND c.activo = TRUE
                                """), {"t": norm_t['db']}).fetchone()
                                
                                if ex: 
                                    st.error(f"⚠️ Este número ya está asignado al cliente: **{ex.nombre_corto}** (ID: {ex.id_cliente}). Para unirlo a este perfil como teléfono adicional, cierra este panel y usa la sección 'Fusionar Clientes Duplicados' arriba.")
                                else:
                                    with engine.begin() as tx:
                                        tx.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono) VALUES (:id, :t)"), {"id": id_cli_sel, "t": norm_t['db']})
                                    st.success("Teléfono añadido exitosamente.")
                                    time.sleep(1)
                                    st.rerun()
                        else: 
                            st.error("Formato de número inválido.")

            with tab_dir:
                st.markdown("#### Lista de Direcciones")
                with engine.connect() as conn:
                    dirs = pd.read_sql(text("SELECT * FROM direcciones WHERE id_cliente=:id AND activo=TRUE ORDER BY id_direccion DESC"), conn, params={"id": id_cli_sel})

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
                            e_ref = st.text_input("Referencia", r_dir.get('referencia', ''))
                            if st.form_submit_button("💾 Guardar Dirección"):
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        UPDATE direcciones SET nombre_receptor=:n, telefono_receptor=:t, distrito=:dis, direccion_texto=:dt, referencia=:r
                                        WHERE id_direccion=:id
                                    """), {"n":e_nom, "t":e_tel, "dis":e_dist, "dt":e_dir, "r":e_ref, "id": int(r_dir['id_direccion'])})
                                st.success("Dirección actualizada.")
                                time.sleep(1)
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
                                        INSERT INTO direcciones (id_cliente, nombre_receptor, telefono_receptor, distrito, direccion_texto, referencia, activo)
                                        VALUES (:idc, :n, :t, :dis, :dt, :r, TRUE)
                                    """), {"idc": id_cli_sel, "n":nn_nom, "t":nn_tel, "dis":nn_dist, "dt":nn_dir, "r":nn_ref})
                                st.success("Dirección creada.")
                                time.sleep(1)
                                st.rerun()
    else:
        st.info("No se encontraron clientes.")