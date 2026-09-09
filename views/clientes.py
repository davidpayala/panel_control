import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia, actualizar_en_google, obtener_lid_de_waha, get_google_service
import time

ESTADOS_CLIENTE_FALLBACK = [
    "Sin empezar", "Responder duda", "Interesado en venta", 
    "Proveedor nacional", "Proveedor internacional", 
    "Venta motorizado", "Venta agencia", "Venta express moto", 
    "En camino moto", "En camino agencia", "Contraentrega agencia", 
    "Pendiente agradecer", "Problema post"
]

def render_clientes():
    # --- 1. CARGA INICIAL DE ETAPAS ---
    try:
        with engine.connect() as conn:
            df_etapas = pd.read_sql(text("SELECT id_etapa, subgrupo FROM EtapasCliente WHERE activo = TRUE ORDER BY grupo, id_etapa"), conn)
        if not df_etapas.empty:
            estados_opciones = df_etapas['subgrupo'].tolist()
            mapa_subgrupo_id = dict(zip(df_etapas['subgrupo'], df_etapas['id_etapa']))
        else:
            estados_opciones = ESTADOS_CLIENTE_FALLBACK
            mapa_subgrupo_id = {}
    except:
        estados_opciones = ESTADOS_CLIENTE_FALLBACK
        mapa_subgrupo_id = {}

    st.title("👤 Gestión de Clientes y Proveedores")

    # --- 2. CREACIÓN DE PESTAÑAS PRINCIPALES ---
    tab_buscador, tab_gestion = st.tabs([
        "🔍 Buscador de Clientes", 
        "⚙️ Creación y Mantenimiento"
    ])

    # ==============================================================================
    # PESTAÑA 1: BUSCADOR Y GESTIÓN INDIVIDUAL
    # ==============================================================================
    with tab_buscador:
        st.subheader("🔍 Buscador y Editor Masivo")
        
        # Filtros de búsqueda
        col_b1, col_b2, col_b3, col_b4 = st.columns([2, 1.5, 1, 1])
        busqueda = col_b1.text_input("Buscar registro...", placeholder="Nombre, Teléfono, LID o Etiquetas")
        filtro_estados = col_b2.multiselect("Filtrar por Estado", options=estados_opciones)
        filtro_sin_mkt = col_b3.checkbox("🚫 Solo 'Sin Mkt'")
        filtro_bloqueados = col_b4.checkbox("🔒 Incluir Bloqueados")

        busqueda_limpia = "".join(filter(str.isdigit, busqueda))
        term_tel = f"%{busqueda_limpia}%" if busqueda_limpia else f"%{busqueda}%"
        term_gen = f"%{busqueda}%"

        # Consulta centralizada en TelefonosCliente
        query = """
            SELECT c.id_cliente, c.nombre_corto, c.estado, c.excluir_publicidad, c.activo, c.nombre, c.apellido, c.etiquetas, c.google_id, c.nombre_ia,
                   (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1) as tel_principal,
                   (SELECT STRING_AGG(telefono, ' | ') FROM telefonoscliente WHERE id_cliente = c.id_cliente AND activo = TRUE AND telefono IS NOT NULL) as todos_telefonos
            FROM clientes c
            WHERE 1=1
        """
        params = {}
        
        if not filtro_bloqueados:
            query += " AND c.activo = TRUE"

        if busqueda:
            query += """ AND (
                CAST(c.id_cliente AS TEXT) ILIKE :g OR 
                c.nombre_corto ILIKE :g OR c.nombre ILIKE :g OR c.apellido ILIKE :g OR c.etiquetas ILIKE :g OR c.nombre_ia ILIKE :g
                OR EXISTS (SELECT 1 FROM telefonoscliente t WHERE t.id_cliente = c.id_cliente AND (t.telefono ILIKE :t OR t.lid ILIKE :g OR t.alias ILIKE :g) AND t.activo = TRUE)
            )"""
            params["g"] = term_gen
            params["t"] = term_tel

        if filtro_estados:
            query += " AND c.estado IN :estados"
            params["estados"] = tuple(filtro_estados)

        if filtro_sin_mkt:
            query += " AND c.excluir_publicidad = TRUE"

        query += " ORDER BY c.id_cliente DESC LIMIT 50"

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        if not df.empty:
            df_view = df.copy()
            df_view['excluir_publicidad'] = df_view['excluir_publicidad'].fillna(False).astype(bool)
            df_view['activo'] = df_view['activo'].fillna(True).astype(bool)
            df_view.insert(0, "Seleccionar", False)

            edited_df = st.data_editor(
                df_view,
                key="ed_clientes_main",
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn("👉", width="small"),
                    "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "nombre_corto": st.column_config.TextColumn("Alias Original", width="medium"),
                    "nombre_ia": st.column_config.TextColumn("Nombre IA", width="medium"),
                    "estado": st.column_config.SelectboxColumn("Estado", options=estados_opciones, width="medium"),
                    "excluir_publicidad": st.column_config.CheckboxColumn("🚫 Sin Mkt"),
                    "activo": st.column_config.CheckboxColumn("✅ Activo", help="Desmarca para bloquear cliente"),
                    "tel_principal": st.column_config.TextColumn("Telf. Principal", disabled=True),
                    "todos_telefonos": st.column_config.TextColumn("Todos los Teléfonos", disabled=True, width="large"),
                    "nombre": None, "apellido": None, "google_id": None, "etiquetas": None
                },
                hide_index=True, use_container_width=True
            )

            # Guardado rápido desde la tabla
            if st.button("💾 Guardar Cambios Rápidos", type="primary"):
                with engine.begin() as conn:
                    for idx, row in edited_df.iterrows():
                        id_etapa_val = mapa_subgrupo_id.get(row['estado'])
                        nia_val = row['nombre_ia'] if pd.notna(row['nombre_ia']) else ""
                        exc_val = bool(row['excluir_publicidad'])
                        act_val = bool(row['activo'])
                        
                        conn.execute(text("""
                            UPDATE clientes 
                            SET nombre_corto=:nc, nombre_ia=:nia, estado=:est, id_etapa=:id_etapa, excluir_publicidad=:exc, activo=:act
                            WHERE id_cliente=:id
                        """), {"nc": row['nombre_corto'], "nia": nia_val, "est": row['estado'], "id_etapa": id_etapa_val, "exc": exc_val, "act": act_val, "id": row['id_cliente']})
                st.success("Cambios guardados.")
                time.sleep(1)
                st.rerun()

            # Gestión Individual al seleccionar checkbox
            filas_sel = edited_df[edited_df["Seleccionar"] == True]
            if not filas_sel.empty:
                row_full = df.loc[filas_sel.index[0]]
                id_cli_sel = int(row_full['id_cliente'])

                st.divider()
                st.subheader(f"⚙️ Gestión Individual: {row_full['nombre_corto']}")
                tab_datos, tab_tel, tab_dir = st.tabs(["👤 Datos Personales", "📞 Teléfonos", "🏠 Direcciones"])
                
                # --- DATOS PERSONALES ---
                with tab_datos:
                    with st.form(f"form_cli_{id_cli_sel}"):
                        c1, c2, c3 = st.columns(3)
                        new_nombre = c1.text_input("Alias Original", value=row_full['nombre_corto'] or "")
                        new_nombre_ia = c2.text_input("Nombre IA", value=row_full['nombre_ia'] if pd.notna(row_full['nombre_ia']) else "")
                        curr_est = row_full['estado']
                        new_estado = c3.selectbox("Estado", options=estados_opciones, index=estados_opciones.index(curr_est) if curr_est in estados_opciones else 0)

                        st.write("")
                        col_tg1, col_tg2 = st.columns(2)
                        new_excluir = col_tg1.toggle("🚫 Excluir de campañas publicitarias", value=bool(row_full.get('excluir_publicidad', False)))
                        bloquear_cliente = col_tg2.toggle("🔒 Bloquear / Desactivar Cliente", value=not bool(row_full.get('activo', True)))

                        st.markdown("##### 👥 Sincronización y Datos")
                        
                        val_nom = row_full['nombre'] if pd.notna(row_full['nombre']) else ""
                        val_ape = row_full['apellido'] if pd.notna(row_full['apellido']) else ""
                        val_eti = row_full['etiquetas'] if pd.notna(row_full['etiquetas']) else ""

                        with engine.connect() as conn:
                            prin_data = conn.execute(text("SELECT telefono, alias, lid FROM telefonoscliente WHERE id_cliente=:id AND es_principal=TRUE AND activo=TRUE LIMIT 1"), {"id": id_cli_sel}).fetchone()

                        tel_bd_actual = prin_data.telefono if prin_data else None
                        es_numero_real = tel_bd_actual and not (str(tel_bd_actual).startswith("LID_") or "@lid" in str(tel_bd_actual))
                        bloquear_google = not es_numero_real
                        
                        if bloquear_google:
                            st.info("⚠️ El contacto principal no tiene un teléfono real. Edítalo en la pestaña 'Teléfonos' para habilitar la vinculación con Google.")

                        c4, c5 = st.columns(2)
                        new_real_nombre = c4.text_input("Nombre Real", value=val_nom, disabled=bloquear_google)
                        new_apellido = c5.text_input("Apellido", value=val_ape, disabled=bloquear_google)
                        new_etiquetas = st.text_area("Etiquetas / Notas", value=val_eti)

                        if st.form_submit_button("💾 Guardar Datos Personales"):
                            id_etapa_val = mapa_subgrupo_id.get(new_estado)
                            google_id_crudo = row_full['google_id']
                            tiene_google_id = pd.notna(google_id_crudo) and str(google_id_crudo).strip().lower() not in ['', 'nan', 'none']
                            nuevo_google_id_db = google_id_crudo if tiene_google_id else None

                            if not bloquear_google:
                                norm_t = normalizar_telefono_maestro(tel_bd_actual)
                                tel_g = norm_t['google'] if norm_t else tel_bd_actual
                                
                                if tiene_google_id:
                                    with st.spinner("Actualizando en Google Contacts..."):
                                        exito_google = actualizar_en_google(str(google_id_crudo), new_real_nombre, new_apellido, tel_g)
                                    if exito_google: st.toast("✅ Contacto actualizado en Google", icon="👥")
                                    else: st.error("❌ Falló la actualización en Google Contacts.")
                                else:
                                    with st.spinner("Creando y vinculando en Google Contacts..."):
                                        nuevo_gid = crear_en_google(new_real_nombre or new_nombre, new_apellido, tel_g)
                                        if nuevo_gid:
                                            nuevo_google_id_db = nuevo_gid
                                            st.toast("✨ Contacto vinculado a Google", icon="🔗")
                            
                            nuevo_activo_val = False if bloquear_cliente else True

                            with engine.begin() as conn:
                                conn.execute(text("""
                                    UPDATE clientes 
                                    SET nombre_corto=:nc, nombre_ia=:nia, nombre=:n, apellido=:a, etiquetas=:e, estado=:est, id_etapa=:id_etapa, excluir_publicidad=:exc, activo=:act, google_id=:gid
                                    WHERE id_cliente=:id
                                """), {
                                    "nc": new_nombre, "nia": new_nombre_ia, "n": new_real_nombre, "a": new_apellido,
                                    "e": new_etiquetas, "est": new_estado, "id_etapa": id_etapa_val, "exc": new_excluir, "act": nuevo_activo_val, "gid": nuevo_google_id_db, "id": id_cli_sel
                                })
                                
                            if bloquear_cliente: st.warning("🔒 Cliente bloqueado exitosamente.")
                            else: st.success("Guardado en Base de Datos.")
                            time.sleep(1)
                            st.rerun()

                # --- TELÉFONOS Y ALIAS ---
                with tab_tel:
                    st.markdown("##### 📱 Gestión de Números Asociados y Alias")
                    with engine.connect() as conn:
                        tels = pd.read_sql(text("SELECT id_telefono, telefono, alias, lid, es_principal FROM telefonoscliente WHERE id_cliente=:id AND activo=TRUE ORDER BY es_principal DESC"), conn, params={"id": id_cli_sel})

                    cambios = {}
                    for idx, t_row in tels.iterrows():
                        es_prin_label = "⭐ Principal" if t_row['es_principal'] else "Secundario"
                        st.markdown(f"**Contacto {idx+1}** ({es_prin_label})")
                        
                        col_t1, col_t2, col_t3, col_t4 = st.columns([2, 2, 2, 1.5])
                        cambios[t_row['id_telefono']] = {
                            'tel_old': t_row['telefono'],
                            'tel_new': col_t1.text_input("Teléfono", value=t_row['telefono'] or "", key=f"t_{t_row['id_telefono']}"),
                            'alias': col_t2.text_input("Alias", value=t_row['alias'] or "", key=f"a_{t_row['id_telefono']}"),
                            'lid': col_t3.text_input("LID", value=t_row['lid'] or "", disabled=True, key=f"l_{t_row['id_telefono']}")
                        }

                        if not t_row['es_principal']:
                            if col_t4.button("⭐ Hacer Principal", key=f"p_{t_row['id_telefono']}", use_container_width=True):
                                with engine.begin() as tx:
                                    tx.execute(text("UPDATE telefonoscliente SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                    tx.execute(text("UPDATE telefonoscliente SET es_principal=TRUE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                                st.rerun()
                        
                        if len(tels) > 1:
                            if col_t4.button("🗑️ Eliminar", key=f"d_{t_row['id_telefono']}", use_container_width=True):
                                with engine.begin() as tx:
                                    tx.execute(text("UPDATE telefonoscliente SET activo=FALSE WHERE id_telefono=:idt"), {"idt": t_row['id_telefono']})
                                st.rerun()
                        st.write("")
                    
                    if st.button("💾 Guardar Cambios de Teléfonos", type="primary"):
                        hay_error = False
                        with engine.begin() as tx:
                            for id_tel, data in cambios.items():
                                t_old = data['tel_old']
                                t_new = data['tel_new'].strip() if data['tel_new'] else None
                                a_val = data['alias'].strip() if data['alias'] else None
                                
                                t_clean = normalizar_telefono_maestro(t_new)['db'] if t_new and normalizar_telefono_maestro(t_new) else t_new
                                
                                if t_clean != t_old:
                                    if t_clean:
                                        ex_activo = tx.execute(text("SELECT 1 FROM telefonoscliente t JOIN clientes c ON t.id_cliente = c.id_cliente WHERE t.telefono = :t AND t.activo = TRUE AND c.activo = TRUE AND t.id_cliente != :id"), {"t": t_clean, "id": id_cli_sel}).fetchone()
                                        if ex_activo:
                                            st.error(f"⚠️ El número {t_clean} ya está en uso por otro cliente.")
                                            hay_error = True
                                            continue
                                        
                                        lid_api = obtener_lid_de_waha(t_clean)
                                        if lid_api:
                                            tx.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE lid = :l AND id_telefono != :id"), {"l": lid_api, "id": id_tel})
                                            tx.execute(text("UPDATE telefonoscliente SET telefono=:t, alias=:a, lid=:l WHERE id_telefono=:id"), {"t": t_clean, "a": a_val, "l": lid_api, "id": id_tel})
                                        else:
                                            tx.execute(text("UPDATE telefonoscliente SET telefono=:t, alias=:a WHERE id_telefono=:id"), {"t": t_clean, "a": a_val, "id": id_tel})
                                    else:
                                        tx.execute(text("UPDATE telefonoscliente SET telefono=NULL, alias=:a, lid=NULL WHERE id_telefono=:id"), {"a": a_val, "id": id_tel})
                                else:
                                    tx.execute(text("UPDATE telefonoscliente SET alias=:a WHERE id_telefono=:id"), {"a": a_val, "id": id_tel})
                        
                        if not hay_error:
                            st.success("Teléfonos actualizados exitosamente.")
                            time.sleep(1)
                            st.rerun()

                    st.divider()
                    with st.form(f"add_tel_{id_cli_sel}", clear_on_submit=True):
                        st.write("➕ Agregar Número o Alias")
                        c_n1, c_n2 = st.columns(2)
                        new_tel = c_n1.text_input("Teléfono (Ej: +51 999...)")
                        new_alias = c_n2.text_input("Alias")
                        
                        if st.form_submit_button("Añadir Contacto"):
                            tel_clean = normalizar_telefono_maestro(new_tel)['db'] if new_tel and normalizar_telefono_maestro(new_tel) else (new_tel.strip() if new_tel else None)
                            alias_clean = new_alias.strip() if new_alias else None
                            
                            if tel_clean or alias_clean:
                                ex_activo = None
                                if tel_clean:
                                    with engine.connect() as conn:
                                        ex_activo = conn.execute(text("SELECT 1 FROM telefonoscliente t JOIN clientes c ON t.id_cliente = c.id_cliente WHERE t.telefono = :t AND t.activo = TRUE AND c.activo = TRUE"), {"t": tel_clean}).fetchone()
                                
                                if ex_activo:
                                    st.error(f"⚠️ El número {tel_clean} ya está registrado.")
                                else:
                                    with engine.begin() as tx:
                                        lid_api = obtener_lid_de_waha(tel_clean) if tel_clean else None
                                        if lid_api: tx.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE lid = :l"), {"l": lid_api})
                                        if tel_clean: tx.execute(text("UPDATE telefonoscliente SET activo = FALSE, telefono = NULL WHERE telefono = :t"), {"t": tel_clean})
                                        tx.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, alias, lid, es_principal, activo) VALUES (:id, :t, :a, :l, FALSE, TRUE)"), {"id": id_cli_sel, "t": tel_clean, "a": alias_clean, "l": lid_api})
                                    st.success("Añadido exitosamente.")
                                    time.sleep(1)
                                    st.rerun()
                            else: 
                                st.error("Debes ingresar un teléfono o un alias.")

                # --- DIRECCIONES ---
                with tab_dir:
                    st.markdown("#### 🏠 Gestión de Direcciones")
                    with engine.connect() as conn:
                        dirs_existentes = pd.read_sql(text("SELECT id_direccion FROM direcciones WHERE id_cliente=:id AND activo=TRUE"), conn, params={"id": id_cli_sel})
                    
                    mapa_ui_to_db = {"Motorizado": "MOTO", "Agencia": "AGENCIA", "Otros": "OTROS"}
                    
                    with st.expander("➕ Agregar Nueva Dirección", expanded=False):
                        nn_tipo_ui = st.selectbox("Tipo de Envío", ["Motorizado", "Agencia", "Otros"])
                        nn_tipo_db = mapa_ui_to_db[nn_tipo_ui]
                        
                        with st.form("form_new_dir"):
                            n1, n2 = st.columns(2)
                            nn_nom = n1.text_input("Nombre Receptor")
                            nn_tel = n2.text_input("Telf. Receptor")
                            
                            nn_dist, nn_dir, nn_ref, nn_gps_link, nn_dni, nn_agencia, nn_sede = [None]*7
                            
                            if nn_tipo_db == "MOTO":
                                d1, d2 = st.columns(2)
                                nn_dist = d1.text_input("Distrito")
                                nn_dir = d2.text_input("Dirección Exacta")
                                nn_ref = st.text_input("Referencia")
                                nn_gps_link = st.text_input("Link GPS")
                            elif nn_tipo_db == "AGENCIA":
                                d1, d2, d3 = st.columns(3)
                                nn_dni = d1.text_input("DNI Receptor")
                                nn_agencia = d2.text_input("Nombre Agencia")
                                nn_sede = d3.text_input("Sede de Entrega")
                                
                            nn_obs = st.text_area("Observación")
                            
                            if st.form_submit_button("Crear Dirección"):
                                es_prin = True if dirs_existentes.empty else False
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                                                                 direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                                                                 sede_entrega, observacion, activo, es_principal)
                                        VALUES (:idc, :tipo, :n, :t, :dis, :dt, :r, :glink, :dni, :anom, :sede, :obs, TRUE, :es_prin)
                                    """), {
                                        "idc": id_cli_sel, "tipo": nn_tipo_db, "n": nn_nom, "t": nn_tel, "dis": nn_dist, "dt": nn_dir, 
                                        "r": nn_ref, "glink": nn_gps_link, "dni": nn_dni, "anom": nn_agencia, "sede": nn_sede, "obs": nn_obs, "es_prin": es_prin
                                    })
                                st.success("Dirección guardada.")
                                time.sleep(1)
                                st.rerun()

                    st.divider()
                    with engine.connect() as conn:
                        dirs = pd.read_sql(text("SELECT * FROM direcciones WHERE id_cliente=:id AND activo=TRUE ORDER BY es_principal DESC, id_direccion DESC"), conn, params={"id": id_cli_sel})

                    if not dirs.empty:
                        for _, d_row in dirs.iterrows():
                            col_d1, col_d2, col_d3 = st.columns([4, 2, 0.5])
                            es_prin_dir = "⭐ Principal" if d_row['es_principal'] else "Secundaria"
                            
                            if d_row['tipo_envio'] == 'MOTO': res_dir = f"{d_row['direccion_texto']}, {d_row['distrito']}"
                            elif d_row['tipo_envio'] == 'AGENCIA': res_dir = f"Agencia: {d_row['agencia_nombre']}"
                            else: res_dir = d_row['observacion'][:40] + "..." if d_row['observacion'] else "Sin detalles"

                            col_d1.markdown(f"**[{d_row['tipo_envio']}] {d_row['nombre_receptor'] or 'Receptor'}** — {res_dir} ({es_prin_dir})")

                            if not d_row['es_principal']:
                                if col_d2.button("Hacer Principal", key=f"p_dir_{d_row['id_direccion']}"):
                                    with engine.begin() as tx:
                                        tx.execute(text("UPDATE direcciones SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                        tx.execute(text("UPDATE direcciones SET es_principal=TRUE WHERE id_direccion=:idd"), {"idd": int(d_row['id_direccion'])})
                                    st.rerun()

                            if col_d3.button("🗑️", key=f"d_dir_{d_row['id_direccion']}"):
                                with engine.begin() as tx:
                                    tx.execute(text("UPDATE direcciones SET activo=FALSE WHERE id_direccion=:idd"), {"idd": int(d_row['id_direccion'])})
                                st.rerun()
                    else:
                        st.info("Sin direcciones registradas.")

        else:
            st.info("No se encontraron registros.")

    # ==============================================================================
    # PESTAÑA 2: CREACIÓN, FUSIÓN Y SINCRONIZACIÓN
    # ==============================================================================
    with tab_gestion:
        st.write("Gestiona la creación de registros y el mantenimiento avanzado de la base de datos.")
        
        # --- 1. REGISTRAR CLIENTE ---
        with st.expander("➕ Registrar Nuevo Cliente / Proveedor", expanded=False):
            with st.form("form_nuevo_cliente"):
                c1, c2, c3 = st.columns([2, 2, 2])
                nuevo_tel = c1.text_input("Teléfono o LID (@lid) *", placeholder="Ej: +51999... o 12345@lid")
                nuevo_alias = c2.text_input("Alias / Nombre Corto")
                nuevo_estado = c3.selectbox("Estado Inicial", options=estados_opciones, index=0)

                nuevas_etiquetas = st.text_input("Etiquetas (Separadas por coma)")
                
                st.write("")
                c_goo, c_mkt = st.columns(2)
                vincular_google = c_goo.checkbox("🔍 Intentar vincular con Google Contactos", value=True)
                excluir_publicidad = c_mkt.checkbox("🚫 Excluir de campañas publicitarias", value=False)

                if st.form_submit_button("💾 Crear Registro", type="primary"):
                    input_raw = nuevo_tel.strip()
                    if not input_raw:
                        st.error("Debes ingresar un número de teléfono o un código LID.")
                    else:
                        es_lid = "@lid" in input_raw.lower()
                        tel_db, lid_db = None, None
                        es_valido = True
                        
                        if es_lid:
                            lid_db = input_raw.lower()
                        else:
                            norm = normalizar_telefono_maestro(input_raw)
                            if norm: tel_db = norm['db']
                            else: st.error("Formato inválido."); es_valido = False

                        if es_valido:
                            with engine.connect() as conn:
                                if tel_db: existe = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono=:t AND activo=TRUE LIMIT 1"), {"t": tel_db}).fetchone()
                                else: existe = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE lid=:l AND activo=TRUE LIMIT 1"), {"l": lid_db}).fetchone()

                            if existe:
                                st.warning(f"Este contacto ya pertenece al cliente ID {existe[0]}.")
                            else:
                                g_id, g_nom, g_ape = None, None, None
                                if not nuevo_alias: nuevo_alias = "Cliente Nuevo"
                                
                                if vincular_google and tel_db:
                                    res_g = buscar_contacto_google(tel_db)
                                    if res_g and res_g.get('encontrado'):
                                        g_id, g_nom, g_ape = res_g['google_id'], res_g['nombre'], res_g['apellido']
                                        nuevo_alias = f"{g_nom} {g_ape}".strip() or nuevo_alias
                                        st.toast("✅ Vinculado a un contacto en Google.", icon="🔗")
                                    else:
                                        tel_google = norm.get('google', tel_db)
                                        nuevo_gid = crear_en_google(nuevo_alias, "", tel_google)
                                        if nuevo_gid:
                                            g_id, g_nom, g_ape = nuevo_gid, nuevo_alias, ""
                                            st.toast("🆕 Nuevo contacto en Google.", icon="👤")
                                elif vincular_google and lid_db:
                                    st.toast("⚠️ Se omitió Google Contacts por ser anónimo.", icon="🛡️")

                                nombre_ia = generar_nombre_ia(nuevo_alias, g_nom or "")
                                id_etapa_val = mapa_subgrupo_id.get(nuevo_estado)

                                try:
                                    with engine.begin() as tx:
                                        if tel_db and not lid_db:
                                            lid_existente = tx.execute(text("SELECT lid FROM telefonoscliente WHERE telefono = :t AND lid IS NOT NULL LIMIT 1"), {"t": tel_db}).scalar()
                                            lid_db = lid_existente or obtener_lid_de_waha(tel_db)

                                        if tel_db: tx.execute(text("UPDATE telefonoscliente SET activo = FALSE WHERE telefono = :t"), {"t": tel_db})
                                        if lid_db: tx.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE lid = :l"), {"l": lid_db})

                                        res = tx.execute(text("""
                                            INSERT INTO clientes (nombre_corto, estado, id_etapa, etiquetas, google_id, nombre, apellido, nombre_ia, excluir_publicidad, activo, fecha_registro)
                                            VALUES (:nc, :e, :id_etapa, :et, :gid, :n, :a, :nia, :exc, TRUE, NOW())
                                            RETURNING id_cliente
                                        """), {"nc": nuevo_alias, "e": nuevo_estado, "id_etapa": id_etapa_val, "et": nuevas_etiquetas, "gid": g_id, "n": g_nom, "a": g_ape, "nia": nombre_ia, "exc": excluir_publicidad})
                                        nuevo_id = res.fetchone()[0]

                                        tx.execute(text("""
                                            INSERT INTO telefonoscliente (id_cliente, telefono, lid, es_principal, activo)
                                            VALUES (:id, :t, :lid, TRUE, TRUE)
                                        """), {"id": nuevo_id, "t": tel_db, "lid": lid_db})

                                    st.success(f"✅ Registro guardado.")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error al insertar: {e}")

        # --- 2. IMPORTACIÓN MASIVA ---
        with st.expander("📥 Importación Masiva (Pega tu lista aquí)", expanded=False):
            st.markdown("Pega una lista de números de teléfono (uno por línea). Omitirá duplicados y sincronizará con Google.")
            lista_telefonos = st.text_area("Lista de Teléfonos", height=200, placeholder="Ej:\n+51987654321\n999888777")
            
            if st.button("🚀 Procesar e Importar Lista", type="primary"):
                if not lista_telefonos.strip():
                    st.warning("La lista está vacía.")
                else:
                    numeros = lista_telefonos.strip().split('\n')
                    stats = {"agregados": 0, "duplicados": 0, "errores": 0, "google_vinculados": 0, "google_creados": 0}
                    estado_default = estados_opciones[0] if estados_opciones else "Sin empezar"
                    id_etapa_val = mapa_subgrupo_id.get(estado_default)
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for idx, raw_tel in enumerate(numeros):
                        progress_bar.progress(int(((idx + 1) / len(numeros)) * 100))
                        status_text.text(f"Procesando: {raw_tel.strip()} ({idx + 1}/{len(numeros)})")
                        
                        num_limpio = raw_tel.strip()
                        if not num_limpio: continue
                        
                        norm = normalizar_telefono_maestro(num_limpio)
                        if not norm: stats["errores"] += 1; continue
                            
                        tel_db = norm['db']
                        tel_google = norm.get('google', tel_db)
                        
                        with engine.begin() as conn:
                            existe = conn.execute(text("SELECT id_cliente FROM telefonoscliente WHERE telefono=:t AND activo=TRUE LIMIT 1"), {"t": tel_db}).fetchone()
                            if existe: stats["duplicados"] += 1; continue
                        
                        g_id, g_nom, g_ape = None, None, None
                        alias_temp = "Cliente Nuevo"
                        
                        res_g = buscar_contacto_google(tel_db)
                        if not (res_g and res_g.get('encontrado')): res_g = buscar_contacto_google(tel_google)
                        
                        if res_g and res_g.get('encontrado'):
                            g_id, g_nom, g_ape = res_g['google_id'], res_g['nombre'], res_g['apellido']
                            alias_temp = f"{g_nom} {g_ape}".strip() or "Cliente Nuevo"
                            stats["google_vinculados"] += 1
                        else:
                            nuevo_gid = crear_en_google("Cliente Nuevo", "", tel_google)
                            if nuevo_gid: g_id, g_nom, stats["google_creados"] = nuevo_gid, "Cliente Nuevo", stats["google_creados"] + 1
                        
                        nombre_ia_val = generar_nombre_ia(alias_temp, g_nom or "")
                        
                        try:
                            with engine.begin() as conn:
                                lid_existente = conn.execute(text("SELECT lid FROM telefonoscliente WHERE telefono = :t AND lid IS NOT NULL LIMIT 1"), {"t": tel_db}).scalar()
                                if lid_existente: conn.execute(text("UPDATE telefonoscliente SET lid = NULL WHERE lid = :l"), {"l": lid_existente})
                                
                                res = conn.execute(text("""
                                    INSERT INTO clientes (nombre_corto, estado, id_etapa, google_id, nombre, apellido, nombre_ia, activo, fecha_registro)
                                    VALUES (:nc, :e, :id_etapa, :gid, :n, :a, :nia, TRUE, NOW()) RETURNING id_cliente
                                """), {"nc": alias_temp, "e": estado_default, "id_etapa": id_etapa_val, "gid": g_id, "n": g_nom, "a": g_ape, "nia": nombre_ia_val})
                                
                                conn.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, lid, es_principal, activo) VALUES (:id, :t, :lid, TRUE, TRUE)"), {"id": res.fetchone()[0], "t": tel_db, "lid": lid_existente})
                            stats["agregados"] += 1
                        except Exception: stats["errores"] += 1

                    status_text.text("¡Proceso Finalizado!")
                    st.success(f"✅ Agregados: {stats['agregados']} | Vinculados G: {stats['google_vinculados']} | Duplicados omitidos: {stats['duplicados']}")
                    time.sleep(2)
                    st.rerun()

        # --- 3. FUSIÓN DE CLIENTES ---
        with st.expander("🔄 Fusionar Clientes Duplicados", expanded=False):
            try:
                with engine.connect() as conn:
                    df_fusion = pd.read_sql(text("""
                        SELECT c.id_cliente, c.nombre_corto,
                               (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1) as tel_prin
                        FROM clientes c WHERE c.activo=TRUE ORDER BY c.nombre_corto
                    """), conn)
                
                if not df_fusion.empty:
                    opciones = df_fusion.apply(lambda x: f"{x['nombre_corto']} | {x['tel_prin'] or 'Sin Tel'} (ID: {x['id_cliente']})", axis=1).tolist()
                    mapa_ids = dict(zip(opciones, df_fusion['id_cliente']))

                    c1, c2 = st.columns(2)
                    sel_keep = c1.selectbox("✅ Cliente a CONSERVAR (Destino)", opciones, key="fusion_keep")
                    sel_del = c2.selectbox("❌ Cliente a ELIMINAR (Origen)", opciones, key="fusion_del")

                    if st.button("🚀 Confirmar Fusión"):
                        if sel_keep and sel_del:
                            id_keep, id_del = mapa_ids[sel_keep], mapa_ids[sel_del]
                            if id_keep == id_del: st.error("Selecciona dos clientes distintos.")
                            else:
                                with st.spinner("Fusionando..."):
                                    try:
                                        with engine.begin() as tx:
                                            # Mover todos los teléfonos, ventas y direcciones al conservado
                                            tx.execute(text("UPDATE telefonoscliente SET id_cliente = :new, es_principal = FALSE WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE ventas SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE direcciones SET id_cliente = :new WHERE id_cliente = :old"), {"new": id_keep, "old": id_del})
                                            tx.execute(text("UPDATE clientes SET activo = FALSE WHERE id_cliente = :id"), {"id": id_del})
                                        st.success("¡Fusión completada!")
                                        time.sleep(1)
                                        st.rerun()
                                    except Exception as e: st.error(f"Error: {e}")
            except Exception as e: st.error(f"Error cargando herramienta: {e}")

# --- 4. SINCRONIZACIÓN GOOGLE ---
        with st.expander("📱 Sincronización Masiva Bidireccional con Google Contacts", expanded=False):
            if st.button("🚀 Iniciar Sincronización Masiva", key="btn_sync"):
                with st.spinner("Sincronizando bidireccionalmente y depurando duplicados. Esto puede tomar unos minutos..."):
                    try:
                        from utils import get_google_service
                        service = get_google_service()
                        
                        if not service:
                            st.error("❌ No se pudo conectar a Google Contacts. Revisa las credenciales.")
                        else:
                            # =====================================================================
                            # PASO 1: Descargar Google Contacts
                            # =====================================================================
                            st.info("📥 [Paso 1] Descargando libreta de Google agrupada por ID...")
                            mapa_g_personas = {} 
                            mapa_g_tels = {} 
                            
                            request = service.people().connections().list(
                                resourceName='people/me', pageSize=1000, personFields='names,phoneNumbers'
                            )
                            
                            while request is not None:
                                response = request.execute()
                                for person in response.get('connections', []):
                                    g_id = person.get('resourceName')
                                    nombres = person.get('names', [])
                                    nombre_completo = nombres[0].get('displayName', 'Sin Nombre') if nombres else "Sin Nombre"
                                    
                                    tels_raw = []
                                    tels_cortos = []
                                    for tel in person.get('phoneNumbers', []):
                                        tel_val = tel.get('value', '')
                                        norm_g = normalizar_telefono_maestro(tel_val)
                                        if norm_g and norm_g.get('corto'):
                                            tels_raw.append(tel_val)
                                            tels_cortos.append(norm_g['corto'])
                                            mapa_g_tels[norm_g['corto']] = g_id
                                            
                                    mapa_g_personas[g_id] = {
                                        "nombre": nombre_completo,
                                        "tels_raw": tels_raw,
                                        "tels_cortos": tels_cortos
                                    }
                                request = service.people().connections().list_next(request, response)

                            # =====================================================================
                            # PASO 2: Preparar Datos BD
                            # =====================================================================
                            st.info("🔍 [Paso 2] Extrayendo historial multi-número local...")
                            with engine.connect() as conn:
                                df_db = pd.read_sql(text("""
                                    SELECT c.id_cliente, c.nombre_corto, c.nombre, c.apellido, c.google_id, c.activo as cliente_activo, 
                                           t.telefono, t.activo as tel_activo 
                                    FROM clientes c
                                    LEFT JOIN telefonoscliente t ON c.id_cliente = t.id_cliente 
                                """), conn)
                                
                            mapa_db_clientes = {}
                            mapa_db_tels = {}

                            for idx, row in df_db.iterrows():
                                id_cli = row['id_cliente']
                                if id_cli not in mapa_db_clientes:
                                    mapa_db_clientes[id_cli] = {
                                        'google_id': row['google_id'],
                                        'nombre_corto': row['nombre_corto'],
                                        'nombre': row['nombre'] or '',
                                        'apellido': row['apellido'] or '',
                                        'es_activo': row['cliente_activo'],
                                        'tels_google': [],
                                        'tels_db': []
                                    }
                                
                                tel = row['telefono']
                                if pd.notna(tel) and str(tel).strip() and row['tel_activo']:
                                    norm = normalizar_telefono_maestro(tel)
                                    if norm and norm.get('corto'):
                                        mapa_db_clientes[id_cli]['tels_google'].append(norm.get('google', norm['db']))
                                        mapa_db_clientes[id_cli]['tels_db'].append(norm['db'])
                                        mapa_db_tels[norm['corto']] = id_cli

                            agregados_a_bbdd = 0
                            agregados_a_google = 0
                            perfiles_google_actualizados = 0
                            info_completada = 0
                            contactos_eliminados = 0

                            with engine.begin() as conn_tx:
                                # =====================================================================
                                # PASO 1.2: Importar a BD (Google -> BD)
                                # =====================================================================
                                for g_id, datos_g in mapa_g_personas.items():
                                    existe_en_bd = conn_tx.execute(text("SELECT id_cliente FROM clientes WHERE google_id = :gid LIMIT 1"), {"gid": g_id}).fetchone()
                                    
                                    if not existe_en_bd:
                                        encontrado_por_tel = any(corto in mapa_db_tels for corto in datos_g['tels_cortos'])
                                        if not encontrado_por_tel and len(datos_g['tels_raw']) > 0:
                                            res_insert = conn_tx.execute(text("""
                                                INSERT INTO clientes (nombre_corto, nombre, google_id, estado, activo, fecha_registro)
                                                VALUES (:nc, :n, :gid, 'Sin empezar', TRUE, NOW())
                                                RETURNING id_cliente
                                            """), {"nc": datos_g['nombre'], "n": datos_g['nombre'], "gid": g_id})
                                            nuevo_id_cli = res_insert.fetchone()[0]
                                            
                                            es_primero = True
                                            for t_raw in datos_g['tels_raw']:
                                                norm_t = normalizar_telefono_maestro(t_raw)
                                                tel_db_final = norm_t['db'] if norm_t else t_raw
                                                conn_tx.execute(text("""
                                                    INSERT INTO telefonoscliente (id_cliente, telefono, es_principal, activo) 
                                                    VALUES (:id, :t, :prin, TRUE)
                                                """), {"id": nuevo_id_cli, "t": tel_db_final, "prin": es_primero})
                                                es_primero = False
                                            agregados_a_bbdd += 1

                                # =====================================================================
                                # PASO 3: Exportar a Google y Rastrear
                                # =====================================================================
                                st.info("🔄 [Paso 3] Vinculando, exportando y completando Nombres...")
                                for id_cli, datos_bd in mapa_db_clientes.items():
                                    if not datos_bd['es_activo']: continue
                                    
                                    tels_export = datos_bd['tels_google']
                                    if not tels_export: continue 
                                    
                                    gid_actual = datos_bd['google_id']
                                    g_id_real = next((mapa_g_tels[normalizar_telefono_maestro(t)['corto']] for t in datos_bd['tels_db'] if normalizar_telefono_maestro(t)['corto'] in mapa_g_tels), None)
                                    
                                    res_g_profundo = None
                                    if not g_id_real and not gid_actual:
                                        t_principal = datos_bd['tels_db'][0]
                                        res_g_profundo = buscar_contacto_google(t_principal)
                                        if not (res_g_profundo and res_g_profundo.get('encontrado')):
                                            res_g_profundo = buscar_contacto_google(tels_export[0])
                                            
                                        if res_g_profundo and res_g_profundo.get('encontrado'):
                                            g_id_real = res_g_profundo['google_id']

                                    if g_id_real:
                                        g_nom = datos_bd['nombre']
                                        g_ape = datos_bd['apellido']
                                        if res_g_profundo:
                                            g_nom = res_g_profundo.get('nombre') or datos_bd['nombre'] or datos_bd['nombre_corto']
                                            g_ape = res_g_profundo.get('apellido') or datos_bd['apellido'] or ""
                                            info_completada += 1
                                        
                                        if gid_actual != g_id_real or res_g_profundo:
                                            conn_tx.execute(text("UPDATE clientes SET google_id = :gid, nombre = :n, apellido = :a WHERE id_cliente = :id"), 
                                                            {"gid": g_id_real, "n": g_nom, "a": g_ape, "id": id_cli})
                                        
                                        tels_en_google = len(mapa_g_personas[g_id_real]['tels_raw']) if g_id_real in mapa_g_personas else 0
                                        if len(tels_export) > tels_en_google:
                                            actualizar_en_google(g_id_real, datos_bd['nombre_corto'], "", tels_export)
                                            perfiles_google_actualizados += 1
                                    else:
                                        nuevo_gid = crear_en_google(datos_bd['nombre_corto'], "", tels_export)
                                        if nuevo_gid:
                                            conn_tx.execute(text("UPDATE clientes SET google_id = :gid WHERE id_cliente = :id"), {"gid": nuevo_gid, "id": id_cli})
                                            agregados_a_google += 1

                                # =====================================================================
                                # PASO 4: Depuración
                                # =====================================================================
                                st.info("🗑️ [Paso 4] Limpiando Google Contacts...")
                                for id_cli, datos_bd in mapa_db_clientes.items():
                                    if not datos_bd['es_activo']:
                                        gid_del = datos_bd['google_id']
                                        if gid_del:
                                            try:
                                                res_name = gid_del if gid_del.startswith('people/') else f"people/{gid_del}"
                                                service.people().deleteContact(resourceName=res_name).execute()
                                                contactos_eliminados += 1
                                            except: pass
                                            conn_tx.execute(text("UPDATE clientes SET google_id = NULL WHERE id_cliente = :id"), {"id": id_cli})
                                        
                                        for norm_db in datos_bd['tels_db']:
                                            corto = normalizar_telefono_maestro(norm_db)['corto']
                                            if corto in mapa_g_tels:
                                                g_id_clon = mapa_g_tels[corto]
                                                try:
                                                    res_name = g_id_clon if g_id_clon.startswith('people/') else f"people/{g_id_clon}"
                                                    service.people().deleteContact(resourceName=res_name).execute()
                                                    contactos_eliminados += 1
                                                except: pass

                            st.success(
                                f"✨ **¡Sincronización Total Completada!**\n\n"
                                f"- 📥 Importados a BD (Agrupados): **{agregados_a_bbdd}**\n"
                                f"- 📤 Creados en Google (Multi-número): **{agregados_a_google}**\n"
                                f"- 📲 Fichas de Google enriquecidas: **{perfiles_google_actualizados}**\n"
                                f"- 📝 Nombres extraídos profundamente: **{info_completada}**\n"
                                f"- 🗑️ Bloqueados y clones eliminados: **{contactos_eliminados}**"
                            )
                            time.sleep(5)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error en la sincronización masiva: {e}")