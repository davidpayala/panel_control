import streamlit as st
import pandas as pd
from sqlalchemy import text
from database import engine
from utils import buscar_contacto_google, crear_en_google, normalizar_telefono_maestro, generar_nombre_ia, actualizar_en_google
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
# HERRAMIENTA DE SINCRONIZACIÓN MASIVA GOOGLE (PASO 2 OPTIMIZADO)
# ==============================================================================
def render_sincronizacion_masiva():
    with st.expander("🔄 Sincronización Masiva con Google Contacts", expanded=False):
        st.info("💡 **Sincronización de Historial:** Esta herramienta busca todos los clientes activos que no están vinculados para registrarlos en Google Contacts y asociar su ID permanentemente.")
        if st.button("🚀 Vincular Clientes Antiguos"):
            with st.spinner("Sincronizando historial..."):
                try:
                    with engine.connect() as conn:
                        df_sin_sync = pd.read_sql(text("""
                            SELECT c.id_cliente, c.nombre_corto, 
                                   COALESCE(
                                       (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       c.telefono
                                   ) as tel_prin
                            FROM clientes c 
                            WHERE c.activo=TRUE AND (c.google_id IS NULL OR TRIM(c.google_id) = '')
                        """), conn)
                    
                    if df_sin_sync.empty:
                        st.success("¡Todos los clientes ya se encuentran vinculados!")
                    else:
                        cont = 0
                        detalles_omisiones = []
                        with engine.begin() as conn_tx:
                            for idx, row in df_sin_sync.iterrows():
                                id_cli = row['id_cliente']
                                nombre = row['nombre_corto']
                                tel = row['tel_prin']
                                
                                if pd.isna(tel) or str(tel).strip().lower() in ['nan', '']:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): Sin ningún teléfono en base de datos.")
                                    continue
                                    
                                norm = normalizar_telefono_maestro(tel)
                                if not norm:
                                    detalles_omisiones.append(f"⚠️ ID {id_cli} ({nombre}): Formato inválido para '{tel}'.")
                                    continue
                                    
                                tel_db = norm['db']
                                tel_google = norm.get('google', tel_db)
                                
                                res_g = buscar_contacto_google(tel_db)
                                g_id = None
                                if res_g and res_g.get('encontrado'):
                                    g_id = res_g['google_id']
                                else:
                                    if crear_en_google(nombre, "", tel_google):
                                        res_g2 = buscar_contacto_google(tel_db)
                                        if res_g2 and res_g2.get('encontrado'):
                                            g_id = res_g2['google_id']
                                
                                if g_id:
                                    conn_tx.execute(text("UPDATE clientes SET google_id = :gid WHERE id_cliente = :id"), {"gid": g_id, "id": id_cli})
                                    cont += 1
                        
                        if detalles_omisiones:
                            st.markdown("##### 🔍 Detalles de registros omitidos:")
                            for msg in detalles_omisiones:
                                st.warning(msg)
                                
                        st.success(f"¡Sincronización completada! Se vincularon {cont} clientes con éxito.")
                        if cont > 0:
                            time.sleep(1)
                            st.rerun()
                except Exception as e:
                    st.error(f"Error en la sincronización masiva: {e}")

# ==============================================================================
# RENDERIZADO PRINCIPAL
# ==============================================================================
def render_clientes():
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

    st.title("👤 Gestión de Clientes")
    render_herramienta_fusion()
    render_sincronizacion_masiva()

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
                        if not nuevo_alias: nuevo_alias = "Cliente Nuevo"
                        
                        if vincular_google:
                            res_g = buscar_contacto_google(tel_db)
                            if res_g and res_g.get('encontrado'):
                                g_id = res_g['google_id']
                                g_nom = res_g['nombre']
                                g_ape = res_g['apellido']
                                nuevo_alias = f"{g_nom} {g_ape}".strip()
                            else:
                                tel_google = norm.get('google', tel_db)
                                if crear_en_google(nuevo_alias, "", tel_google):
                                    res_g2 = buscar_contacto_google(tel_db)
                                    if res_g2 and res_g2.get('encontrado'):
                                        g_id = res_g2['google_id']

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
        SELECT c.id_cliente, c.nombre_corto, c.estado, c.nombre, c.apellido, c.etiquetas, c.google_id, c.whatsapp_internal_id, c.nombre_ia,
               COALESCE((SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1), c.telefono) as tel_principal,
               (SELECT STRING_AGG(telefono, ' | ') FROM telefonoscliente WHERE id_cliente = c.id_cliente AND activo = TRUE) as todos_telefonos
        FROM clientes c
        WHERE c.activo = TRUE
    """
    params = {}
    if busqueda:
        query += """ AND (
            c.nombre_corto ILIKE :g OR c.nombre ILIKE :g OR c.apellido ILIKE :g OR c.etiquetas ILIKE :g OR c.nombre_ia ILIKE :g
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
                "nombre_corto": st.column_config.TextColumn("Alias Original", width="medium"),
                "nombre_ia": st.column_config.TextColumn("Nombre IA", width="medium"),
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
                    nia_val = row['nombre_ia'] if pd.notna(row['nombre_ia']) else ""
                    conn.execute(text("UPDATE clientes SET nombre_corto=:nc, nombre_ia=:nia, estado=:est, id_etapa=:id_etapa WHERE id_cliente=:id"),
                                 {"nc": row['nombre_corto'], "nia": nia_val, "est": row['estado'], "id_etapa": id_etapa_val, "id": row['id_cliente']})
            st.success("Cambios guardados.")
            time.sleep(1)
            st.rerun()

        # --- GESTIÓN INDIVIDUAL (PASO 3: DESBLOQUEADO Y VINCULADO) ---
        filas_sel = edited_df[edited_df["Seleccionar"] == True]
        if not filas_sel.empty:
            row_full = df.loc[filas_sel.index[0]]
            id_cli_sel = int(row_full['id_cliente'])

            st.divider()
            st.subheader(f"⚙️ Gestión Individual: {row_full['nombre_corto']}")

            tab_datos, tab_tel, tab_dir = st.tabs(["👤 Datos Personales", "📞 Teléfonos", "🏠 Direcciones"])

            with tab_datos:
                with st.form(f"form_cli_{id_cli_sel}"):
                    c1, c2, c3 = st.columns(3)
                    new_nombre = c1.text_input("Alias Original", value=row_full['nombre_corto'] or "")
                    val_nombre_ia = row_full['nombre_ia'] if pd.notna(row_full['nombre_ia']) else ""
                    new_nombre_ia = c2.text_input("Nombre IA", value=val_nombre_ia)
                    curr_est = row_full['estado']
                    new_estado = c3.selectbox("Estado", options=estados_opciones, index=estados_opciones.index(curr_est) if curr_est in estados_opciones else 0)

                    st.markdown("##### 👥 Sincronización Directa a Google Contacts")
                    c4, c5, c6 = st.columns(3)
                    new_real_nombre = c4.text_input("Nombre Real", value=row_full['nombre'] or "")
                    new_apellido = c5.text_input("Apellido", value=row_full['apellido'] or "")
                    new_tel_principal = c6.text_input("Teléfono Principal", value=row_full['tel_principal'] or "")
                    
                    new_etiquetas = st.text_area("Etiquetas / Notas", value=row_full['etiquetas'] or "")

                    if st.form_submit_button("💾 Guardar Datos"):
                        id_etapa_val = mapa_subgrupo_id.get(new_estado)
                        google_id_crudo = row_full['google_id']
                        
                        # Validación estricta para asegurar que el ID de Google es real y no un NaN/Vacío
                        tiene_google_id = pd.notna(google_id_crudo) and str(google_id_crudo).strip().lower() not in ['', 'nan', 'none']
                        
                        if tiene_google_id:
                            norm_t = normalizar_telefono_maestro(new_tel_principal)
                            tel_g = norm_t['google'] if norm_t else new_tel_principal
                            
                            with st.spinner("Sincronizando con Google Contacts..."):
                                exito_google = actualizar_en_google(str(google_id_crudo), new_real_nombre, new_apellido, tel_g)
                                
                            if exito_google:
                                st.toast("✅ Contacto actualizado en Google", icon="👥")
                            else:
                                st.error("❌ Falló la actualización en Google Contacts. Revisa la función 'actualizar_en_google' en utils.py o las credenciales.")
                        else:
                            st.warning("⚠️ Este cliente no tiene un ID de Google Contacts vinculado. Usa la sincronización masiva o manual primero.")
                        
                        # Guardado en Base de Datos Local
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE clientes 
                                SET nombre_corto=:nc, nombre_ia=:nia, nombre=:n, apellido=:a, etiquetas=:e, estado=:est, id_etapa=:id_etapa 
                                WHERE id_cliente=:id
                            """), {
                                "nc": new_nombre, "nia": new_nombre_ia, "n": new_real_nombre, "a": new_apellido,
                                "e": new_etiquetas, "est": new_estado, "id_etapa": id_etapa_val, "id": id_cli_sel
                            })
                            
                            if str(new_tel_principal).strip() != str(row_full['tel_principal']).strip():
                                norm_t = normalizar_telefono_maestro(new_tel_principal)
                                if norm_t:
                                    conn.execute(text("UPDATE telefonoscliente SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cli_sel})
                                    existe_tel = conn.execute(text("SELECT id_telefono FROM telefonoscliente WHERE id_cliente=:id AND telefono=:t"), {"id": id_cli_sel, "t": norm_t['db']}).fetchone()
                                    
                                    if existe_tel:
                                        conn.execute(text("UPDATE telefonoscliente SET es_principal=TRUE, activo=TRUE WHERE id_telefono=:idt"), {"idt": existe_tel[0]})
                                    else:
                                        conn.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono, es_principal) VALUES (:id, :t, TRUE)"), {"id": id_cli_sel, "t": norm_t['db']})
                                    
                                    conn.execute(text("UPDATE clientes SET telefono=:t WHERE id_cliente=:id"), {"t": norm_t['db'], "id": id_cli_sel})

                        st.success("Guardado en Base de Datos.")
                        time.sleep(1)
                        st.rerun()

                if st.button("🔍 Forzar Sincronización Manual (Obtener ID de Google)"):
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

            # --- PESTAÑA TELÉFONOS ---
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
                                    st.error(f"⚠️ Este número ya está asignado al cliente: **{ex.nombre_corto}** (ID: {ex.id_cliente}).")
                                else:
                                    with engine.begin() as tx:
                                        tx.execute(text("INSERT INTO telefonoscliente (id_cliente, telefono) VALUES (:id, :t)"), {"id": id_cli_sel, "t": norm_t['db']})
                                    st.success("Teléfono añadido exitosamente.")
                                    time.sleep(1)
                                    st.rerun()
                        else: 
                            st.error("Formato de número inválido.")

            with tab_dir:
                # --- PARCHE AUTOMÁTICO PARA DIRECCIÓN PRINCIPAL ---
                try:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE direcciones ADD COLUMN IF NOT EXISTS es_principal BOOLEAN DEFAULT FALSE"))
                except: pass

                st.markdown("#### 🏠 Lista de Direcciones")
                with engine.connect() as conn:
                    dirs = pd.read_sql(text("""
                        SELECT id_direccion, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                               direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                               sede_entrega, observacion, es_principal 
                        FROM direcciones 
                        WHERE id_cliente=:id AND activo=TRUE 
                        ORDER BY es_principal DESC, id_direccion DESC
                    """), conn, params={"id": id_cli_sel})

                # Mapeos entre Interfaz (UI) y Base de Datos (DB)
                mapa_ui_to_db = {"Motorizado": "MOTO", "Agencia": "AGENCIA", "Otros": "OTROS"}
                mapa_db_to_ui = {"MOTO": "Motorizado", "AGENCIA": "Agencia", "OTROS": "Otros"}

                if not dirs.empty:
                    for _, d_row in dirs.iterrows():
                        col_d1, col_d2, col_d3 = st.columns([4, 2, 0.5])
                        es_prin_dir = "⭐ Principal" if d_row['es_principal'] else "Secundaria"
                        
                        tipo_db = d_row['tipo_envio']
                        tipo_ui = mapa_db_to_ui.get(tipo_db, "Otros")
                        tipo_label = f"[{tipo_ui}]"
                        
                        if tipo_db == 'MOTO':
                            resumen_dir = f"{d_row['direccion_texto']}, {d_row['distrito']}"
                        elif tipo_db == 'AGENCIA':
                            resumen_dir = f"Agencia: {d_row['agencia_nombre']} ({d_row['sede_entrega'] or 'Sede no especificada'})"
                        else:
                            resumen_dir = d_row['observacion'][:40] + "..." if d_row['observacion'] else "Sin detalles"

                        col_d1.markdown(f"**{tipo_label} {d_row['nombre_receptor'] or 'Receptor'}** — {resumen_dir} ({es_prin_dir})")

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

                    st.divider()

                    # --- FORMULARIO DE EDICIÓN ---
                    dirs_view = dirs.copy()
                    dirs_view.insert(0, "Editar", False)
                    ed_dirs = st.data_editor(
                        dirs_view[["Editar", "id_direccion", "tipo_envio", "nombre_receptor", "distrito"]],
                        key="ed_dirs_panel",
                        column_config={"Editar": st.column_config.CheckboxColumn("✏️", width="small"), "id_direccion": None},
                        hide_index=True, use_container_width=True
                    )
                    
                    dir_sel = ed_dirs[ed_dirs["Editar"] == True]
                    if not dir_sel.empty:
                        r_dir = dirs.loc[dir_sel.index[0]]
                        
                        with st.form("form_edit_dir"):
                            st.markdown("##### 📝 Modificar Dirección Seleccionada")
                            
                            tipo_db_act = r_dir['tipo_envio']
                            tipo_ui_act = mapa_db_to_ui.get(tipo_db_act, "Otros")
                            opciones_tipo = ["Motorizado", "Agencia", "Otros"]
                            idx_tipo = opciones_tipo.index(tipo_ui_act) if tipo_ui_act in opciones_tipo else 2
                            
                            e_tipo_ui = st.selectbox("Tipo de Envío", opciones_tipo, index=idx_tipo)
                            e_tipo_db = mapa_ui_to_db[e_tipo_ui]
                            
                            c1, c2 = st.columns(2)
                            e_nom = c1.text_input("Nombre Receptor", value=r_dir['nombre_receptor'] or "")
                            e_tel = c2.text_input("Telf. Receptor", value=r_dir['telefono_receptor'] or "")
                            
                            e_dist, e_dir, e_ref, e_gps_link = None, None, None, None
                            e_dni, e_agencia, e_sede = None, None, None
                            
                            if e_tipo_db == "MOTO":
                                d1, d2 = st.columns(2)
                                e_dist = d1.text_input("Distrito", value=r_dir['distrito'] or "")
                                e_dir = d2.text_input("Dirección Exacta", value=r_dir['direccion_texto'] or "")
                                e_ref = st.text_input("Referencia", value=r_dir['referencia'] or "")
                                e_gps_link = st.text_input("Link GPS", value=r_dir['gps_link'] or "")
                            elif e_tipo_db == "AGENCIA":
                                d1, d2, d3 = st.columns(3)
                                e_dni = d1.text_input("DNI Receptor", value=r_dir['dni_receptor'] or "")
                                e_agencia = d2.text_input("Nombre Agencia", value=r_dir['agencia_nombre'] or "")
                                e_sede = d3.text_input("Sede de Entrega", value=r_dir['sede_entrega'] or "")
                                
                            e_obs = st.text_area("Observación", value=r_dir['observacion'] or "")
                            
                            if st.form_submit_button("💾 Guardar Dirección"):
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        UPDATE direcciones 
                                        SET tipo_envio=:tipo, nombre_receptor=:n, telefono_receptor=:t, distrito=:dis, 
                                            direccion_texto=:dt, referencia=:r, gps_link=:glink, dni_receptor=:dni, 
                                            agencia_nombre=:anom, sede_entrega=:sede, observacion=:obs
                                        WHERE id_direccion=:id
                                    """), {
                                        "tipo": e_tipo_db, "n": e_nom, "t": e_tel, "dis": e_dist, "dt": e_dir, "r": e_ref, 
                                        "glink": e_gps_link, "dni": e_dni, "anom": e_agencia, "sede": e_sede, "obs": e_obs, 
                                        "id": int(r_dir['id_direccion'])
                                    })
                                st.success("Dirección actualizada con éxito.")
                                time.sleep(1)
                                st.rerun()
                else:
                    st.info("El cliente no cuenta con direcciones registradas.")

                # --- FORMULARIO PARA CREAR NUEVA DIRECCIÓN ---
                with st.expander("➕ Agregar Nueva Dirección", expanded=False):
                    nn_tipo_ui = st.selectbox("Tipo de Envío para Nueva Dirección", ["Motorizado", "Agencia", "Otros"], key="sb_new_tipo")
                    nn_tipo_db = mapa_ui_to_db[nn_tipo_ui]
                    
                    with st.form("form_new_dir"):
                        n1, n2 = st.columns(2)
                        nn_nom = n1.text_input("Nombre Receptor")
                        nn_tel = n2.text_input("Telf. Receptor")
                        
                        nn_dist, nn_dir, nn_ref, nn_gps_link = None, None, None, None
                        nn_dni, nn_agencia, nn_sede = None, None, None
                        
                        if nn_tipo_db == "MOTO":
                            d1, d2 = st.columns(2)
                            nn_dist = d1.text_input("Distrito")
                            nn_dir = d2.text_input("Dirección Exacta")
                            nn_ref = st.text_input("Referencia")
                            nn_gps_link = st.text_input("Link GPS")
                        elif nn_tipo_db == "AGENCIA":
                            d1, d2, d3 = st.columns(3)
                            nn_dni = d1.text_input("DNI Receptor")
                            nn_agencia = d2.text_input("Nombre Agencia (Ej: Olva, Shalom)")
                            nn_sede = d3.text_input("Sede de Entrega")
                            
                        nn_obs = st.text_area("Observación")
                        
                        if st.form_submit_button("Crear Dirección"):
                            # Detectar si es la primera dirección activa del cliente
                            es_primera_direccion = True if dirs.empty else False
                            
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, distrito, 
                                                             direccion_texto, referencia, gps_link, dni_receptor, agencia_nombre, 
                                                             sede_entrega, observacion, activo, es_principal)
                                    VALUES (:idc, :tipo, :n, :t, :dis, :dt, :r, :glink, :dni, :anom, :sede, :obs, TRUE, :es_prin)
                                """), {
                                    "idc": id_cli_sel, "tipo": nn_tipo_db, "n": nn_nom, "t": nn_tel, "dis": nn_dist, "dt": nn_dir, 
                                    "r": nn_ref, "glink": nn_gps_link, "dni": nn_dni, "anom": nn_agencia, "sede": nn_sede, "obs": nn_obs,
                                    "es_prin": es_primera_direccion
                                })
                            st.success("Nueva dirección creada.")
                            time.sleep(1)
                            st.rerun()