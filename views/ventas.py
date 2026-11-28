import streamlit as st
import pandas as pd
import random
import time
from sqlalchemy import text
from database import engine
import os
import threading
from utils import sync_woo_background, actualizar_en_google, recalcular_comprador_en_segundo_plano

# Asegurar que existan las columnas necesarias en la base de datos
try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE Ventas ADD COLUMN IF NOT EXISTS anulado BOOLEAN DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE Ventas ADD COLUMN IF NOT EXISTS pendiente_pago NUMERIC DEFAULT 0"))
        
        conn.execute(text("ALTER TABLE Clientes ADD COLUMN IF NOT EXISTS id_etapa INTEGER"))
        conn.execute(text("ALTER TABLE DetalleVenta ADD COLUMN IF NOT EXISTS macro_categoria VARCHAR(50)"))
        conn.execute(text("ALTER TABLE DetalleVenta ADD COLUMN IF NOT EXISTS id_ubicacion INTEGER"))
except:
    pass

def render_ventas():
    # Creamos 4 pestañas para integrar todo en un solo lugar
    tab_nueva, tab_web, tab_historial, tab_facturacion, tab_reportes = st.tabs([
        "🛒 Nueva Venta / Salida", 
        "🌐 Pedidos web",
        "📜 Historial y Anulaciones",
        "🧾 Facturación Individual",
        "📊 Reportes de Facturación"
    ])

    with tab_web:
        from views.pedidos_web import render_pedidos_web
        render_pedidos_web()

    with tab_nueva:
        render_nueva_venta()

    with tab_historial:
        render_gestion_ventas()
        
    with tab_facturacion:
        render_facturacion()
        
    with tab_reportes:
        render_reporte_mensual()

def render_nueva_venta():
    
    # =========================================================================
    # ⚙️ CONFIGURACIÓN DE BOTONES DE ACCESO RÁPIDO
    # Agrega, edita o elimina los diccionarios de esta lista para crear botones.
    # - Si tiene stock real en DB: Pon el "sku" exacto (ej. "ACC-SOL-01").
    # - Si es un ítem manual libre: Pon "sku": None y llena "desc_manual".
    # =========================================================================
    ACCESOS_RAPIDOS = [
        {"label": "🎁 Promo Multimax 60ml", "sku": "7751030000173", "precio": 0.0, "desc_manual": "", "macro": "Lentes"},
        {"label": "🎁 Promo Multimax 120ml", "sku": "7751030000494", "precio": 0.0, "desc_manual": "", "macro": "Lentes"},
        {"label": "Pinza y Aplicador", "sku": None, "precio": 2.0, "desc_manual": "Pinza y aplicador", "macro": "Lentes"},
        # Puedes seguir añadiendo más aquí... {"label": "Botón Nuevo", ...}
    ]

    # --- FUNCIÓN AUXILIAR LOCAL ---
    def agregar_al_carrito(sku, nombre, cantidad, precio, es_inventario, stock_max=None, macro_cat="Otros", id_ubi=None, nombre_ubi=""):
        for item in st.session_state.carrito:
            # Agrupar si es el mismo SKU y el mismo estante
            if item['sku'] == sku and sku is not None and item.get('id_ubicacion') == id_ubi:
                if es_inventario and (item['cantidad'] + cantidad) > stock_max:
                    st.error(f"❌ Stock insuficiente en el estante {nombre_ubi or 'General'}. Disponibles: {stock_max}, En carrito: {item['cantidad']}")
                    return
                item['cantidad'] += int(cantidad)
                item['subtotal'] = item['cantidad'] * item['precio']
                st.toast(f"Actualizado: {nombre}")
                return

        if es_inventario and cantidad > stock_max:
            st.error(f"❌ Stock insuficiente en el estante {nombre_ubi or 'General'}. Disponibles: {stock_max}")
            return

        st.session_state.carrito.append({
            "sku": sku,
            "descripcion": nombre + (f" (Ubi: {nombre_ubi})" if nombre_ubi else ""),
            "cantidad": int(cantidad),
            "precio": float(precio),
            "subtotal": float(precio * cantidad),
            "es_inventario": es_inventario,
            "macro_categoria": macro_cat,
            "id_ubicacion": id_ubi,
            "nombre_ubicacion": nombre_ubi
        })
        st.success(f"Añadido: {nombre}")

    def procesar_acceso_rapido(btn_conf):
        if btn_conf['sku']:
            # Ítem con inventario (busca su info para descontar)
            with engine.connect() as conn:
                res = pd.read_sql(text("""
                    SELECT v.sku, p.modelo, p.nombre as color, v.medida, v.stock_interno, v.precio, COALESCE(p.macro_categoria, 'Lentes') as macro_categoria
                    FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku = :sku
                """), conn, params={"sku": btn_conf['sku']})
                
                if not res.empty:
                    prod = res.iloc[0]
                    if prod['stock_interno'] <= 0:
                        st.error(f"❌ Sin stock físico para el SKU: {btn_conf['sku']}")
                        return
                        
                    nombre_full = f"{prod['modelo']} {prod['color']} ({prod['medida']})"
                    
                    # Extraer de la primera ubicación disponible
                    df_ubis = pd.read_sql(text("""
                        SELECT su.id_ubicacion, u.nombre || COALESCE(' · ' || um.marca, '') AS nombre, su.cantidad
                        FROM Stock_Ubicaciones su
                        JOIN Ubicaciones_Estandar u ON su.id_ubicacion = u.id_ubicacion
                        LEFT JOIN Ubicacion_Marca um ON um.sku = su.sku AND um.id_ubicacion = su.id_ubicacion
                        WHERE su.sku = :sku AND su.cantidad > 0
                        LIMIT 1
                    """), conn, params={"sku": btn_conf['sku']})
                    
                    id_ubi, nombre_ubi = None, ""
                    if not df_ubis.empty:
                        id_ubi = int(df_ubis.iloc[0]['id_ubicacion'])
                        nombre_ubi = df_ubis.iloc[0]['nombre']
                        
                    agregar_al_carrito(prod['sku'], nombre_full, 1, btn_conf['precio'], True, prod['stock_interno'], prod['macro_categoria'], id_ubi, nombre_ubi)
                else:
                    st.error(f"❌ El SKU '{btn_conf['sku']}' configurado en el botón no existe en la base de datos.")
        else:
            # Ítem manual sin inventario
            agregar_al_carrito(None, f"[{btn_conf['macro']}] {btn_conf['desc_manual']}", 1, btn_conf['precio'], False, None, btn_conf['macro'])


    # --- INICIO DE LA VISTA ---
    col_modo, col_titulo = st.columns([1, 3])
    with col_modo:
        modo_operacion = st.radio("Modo:", ["💰 Venta", "📉 Salida / Merma"], horizontal=True)
    with col_titulo:
        if modo_operacion == "💰 Venta":
            st.subheader("🛒 Punto de Venta (Ingresos)")
        else:
            st.subheader("📉 Registro de Salidas (Mermas / Uso Interno)")

    st.divider()

    col_izq, col_der = st.columns([1, 1])

    # ------------------------------------------------------------------
    # COLUMNA IZQUIERDA: BUSCADOR
    # ------------------------------------------------------------------
    with col_izq:
        st.caption("1. Buscar Productos o Ingresar Manual")
        
        # --- MEJORA 1: BOTONES DINÁMICOS DE ACCESO RÁPIDO ---
        st.write("⚡ **Accesos Rápidos:**")
        cols_per_row = 3
        for i in range(0, len(ACCESOS_RAPIDOS), cols_per_row):
            cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                if i + j < len(ACCESOS_RAPIDOS):
                    btn_conf = ACCESOS_RAPIDOS[i + j]
                    if cols[j].button(btn_conf["label"], key=f"btn_qr_{i+j}", use_container_width=True):
                        procesar_acceso_rapido(btn_conf)
        st.write("")

        tipo_producto = st.radio("Origen:", ["Inventario (SQL)", "Manual/Extra"], horizontal=True, label_visibility="collapsed")
        
        if tipo_producto == "Inventario (SQL)":
            
            sku_input = st.text_input("Escanear SKU o Buscar Nombre:", placeholder="Ej: CL-01 o 'Solución'...", key="sku_pos")
            
            if sku_input:
                with engine.connect() as conn:
                    # --- MEJORA 2: BÚSQUEDA AMPLIA FILTRANDO SOLO LOS QUE TIENEN STOCK (>0) ---
                    res = pd.read_sql(text("""
                        SELECT v.sku, p.modelo, p.nombre as color, v.medida, v.stock_interno, v.precio, COALESCE(p.macro_categoria, 'Lentes') as macro_categoria
                        FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE (v.sku ILIKE :sku OR p.nombre ILIKE :sku OR p.modelo ILIKE :sku)
                          AND v.stock_interno > 0
                        LIMIT 15
                    """), conn, params={"sku": f"%{sku_input}%"})
                
                if not res.empty:
                    if len(res) > 1:
                        st.warning(f"Se encontraron {len(res)} coincidencias con stock:")
                        opciones_prod = {row['sku']: f"[{row['sku']}] {row['modelo']} {row['color']} ({row['medida']}) - S/ {row['precio']}" for _, row in res.iterrows()}
                        sku_seleccionado = st.selectbox("Selecciona el producto correcto:", options=list(opciones_prod.keys()), format_func=lambda x: opciones_prod[x], label_visibility="collapsed")
                        prod = res[res['sku'] == sku_seleccionado].iloc[0]
                    else:
                        prod = res.iloc[0]
                        
                    nombre_full = f"{prod['modelo']} {prod['color']} ({prod['medida']})"
                    st.success(f"✅ Stock Físico Total: {prod['stock_interno']}")
                    st.markdown(f"**{nombre_full}**")
                    
                    with engine.connect() as conn:
                        df_ubis = pd.read_sql(text("""
                            SELECT su.id_ubicacion, u.nombre || COALESCE(' · ' || um.marca, '') AS nombre, su.cantidad
                            FROM Stock_Ubicaciones su
                            JOIN Ubicaciones_Estandar u ON su.id_ubicacion = u.id_ubicacion
                            LEFT JOIN Ubicacion_Marca um ON um.sku = su.sku AND um.id_ubicacion = su.id_ubicacion
                            WHERE su.sku = :sku AND su.cantidad > 0
                            ORDER BY um.marca NULLS LAST, u.nombre
                        """), conn, params={"sku": prod['sku']})
                    
                    ubi_selec_id = None
                    ubi_selec_nombre = ""
                    max_disp = prod['stock_interno']

                    if df_ubis.empty and prod['stock_interno'] > 0:
                        st.warning("⚠️ Este producto no ha sido distribuido en ubicaciones. Se descontará del general.")
                    elif not df_ubis.empty:
                        st.markdown("📍 **Selecciona el estante (y marca) de retiro:**")
                        mapa_ubis = {f"🗄️ {row['nombre']} (Disp: {row['cantidad']} un.)": (row['id_ubicacion'], row['nombre'], row['cantidad']) for _, row in df_ubis.iterrows()}
                        sel_ubi = st.selectbox("Extraer de:", list(mapa_ubis.keys()), label_visibility="collapsed")
                        ubi_selec_id, ubi_selec_nombre, max_disp = mapa_ubis[sel_ubi]

                    c1, c2 = st.columns(2)
                    cantidad = c1.number_input("Cant.", min_value=1, max_value=int(max_disp) if max_disp > 0 else 1, value=1)
                    precio_sugerido = float(prod['precio']) if modo_operacion == "💰 Venta" else 0.0
                    precio_final = c2.number_input("Precio Unit.", value=precio_sugerido, disabled=(modo_operacion != "💰 Venta"))
                    
                    if st.button("➕ Agregar al Carrito", disabled=(max_disp <= 0)):
                        agregar_al_carrito(prod['sku'], nombre_full, cantidad, precio_final, True, max_disp, prod['macro_categoria'], ubi_selec_id, ubi_selec_nombre)
                else:
                    st.warning("Producto sin stock o no encontrado en inventario.")
        
        else: 
            st.info("📦 Ítem Manual / Extra (Sin SKU en Base de Datos)")
            macro_manual = st.selectbox("Macrocategoría:", ["Pelucas", "Lentes", "Otros"], key="macro_man")
            desc_manual = st.text_input("Detalle / Descripción del producto o servicio:", placeholder="Ej: Peinado especial, redecilla extra...")
            
            c1, c2 = st.columns(2)
            cant_manual = c1.number_input("Cant.", min_value=1, value=1, key="cm")
            precio_manual = c2.number_input("Precio Unit.", value=0.0, key="pm", disabled=(modo_operacion != "💰 Venta"))
            
            if st.button("➕ Agregar Ítem Manual"):
                if desc_manual and desc_manual.strip():
                    nombre_formateado = f"[{macro_manual}] {desc_manual.strip()}"
                    agregar_al_carrito(None, nombre_formateado, cant_manual, precio_manual, False, None, macro_manual)
                else:
                    st.warning("⚠️ Debes ingresar el detalle o descripción del ítem.")

    # ------------------------------------------------------------------
    # COLUMNA DERECHA: PROCESAR (CARRITO EDITABLE)
    # ------------------------------------------------------------------
    with col_der:
        st.caption("2. Confirmación (Puedes editar Cantidad/Precio o eliminar filas)")
        
        if len(st.session_state.carrito) > 0:
            df_cart = pd.DataFrame(st.session_state.carrito)
            
            edited_cart = st.data_editor(
                df_cart,
                column_config={
                    "cantidad": st.column_config.NumberColumn("Cant.", min_value=1, step=1, width="small"),
                    "macro_categoria": st.column_config.TextColumn("Línea", disabled=True, width="small"),
                    "precio": st.column_config.NumberColumn("Precio", min_value=0.0, width="small"),
                    "subtotal": st.column_config.NumberColumn("Subtotal", disabled=True, width="small"),
                    "descripcion": st.column_config.TextColumn("Descripción", width="medium"),
                    "sku": st.column_config.TextColumn("SKU", disabled=True),
                    "nombre_ubicacion": st.column_config.TextColumn("Estante", disabled=True, width="small"),
                    "id_ubicacion": None,
                    "es_inventario": None
                },
                num_rows="dynamic",
                hide_index=True,
                use_container_width=True,
                key="editor_carrito"
            )

            edited_cart['subtotal'] = edited_cart['cantidad'] * edited_cart['precio']
            st.session_state.carrito = edited_cart.to_dict('records')
            
            suma_subtotal = float(edited_cart['subtotal'].sum()) if not edited_cart.empty else 0.0
            
            st.divider()

            if edited_cart.empty:
                st.warning("El carrito está vacío.")
                return

            # ==========================================================
            # MODO A: VENTA
            # ==========================================================
            if modo_operacion == "💰 Venta":
                st.markdown(f"**Subtotal Items:** S/ {suma_subtotal:.2f}")

                with engine.connect() as conn:
                    grupos_df = pd.read_sql(text("SELECT DISTINCT grupo FROM EtapasCliente WHERE activo = TRUE ORDER BY grupo"), conn)
                
                lista_grupos = ["Todos"] + grupos_df['grupo'].tolist()
                idx_default = lista_grupos.index("Etapa 2") if "Etapa 2" in lista_grupos else 0
                
                col_g, col_c = st.columns(2)
                grupo_sel = col_g.selectbox("Filtrar por Grupo:", options=lista_grupos, index=idx_default)

                with engine.connect() as conn:
                    if grupo_sel == "Todos":
                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto,
                                   COALESCE(
                                       (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       (SELECT lid FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       'Sin Contacto'
                                   ) as identificador
                            FROM Clientes c 
                            WHERE c.activo = TRUE 
                            ORDER BY c.nombre_corto
                        """)
                        cli_df = pd.read_sql(query_clientes, conn)
                    else:
                        query_clientes = text("""
                            SELECT c.id_cliente, c.nombre_corto,
                                   COALESCE(
                                       (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       (SELECT lid FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                                       'Sin Contacto'
                                   ) as identificador
                            FROM Clientes c
                            JOIN EtapasCliente e ON c.id_etapa = e.id_etapa
                            WHERE c.activo = TRUE AND e.grupo = :grupo
                            ORDER BY c.nombre_corto
                        """)
                        cli_df = pd.read_sql(query_clientes, conn, params={"grupo": grupo_sel})

                # Diccionario blindado: Ahora muestra "Nombre (Teléfono/LID)" para evitar colisiones
                lista_cli = {f"{row['nombre_corto']} ({row['identificador']})": row['id_cliente'] for i, row in cli_df.iterrows()}
                
                if not lista_cli:
                    col_c.warning(f"No hay clientes en {grupo_sel}.")
                    st.stop()

                nombre_cli = col_c.selectbox("Cliente:", options=list(lista_cli.keys()))
                id_cliente = lista_cli[nombre_cli]

                costo_envio = st.number_input("Costo de Envío (Adicional al subtotal):", min_value=0.0, value=0.0, step=1.0)

                with engine.connect() as conn:
                    q_dir = text("SELECT * FROM Direcciones WHERE id_cliente = :id AND activo = TRUE ORDER BY es_principal DESC, id_direccion DESC")
                    df_dirs = pd.read_sql(q_dir, conn, params={"id": id_cliente})

                usar_guardada = False
                datos_nuevos = {} 
                texto_direccion_final = ""
                tipo_envio_db = "OTROS"
                tipo_envio_ui = "Otros"
                
                opciones_visuales = {}
                if not df_dirs.empty:
                    for idx, row in df_dirs.iterrows():
                        t_db = row.get('tipo_envio', 'OTROS')
                        
                        if t_db == 'MOTO':
                            lbl = f"🏠 [Moto] {row.get('direccion_texto', '')} - {row.get('distrito', '')}"
                            if row.get('referencia') and pd.notna(row['referencia']): lbl += f" (Ref: {row['referencia']})"
                        elif t_db == 'AGENCIA':
                            lbl = f"🏢 [Age] {row.get('agencia_nombre', '')} - {row.get('sede_entrega', '')}"
                        else:
                            lbl = f"📦 [Otro] {str(row.get('observacion', ''))[:30]}"
                        
                        if row.get('es_principal'): lbl = "⭐ " + lbl
                        lbl += f" [ID:{row['id_direccion']}]"
                        
                        opciones_visuales[lbl] = row

                KEY_NUEVA = "➕ Usar una Nueva Dirección..."
                lista_desplegable = list(opciones_visuales.keys()) + [KEY_NUEVA]
                
                st.markdown("📍 **Datos de Entrega:**")
                seleccion_dir = st.selectbox("Elige destino:", options=lista_desplegable, label_visibility="collapsed")
                
                if seleccion_dir != KEY_NUEVA:
                    usar_guardada = True
                    dir_data = opciones_visuales[seleccion_dir]
                    tipo_envio_db = dir_data.get('tipo_envio', 'OTROS')
                    mapa_db_to_ui = {"MOTO": "Motorizado", "AGENCIA": "Agencia", "OTROS": "Otros"}
                    tipo_envio_ui = mapa_db_to_ui.get(tipo_envio_db, "Otros")
                    
                    if tipo_envio_db == 'AGENCIA':
                        texto_direccion_final = f"{dir_data.get('agencia_nombre', '')} - {dir_data.get('sede_entrega', '')} [DNI: {dir_data.get('dni_receptor', '')}]"
                        st.info(f"📦 Destino: **{texto_direccion_final}**")
                    elif tipo_envio_db == 'MOTO':
                        texto_direccion_final = f"{dir_data.get('direccion_texto', '')} - {dir_data.get('distrito', '')}"
                        if dir_data.get('referencia') and pd.notna(dir_data['referencia']): texto_direccion_final += f" (Ref: {dir_data['referencia']})"
                        st.info(f"🏠 Destino: **{texto_direccion_final}**")
                    else:
                        texto_direccion_final = f"{dir_data.get('observacion', 'Otros')}"
                        st.info(f"📍 Destino: **{texto_direccion_final}**")
                        
                    if not dir_data.get('es_principal'):
                        if st.button("⭐ Establecer como Dirección Principal", help="Hará que esta sea la dirección por defecto del cliente."):
                            with engine.begin() as tx:
                                tx.execute(text("UPDATE Direcciones SET es_principal=FALSE WHERE id_cliente=:id"), {"id": id_cliente})
                                tx.execute(text("UPDATE Direcciones SET es_principal=TRUE WHERE id_direccion=:idd"), {"idd": int(dir_data['id_direccion'])})
                            st.success("¡Dirección principal actualizada!")
                            time.sleep(1)
                            st.rerun()

                else:
                    st.warning("📝 Registro de Nuevos Datos de Envío:")
                    tipo_envio_ui = st.selectbox("Método de Envío", ["Motorizado", "Agencia", "Otros"])
                    mapa_ui_to_db = {"Motorizado": "MOTO", "Agencia": "AGENCIA", "Otros": "OTROS"}
                    tipo_envio_db = mapa_ui_to_db[tipo_envio_ui]
                    
                    with st.container(border=True):
                        c_nom, c_tel = st.columns(2)
                        recibe = c_nom.text_input("Nombre Recibe:", value=nombre_cli)
                        telf = c_tel.text_input("Teléfono:", key="telf_new")
                        
                        direcc, dist, ref, gps_link, dni, agencia, sede, obs_extra = "", "", "", "", "", "", "", ""
                        
                        if tipo_envio_ui == "Motorizado":
                            direcc = st.text_input("Dirección Exacta:")
                            c_dist, c_ref = st.columns(2)
                            dist = c_dist.text_input("Distrito:")
                            ref = c_ref.text_input("Referencia:")
                            gps_link = st.text_input("📍 Link GPS:")
                            obs_extra = st.text_input("Observación:")
                            texto_direccion_final = f"{direcc} - {dist} (Ref: {ref})"
                            
                        elif tipo_envio_ui == "Agencia":
                            c_dni, c_age = st.columns(2)
                            dni = c_dni.text_input("DNI:")
                            agencia = c_age.text_input("Agencia:", value="Shalom")
                            sede = st.text_input("Sede:")
                            obs_extra = st.text_input("Obs:")
                            texto_direccion_final = f"{agencia} - {sede}"
                            
                        else:
                            obs_extra = st.text_input("Observación / Lugar:")
                            texto_direccion_final = "Entrega Directa / Otro"

                        datos_nuevos = {
                            "tipo": tipo_envio_db, "nom": recibe, "tel": telf, 
                            "dir": direcc, "dist": dist, "ref": ref, "glink": gps_link, 
                            "dni": dni, "age": agencia, "sede": sede, "obs": obs_extra
                        }

                clave_agencia = None
                es_agencia_clave = (tipo_envio_db == 'AGENCIA')
                
                if es_agencia_clave:
                    if 'clave_temp' not in st.session_state: st.session_state['clave_temp'] = str(random.randint(1000, 9999))
                    col_k1, col_k2 = st.columns([1,2])
                    clave_agencia = col_k1.text_input("Clave", value=st.session_state['clave_temp'])
                    col_k2.info("🔐 Clave de Entrega (Requerido para Agencia)")

                total_final = suma_subtotal + costo_envio
                
                st.divider()
                st.markdown(f"### 💰 Total a Cobrar: S/ {total_final:.2f}")
                
                c_adelanto, c_restante = st.columns(2)
                adelanto = c_adelanto.number_input("Adelanto pagado (S/):", min_value=0.0, max_value=float(total_final), value=float(total_final), step=1.0)
                restante = total_final - adelanto
                
                if restante > 0:
                    c_restante.error(f"⚠️ Restante por cobrar: S/ {restante:.2f}")
                else:
                    c_restante.success("✅ Pagado en su totalidad")
                
                nota_venta = st.text_input("Nota Interna:", placeholder="Opcional")

                if st.button("✅ REGISTRAR VENTA", type="primary", use_container_width=True):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            
                            has_macro_col = False
                            try:
                                conn.execute(text("SELECT macro_categoria FROM DetalleVenta LIMIT 1"))
                                has_macro_col = True
                            except: pass

                            has_ubi_col = False
                            try:
                                conn.execute(text("SELECT id_ubicacion FROM DetalleVenta LIMIT 1"))
                                has_ubi_col = True
                            except: pass

                            if not usar_guardada and datos_nuevos:
                                conn.execute(text("UPDATE Direcciones SET es_principal = FALSE WHERE id_cliente = :id"), {"id": id_cliente})
                                
                                conn.execute(text("""
                                    INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, referencia, gps_link, dni_receptor, agencia_nombre, sede_entrega, observacion, activo, es_principal)
                                    VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :ref, :glink, :dni, :age, :sede, :obs, TRUE, TRUE)
                                """), {"id": id_cliente, **datos_nuevos})

                            nota_full = f"{nota_venta} | Envío: {texto_direccion_final}"
                            
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad, pendiente_pago)
                                VALUES (:idc, :tipo, :costo, :total, :nota, :clave, :pendiente) RETURNING id_venta
                            """), {"idc": id_cliente, "tipo": tipo_envio_ui, "costo": costo_envio, "total": total_final, "nota": nota_full, "clave": clave_agencia, "pendiente": restante})
                            
                            id_venta = res_v.fetchone()[0]

                            for item in st.session_state.carrito:
                                mac_val = item.get('macro_categoria', 'Otros')
                                id_ubi = item.get('id_ubicacion')
                                
                                if pd.isna(id_ubi):
                                    id_ubi = None
                                else:
                                    id_ubi = int(id_ubi)
                                
                                # 💡 NUEVA LÍNEA: Convertir NaN de Pandas a None para SQL
                                sku_val = None if pd.isna(item['sku']) else item['sku']
                                
                                if has_macro_col and has_ubi_col:
                                    conn.execute(text("""
                                        INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario, macro_categoria, id_ubicacion)
                                        VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv, :mac, :idu)
                                    """), {"idv": id_venta, "sku": sku_val, "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario'], "mac": mac_val, "idu": id_ubi})
                                elif has_macro_col:
                                    conn.execute(text("""
                                        INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario, macro_categoria)
                                        VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv, :mac)
                                    """), {"idv": id_venta, "sku": sku_val, "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario'], "mac": mac_val})
                                else:
                                    conn.execute(text("""
                                        INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                        VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv)
                                    """), {"idv": id_venta, "sku": sku_val, "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario']})

                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                         {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    
                                    if id_ubi is not None:
                                        conn.execute(text("UPDATE Stock_Ubicaciones SET cantidad = cantidad - :c WHERE sku = :s AND id_ubicacion = :idu"),
                                                     {"c": int(item['cantidad']), "s": item['sku'], "idu": id_ubi})
                                    elif nuevo_s <= 0: 
                                        conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    nota_mov = f"Venta #{id_venta}" + (f" ({item['nombre_ubicacion']})" if item.get('nombre_ubicacion') else "")
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, id_cliente) 
                                        VALUES (:sku, 'VENTA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota, :idc)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": nota_mov, "idc": id_cliente})
                            
                            trans.commit()
                        
                        skus_vendidos = [item["sku"] for item in st.session_state.carrito if item["sku"] is not None]
                        if skus_vendidos:
                            threading.Thread(target=sync_woo_background, args=(skus_vendidos,)).start()

                        st.balloons()
                        recalcular_comprador_en_segundo_plano(id_cliente)
                        st.success(f"¡Venta #{id_venta} registrada exitosamente!")
                        st.session_state.carrito = []
                        if 'clave_temp' in st.session_state: del st.session_state['clave_temp']
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al registrar la venta: {e}")

            # ==========================================================
            # MODO B: SALIDA (Merma)
            # ==========================================================
            else:
                st.warning("⚠️ Estás registrando una salida de stock (Sin cobro).")
                motivo_salida = st.selectbox("Motivo:", ["Merma / Dañado", "Regalo / Marketing", "Uso Personal", "Ajuste Inventario"])
                detalle_motivo = st.text_input("Detalle (Opcional):", placeholder="Ej: Se rompió una luna...")
                
                if st.button("📉 CONFIRMAR SALIDA", type="primary"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            items_procesados = 0
                            for item in st.session_state.carrito:
                                if item['es_inventario']:
                                    id_ubi = item.get('id_ubicacion')
                                    
                                    if pd.isna(id_ubi):
                                        id_ubi = None
                                    else:
                                        id_ubi = int(id_ubi)
                                        
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                        {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    
                                    if id_ubi is not None:
                                        conn.execute(text("UPDATE Stock_Ubicaciones SET cantidad = cantidad - :c WHERE sku = :sku AND id_ubicacion = :idu"),
                                                     {"c": int(item['cantidad']), "sku": item['sku'], "idu": id_ubi})
                                    elif nuevo_s <= 0: 
                                        conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                        
                                    nota_completa = f"{motivo_salida}" + (f" - {detalle_motivo}" if detalle_motivo else "") + (f" (Estante: {item['nombre_ubicacion']})" if item.get('nombre_ubicacion') else "")
                                    conn.execute(text("""
                                            INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                            VALUES (:sku, 'SALIDA', :c, :ant, :nue, :nota)
                                        """), {"sku": item['sku'], "c": int(item['cantidad']), "ant": nuevo_s + int(item['cantidad']), "nue": nuevo_s, "nota": nota_completa})
                                    items_procesados += 1
                            trans.commit()

                        skus_vendidos = [item["sku"] for item in st.session_state.carrito if item["sku"] is not None]
                        if skus_vendidos:
                            threading.Thread(target=sync_woo_background, args=(skus_vendidos,)).start()

                        if items_procesados > 0:
                            st.success(f"✅ ¡Salida registrada! ({items_procesados} productos actualizados)")
                        else:
                            st.warning("⚠️ El carrito solo tenía ítems manuales; no se descontó inventario físico.")
                            
                        st.session_state.carrito = []
                        time.sleep(1.5)
                        st.rerun()

                    except Exception as e:
                        st.error(f"❌ Error al procesar la salida: {e}")

def render_gestion_ventas():
    st.subheader("📜 Búsqueda y Anulación de Ventas")
    
    with engine.connect() as conn:
        query = text("""
            SELECT v.id_venta, v.fecha_venta, c.nombre_corto as cliente, 
                   COALESCE(
                       (SELECT telefono FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                       (SELECT lid FROM telefonoscliente WHERE id_cliente = c.id_cliente AND es_principal = TRUE AND activo = TRUE LIMIT 1),
                       'Sin Contacto'
                   ) as identificador,
                   v.total_venta, v.nota, v.anulado
            FROM Ventas v
            LEFT JOIN Clientes c ON v.id_cliente = c.id_cliente
            ORDER BY v.id_venta DESC LIMIT 100
        """)
        df_ventas = pd.read_sql(query, conn)
        
    if df_ventas.empty:
        st.info("No hay ventas registradas.")
        return

    # Inyectamos el identificador dinámico en la lista de opciones
    opciones = df_ventas.apply(
        lambda row: f"#{row['id_venta']} | {row['fecha_venta'].strftime('%d/%m/%Y %H:%M') if pd.notnull(row['fecha_venta']) else ''} | {row['cliente']} ({row['identificador']}) | S/ {row['total_venta']} {'(❌ ANULADA)' if row['anulado'] else ''}", 
        axis=1
    ).tolist()
    
    mapa_ids = dict(zip(opciones, df_ventas['id_venta']))
    
    seleccion = st.selectbox("Selecciona una venta reciente (Últimas 100):", opciones)
    id_venta_sel = mapa_ids[seleccion]
    
    with engine.connect() as conn:
        venta_info = conn.execute(text("SELECT * FROM Ventas WHERE id_venta = :id"), {"id": int(id_venta_sel)}).fetchone()
        
        has_mac = False
        try:
            conn.execute(text("SELECT macro_categoria FROM DetalleVenta LIMIT 1"))
            has_mac = True
        except: pass

        has_ubi_col = False
        try:
            conn.execute(text("SELECT id_ubicacion FROM DetalleVenta LIMIT 1"))
            has_ubi_col = True
        except: pass

        if has_mac and has_ubi_col:
            detalles = pd.read_sql(text("SELECT sku, macro_categoria as linea, descripcion, cantidad, precio_unitario, subtotal, es_inventario, id_ubicacion FROM DetalleVenta WHERE id_venta = :id"), conn, params={"id": int(id_venta_sel)})
        elif has_mac:
            detalles = pd.read_sql(text("SELECT sku, macro_categoria as linea, descripcion, cantidad, precio_unitario, subtotal, es_inventario FROM DetalleVenta WHERE id_venta = :id"), conn, params={"id": int(id_venta_sel)})
        else:
            detalles = pd.read_sql(text("SELECT sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario FROM DetalleVenta WHERE id_venta = :id"), conn, params={"id": int(id_venta_sel)})

    st.markdown(f"### Detalle de Venta #{id_venta_sel}")
    if venta_info.anulado:
        st.error("⚠️ ESTA VENTA SE ENCUENTRA ANULADA.")
    
    st.dataframe(detalles.drop(columns=['id_ubicacion'], errors='ignore'), use_container_width=True, hide_index=True)
    st.caption(f"**Nota de Venta:** {venta_info.nota}")
    
    if not venta_info.anulado:
        st.warning("Al anular, se devolverá el stock a la ubicación física de origen automáticamente.")
        if st.button("🚫 Anular Venta y Devolver Stock", type="primary"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text("UPDATE Ventas SET anulado = TRUE WHERE id_venta = :id"), {"id": int(id_venta_sel)})
                    
                    for idx, item in detalles.iterrows():
                        if item['es_inventario']:
                            # Restaurar Stock Global
                            res = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno + :c WHERE sku = :s RETURNING stock_interno"), 
                                               {"c": int(item['cantidad']), "s": item['sku']})
                            nuevo_stock = res.scalar()
                            
                            # Restaurar en el Estante Exacto
                            if has_ubi_col and pd.notna(item.get('id_ubicacion')):
                                conn.execute(text("""
                                    UPDATE Stock_Ubicaciones 
                                    SET cantidad = cantidad + :c 
                                    WHERE sku = :sku AND id_ubicacion = :idu
                                """), {"c": int(item['cantidad']), "sku": item['sku'], "idu": int(item['id_ubicacion'])})
                            
                            conn.execute(text("""
                                INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                VALUES (:s, 'ANULACION', :c, :ant, :nue, :nota)
                            """), {
                                "s": item['sku'], "c": int(item['cantidad']), 
                                "ant": nuevo_stock - int(item['cantidad']), 
                                "nue": nuevo_stock, "nota": f"Anulación Venta #{id_venta_sel}"
                            })
                    trans.commit()

                    skus_anulados = [item['sku'] for idx, item in detalles.iterrows() if item['es_inventario'] and item['sku'] is not None]
                    if skus_anulados:
                        threading.Thread(target=sync_woo_background, args=(skus_anulados,)).start()

                    recalcular_comprador_en_segundo_plano(getattr(venta_info, 'id_cliente', None))
                    st.success("✅ Venta anulada y stock restaurado en su ubicación original.")
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error al anular: {e}")

SUNAT_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "recurrente", "boletas_sunat.py")
SUNAT_LOG = os.path.expanduser("~/logs_sunat/boletas.log")
SUNAT_LOCK = os.path.expanduser("~/.boletas_sunat.lock")


def _sunat_en_marcha():
    import fcntl
    try:
        with open(SUNAT_LOCK, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    except Exception:
        return False


def render_boletas_auto():
    """Boletas automáticas en SUNAT (recurrente/boletas_sunat.py): límites, botón y registro."""
    import subprocess
    import sys
    from datetime import datetime
    from zoneinfo import ZoneInfo

    with st.expander("🤖 Boletas automáticas en SUNAT (todos los días 05:00)", expanded=False):
        try:
            with engine.begin() as conn:
                conn.execute(text("""CREATE TABLE IF NOT EXISTS sunat_config (
                    clave VARCHAR(40) PRIMARY KEY, valor TEXT, actualizado TIMESTAMP DEFAULT NOW())"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS sunat_boletas_log (
                    id SERIAL PRIMARY KEY, fecha TIMESTAMP DEFAULT (NOW() - INTERVAL '5 hours'),
                    corrida VARCHAR(40), origen VARCHAR(20), id_venta INTEGER, monto NUMERIC(10,2),
                    estado VARCHAR(20), boleta VARCHAR(30), documento VARCHAR(40), detalle TEXT)"""))
                cfg = dict(conn.execute(text("SELECT clave, valor FROM sunat_config")).fetchall())
                hoy = datetime.now(ZoneInfo("America/Lima")).date()
                n_hoy = conn.execute(text("""SELECT COUNT(*) FROM ventas WHERE fecha_facturacion = :h
                    AND numero_boleta LIKE 'EB%%' AND anulado = FALSE"""), {"h": hoy}).scalar()
                mes = conn.execute(text("""SELECT COALESCE(SUM(total_venta), 0) FROM ventas
                    WHERE numero_boleta LIKE 'EB%%' AND anulado = FALSE
                      AND fecha_facturacion >= date_trunc('month', CAST(:h AS date))
                      AND fecha_facturacion <= :h"""), {"h": hoy}).scalar()
        except Exception as e:
            st.error(f"No se pudo leer la configuración: {e}")
            return

        activo = str(cfg.get("activo", "1")) == "1"
        monto_max = float(cfg.get("monto_max", 400))
        max_dia = int(cfg.get("max_dia", 10))
        max_mes = float(cfg.get("max_mes", 5000))
        espera_seg = int(float(cfg.get("espera_seg", 30)))

        c1, c2 = st.columns(2)
        c1.metric("Boletas emitidas hoy", f"{n_hoy} / {max_dia}")
        c2.metric("Emitido este mes", f"S/ {float(mes):,.2f}", f"tope S/ {max_mes:,.0f}", delta_color="off")

        st.caption("Reglas: ventas de días anteriores, de la más antigua a la más nueva; se saltan las de pago "
                   "pendiente, las mayores al máximo por venta y las que repiten el monto de la boleta anterior del "
                   "mismo día. Si el portal de SUNAT falla, se detiene y sigue al día siguiente.")

        with st.form("form_sunat_cfg"):
            n_activo = st.checkbox("Emisión automática activada (05:00)", value=activo)
            f1, f2, f3 = st.columns(3)
            n_monto = f1.number_input("Máximo por venta (S/)", min_value=1.0, value=monto_max, step=10.0)
            n_dia = f2.number_input("Máximo de boletas por día", min_value=0, value=max_dia, step=1)
            n_mes = f3.number_input("Máximo emitido al mes (S/)", min_value=0.0, value=max_mes, step=100.0)
            n_espera = st.number_input("Segundos de espera entre boletas (SUNAT exige al menos 20)",
                                       min_value=20, value=max(espera_seg, 20), step=5)
            if st.form_submit_button("💾 Guardar límites"):
                with engine.begin() as conn:
                    for k, v in {"activo": "1" if n_activo else "0", "monto_max": str(n_monto),
                                 "max_dia": str(int(n_dia)), "max_mes": str(n_mes),
                                 "espera_seg": str(int(n_espera))}.items():
                        conn.execute(text("""INSERT INTO sunat_config (clave, valor, actualizado) VALUES (:k, :v, NOW())
                            ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor, actualizado = NOW()"""),
                                     {"k": k, "v": v})
                st.success("Límites guardados.")
                st.rerun()

        en_marcha = _sunat_en_marcha()
        if en_marcha:
            st.info("⏳ Hay una emisión de boletas en marcha. Actualiza la página en unos minutos para ver el resultado.")
        if st.button("🚀 Emitir boletas ahora", type="primary", disabled=en_marcha,
                     help="Emite en SUNAT las boletas que tocan hoy según los límites (lo mismo que hace el proceso de las 05:00)."):
            os.makedirs(os.path.dirname(SUNAT_LOG), exist_ok=True)
            with open(SUNAT_LOG, "a") as log:
                subprocess.Popen([sys.executable, "-u", SUNAT_SCRIPT, "--aplicar", "--origen=panel"],
                                 cwd=os.path.dirname(os.path.dirname(SUNAT_SCRIPT)),
                                 stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            st.success("Emisión iniciada. Cada boleta tarda alrededor de un minuto; actualiza para ver el avance.")
            time.sleep(2)
            st.rerun()

        with engine.connect() as conn:
            revisar = pd.read_sql(text("""
                SELECT l.id, l.fecha, l.id_venta, l.monto, l.boleta, l.detalle FROM sunat_boletas_log l
                WHERE l.estado = 'POR_REVISAR' AND NOT EXISTS (
                    SELECT 1 FROM sunat_boletas_log r WHERE r.estado = 'REVISADA'
                      AND r.id_venta = l.id_venta AND r.id > l.id)
                ORDER BY l.id"""), conn)
            registro = pd.read_sql(text("""
                SELECT fecha AS "Fecha", origen AS "Origen", id_venta AS "Venta", monto AS "Monto",
                       estado AS "Estado", boleta AS "Boleta", documento AS "Cliente en boleta", detalle AS "Detalle"
                FROM sunat_boletas_log ORDER BY id DESC LIMIT 60"""), conn)

        if not revisar.empty:
            st.error("⚠️ Ventas por revisar: falló algo justo al emitir y no se sabe si la boleta salió. "
                     "Búscalas en SUNAT → Consultar Boleta de Venta. Si salió, pon el número abajo en Facturación "
                     "Individual; después márcala como revisada para que el proceso automático la vuelva a considerar.")
            for _, r in revisar.iterrows():
                cc1, cc2 = st.columns([4, 1])
                cc1.write(f"Venta **{r['id_venta']}** · S/ {r['monto']} · {r['fecha']:%d/%m %H:%M} — {r['detalle']}")
                if cc2.button("✔️ Revisada", key=f"sunat_rev_{r['id']}"):
                    with engine.begin() as conn:
                        conn.execute(text("""INSERT INTO sunat_boletas_log (origen, id_venta, monto, estado, detalle)
                            VALUES ('panel', :v, :m, 'REVISADA', 'marcada como revisada en el panel')"""),
                                     {"v": int(r["id_venta"]), "m": r["monto"]})
                    st.rerun()

        st.markdown("**Registro reciente**")
        if registro.empty:
            st.caption("Todavía no hay corridas.")
        else:
            st.dataframe(registro, hide_index=True, width='stretch')


def render_facturacion():
    render_boletas_auto()
    st.subheader("🧾 Facturación Individual")
    st.info("Sistema protegido: No permite boletas duplicadas y formatea los nombres automáticamente.")

    # --- 1. CARGAR LISTA DE VENTAS PENDIENTES ---
    with engine.connect() as conn:
        # AQUÍ ESTÁ EL CAMBIO: Se agregó "AND v.anulado = FALSE"
        query_pendientes = text("""
            SELECT 
                v.id_venta,
                c.nombre_corto,
                v.fecha_venta,
                v.total_venta
            FROM Ventas v
            JOIN Clientes c ON v.id_cliente = c.id_cliente
            WHERE v.facturado = FALSE AND v.anulado = FALSE
            ORDER BY v.id_venta ASC
        """)
        df_pendientes = pd.read_sql(query_pendientes, conn)

    if df_pendientes.empty:
        st.success("🎉 ¡Felicidades! No hay facturas pendientes.")
    else:
        # (El resto del código de render_facturacion se mantiene exactamente igual...)
        # --- 2. SELECTOR DE VENTA ---
        opciones_venta = df_pendientes['id_venta'].tolist()
        
        def formato_opcion(id_v):
            fila = df_pendientes[df_pendientes['id_venta'] == id_v]
            if not fila.empty:
                row = fila.iloc[0]
                return f"🆔 {row['id_venta']} | 📅 {row['fecha_venta']} | 👤 {row['nombre_corto']} | 💰 S/ {row['total_venta']}"
            return f"Venta {id_v}"

        seleccion_id = st.selectbox("👇 Elige la venta a procesar:", options=opciones_venta, format_func=formato_opcion)

        st.divider()

        if seleccion_id:
            with engine.connect() as conn:
                # A) Datos Cliente (Incluimos DNI para el cambio 1.2)
                query_cliente = text("""
                    SELECT c.id_cliente, c.nombre, c.apellido, c.dni, c.google_id, c.telefono, c.nombre_corto 
                    FROM Ventas v JOIN Clientes c ON v.id_cliente = c.id_cliente 
                    WHERE v.id_venta = :id
                """)
                cliente_data = pd.read_sql(query_cliente, conn, params={"id": int(seleccion_id)}).iloc[0]

                # B) Datos de Dirección (Cambio 1.3: nombre_receptor y dni_receptor)
                query_dir = text("""
                    SELECT nombre_receptor, dni_receptor 
                    FROM Direcciones 
                    WHERE id_cliente = :id_cli 
                    ORDER BY id_direccion DESC LIMIT 1
                """)
                dir_res = conn.execute(query_dir, {"id_cli": int(cliente_data['id_cliente'])}).fetchone()
                
                # C) Items (Se mantiene igual)
                query_items = text("""
                    SELECT d.sku as "Código", d.descripcion as "Descripción", d.cantidad as "Cant.", 
                           d.precio_unitario as "P.Unit", (d.cantidad * d.precio_unitario) as "Total"
                    FROM DetalleVenta d WHERE d.id_venta = :id
                    UNION ALL
                    SELECT 'ENVIO', 'Servicio de Envío', 1, v.costo_envio, v.costo_envio
                    FROM Ventas v WHERE v.id_venta = :id AND v.costo_envio > 0
                """)
                df_items = pd.read_sql(query_items, conn, params={"id": int(seleccion_id)})

            col_datos, col_tabla = st.columns([1, 2])
            
            with col_datos:
                st.markdown(f"#### 👤 {cliente_data['nombre_corto']}")
                
                # Cambio 1.3: Mostrar datos de dirección si existen
                if dir_res:
                    st.caption(f"📦 **Datos de Envío:** {dir_res[0]} (DNI: {dir_res[1] or 'No reg.'})")

                with st.form("form_facturacion"):
                    val_nombre = cliente_data['nombre'] or ""
                    val_apellido = cliente_data['apellido'] or ""
                    # Cambio 1.2: El valor del DNI será el del cliente actual (vacío si no tiene)
                    val_dni = cliente_data['dni'] or ""

                    nuevo_nombre = st.text_input("Nombre", value=val_nombre)
                    nuevo_apellido = st.text_input("Apellido", value=val_apellido)
                    nuevo_dni = st.text_input("DNI / RUC", value=val_dni)
                    
                    st.markdown("---")
                    numero_boleta = st.text_input("N° Boleta (EB01...)")
                    sin_boleta = st.checkbox("🚫 Registrar sin boleta")
                    
                    btn_guardar = st.form_submit_button("✅ Guardar y Archivar", type="primary")
            
            with col_tabla:
                st.markdown(f"#### 🛒 Detalle de Items (Venta {seleccion_id})")
                st.dataframe(df_items, hide_index=True, width='stretch')
                st.caption("👆 Copia estas filas y pégalas en tu sistema contable.")

            # --- 5. LÓGICA DE GUARDADO ---
            if btn_guardar:
                if not numero_boleta and not sin_boleta:
                    st.warning("⚠️ Debes ingresar el Número de Boleta o marcar 'Registrar sin boleta' para continuar.")
                else:
                    nombre_formateado = nuevo_nombre.strip().title() if nuevo_nombre else ""
                    apellido_formateado = nuevo_apellido.strip().title() if nuevo_apellido else ""
                    
                    # Si marca sin boleta, asignamos un texto por defecto
                    boleta_limpia = "SIN_BOLETA" if sin_boleta else numero_boleta.strip().upper() 

                    with engine.connect() as conn:
                        trans = conn.begin() 
                        try:
                            # 1. VERIFICAR DUPLICADOS DENTRO DE LA TRANSACCIÓN (Omitir si es sin boleta)
                            existe_boleta = None
                            if not sin_boleta:
                                existe_boleta = conn.execute(
                                    text("SELECT id_venta FROM Ventas WHERE numero_boleta = :b"),
                                    {"b": boleta_limpia}
                                ).fetchone()

                            if existe_boleta:
                                st.error(f"⛔ ¡ERROR! La boleta '{boleta_limpia}' ya está registrada en la Venta #{existe_boleta[0]}.")
                            else:
                                # 2. SI NO EXISTE, PROCEDEMOS A GUARDAR TODO
                                
                                # A. Actualizar Cliente
                                conn.execute(text("""
                                    UPDATE Clientes 
                                    SET nombre = :n, apellido = :a, dni = :d 
                                    WHERE id_cliente = :cid
                                """), {
                                    "n": nombre_formateado, 
                                    "a": apellido_formateado, 
                                    "d": nuevo_dni, 
                                    "cid": int(cliente_data['id_cliente'])
                                })

                                # B. Sincronizar Google
                                if cliente_data['google_id']:
                                    actualizar_en_google(
                                        cliente_data['google_id'], 
                                        nombre_formateado, 
                                        apellido_formateado, 
                                        cliente_data['telefono']
                                    )

                                # C. Actualizar Venta
                                conn.execute(text("""
                                    UPDATE Ventas 
                                    SET facturado = TRUE, 
                                    fecha_facturacion = (NOW() - INTERVAL '5 hours'),
                                        numero_boleta = :bol
                                    WHERE id_venta = :vid
                                """), {
                                    "bol": boleta_limpia, 
                                    "vid": int(seleccion_id)
                                })
                                
                                # D. Confirmar todo
                                trans.commit()
                                st.balloons()
                                if sin_boleta:
                                    st.success("¡Correcto! Venta archivada sin boleta.")
                                else:
                                    st.success(f"¡Correcto! Venta guardada con boleta {boleta_limpia}.")
                                
                                time.sleep(1.5)
                                st.rerun()
                                
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error al guardar: {e}")

def render_reporte_mensual():
    st.subheader("📊 Reporte Mensual para Declaración")
    
    # Se agrega el filtro para excluir 'SIN_BOLETA'
    query_reporte = text("""
        SELECT 
            TO_CHAR(fecha_facturacion, 'YYYY-MM') AS "Mes",
            COUNT(id_venta) AS "Cant. Boletas",
            SUM(total_venta) AS "Total Ventas (S/)"
        FROM Ventas
        WHERE facturado = TRUE 
          AND numero_boleta != 'SIN_BOLETA'
          AND numero_boleta IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    
    with engine.connect() as conn:
        df_reporte = pd.read_sql(query_reporte, conn)
    
    if df_reporte.empty:
        st.warning("No hay datos facturados para generar reportes.")
    else:
        st.dataframe(df_reporte, use_container_width=True, hide_index=True)
        
        # Opción para descargar en Excel/CSV
        csv = df_reporte.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar Reporte (CSV)",
            data=csv,
            file_name='reporte_mensual_facturacion.csv',
            mime='text/csv',
        )

        # --- DETALLE DEL MES SELECCIONADO ---
        mes_seleccionar = st.selectbox("Ver detalle de un mes específico:", df_reporte["Mes"].unique())
        
        if mes_seleccionar:
            # Cambio 2.1: Ordenar por fecha_facturacion ASC y luego numero_boleta ASC
            query_detalle = text("""
                SELECT v.fecha_facturacion, v.numero_boleta, c.nombre || ' ' || c.apellido as cliente, v.total_venta
                FROM Ventas v
                JOIN Clientes c ON v.id_cliente = c.id_cliente
                WHERE TO_CHAR(v.fecha_facturacion, 'YYYY-MM') = :mes
                  AND v.numero_boleta != 'SIN_BOLETA'
                  AND v.numero_boleta IS NOT NULL
                ORDER BY v.fecha_facturacion ASC, v.numero_boleta ASC
            """)
            with engine.connect() as conn:
                df_detalle = pd.read_sql(query_detalle, conn, params={"mes": mes_seleccionar})
            st.write(f"Detalle de {mes_seleccionar}:")
            st.table(df_detalle)