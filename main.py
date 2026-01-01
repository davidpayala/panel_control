import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import urllib.parse
import time
import random 
from datetime import date
from datetime import datetime # Asegúrate de tener este import arriba

load_dotenv()

st.set_page_config(page_title="POS KMLentes", page_icon="🛒", layout="wide")

# --- CONEXIÓN ---
def get_connection():
    try:
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        password_encoded = urllib.parse.quote_plus(password)
        return create_engine(f'postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{dbname}')
    except Exception as e:
        st.error(f"Error BD: {e}")
        return None

engine = get_connection()

# --- INICIALIZAR CARRITO DE COMPRAS (Memoria Temporal) ---
if 'carrito' not in st.session_state:
    st.session_state.carrito = []

# --- FUNCIONES AUXILIARES ---
def agregar_al_carrito(sku, nombre, cantidad, precio, es_inventario, stock_max=None):
    # Validar stock si es de inventario
    if es_inventario:
        # Verificar si ya está en el carrito para sumar
        cant_en_carrito = sum(item['cantidad'] for item in st.session_state.carrito if item['sku'] == sku)
        if (cant_en_carrito + cantidad) > stock_max:
            st.error(f"❌ No hay suficiente stock. Disponibles: {stock_max}, En carrito: {cant_en_carrito}")
            return

    st.session_state.carrito.append({
        "sku": sku,
        "descripcion": nombre,
        "cantidad": int(cantidad),
        "precio": float(precio),
        "subtotal": float(precio * cantidad),
        "es_inventario": es_inventario
    })
    st.success(f"Añadido: {nombre}")

# --- INTERFAZ ---
st.title("🛒 KMLentes - Punto de Venta v2")
st.markdown("---")

    # --- CONSTANTES (TUS ESTADOS) ---
ESTADOS_CLIENTE = [
    "Sin empezar", "Responder duda", "Interesado en venta", 
    "Proveedor nacional", "Proveedor internacional", 
    "Venta motorizado", "Venta agencia", "Venta express moto",
    "En camino moto", "En camino agencia", "Contraentrega agencia", 
    "Pendiente agradecer", "Problema post", "Cliente Finalizado"
]
MEDIOS_CONTACTO = ["Wsp 941380271", "Wsp 936041531", "Facebook-Instagram", "TikTok", "Físico/Tienda"]

# AHORA SON 7 PESTAÑAS
tabs = st.tabs(["🛒 VENTA (POS)", "📦 Compras", "🔎 Inventario", "👤 Clientes", "📜 Historial", "📆 Seguimiento", "🔧 Catálogo"])

# ==============================================================================
# PESTAÑA 1: VENTAS / SALIDAS (CON MULTI-DIRECCIÓN)
# ==============================================================================
with tabs[0]:
    # --- CABECERA ---
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
    # COLUMNA IZQUIERDA: BUSCADOR (Igual que antes)
    # ------------------------------------------------------------------
    with col_izq:
        st.caption("1. Buscar Productos")
        tipo_producto = st.radio("Origen:", ["Inventario (SQL)", "Manual/Extra"], horizontal=True, label_visibility="collapsed")
        
        if tipo_producto == "Inventario (SQL)":
            sku_input = st.text_input("Escanear/Escribir SKU:", placeholder="Ej: CL-01...", key="sku_pos")
            if sku_input:
                with engine.connect() as conn:
                    res = pd.read_sql(text("""
                        SELECT v.sku, p.modelo, p.nombre as color, v.medida, v.stock_interno, v.precio, v.ubicacion 
                        FROM Variantes v JOIN Productos p ON v.id_producto = p.id_producto
                        WHERE v.sku = :sku
                    """), conn, params={"sku": sku_input})
                
                if not res.empty:
                    prod = res.iloc[0]
                    # Nombre compuesto mejorado
                    nombre_full = f"{prod['modelo']} {prod['color']} ({prod['medida']})"
                    
                    if prod['stock_interno'] <= 0:
                        st.error(f"❌ Sin Stock ({prod['stock_interno']})")
                    else:
                        st.success(f"✅ Stock: {prod['stock_interno']} | 📍 {prod['ubicacion']}")

                    st.markdown(f"**{nombre_full}**")
                    
                    c1, c2 = st.columns(2)
                    cantidad = c1.number_input("Cant.", min_value=1, value=1)
                    precio_sugerido = float(prod['precio']) if modo_operacion == "💰 Venta" else 0.0
                    precio_final = c2.number_input("Precio Unit.", value=precio_sugerido, disabled=(modo_operacion != "💰 Venta"))
                    
                    if st.button("➕ Agregar"):
                        agregar_al_carrito(prod['sku'], nombre_full, cantidad, precio_final, True, prod['stock_interno'])
                else:
                    st.warning("SKU no encontrado.")
        
        else: 
            st.info("Item Manual (Servicios, etc.)")
            desc_manual = st.text_input("Descripción:")
            c1, c2 = st.columns(2)
            cant_manual = c1.number_input("Cant.", min_value=1, value=1, key="cm")
            precio_manual = c2.number_input("Precio", value=0.0, key="pm", disabled=(modo_operacion != "💰 Venta"))
            if st.button("➕ Agregar Manual"):
                if desc_manual: agregar_al_carrito(None, desc_manual, cant_manual, precio_manual, False)

    # ------------------------------------------------------------------
    # COLUMNA DERECHA: PROCESAR
    # ------------------------------------------------------------------
    with col_der:
        st.caption("2. Confirmación")
        
        if len(st.session_state.carrito) > 0:
            df_cart = pd.DataFrame(st.session_state.carrito)
            st.dataframe(df_cart[['descripcion', 'cantidad', 'subtotal']], width='stretch', hide_index=True)
            
            suma_subtotal = float(df_cart['subtotal'].sum())
            
            st.divider()

            # ==========================================================
            # MODO A: VENTA (Con Cliente y Selección de Dirección)
            # ==========================================================
            if modo_operacion == "💰 Venta":
                st.markdown(f"**Subtotal Items:** S/ {suma_subtotal:.2f}")

                # 1. CLIENTE
                with engine.connect() as conn:
                    cli_df = pd.read_sql(text("SELECT id_cliente, nombre_corto FROM Clientes WHERE activo = TRUE ORDER BY nombre_corto"), conn)
                lista_cli = {row['nombre_corto']: row['id_cliente'] for i, row in cli_df.iterrows()}
                
                if not lista_cli:
                    st.error("No hay clientes. Crea uno en la pestaña Clientes.")
                    st.stop()

                nombre_cli = st.selectbox("Cliente:", options=list(lista_cli.keys()))
                id_cliente = lista_cli[nombre_cli]

                # 2. TIPO DE ENVÍO
                col_e1, col_e2 = st.columns(2)
                tipo_envio = col_e1.selectbox("Método Envío", ["Gratis", "Express (Moto)", "Agencia (Pago Destino)", "Agencia (Pagado)"])
                costo_envio = col_e2.number_input("Costo Envío", value=0.0)

                # 3. SELECCIÓN DE DIRECCIÓN (Lógica Nueva)
                es_agencia = "Agencia" in tipo_envio
                cat_direccion = "AGENCIA" if es_agencia else "MOTO"
                
                # Buscamos TODAS las direcciones activas
                with engine.connect() as conn:
                    q_dir = text("""
                        SELECT * FROM Direcciones 
                        WHERE id_cliente = :id AND tipo_envio = :tipo AND activo = TRUE 
                        ORDER BY id_direccion DESC
                    """)
                    df_dirs = pd.read_sql(q_dir, conn, params={"id": id_cliente, "tipo": cat_direccion})

                usar_guardada = False
                datos_nuevos = {} 
                texto_direccion_final = ""
                
                # Preparamos las opciones para el SelectBox
                opciones_visuales = {}
                if not df_dirs.empty:
                    for idx, row in df_dirs.iterrows():
                        # Creamos una etiqueta bonita para identificar la dirección
                        if es_agencia:
                            lbl = f"🏢 {row['agencia_nombre']} - {row['sede_entrega']} (Recibe: {row['nombre_receptor']})"
                        else:
                            lbl = f"🏠 {row['direccion_texto']} ({row['distrito']})"
                        
                        if row['observacion']:
                            lbl += f" | 👁️ {row['observacion']}"
                            
                        opciones_visuales[lbl] = row

                # Opción Especial para Nueva Dirección
                KEY_NUEVA = "➕ Usar una Nueva Dirección..."
                lista_desplegable = list(opciones_visuales.keys()) + [KEY_NUEVA]
                
                # WIDGET SELECTOR
                st.markdown("📍 **Destino del Pedido:**")
                seleccion_dir = st.selectbox("Elige la dirección:", options=lista_desplegable, label_visibility="collapsed")
                
                # --- LÓGICA DE SELECCIÓN ---
                if seleccion_dir != KEY_NUEVA:
                    # CASO: Dirección Guardada
                    usar_guardada = True
                    dir_data = opciones_visuales[seleccion_dir]
                    
                    if es_agencia:
                        texto_direccion_final = f"{dir_data['agencia_nombre']} - {dir_data['sede_entrega']} [{dir_data['dni_receptor']}]"
                        st.info(f"✅ Enviar a: **{texto_direccion_final}**")
                    else:
                        texto_direccion_final = f"{dir_data['direccion_texto']} - {dir_data['distrito']}"
                        st.info(f"✅ Enviar a: **{texto_direccion_final}**\n\nRef: {dir_data['referencia']}")
                    
                    if dir_data['observacion']:
                        st.caption(f"📝 Obs: {dir_data['observacion']}")

                else:
                    # CASO: Nueva Dirección (Formulario)
                    st.warning("📝 Ingresa los nuevos datos:")
                    with st.container(border=True):
                        recibe = st.text_input("Recibe:", value=nombre_cli)
                        telf = st.text_input("Teléfono:", key="telf_new")
                        
                        obs_new = st.text_input("Observaciones:", placeholder="Fachada, timbre, pago destino...")

                        if es_agencia:
                            dni = st.text_input("DNI:")
                            agencia = st.text_input("Agencia:", value="Shalom")
                            sede = st.text_input("Sede:")
                            datos_nuevos = {
                                "tipo": "AGENCIA", "nom": recibe, "tel": telf, "dni": dni, 
                                "age": agencia, "sede": sede, "obs": obs_new,
                                "dir": "", "dist": "", "ref": ""
                            }
                            texto_direccion_final = f"{agencia} - {sede}"
                        else:
                            direcc = st.text_input("Dirección:")
                            dist = st.text_input("Distrito:")
                            datos_nuevos = {
                                "tipo": "MOTO", "nom": recibe, "tel": telf, 
                                "dir": direcc, "dist": dist, "obs": obs_new,
                                "ref": "", "gps": "", "dni": "", "age": "", "sede": ""
                            }
                            texto_direccion_final = f"{direcc} - {dist}"

                # 4. CLAVE DE AGENCIA
                clave_agencia = None
                if es_agencia:
                    if 'clave_temp' not in st.session_state: st.session_state['clave_temp'] = str(random.randint(1000, 9999))
                    col_k1, col_k2 = st.columns([1,2])
                    clave_agencia = col_k1.text_input("Clave", value=st.session_state['clave_temp'])
                    col_k2.info("🔐 Clave Entrega")

                # TOTALES
                total_final = suma_subtotal + costo_envio
                st.markdown(f"### Total: S/ {total_final:.2f}")
                nota_venta = st.text_input("Nota Venta:")

                if st.button("✅ FINALIZAR VENTA", type="primary"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            
                            # A) Guardar Dirección Nueva si aplica
                            if not usar_guardada and datos_nuevos:
                                conn.execute(text("""
                                    INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, dni_receptor, agencia_nombre, sede_entrega, observacion, activo)
                                    VALUES (:id, :tipo, :nom, :tel, :dir, :dist, :dni, :age, :sede, :obs, TRUE)
                                """), {"id": id_cliente, **datos_nuevos})

                            # B) Registrar Venta
                            nota_full = f"{nota_venta} | Envío: {texto_direccion_final}"
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (id_cliente, tipo_envio, costo_envio, total_venta, nota, clave_seguridad)
                                VALUES (:idc, :tipo, :costo, :total, :nota, :clave) RETURNING id_venta
                            """), {"idc": id_cliente, "tipo": tipo_envio, "costo": costo_envio, "total": total_final, "nota": nota_full, "clave": clave_agencia})
                            id_venta = res_v.fetchone()[0]

                            # C) Detalles y Stock
                            for item in st.session_state.carrito:
                                conn.execute(text("""
                                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                    VALUES (:idv, :sku, :desc, :cant, :pu, :sub, :inv)
                                """), {"idv": id_venta, "sku": item['sku'], "desc": item['descripcion'], "cant": int(item['cantidad']), "pu": float(item['precio']), "sub": float(item['subtotal']), "inv": item['es_inventario']})
                                
                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                 {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    
                                    if nuevo_s <= 0: 
                                        conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota, id_cliente) 
                                        VALUES (:sku, 'VENTA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota, :idc)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": f"Venta #{id_venta}", "idc": id_cliente})
                            
                            trans.commit()
                        st.balloons()
                        st.success("¡Venta Exitosa!")
                        st.session_state.carrito = []
                        if 'clave_temp' in st.session_state: del st.session_state['clave_temp']
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

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
                            res_v = conn.execute(text("""
                                INSERT INTO Ventas (tipo_envio, costo_envio, total_venta, nota, id_cliente)
                                VALUES ('SALIDA', 0, 0, :nota, NULL) RETURNING id_venta
                            """), {"nota": f"[{motivo_salida}] {detalle_motivo}"})
                            id_salida = res_v.fetchone()[0]

                            for item in st.session_state.carrito:
                                conn.execute(text("""
                                    INSERT INTO DetalleVenta (id_venta, sku, descripcion, cantidad, precio_unitario, subtotal, es_inventario)
                                    VALUES (:idv, :sku, :desc, :cant, 0, 0, :inv)
                                """), {"idv": id_salida, "sku": item['sku'], "desc": item['descripcion'], "cant": int(item['cantidad']), "inv": item['es_inventario']})
                                
                                if item['es_inventario']:
                                    res_s = conn.execute(text("UPDATE Variantes SET stock_interno = stock_interno - :c WHERE sku=:s RETURNING stock_interno"),
                                                 {"c": int(item['cantidad']), "s": item['sku']})
                                    nuevo_s = res_s.scalar()
                                    if nuevo_s <= 0: conn.execute(text("UPDATE Variantes SET ubicacion = '' WHERE sku=:s"), {"s": item['sku']})
                                    
                                    conn.execute(text("""
                                        INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                        VALUES (:sku, 'SALIDA', :c, (SELECT stock_interno + :c FROM Variantes WHERE sku=:sku), :nue, :nota)
                                    """), {"sku": item['sku'], "c": int(item['cantidad']), "nue": nuevo_s, "nota": f"Salida #{id_salida} | {motivo_salida}"})
                            
                            trans.commit()
                        st.success(f"✅ Salida #{id_salida} registrada correctamente.")
                        st.session_state.carrito = []
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                         # Si falla por NULL en id_cliente (depende config DB), manejarlo
                        if "null value in column" in str(e).lower() and "id_cliente" in str(e).lower():
                            st.error("Error: La base de datos requiere Cliente. Crea un cliente 'Interno' y modifica el código para usar su ID.")
                        else:
                            st.error(f"Error al registrar salida: {e}")

        else:
            st.info("El carrito está vacío.")
            
        if st.button("🗑️ Limpiar Todo"):
            st.session_state.carrito = []
            st.rerun()
            
# ==============================================================================
# PESTAÑA 2: COMPRAS (CORREGIDO: 2026 + NUMPY + WIDTH STRETCH)
# ==============================================================================
with tabs[1]:
    st.subheader("📦 Gestión de Compras y Reposición")
    
    tab_asistente, tab_registro = st.tabs(["💡 Asistente de Reposición (IA)", "📝 Registrar Ingreso Manual"])
    
    # --- A) ASISTENTE INTELIGENTE ---
    with tab_asistente:
        # 1. CONTROLES
        with st.container(border=True):
            c_filtros, c_acciones = st.columns([3, 1])
            with c_filtros:
                st.markdown("**Configuración del Reporte**")
                col_f1, col_f2 = st.columns(2)
                umbral_stock = col_f1.slider("Mostrar productos con Stock menor a:", 0, 20, 1)
                solo_con_externo = col_f2.checkbox("Solo lo que tiene el Proveedor (Stock Ext. > 0)", value=True)
            
            with c_acciones:
                st.write("")
                # NOTA: st.button SÍ usa use_container_width (boolean), eso está bien. 
                # El cambio a width='stretch' es solo para dataframes.
                if st.button("🔄 Actualizar Datos", type="primary", use_container_width=True):
                    st.rerun()

        # 2. DEFINIR AÑOS DINÁMICAMENTE
        year_actual = datetime.now().year 
        y1, y2, y3 = year_actual, year_actual - 1, year_actual - 2 

        # Función auxiliar para evitar error de columna inexistente (2026, 2027...)
        def get_hist_sql(year):
            if year <= 2025: 
                return f"COALESCE(h.v{year}, 0)"
            else:
                return "0" 

        # 3. CONSULTA HÍBRIDA
        with engine.connect() as conn:
            try:
                hist_y3 = get_hist_sql(y3)
                hist_y2 = get_hist_sql(y2)
                hist_y1 = get_hist_sql(y1)

                query_hybrid = text(f"""
                    WITH VentasSQL AS (
                        SELECT 
                            d.sku,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y3 THEN d.cantidad ELSE 0 END) as sql_y3,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y2 THEN d.cantidad ELSE 0 END) as sql_y2,
                            SUM(CASE WHEN EXTRACT(YEAR FROM v.fecha_venta) = :y1 THEN d.cantidad ELSE 0 END) as sql_y1
                        FROM DetalleVenta d
                        JOIN Ventas v ON d.id_venta = v.id_venta
                        GROUP BY d.sku
                    )
                    SELECT 
                        v.sku, 
                        p.marca || ' ' || p.modelo || ' - ' || COALESCE(p.nombre, '') || ' (' || v.medida || ')' as nombre,
                        v.stock_interno,
                        v.stock_externo,
                        ({hist_y3} + COALESCE(live.sql_y3, 0)) as venta_year_3,
                        ({hist_y2} + COALESCE(live.sql_y2, 0)) as venta_year_2,
                        ({hist_y1} + COALESCE(live.sql_y1, 0)) as venta_year_1
                    FROM Variantes v
                    JOIN Productos p ON v.id_producto = p.id_producto
                    LEFT JOIN HistorialAnual h ON v.sku = h.sku
                    LEFT JOIN VentasSQL live ON v.sku = live.sku
                    WHERE v.stock_interno <= :umbral
                """)
                
                df_reco = pd.read_sql(query_hybrid, conn, params={
                    "umbral": umbral_stock,
                    "y1": y1, "y2": y2, "y3": y3
                })
                
                if not df_reco.empty:
                    df_reco['demanda_historica'] = (
                        df_reco['venta_year_1'] + df_reco['venta_year_2'] + df_reco['venta_year_3']
                    )

            except Exception as e:
                st.error(f"⚠️ Error en consulta: {e}")
                df_reco = pd.DataFrame()

        # 4. FILTROS
        if not df_reco.empty:
            df_reco['sku'] = df_reco['sku'].astype(str).str.strip()
            if solo_con_externo:
                df_reco = df_reco[df_reco['stock_externo'] > 0]

            patron_medida = r'-\d{4}$'
            es_medida = df_reco['sku'].str.contains(patron_medida, regex=True, na=False)
            es_base = df_reco['sku'].str.endswith('-0000', na=False)
            df_reco = df_reco[~es_medida | es_base]

            df_reco = df_reco.sort_values(by='demanda_historica', ascending=False)

        # 5. VISUALIZACIÓN
        val_max = int(df_reco['demanda_historica'].max()) if not df_reco.empty else 10
        if val_max == 0: val_max = 10

        st.divider()
        col_res_txt, col_res_btn = st.columns([3, 1])
        with col_res_txt:
            st.markdown(f"### 📋 Lista Sugerida ({len(df_reco)} modelos)")
            if not df_reco.empty:
                st.caption(f"🔥 Top #1: **{df_reco.iloc[0]['nombre']}**")

        with col_res_btn:
            if not df_reco.empty:
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_reco.to_excel(writer, index=False, sheet_name='Reposicion')
                
                st.download_button(
                    label="📥 Descargar Excel",
                    data=buffer.getvalue(),
                    file_name=f"Reposicion_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True # En botones sí se usa este nombre
                )

        # CORRECCIÓN AQUÍ: width='stretch' en lugar de use_container_width=True para dataframe
        st.dataframe(
            df_reco,
            column_config={
                "sku": "SKU",
                "nombre": st.column_config.TextColumn("Producto", width="large"),
                "stock_interno": st.column_config.NumberColumn("Mi Stock", format="%d"),
                "stock_externo": st.column_config.NumberColumn("Prov.", format="%d"),
                "demanda_historica": st.column_config.ProgressColumn(
                    "Demanda 3 Años", format="%d", min_value=0, max_value=val_max
                ),
                "venta_year_3": st.column_config.NumberColumn(f"{y3}", width="small"),
                "venta_year_2": st.column_config.NumberColumn(f"{y2}", width="small"),
                "venta_year_1": st.column_config.NumberColumn(f"{y1}", width="small"), 
            },
            hide_index=True,
            width='stretch' # <--- CORREGIDO
        )

# --- B) REGISTRO MANUAL (CON EDICIÓN DE UBICACIÓN) ---
    with tab_registro:
        st.subheader("📦 Ingreso de Mercadería")
        sku_compra = st.text_input("SKU Producto a ingresar:", key="sku_compra_tab2")
        
        if sku_compra:
            with engine.connect() as conn:
                # CAMBIO 1: Traemos también la ubicación
                res = pd.read_sql(text("SELECT sku, stock_interno, ubicacion FROM Variantes WHERE sku = :s"), conn, params={"s": sku_compra})
            
            if not res.empty:
                # Convertimos a int/str nativos para evitar problemas
                curr_stock = int(res.iloc[0]['stock_interno'])
                curr_ubi = str(res.iloc[0]['ubicacion']) if res.iloc[0]['ubicacion'] else ""
                
                st.info(f"📊 Stock Actual: **{curr_stock}**")
                
                with st.form("form_ingreso_manual"):
                    c1, c2 = st.columns(2)
                    cant_ingreso = c1.number_input("Cantidad a sumar (+):", min_value=1, step=1)
                    # CAMBIO 2: Campo para ver y editar la ubicación
                    ubi_ingreso = c2.text_input("Ubicación (Editar si es necesario):", value=curr_ubi)
                    
                    nota_ingreso = st.text_input("Nota / Proveedor:")
                    
                    if st.form_submit_button("💾 Registrar Entrada y Actualizar", use_container_width=True):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                nuevo_st = int(curr_stock + cant_ingreso)
                                
                                # CAMBIO 3: El UPDATE ahora actualiza stock Y ubicación
                                conn.execute(text("UPDATE Variantes SET stock_interno = :n, ubicacion = :u WHERE sku=:s"), 
                                            {"n": nuevo_st, "u": ubi_ingreso, "s": sku_compra})
                                
                                conn.execute(text("""
                                    INSERT INTO Movimientos (sku, tipo_movimiento, cantidad, stock_anterior, stock_nuevo, nota)
                                    VALUES (:s, 'COMPRA', :c, :ant, :nue, :nota)
                                """), {
                                    "s": sku_compra, 
                                    "c": int(cant_ingreso), 
                                    "ant": curr_stock, 
                                    "nue": nuevo_st, 
                                    "nota": nota_ingreso
                                })
                                
                                trans.commit()
                                st.success(f"✅ Stock actualizado a {nuevo_st}. Ubicación: {ubi_ingreso}")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error: {e}")
            else:
                st.warning("⚠️ SKU no encontrado. Ve a la pestaña 'Catálogo' para crearlo primero.")

# ==============================================================================
# PESTAÑA 3: INVENTARIO (VISTA DETALLADA Y UBICACIONES)
# ==============================================================================
with tabs[2]:
    st.subheader("🔎 Gestión de Inventario Detallado")

    # --- 1. BARRA DE HERRAMIENTAS ---
    col_search, col_btn = st.columns([4, 1])
    with col_search:
        filtro_inv = st.text_input("🔍 Buscar:", placeholder="Escribe SKU, Marca, Modelo o Ubicación...")
    with col_btn:
        st.write("") # Espaciador
        if st.button("🔄 Recargar Tabla"):
            if 'df_inventario' in st.session_state: del st.session_state['df_inventario']
            st.rerun()

    # --- 2. CARGA DE DATOS ---
    if 'df_inventario' not in st.session_state:
        with engine.connect() as conn:
            # Traemos las columnas RAW de ambas tablas
            # Usamos COALESCE para que si algún campo está vacío no salga 'None'
            q_inv = """
                SELECT 
                    v.sku, 
                    p.categoria,
                    p.marca, 
                    p.modelo, 
                    v.nombre_variante,
                    p.color_principal, 
                    p.diametro, 
                    v.medida,
                    v.stock_interno,
                    v.stock_externo,
                    v.ubicacion
                FROM Variantes v
                JOIN Productos p ON v.id_producto = p.id_producto
                ORDER BY p.marca, p.modelo, v.sku ASC
            """
            st.session_state.df_inventario = pd.read_sql(text(q_inv), conn)

    # Trabajamos con una copia
    df_calc = st.session_state.df_inventario.copy()

    # --- 3. CREACIÓN DE COLUMNAS COMBINADAS (Python) ---
    # Esto es más seguro hacerlo en Python para manejar formatos y nulos fácilmente
    
    # A) Columna NOMBRE: Marca + Modelo + Variante
    df_calc['nombre_completo'] = (
        df_calc['marca'].fillna('') + " " + 
        df_calc['modelo'].fillna('') + " - " + 
        df_calc['nombre_variante'].fillna('')
    ).str.strip()

    # B) Columna DETALLES: ColorPrin + Diametro + Medida
    # Función auxiliar para formatear bonito
    def formatear_detalles(row):
        partes = []
        if row['color_principal']: partes.append(str(row['color_principal']))
        if row['diametro']: partes.append(f"Dia:{row['diametro']}")
        if row['medida']: partes.append(f"Med:{row['medida']}")
        return " | ".join(partes)

    df_calc['detalles_info'] = df_calc.apply(formatear_detalles, axis=1)

    # --- 4. FILTRADO ---
    if filtro_inv:
        f = filtro_inv.lower()
        df_calc = df_calc[
            df_calc['nombre_completo'].str.lower().str.contains(f, na=False) |
            df_calc['sku'].str.lower().str.contains(f, na=False) |
            df_calc['ubicacion'].str.lower().str.contains(f, na=False)
        ]

    # Seleccionamos y ordenamos SOLO las columnas que pediste ver
    df_final = df_calc[[
        'sku', 
        'categoria', 
        'nombre_completo', 
        'detalles_info', 
        'stock_interno', 
        'stock_externo', 
        'ubicacion'
    ]]

    # --- 5. TABLA EDITABLE ---
    st.caption("📝 Solo la columna **'Ubicación'** es editable.")
    
    cambios_inv = st.data_editor(
        df_final,
        key="editor_inventario_v2",
        column_config={
            "sku": st.column_config.TextColumn("SKU", disabled=True, width="small"),
            "categoria": st.column_config.TextColumn("Cat.", disabled=True, width="small"),
            "nombre_completo": st.column_config.TextColumn("Nombre del Producto", disabled=True, width="large"),
            "detalles_info": st.column_config.TextColumn("Detalles Técnicos", disabled=True, width="medium"),
            "stock_interno": st.column_config.NumberColumn("S. Int.", disabled=True, format="%d"),
            "stock_externo": st.column_config.NumberColumn("S. Ext.", disabled=True, format="%d"),
            "ubicacion": st.column_config.TextColumn("Ubicación 📍", required=False, width="small")
        },
        hide_index=True,
        width='stretch',
        num_rows="fixed"
    )

    # --- 6. GUARDAR CAMBIOS ---
    edited_rows = st.session_state["editor_inventario_v2"].get("edited_rows")

    if edited_rows:
        st.info(f"💾 Tienes {len(edited_rows)} cambios de ubicación pendientes...")
        
        if st.button("Confirmar Cambios"):
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    count = 0
                    for idx, updates in edited_rows.items():
                        # Recuperamos el SKU usando el índice del dataframe visual
                        sku_target = df_final.iloc[idx]['sku']
                        nueva_ubi = updates.get('ubicacion')
                        
                        if nueva_ubi is not None:
                            conn.execute(
                                text("UPDATE Variantes SET ubicacion = :u WHERE sku = :s"),
                                {"u": nueva_ubi, "s": sku_target}
                            )
                            count += 1
                    
                    trans.commit()
                    st.success(f"✅ ¡Se actualizaron {count} ubicaciones!")
                    del st.session_state['df_inventario'] # Limpiar caché
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error: {e}")

# ==============================================================================
# PESTAÑA 4: GESTIÓN DE CLIENTES (COMPLETA)
# ==============================================================================
with tabs[3]:
    col_c1, col_c2 = st.columns([1, 2])
    
    # --- A) FORMULARIO NUEVO CLIENTE (IZQUIERDA) ---
    with col_c1:
        st.subheader("👤 Nuevo Cliente")
        with st.container(border=True):
            with st.form("form_cliente"):
                nombre = st.text_input("Nombre Corto / Alias")
                medio = st.selectbox("Medio de Contacto", MEDIOS_CONTACTO)
                contacto = st.text_input("Código/Link (Wsp/FB)")
                telf = st.text_input("Teléfono Principal")
                estado = st.selectbox("Estado Inicial", ESTADOS_CLIENTE)
                f_seguimiento = st.date_input("Fecha Seguimiento", value=date.today())
                
                if st.form_submit_button("💾 Guardar Nuevo Cliente"):
                    if nombre:
                        with engine.connect() as conn:
                            # Insertamos por defecto como activo=TRUE
                            conn.execute(text("""
                                INSERT INTO Clientes (nombre_corto, medio_contacto, codigo_contacto, telefono, estado, fecha_seguimiento, activo)
                                VALUES (:n, :m, :c, :t, :e, :f, TRUE)
                            """), {"n": nombre, "m": medio, "c": contacto, "t": telf, "e": estado, "f": f_seguimiento})
                            conn.commit()
                        st.success(f"Cliente '{nombre}' creado.")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("El nombre es obligatorio.")

    # --- B) DIRECTORIO Y EDICIÓN (DERECHA) ---
    with col_c2:
        st.subheader("📂 Directorio y Edición")
        
        # 1. BUSCADOR (Solo mostramos clientes ACTIVOS)
        search_cli = st.text_input("🔍 Buscar Cliente:", placeholder="Nombre, Teléfono...")
        
        with engine.connect() as conn:
            q_cli = "SELECT * FROM Clientes WHERE activo = TRUE"
            params_cli = {}
            if search_cli:
                q_cli += " AND (nombre_corto ILIKE :s OR telefono ILIKE :s)"
                params_cli = {"s": f"%{search_cli}%"}
            q_cli += " ORDER BY id_cliente DESC LIMIT 20"
            df_cli = pd.read_sql(text(q_cli), conn, params=params_cli)

        # Mostramos la lista para seleccionar
        st.dataframe(df_cli[['id_cliente', 'nombre_corto', 'telefono', 'estado']], width='stretch', hide_index=True)

        # --- C) ZONA DE EDICIÓN DEL CLIENTE SELECCIONADO ---
        if not df_cli.empty:
            st.divider()
            
            # Selector del cliente a trabajar
            opciones_cli = df_cli.set_index('id_cliente')['nombre_corto'].to_dict()
            id_sel = st.selectbox("Seleccionar Cliente para Editar/Ver:", options=opciones_cli.keys(), format_func=lambda x: opciones_cli[x])
            
            # Traemos datos frescos de ese cliente
            cliente_data = df_cli[df_cli['id_cliente'] == id_sel].iloc[0]

            # --- SECCIÓN 1: MODIFICAR DATOS DEL CLIENTE ---
            with st.expander(f"📝 Editar Datos de: {cliente_data['nombre_corto']}", expanded=False):
                with st.form("form_edit_cli"):
                    c_e1, c_e2 = st.columns(2)
                    new_nombre = c_e1.text_input("Nombre", value=cliente_data['nombre_corto'])
                    new_telf = c_e2.text_input("Teléfono", value=cliente_data['telefono'])
                    new_medio = c_e1.selectbox("Medio", MEDIOS_CONTACTO, index=MEDIOS_CONTACTO.index(cliente_data['medio_contacto']) if cliente_data['medio_contacto'] in MEDIOS_CONTACTO else 0)
                    new_estado = c_e2.selectbox("Estado", ESTADOS_CLIENTE, index=ESTADOS_CLIENTE.index(cliente_data['estado']) if cliente_data['estado'] in ESTADOS_CLIENTE else 0)
                    
                    col_btn_save, col_btn_del = st.columns([3, 1])
                    guardar = col_btn_save.form_submit_button("✅ Actualizar Datos")
                    # Botón truco para borrar (requiere confirmación visual)
                    eliminar = col_btn_del.checkbox("🗑️ Eliminar Cliente")

                    if guardar:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                if eliminar:
                                    # SOFT DELETE: Solo cambiamos activo = False
                                    conn.execute(text("UPDATE Clientes SET activo = FALSE WHERE id_cliente = :id"), {"id": id_sel})
                                    st.warning("Cliente eliminado (oculto).")
                                else:
                                    conn.execute(text("""
                                        UPDATE Clientes SET nombre_corto=:n, telefono=:t, medio_contacto=:m, estado=:e 
                                        WHERE id_cliente=:id
                                    """), {"n": new_nombre, "t": new_telf, "m": new_medio, "e": new_estado, "id": id_sel})
                                    st.success("Datos actualizados.")
                                trans.commit()
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error: {e}")

            # --- SECCIÓN 2: GESTIONAR DIRECCIONES ---
            st.write("📍 **Gestión de Direcciones de Envío**")
            
            with engine.connect() as conn:
                # Traemos direcciones activas e incluimos observación
                q_dir = """
                    SELECT id_direccion, tipo_envio, direccion_texto, distrito, referencia, 
                           agencia_nombre, sede_entrega, dni_receptor, nombre_receptor, telefono_receptor, 
                           observacion 
                    FROM Direcciones 
                    WHERE id_cliente = :id AND activo = TRUE
                    ORDER BY id_direccion DESC
                """
                df_dirs = pd.read_sql(text(q_dir), conn, params={"id": id_sel})

            if not df_dirs.empty:
                # Agregamos columna falsa para el checkbox de eliminar
                df_dirs["¿Eliminar?"] = False
                
                # EDITOR DE DATOS
                st.caption("Edita las celdas directamente. Marca '¿Eliminar?' para borrar una fila.")
                cambios_dir = st.data_editor(
                    df_dirs,
                    key="editor_direcciones",
                    column_config={
                        "id_direccion": None, # Ocultar ID
                        "tipo_envio": st.column_config.SelectboxColumn("Tipo", options=["MOTO", "AGENCIA"], width="small"),
                        "direccion_texto": st.column_config.TextColumn("Dirección", width="medium"),
                        "observacion": st.column_config.TextColumn("Observaciones 👀", width="medium"),
                        "¿Eliminar?": st.column_config.CheckboxColumn("¿Borrar?", help="Marca y guarda para eliminar")
                    },
                    hide_index=True,
                    num_rows="fixed",
                    width='stretch'
                )
                
                # BOTÓN GUARDAR CAMBIOS DIRECCIONES
                edited_rows = st.session_state["editor_direcciones"].get("edited_rows")
                
                # Solo mostramos el botón si hay cambios detectados
                if edited_rows: 
                    if st.button("💾 Guardar Cambios en Direcciones"):
                        with engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                count = 0
                                for idx, updates in edited_rows.items():
                                    id_dir_real = df_dirs.iloc[idx]['id_direccion']
                                    
                                    # 1. Chequear si se marcó eliminar
                                    if updates.get("¿Eliminar?") is True:
                                        conn.execute(text("UPDATE Direcciones SET activo = FALSE WHERE id_direccion = :id"), {"id": id_dir_real})
                                        count += 1
                                    else:
                                        # 2. Actualizar campos modificados
                                        # Filtramos solo las columnas que existen en la BD (quitamos la columna checkbox)
                                        campos_validos = [k for k in updates.keys() if k != "¿Eliminar?"]
                                        if campos_validos:
                                            set_clause = ", ".join([f"{col} = :{col}" for col in campos_validos])
                                            query = text(f"UPDATE Direcciones SET {set_clause} WHERE id_direccion = :id_dir")
                                            # Unimos los nuevos valores con el ID para los parametros
                                            params = updates.copy()
                                            params["id_dir"] = id_dir_real
                                            if "¿Eliminar?" in params: del params["¿Eliminar?"] # Limpieza
                                            
                                            conn.execute(query, params)
                                            count += 1
                                
                                trans.commit()
                                st.success(f"Procesados {count} cambios.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                trans.rollback()
                                st.error(f"Error al guardar: {e}")

            else:
                st.info("Este cliente no tiene direcciones registradas.")
# --- SECCIÓN 3: AGREGAR NUEVA DIRECCIÓN (DINÁMICA) ---
            with st.expander("➕ Agregar Nueva Dirección", expanded=False):
                # 1. Selector de Tipo
                tipo_envio = st.radio("Tipo de Envío:", ["MOTO", "AGENCIA"], horizontal=True, key="new_tipo_envio")
                
                # Inicializamos variables vacías para evitar errores en el INSERT
                recibe, telf_recibe = "", ""
                direcc, dist, ref = "", "", ""
                dni, agencia, sede = "", "", ""
                obs = ""

                # 2. Formulario Dinámico
                if tipo_envio == "MOTO":
                    st.caption("🛵 Datos para envío local (Motorizado)")
                    c_m1, c_m2 = st.columns(2)
                    recibe = c_m1.text_input("Nombre quien recibe:", value=cliente_data['nombre_corto'])
                    telf_recibe = c_m2.text_input("Teléfono contacto:", value=cliente_data['telefono'])
                    
                    direcc = st.text_input("Dirección Exacta (Calle/Av + Número):")
                    c_m3, c_m4 = st.columns(2)
                    dist = c_m3.text_input("Distrito:")
                    obs = c_m4.text_input("Observaciones (Fachada, timbre, etc):")
                    
                    # Campos de Agencia quedan vacíos
                    dni, agencia, sede = "", "", ""

                else: # AGENCIA
                    st.caption("🚛 Datos para envío provincial (Encomienda)")
                    c_a1, c_a2, c_a3 = st.columns(3)
                    recibe = c_a1.text_input("Nombre quien recibe:", value=cliente_data['nombre_corto'])
                    dni = c_a2.text_input("DNI Receptor:")
                    telf_recibe = c_a3.text_input("Teléfono contacto:", value=cliente_data['telefono'])
                    
                    c_a4, c_a5 = st.columns(2)
                    agencia = c_a4.text_input("Agencia:", value="Shalom") # Por defecto Shalom
                    sede = c_a5.text_input("Sede / Ciudad Destino:")
                    obs = st.text_input("Observaciones (Ej: Pago en destino):")
                    
                    # Campos de Moto quedan vacíos
                    direcc, dist, ref = "", "", ""

                # 3. Botón de Guardado
                if st.button("💾 Guardar Dirección"):
                    # Validación mínima
                    if not recibe or not telf_recibe:
                        st.error("El nombre y teléfono son obligatorios.")
                    elif tipo_envio == "MOTO" and not direcc:
                         st.error("Para Moto debes poner la dirección.")
                    elif tipo_envio == "AGENCIA" and (not dni or not sede):
                         st.error("Para Agencia el DNI y la Sede son obligatorios.")
                    else:
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO Direcciones (id_cliente, tipo_envio, nombre_receptor, telefono_receptor, 
                                    direccion_texto, distrito, referencia, dni_receptor, agencia_nombre, sede_entrega, observacion, activo)
                                VALUES (:id, :tipo, :nom, :tel, :dir, :dist, '', :dni, :age, :sede, :obs, TRUE)
                            """), {
                                "id": int(id_sel), "tipo": tipo_envio, "nom": recibe, "tel": telf_recibe,
                                "dir": direcc, "dist": dist, 
                                "dni": dni, "age": agencia, "sede": sede, "obs": obs
                            })
                            conn.commit()
                        st.success("✅ Dirección agregada correctamente.")
                        time.sleep(1)
                        st.rerun()

# ==============================================================================
# PESTAÑA 5: HISTORIAL VENTAS (NUEVO)
# ==============================================================================
with tabs[4]:
    st.subheader("📜 Historial de Ventas")
    if st.button("Cargar Ventas"):
        with engine.connect() as conn:
            # Query poderosa: Ventas + Nombre Cliente
            q = """
                SELECT v.id_venta, v.fecha_venta, c.nombre_corto as Cliente, 
                       v.total_venta, v.tipo_envio, v.nota
                FROM Ventas v
                JOIN Clientes c ON v.id_cliente = c.id_cliente
                ORDER BY v.id_venta DESC LIMIT 50
            """
            df_v = pd.read_sql(text(q), conn)
        st.dataframe(df_v, width='stretch')
        
        # Ver detalles de una venta
        id_ver = st.number_input("Ver detalles de Venta ID:", min_value=1, step=1)
        if id_ver:
             with engine.connect() as conn:
                q_det = "SELECT descripcion, cantidad, precio_unitario, subtotal FROM DetalleVenta WHERE id_venta = :id"
                df_det = pd.read_sql(text(q_det), conn, params={"id": id_ver})
             if not df_det.empty:
                 st.write(f"**Items de la Venta #{id_ver}:**")
                 st.dataframe(df_det)
             else:
                 st.warning("No se encontraron detalles o venta no existe.")

# ==============================================================================
# PESTAÑA 6: SEGUIMIENTO (CRM) - Lógica Inteligente
# ==============================================================================
with tabs[5]:
    col_head, col_action = st.columns([3, 1])
    with col_head:
        st.subheader("📆 Calendario de Seguimiento")
        st.info("💡 Nota: Los clientes en estado **'Sin empezar'** se ocultarán de esta lista automáticamente.")
    with col_action:
        if st.button("🔄 Actualizar Lista"):
            if 'df_crm' in st.session_state: del st.session_state['df_crm']
            st.rerun()

    # 1. CARGA DE DATOS (FILTRADA)
    if 'df_crm' not in st.session_state:
        with engine.connect() as conn:
            # CAMBIO AQUÍ: Filtramos para NO traer a los "Sin empezar"
            # Solo traemos gente que requiere atención activa
            q_crm = """
                SELECT id_cliente, nombre_corto, telefono, estado, fecha_seguimiento, medio_contacto
                FROM Clientes 
                WHERE estado != 'Sin empezar'
                ORDER BY fecha_seguimiento ASC
            """
            st.session_state.df_crm = pd.read_sql(text(q_crm), conn)
    
    df_show_crm = st.session_state.df_crm.copy()

    if not df_show_crm.empty:
        # 2. EDITOR INTERACTIVO
        cambios_crm = st.data_editor(
            df_show_crm,
            key="editor_crm",
            column_config={
                "id_cliente": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "nombre_corto": st.column_config.TextColumn("Cliente", disabled=True),
                "telefono": st.column_config.TextColumn("Teléfono", disabled=True),
                "medio_contacto": st.column_config.TextColumn("Origen", disabled=True),
                "estado": st.column_config.SelectboxColumn(
                    "Estado Actual",
                    options=ESTADOS_CLIENTE, 
                    width="medium",
                    required=True
                ),
                "fecha_seguimiento": st.column_config.DateColumn(
                    "📅 Próx. Contacto",
                    min_value=date(2023, 1, 1),
                    format="DD/MM/YYYY",
                    step=1
                )
            },
            hide_index=True,
            width='stretch', # Recuerda usar width='stretch' si aplicaste el fix del inicio
            num_rows="fixed"
        )

        # 3. GUARDAR CAMBIOS (LÓGICA MEJORADA)
        edited_rows_crm = st.session_state["editor_crm"].get("edited_rows")

        if edited_rows_crm:
            st.warning(f"Tienes {len(edited_rows_crm)} cambios pendientes.")
            
            if st.button("💾 Guardar Seguimiento"):
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        count = 0
                        for idx, updates in edited_rows_crm.items():
                            id_cliente_real = df_show_crm.iloc[idx]['id_cliente']
                            
                            sql_parts = []
                            params = {"id": int(id_cliente_real)}
                            
                            # --- LÓGICA ESPECIAL "SIN EMPEZAR" ---
                            # Si el usuario cambia el estado a "Sin empezar", forzamos borrar la fecha
                            nuevo_estado = updates.get("estado")
                            
                            if nuevo_estado == "Sin empezar":
                                sql_parts.append("estado = :est")
                                params["est"] = "Sin empezar"
                                # Forzamos fecha NULL en la base de datos
                                sql_parts.append("fecha_seguimiento = NULL")
                            else:
                                # Comportamiento normal
                                if "estado" in updates:
                                    sql_parts.append("estado = :est")
                                    params["est"] = updates["estado"]
                                
                                if "fecha_seguimiento" in updates:
                                    sql_parts.append("fecha_seguimiento = :fec")
                                    params["fec"] = updates["fecha_seguimiento"]
                            
                            if sql_parts:
                                query = text(f"UPDATE Clientes SET {', '.join(sql_parts)} WHERE id_cliente = :id")
                                conn.execute(query, params)
                                count += 1
                        
                        trans.commit()
                        st.success(f"¡{count} registros actualizados!")
                        
                        # Limpiamos caché para que al recargar DESAPAREZCAN los "Sin empezar"
                        del st.session_state['df_crm'] 
                        time.sleep(1)
                        st.rerun()
                        
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error guardando: {e}")
    else:
        st.info("👏 ¡No tienes seguimientos pendientes! Todos tus clientes están 'Sin empezar' o finalizados.")
# ==============================================================================
# PESTAÑA 7: GESTIÓN DE CATÁLOGO (FINAL)
# ==============================================================================
with tabs[6]:
    st.subheader("🔧 Administración de Productos y Variantes")
    
    # --- BARRA LATERAL: BUSCADOR RÁPIDO DE SKU (Para verificar duplicados) ---
    with st.expander("🔎 Verificador Rápido de SKU / Nombre", expanded=False):
        check_str = st.text_input("Escribe para buscar coincidencias:", placeholder="Ej: NL01")
        if check_str:
            with engine.connect() as conn:
                # Busca coincidencias en SKU o en el Nombre del Producto
                q_check = text("""
                    SELECT v.sku, p.modelo, p.nombre as color, v.medida 
                    FROM Variantes v 
                    JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku ILIKE :s OR p.nombre ILIKE :s OR p.modelo ILIKE :s
                    LIMIT 10
                """)
                df_check = pd.read_sql(q_check, conn, params={"s": f"%{check_str}%"})
            if not df_check.empty:
                st.dataframe(df_check, hide_index=True)
            else:
                st.caption("✅ No se encontraron coincidencias.")

    st.divider()
    
    modo_catalogo = st.radio("Acción:", ["🌱 Crear Nuevo", "✏️ Editar / Renombrar"], horizontal=True)

    # LISTA OFICIAL DE COLORES
    COLORES_OFICIALES = ["", "Amarillo", "Azul", "Blanco", "Chocolate", "Dorado", "Gris", "Marrón", "Miel", "Morado", "Multicolor", "Naranja", "Negro", "Rojo", "Rosado", "Turquesa", "Verde"]

    # ------------------------------------------------------------------
    # MODO 1: CREAR NUEVO
    # ------------------------------------------------------------------
    if modo_catalogo == "🌱 Crear Nuevo":
        tipo_creacion = st.selectbox("Tipo de Creación:", 
                                     ["Medida Nueva (Hijo) para Producto Existente", 
                                      "Producto Nuevo (Marca/Color Nuevo)"])
        
        # A) NUEVA MEDIDA (Variante)
        if "Medida Nueva" in tipo_creacion:
            with engine.connect() as conn:
                # Ahora mostramos Marca - Modelo - NOMBRE (Color)
                df_prods = pd.read_sql(text("SELECT id_producto, marca, modelo, nombre FROM Productos ORDER BY marca, modelo, nombre"), conn)
            
            if not df_prods.empty:
                # Helper para el dropdown
                opciones_prod = df_prods.apply(lambda x: f"{x['marca']} {x['modelo']} - {x['nombre']} (ID: {x['id_producto']})", axis=1).to_dict()
                idx_prod = st.selectbox("Selecciona el Producto (Modelo y Color):", options=opciones_prod.keys(), format_func=lambda x: opciones_prod[x])
                id_producto_real = df_prods.iloc[idx_prod]['id_producto']
                
                with st.form("form_add_variante"):
                    st.caption(f"Agregando medida a: **{df_prods.iloc[idx_prod]['nombre']}**")
                    c1, c2 = st.columns(2)
                    sku_new = c1.text_input("Nuevo SKU (Único):").strip()
                    medida_new = c2.text_input("Medida / Graduación:", value="0.00")

                    c3, c4 = st.columns(2)
                    stock_ini = c3.number_input("Stock Inicial:", min_value=0)
                    precio_new = c4.number_input("Precio Venta:", min_value=0.0)
                    
                    ubi_new = st.text_input("Ubicación:")

                    if st.form_submit_button("Guardar Medida"):
                        try:
                            with engine.connect() as conn:
                                # Ya no pedimos nombre_variante ni stock_externo aquí
                                conn.execute(text("""
                                    INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                    VALUES (:sku, :idp, '', :med, :si, :pre, :ubi)
                                """), {
                                    "sku": sku_new, "idp": int(id_producto_real), 
                                    "med": medida_new, "si": stock_ini, "pre": precio_new, "ubi": ubi_new
                                })
                                conn.commit()
                            st.success(f"SKU {sku_new} creado exitosamente.")
                        except Exception as e:
                            st.error(f"Error: {e}")

        # B) PRODUCTO NUEVO (Marca + Modelo + Color)
        else:
            with st.form("form_new_full"):
                st.markdown("**1. Definir Producto (Visual)**")
                c1, c2, c3 = st.columns(3)
                marca = c1.text_input("Marca:")
                modelo = c2.text_input("Modelo:")
                # AQUI va el Nombre (Color) ahora
                nombre_prod = c3.text_input("Nombre (Color):", placeholder="Ej: Gris, Azul...")
                
                c_cat, c_col = st.columns(2)
                categ = c_cat.selectbox("Categoría:", ["Lentes Contacto", "Pelucas", "Accesorios", "Liquidos"])
                color_prin = c_col.selectbox("Color Filtro (Base):", COLORES_OFICIALES)

                c_dia, c_url1 = st.columns(2)
                diametro = c_dia.number_input("Diámetro (mm):", min_value=0.0, step=0.1, format="%.1f")
                url_img = c_url1.text_input("URL Imagen (Foto):")
                url_buy = st.text_input("URL Compra (Importación):")

                st.markdown("**2. Crear Primera Medida (Ej: Plano)**")
                c4, c5, c6 = st.columns(3)
                sku_1 = c4.text_input("SKU Variante:")
                medida_1 = c5.text_input("Medida:", value="0.00")
                prec_1 = c6.number_input("Precio Venta", 0.0)
                
                ubi_1 = st.text_input("Ubicación")

                if st.form_submit_button("Crear Producto Completo"):
                    try:
                        with engine.connect() as conn:
                            trans = conn.begin()
                            # Insertamos 'nombre' en Productos
                            res_p = conn.execute(text("""
                                INSERT INTO Productos (marca, modelo, nombre, categoria, color_principal, diametro, url_imagen, url_compra) 
                                VALUES (:m, :mod, :nom, :cat, :col, :dia, :uimg, :ubuy) RETURNING id_producto
                            """), {
                                "m": marca, "mod": modelo, "nom": nombre_prod, "cat": categ, "col": color_prin, 
                                "dia": str(diametro), "uimg": url_img, "ubuy": url_buy
                            })
                            new_id = res_p.fetchone()[0]

                            # Crear Variante (Medida)
                            conn.execute(text("""
                                INSERT INTO Variantes (sku, id_producto, nombre_variante, medida, stock_interno, precio, ubicacion)
                                VALUES (:sku, :idp, '', :med, 0, :pr, :ub)
                            """), {
                                "sku": sku_1, "idp": new_id, "med": medida_1,
                                "pr": prec_1, "ub": ubi_1
                            })
                            trans.commit()
                        st.success(f"Producto '{nombre_prod}' creado con éxito.")
                    except Exception as e:
                        st.error(f"Error: {e}")

    # ------------------------------------------------------------------
    # MODO 2: EDITAR / RENOMBRAR
    # ------------------------------------------------------------------
    else:
        st.markdown("#### ✏️ Modificar Producto")
        
        sku_edit = st.text_input("Ingresa SKU exacto para editar:", placeholder="Ej: NL152D-0000")
        
        if sku_edit:
            # Traemos 'nombre' de Productos
            with engine.connect() as conn:
                query_full = text("""
                    SELECT v.*, p.marca, p.modelo, p.nombre as nombre_prod, p.categoria, p.diametro, p.color_principal, p.url_imagen, p.url_compra
                    FROM Variantes v 
                    JOIN Productos p ON v.id_producto = p.id_producto
                    WHERE v.sku = :sku
                """)
                df_data = pd.read_sql(query_full, conn, params={"sku": sku_edit})
            
            if not df_data.empty:
                curr = df_data.iloc[0]
                
                # --- VISUALIZACIÓN IMAGEN ---
                col_img, col_form = st.columns([1, 3])
                
                with col_img:
                    if curr['url_imagen']:
                        st.image(curr['url_imagen'], caption="Foto Actual", width='stretch')
                    else:
                        st.info("Sin imagen")

                with col_form:
                    st.info(f"Editando: **{curr['marca']} {curr['modelo']}** - Color: **{curr['nombre_prod']}**")
                    
                    with st.form("form_edit_sku"):
                        # 1. PRODUCTO (Ahora incluye el Nombre/Color)
                        st.markdown("📦 **Datos Generales (Producto)**")
                        
                        c_p1, c_p2, c_p3 = st.columns(3)
                        new_marca = c_p1.text_input("Marca:", value=curr['marca'])
                        new_modelo = c_p2.text_input("Modelo:", value=curr['modelo'])
                        new_nombre_prod = c_p3.text_input("Nombre (Color):", value=curr['nombre_prod'])
                        
                        c_p4, c_p5 = st.columns(2)
                        idx_col = COLORES_OFICIALES.index(curr['color_principal']) if curr['color_principal'] in COLORES_OFICIALES else 0
                        new_color_prin = c_p4.selectbox("Color Filtro:", COLORES_OFICIALES, index=idx_col)
                        val_dia = float(curr['diametro']) if curr['diametro'] else 0.0
                        new_diametro = c_p5.number_input("Diámetro:", value=val_dia, step=0.1, format="%.1f")

                        new_url_img = st.text_input("URL Imagen:", value=curr['url_imagen'] if curr['url_imagen'] else "")
                        new_url_buy = st.text_input("URL Compra:", value=curr['url_compra'] if curr['url_compra'] else "")

                        st.divider()

                        # 2. VARIANTE (SKU y Medidas) - STOCK PROV OCULTO
                        st.markdown(f"🏷️ **Datos de Variante ({curr['sku']})**")
                        col_a, col_b = st.columns(2)
                        new_sku_val = col_a.text_input("SKU:", value=curr['sku'])
                        new_medida = col_b.text_input("Medida:", value=curr['medida'] if curr['medida'] else "0.00")
                        
                        col_e, col_f = st.columns(2)
                        new_precio = col_e.number_input("Precio:", value=float(curr['precio']))
                        new_precio_reb = col_f.number_input("Precio Rebajado:", value=float(curr['precio_rebajado'] if curr['precio_rebajado'] else 0.0))

                        if st.form_submit_button("💾 Guardar Cambios"):
                            try:
                                with engine.connect() as conn:
                                    trans = conn.begin()
                                    
                                    # A) Actualizar Variante
                                    conn.execute(text("""
                                        UPDATE Variantes 
                                        SET sku=:n_sku, medida=:n_med, precio=:n_pre, precio_rebajado=:n_prer
                                        WHERE sku=:old_sku
                                    """), {
                                        "n_sku": new_sku_val, "n_med": new_medida,
                                        "n_pre": new_precio, "n_prer": new_precio_reb,
                                        "old_sku": curr['sku']
                                    })

                                    # B) Actualizar Producto
                                    conn.execute(text("""
                                        UPDATE Productos 
                                        SET marca=:mar, modelo=:mod, nombre=:nom, color_principal=:col, diametro=:dia,
                                            url_imagen=:uimg, url_compra=:ubuy
                                        WHERE id_producto=:idp
                                    """), {
                                        "mar": new_marca, "mod": new_modelo, "nom": new_nombre_prod,
                                        "col": new_color_prin, "dia": str(new_diametro), 
                                        "uimg": new_url_img, "ubuy": new_url_buy,
                                        "idp": int(curr['id_producto'])
                                    })
                                    
                                    trans.commit()
                                
                                st.success("✅ ¡Actualizado!")
                                time.sleep(1.5)
                                st.rerun()

                            except Exception as e:
                                st.error(f"Error: {e}")
            else:
                st.warning("SKU no encontrado.")