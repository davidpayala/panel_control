import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
from database import engine
# Importamos la función para actualizar Google (ya que actualizas datos del cliente aquí)
from utils import actualizar_en_google

def render_facturacion():
    st.subheader("🧾 Facturación Individual")
    st.info("Sistema protegido: No permite boletas duplicadas y formatea los nombres automáticamente.")

    # --- 1. CARGAR LISTA DE VENTAS PENDIENTES ---
    with engine.connect() as conn:
        query_pendientes = text("""
            SELECT 
                v.id_venta,
                c.nombre || ' ' || c.apellido as nombre_completo,
                v.fecha_venta,
                v.total_venta
            FROM Ventas v
            JOIN Clientes c ON v.id_cliente = c.id_cliente
            WHERE v.facturado = FALSE
            ORDER BY v.id_venta ASC
        """)
        df_pendientes = pd.read_sql(query_pendientes, conn)

    if df_pendientes.empty:
        st.success("🎉 ¡Felicidades! No hay facturas pendientes.")
    else:
        # --- 2. SELECTOR DE VENTA ---
        opciones_venta = df_pendientes['id_venta'].tolist()
        
        def formato_opcion(id_v):
            fila = df_pendientes[df_pendientes['id_venta'] == id_v]
            if not fila.empty:
                row = fila.iloc[0]
                return f"🆔 {row['id_venta']} | 📅 {row['fecha_venta']} | 👤 {row['nombre_completo']} | 💰 S/ {row['total_venta']}"
            return f"Venta {id_v}"

        seleccion_id = st.selectbox(
            "👇 Elige la venta a procesar:", 
            options=opciones_venta, 
            format_func=formato_opcion
        )

        st.divider()

        # --- 3. CARGAR DETALLES ---
        if seleccion_id:
            with engine.connect() as conn:
                # A) Datos Cliente
                query_cliente = text("""
                    SELECT c.id_cliente, c.nombre, c.apellido, c.dni, c.google_id, c.telefono 
                    FROM Ventas v JOIN Clientes c ON v.id_cliente = c.id_cliente 
                    WHERE v.id_venta = :id
                """)
                cliente_data = pd.read_sql(query_cliente, conn, params={"id": int(seleccion_id)}).iloc[0]

                # B) Ítems
                query_items = text("""
                    SELECT 
                        d.sku as "Código",
                        d.descripcion as "Descripción",
                        d.cantidad as "Cant.",
                        d.precio_unitario as "P.Unit",
                        (d.cantidad * d.precio_unitario) as "Total"
                    FROM DetalleVenta d
                    WHERE d.id_venta = :id
                    
                    UNION ALL
                    
                    SELECT 
                        'ENVIO' as "Código",
                        'Servicio de Envío' as "Descripción",
                        1 as "Cant.",
                        v.costo_envio as "P.Unit",
                        v.costo_envio as "Total"
                    FROM Ventas v
                    WHERE v.id_venta = :id AND v.costo_envio > 0
                """)
                df_items = pd.read_sql(query_items, conn, params={"id": int(seleccion_id)})

            # --- 4. INTERFAZ DE REGISTRO ---
            col_datos, col_tabla = st.columns([1, 2])
            
            with col_datos:
                st.markdown("#### 👤 Datos del Cliente")
                with st.form("form_facturacion"):
                    val_nombre = cliente_data['nombre'] if cliente_data['nombre'] else ""
                    val_apellido = cliente_data['apellido'] if cliente_data['apellido'] else ""
                    val_dni = cliente_data['dni'] if cliente_data['dni'] else ""

                    nuevo_nombre = st.text_input("Nombre", value=val_nombre)
                    nuevo_apellido = st.text_input("Apellido", value=val_apellido)
                    nuevo_dni = st.text_input("DNI / RUC", value=val_dni)
                    
                    st.markdown("---")
                    st.markdown("#### 🧾 Datos de Factura")
                    numero_boleta = st.text_input("N° Boleta (EB01...)", placeholder="Ingresa el número")
                    
                    btn_guardar = st.form_submit_button("✅ Guardar y Archivar", type="primary")
            
            with col_tabla:
                st.markdown(f"#### 🛒 Detalle de Items (Venta {seleccion_id})")
                st.dataframe(df_items, hide_index=True, width='stretch')
                st.caption("👆 Copia estas filas y pégalas en tu sistema contable.")

            # --- 5. LÓGICA DE GUARDADO ---
            if btn_guardar:
                if not numero_boleta:
                    st.warning("⚠️ Debes ingresar el Número de Boleta para continuar.")
                else:
                    nombre_formateado = nuevo_nombre.strip().title() if nuevo_nombre else ""
                    apellido_formateado = nuevo_apellido.strip().title() if nuevo_apellido else ""
                    boleta_limpia = numero_boleta.strip().upper() 

                    with engine.connect() as conn:
                        trans = conn.begin() 
                        try:
                            # 1. VERIFICAR DUPLICADOS DENTRO DE LA TRANSACCIÓN
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
                                        fecha_facturacion = CURRENT_DATE,
                                        numero_boleta = :bol
                                    WHERE id_venta = :vid
                                """), {
                                    "bol": boleta_limpia, 
                                    "vid": int(seleccion_id)
                                })
                                
                                # D. Confirmar todo
                                trans.commit()
                                st.balloons()
                                st.success(f"¡Correcto! Venta guardada con boleta {boleta_limpia}.")
                                time.sleep(1.5)
                                st.rerun()
                                
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error al guardar: {e}")

def render_reporte_mensual():
    st.subheader("📊 Reporte Mensual para Declaración")
    
    query_reporte = text("""
        SELECT 
            TO_CHAR(fecha_facturacion, 'YYYY-MM') AS "Mes",
            COUNT(id_venta) AS "Cant. Boletas",
            SUM(total_venta) AS "Total Ventas (S/)"
        FROM Ventas
        WHERE facturado = TRUE
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
            query_detalle = text("""
                SELECT v.fecha_facturacion, v.numero_boleta, c.nombre || ' ' || c.apellido as cliente, v.total_venta
                FROM Ventas v
                JOIN Clientes c ON v.id_cliente = c.id_cliente
                WHERE TO_CHAR(v.fecha_facturacion, 'YYYY-MM') = :mes
                ORDER BY v.fecha_facturacion ASC
            """)
            with engine.connect() as conn:
                df_detalle = pd.read_sql(query_detalle, conn, params={"mes": mes_seleccionar})
            st.write(f"Detalle de {mes_seleccionar}:")
            st.table(df_detalle)