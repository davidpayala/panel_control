"""
🏷️ Marca por ubicación: cada SKU en un estante (fila de Stock_Ubicaciones) tiene UNA marca.

Se guarda en la tabla Ubicacion_Marca (sku, id_ubicacion) → marca, porque Stock_Ubicaciones es de
otro dueño en PostgreSQL y el panel no puede agregarle columnas.

Así, si un lente se compra a dos marcas (ej. FL022C: UYAAI, Meetone, FreshLady), cada marca va en su
propio estante y al ingresar o descontar se elige el estante, y con él la marca.

Las marcas posibles de un SKU salen de Productos.marca (separadas por coma).
La web usa esto para el sello de registro sanitario: solo si todo el stock es FreshLady y/o Meetone.
"""
from sqlalchemy import text

MARCAS_REGISTRO = {"freshlady", "meetone"}


def _norm(m):
    return (m or "").strip().lower().replace(" ", "")


def separar_marcas(valor):
    return [m.strip() for m in str(valor or "").split(",") if m.strip() and _norm(m) not in ("otros", "eliminar")]


def asegurar_columna(conn):
    """Crea la tabla de marcas por ubicación y completa las que faltan donde el producto tiene una sola marca."""
    conn.execute(text("""CREATE TABLE IF NOT EXISTS Ubicacion_Marca (
        sku VARCHAR(50) NOT NULL, id_ubicacion INTEGER NOT NULL, marca VARCHAR(80),
        PRIMARY KEY (sku, id_ubicacion))"""))
    conn.execute(text("""
        INSERT INTO Ubicacion_Marca (sku, id_ubicacion, marca)
        SELECT su.sku, su.id_ubicacion, TRIM(p.marca)
        FROM Stock_Ubicaciones su JOIN Variantes v ON v.sku = su.sku JOIN Productos p USING (id_producto)
        WHERE COALESCE(TRIM(p.marca), '') <> '' AND p.marca NOT LIKE '%,%'
          AND LOWER(TRIM(p.marca)) NOT IN ('otros', 'eliminar')
        ON CONFLICT (sku, id_ubicacion) DO UPDATE SET marca = EXCLUDED.marca
        WHERE Ubicacion_Marca.marca IS NULL"""))


# SQL para unir: ... LEFT JOIN Ubicacion_Marca um ON um.sku = su.sku AND um.id_ubicacion = su.id_ubicacion
JOIN_MARCA = "LEFT JOIN Ubicacion_Marca um ON um.sku = su.sku AND um.id_ubicacion = su.id_ubicacion"


def poner_marca(conn, sku, id_ubicacion, marca):
    conn.execute(text("""INSERT INTO Ubicacion_Marca (sku, id_ubicacion, marca) VALUES (:s, :u, :m)
                         ON CONFLICT (sku, id_ubicacion) DO UPDATE SET marca = EXCLUDED.marca"""),
                 {"s": sku, "u": int(id_ubicacion), "m": marca})


def marcas_de_sku(conn, sku):
    m = conn.execute(text("SELECT p.marca FROM Variantes v JOIN Productos p USING (id_producto) WHERE v.sku = :s"),
                     {"s": sku}).scalar()
    return separar_marcas(m)


def etiqueta(estante, marca):
    return f"{estante} · {marca}" if marca else str(estante)


def sumar_en_ubicacion(conn, sku, id_ubicacion, cantidad, marca=None):
    """Suma unidades de un SKU en un estante con su marca.

    Si el estante ya tiene ese SKU con OTRA marca (y unidades), no mezcla: lanza ValueError.
    Si no se indica marca y el producto tiene una sola, se usa esa.
    """
    if not marca:
        ms = marcas_de_sku(conn, sku)
        marca = ms[0] if len(ms) == 1 else None
    fila = conn.execute(text(f"""SELECT su.cantidad, um.marca FROM Stock_Ubicaciones su {JOIN_MARCA}
                                 WHERE su.sku = :s AND su.id_ubicacion = :u"""),
                        {"s": sku, "u": id_ubicacion}).fetchone()
    if fila and (fila.cantidad or 0) > 0 and fila.marca and marca and _norm(fila.marca) != _norm(marca):
        raise ValueError(f"{sku}: ese estante ya tiene {fila.cantidad} un. de {fila.marca}. "
                         f"Pon las de {marca} en otro estante (una marca por ubicación).")
    conn.execute(text("""
        INSERT INTO Stock_Ubicaciones (sku, id_ubicacion, cantidad, fecha_actualizacion)
        VALUES (:s, :u, :c, CURRENT_TIMESTAMP)
        ON CONFLICT (sku, id_ubicacion) DO UPDATE SET
            cantidad = GREATEST(Stock_Ubicaciones.cantidad, 0) + :c, fecha_actualizacion = CURRENT_TIMESTAMP"""),
        {"s": sku, "u": id_ubicacion, "c": int(cantidad)})
    if marca or not fila or (fila.cantidad or 0) <= 0:
        poner_marca(conn, sku, id_ubicacion, marca)
    return marca


def marcas_con_stock(conn):
    """{sku: set(marcas normalizadas)} del stock físico, para decidir el registro sanitario.

    · stock en estantes → la marca de cada estante (si un estante no tiene marca: las del producto)
    · stock interno sin repartir en estantes → las marcas del producto
    · stock del proveedor (stock_externo) → las del producto (el proveedor solo trae FreshLady/Meetone,
      y esos SKU tienen esa marca en el panel)
    """
    filas = conn.execute(text(f"""
        SELECT v.sku, p.marca AS marca_producto, COALESCE(v.stock_interno,0) AS interno,
               COALESCE(v.stock_externo,0) AS externo,
               COALESCE((SELECT SUM(cantidad) FROM Stock_Ubicaciones su WHERE su.sku = v.sku AND su.cantidad > 0), 0) AS en_estantes,
               (SELECT ARRAY_AGG(DISTINCT COALESCE(um.marca, '')) FROM Stock_Ubicaciones su {JOIN_MARCA}
                 WHERE su.sku = v.sku AND su.cantidad > 0) AS marcas_estantes
        FROM Variantes v JOIN Productos p USING (id_producto) WHERE p.macro_categoria = 'Lentes'""")).mappings()
    out = {}
    for f in filas:
        del_producto = {_norm(m) for m in separar_marcas(f["marca_producto"])}
        ms = set()
        for m in f["marcas_estantes"] or []:
            ms |= {_norm(m)} if m else del_producto
        if f["interno"] > f["en_estantes"] or f["externo"] > 0:
            ms |= del_producto
        out[f["sku"]] = ms
    return out


def tiene_registro(marcas_stock, marcas_producto):
    """Registro sanitario: todo el stock disponible es FreshLady y/o Meetone.
    Sin stock: se decide por las marcas del producto."""
    base = marcas_stock or {_norm(m) for m in marcas_producto}
    return bool(base) and base <= MARCAS_REGISTRO
