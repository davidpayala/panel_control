import streamlit as st
from sqlalchemy import text
from database import engine
from datetime import datetime

st.title("🧪 Inyector de Prueba")

if st.button("Simular Mensaje Entrante"):
    telefono_test = "51900000000" # Número falso
    texto_test = f"Prueba de inyección {datetime.now().strftime('%H:%M:%S')}"
    
    try:
        with engine.begin() as conn:
            # 1. Crear Cliente si no existe
            conn.execute(text("""
                INSERT INTO Clientes (telefono, nombre_corto, estado, activo, fecha_registro)
                VALUES (:t, 'Cliente Prueba', 'Sin empezar', TRUE, NOW())
                ON CONFLICT (telefono) DO NOTHING
            """), {"t": telefono_test})
            
            # 2. Insertar Mensaje
            conn.execute(text("""
                INSERT INTO mensajes (telefono, tipo, contenido, fecha, leido, session_name)
                VALUES (:t, 'ENTRANTE', :txt, NOW(), FALSE, 'default')
            """), {"t": telefono_test, "txt": texto_test})
            
            # 3. Actualizar Sync (Para que el chat lo vea)
            conn.execute(text("UPDATE sync_estado SET version = version + 1 WHERE id = 1"))
            
        st.success(f"✅ Mensaje inyectado para {telefono_test}. ¡Revisa tu Bandeja de Chats!")
    except Exception as e:
        st.error(f"❌ Error DB: {e}")