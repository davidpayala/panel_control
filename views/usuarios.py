# Lógica básica para crear usuario (Solo accesible para el Admin)
with st.form("crear_usuario"):
    nuevo_usr = st.text_input("Nuevo Usuario")
    nuevo_pwd = st.text_input("Contraseña")
    nuevo_rol = st.selectbox("Rol", ["Vendedor", "Logistica", "Admin"])
    modulos = st.multiselect("Módulos permitidos", OPCIONES_BASE)
    
    if st.form_submit_button("Crear Usuario"):
        # Ejecutar INSERT en la tabla Usuarios con ','.join(modulos)
        pass