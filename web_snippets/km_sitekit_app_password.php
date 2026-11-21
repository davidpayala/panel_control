/* KM: que Site Kit reconozca al usuario cuando el panel consulta con contraseña de aplicación.
   Site Kit fija el usuario en init (prioridad -999). En ese momento WordPress todavía no acepta
   contraseñas de aplicación (REST_REQUEST aún no existe) y ya dejó guardado "sin usuario", así que
   Site Kit responde missing_required_scopes. Solo en peticiones a /wp-json/ que traen credenciales
   y sin usuario: se marca como petición de API y se borra el usuario guardado para que WordPress
   valide la contraseña antes de que Site Kit lo lea. La contraseña se verifica igual que siempre.
   Mantenido desde el repo panel-control (web_snippets/km_sitekit_app_password.php). */
if ( ! function_exists( 'km_es_rest_con_credenciales' ) ) {
	function km_es_rest_con_credenciales() {
		$uri = isset( $_SERVER['REQUEST_URI'] ) ? (string) $_SERVER['REQUEST_URI'] : '';
		$es_rest = false !== strpos( $uri, '/wp-json/' ) || isset( $_GET['rest_route'] );
		return $es_rest && ( isset( $_SERVER['PHP_AUTH_USER'] ) || ! empty( $_SERVER['HTTP_AUTHORIZATION'] )
			|| ! empty( $_SERVER['REDIRECT_HTTP_AUTHORIZATION'] ) );
	}
}

add_filter( 'application_password_is_api_request', function ( $es_api ) {
	return $es_api || km_es_rest_con_credenciales();
} );

add_action( 'init', function () {
	if ( km_es_rest_con_credenciales() && 0 === get_current_user_id() ) {
		$GLOBALS['current_user'] = null; // se vuelve a calcular con la contraseña de aplicación
		get_current_user_id();
	}
}, -1000 );
