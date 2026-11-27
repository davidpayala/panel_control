/* Precarga del banner de la portada, solo en la portada (antes se precargaba en todas
   las páginas y competía con la foto principal de cada producto).
   Mantenido desde el repo panel-control (web_snippets/km_banner_preload.php). */
add_action( 'wp_head', function () {
	if ( is_front_page() ) {
		// Con tema de temporada vigente (snippet 'KM: temas de temporada') se precarga su banner; si no, la imagen normal.
		$src = function_exists( 'km_hero_imagen' ) ? km_hero_imagen() : '/wp-content/uploads/2025/03/lentes-de-contacto.webp';
		echo '<link rel="preload" fetchpriority="high" as="image" href="' . esc_url( $src ) . '" type="image/webp">' . "\n";
	}
}, 1 );
