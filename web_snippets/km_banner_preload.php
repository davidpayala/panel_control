/* Precarga del banner de la portada, solo en la portada (antes se precargaba en todas
   las páginas y competía con la foto principal de cada producto).
   Mantenido desde el repo panel-control (web_snippets/km_banner_preload.php). */
add_action( 'wp_head', function () {
	if ( is_front_page() ) {
		echo '<link rel="preload" fetchpriority="high" as="image" href="/wp-content/uploads/2026/11/banner-navidad-2026-lentes-de-contacto.webp" type="image/webp">' . "\n";
	}
}, 1 );
