/* KM: galería de producto más rápida (pedido del usuario, 21-nov-2026).
   1) WooCommerce oculta la galería (opacity:0) hasta que carga el script del carrusel, que en
      celular llega a los ~7 s: la foto ya descargada no se veía. Se muestra desde el inicio.
   2) En pantallas táctiles el zoom no se usa, pero descargaba la foto original (~700 KB) en cada
      visita. Se apaga solo en dispositivos sin "hover"; tocar la foto sigue abriéndola en grande.
   Mantenido desde el repo panel-control (web_snippets/km_galeria_producto.php). */
add_action( 'wp_head', function () {
	if ( function_exists( 'is_product' ) && is_product() ) {
		// El tema ya muestra las fotos 2+ como miniaturas (25 %) antes del carrusel, así que el espacio
		// queda reservado; solo hay que dar tamaño a las miniaturas que crea el carrusel.
		$g   = '.woocommerce-product-gallery';
		$css = $g . '{opacity:1!important}'
			. $g . ' .flex-control-thumbs li img{aspect-ratio:1/1;width:100%;height:auto;object-fit:cover}'
			// Al arrancar, el carrusel apila las fotos un instante (la galería mide ~2000 px y todo salta).
			// La caja del carrusel nace cuadrada; cuando el carrusel fija su altura, manda la suya.
			. $g . ' .flex-viewport{aspect-ratio:1/1;overflow:hidden}';
		echo '<style id="km-galeria">' . $css . '</style>' . "\n";
	}
}, 5 );

add_action( 'wp_enqueue_scripts', function () {
	if ( function_exists( 'is_product' ) && is_product() ) {
		wp_add_inline_script(
			'wc-single-product',
			'if(window.wc_single_product_params&&window.matchMedia&&window.matchMedia("(hover: none)").matches){wc_single_product_params.zoom_enabled="";}',
			'after'
		);
	}
}, 99 );
