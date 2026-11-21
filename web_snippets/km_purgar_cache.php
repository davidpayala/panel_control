/* KM: purga la caché de LiteSpeed y del sitemap una sola vez cada vez que cambia $token.
   Mantenido desde el repo panel-control (web_snippets/km_purgar_cache.php). */
add_action( 'init', function () {
	$token = '2026-11-21-c';
	if ( get_option( 'km_purga_token' ) === $token ) {
		return;
	}
	update_option( 'km_purga_token', $token, false );
	// Rank Math: incluir las páginas de marca (product_brand) en el sitemap.
	$sm = get_option( 'rank-math-options-sitemap' );
	if ( is_array( $sm ) && ( isset( $sm['tax_product_brand_sitemap'] ) ? $sm['tax_product_brand_sitemap'] : '' ) !== 'on' ) {
		$sm['tax_product_brand_sitemap'] = 'on';
		update_option( 'rank-math-options-sitemap', $sm );
	}
	if ( class_exists( '\RankMath\Sitemap\Cache' ) ) {
		\RankMath\Sitemap\Cache::invalidate_storage();
	}
	do_action( 'litespeed_purge_all' );
}, 99 );
