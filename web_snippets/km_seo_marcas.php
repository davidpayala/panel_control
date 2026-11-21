/* KM SEO: marca correcta en el schema de producto y páginas de marca (/marca/...) en español.
   Mantenido desde el repo panel-control (web_snippets/km_seo_marcas.php). */

if ( ! function_exists( 'km_marca_actual' ) ) {
	function km_marca_actual() {
		if ( ! function_exists( 'is_tax' ) || ! is_tax( 'product_brand' ) ) {
			return null;
		}
		$t = get_queried_object();
		return ( $t instanceof WP_Term ) ? $t : null;
	}
}

if ( ! function_exists( 'km_marca_nombre_largo' ) ) {
	function km_marca_nombre_largo( $t ) {
		return ( 'Otros' === $t->name ) ? 'Lentes de Contacto de Otras Marcas' : 'Lentes de Contacto ' . $t->name;
	}
}

// 1) Schema Product: la marca sale de la taxonomía "Marcas" (antes salía la categoría).
add_filter( 'rank_math/json_ld', function ( $data, $jsonld ) {
	if ( ! function_exists( 'is_product' ) || ! is_product() || ! is_array( $data ) ) {
		return $data;
	}
	$id    = get_queried_object_id();
	$marca = '';
	$terms = get_the_terms( $id, 'product_brand' );
	if ( $terms && ! is_wp_error( $terms ) ) {
		$marca = $terms[0]->name;
	}
	if ( '' === $marca || 'Otros' === $marca ) {
		$gla = (string) get_post_meta( $id, '_wc_gla_brand', true );
		if ( '' !== $gla ) {
			$marca = $gla;
		} elseif ( '' === $marca ) {
			$marca = 'KM Lentes';
		}
	}
	foreach ( $data as $k => $entity ) {
		if ( is_array( $entity ) && isset( $entity['@type'] ) && in_array( 'Product', (array) $entity['@type'], true ) ) {
			$data[ $k ]['brand'] = array( '@type' => 'Brand', 'name' => $marca );
		}
	}
	return $data;
}, 99, 2 );

// 2) Páginas de marca: título, descripción, H1 y texto de la marca.
add_filter( 'rank_math/frontend/title', function ( $title ) {
	$t = km_marca_actual();
	return $t ? km_marca_nombre_largo( $t ) . ' en Perú | KM Lentes' : $title;
}, 99 );

add_filter( 'rank_math/frontend/description', function ( $desc ) {
	$t = km_marca_actual();
	if ( ! $t ) {
		return $desc;
	}
	$txt = trim( preg_replace( '/\s+/', ' ', wp_strip_all_tags( term_description( $t ) ) ) );
	if ( '' === $txt ) {
		return $desc;
	}
	if ( mb_strlen( $txt ) > 155 ) {
		$txt = rtrim( mb_substr( $txt, 0, 152 ), " ,.;" ) . '...';
	}
	return $txt;
}, 99 );

add_filter( 'render_block_core/query-title', function ( $html ) {
	$t = km_marca_actual();
	if ( ! $t ) {
		return $html;
	}
	$html = str_replace( '>' . esc_html( $t->name ) . '<', '>' . esc_html( km_marca_nombre_largo( $t ) ) . '<', $html );
	$d    = term_description( $t );
	if ( $d ) {
		$html .= '<div class="km-marca-desc alignwide" style="margin-bottom:1.5em">' . wp_kses_post( $d ) . '</div>';
	}
	return $html;
} );

// 3) Marcas sin productos visibles (todo agotado/oculto): noindex y fuera del sitemap.
if ( ! function_exists( 'km_marca_visibles' ) ) {
	function km_marca_visibles( $term_id ) {
		$clave = 'km_marca_vis_' . (int) $term_id;
		$n     = get_transient( $clave );
		if ( false === $n ) {
			$q = new WP_Query( array(
				'post_type'      => 'product',
				'post_status'    => 'publish',
				'fields'         => 'ids',
				'posts_per_page' => 1,
				'no_found_rows'  => true,
				'tax_query'      => array(
					array( 'taxonomy' => 'product_brand', 'field' => 'term_id', 'terms' => (int) $term_id ),
					array( 'taxonomy' => 'product_visibility', 'field' => 'name', 'terms' => array( 'exclude-from-catalog' ), 'operator' => 'NOT IN' ),
				),
			) );
			$n = count( $q->posts );
			set_transient( $clave, $n, 6 * HOUR_IN_SECONDS );
		}
		return (int) $n;
	}
}

add_filter( 'rank_math/frontend/robots', function ( $robots ) {
	$t = km_marca_actual();
	if ( $t && 0 === km_marca_visibles( $t->term_id ) ) {
		$robots['index'] = 'noindex';
	}
	return $robots;
}, 99 );

add_filter( 'rank_math/sitemap/entry', function ( $url, $type, $object ) {
	if ( 'term' === $type && $object instanceof WP_Term && 'product_brand' === $object->taxonomy
		&& 0 === km_marca_visibles( $object->term_id ) ) {
		return false;
	}
	return $url;
}, 99, 3 );
