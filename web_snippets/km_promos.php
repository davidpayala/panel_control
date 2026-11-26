<?php
// KM: promociones — packs de lentes (por ejemplo, Navidad: 2 pares S/99, 3 pares S/139).
// La configuración vive en la opción km_promo. La escribe el panel (Campañas → Promociones) por
// POST /wp-json/km/v1/promo. Fuera de las fechas o con activo=false no se muestra nada ni se descuenta.
// Aplica a lentes de las categorías 148 (naturales) y 307 (fantasía) cuyo precio regular sea <= precio_base.

if ( ! function_exists( 'km_promo_cfg' ) ) {
	function km_promo_cfg() {
		$def = array(
			'activo'      => false,
			'nombre'      => 'Promo Navidad',
			'desde'       => '',
			'hasta'       => '',
			'precio_base' => 55,
			'packs'       => array( array( 'n' => 2, 'precio' => 99 ), array( 'n' => 3, 'precio' => 139 ) ),
			'regalo'      => 'Estuche de regalo con cada par',
			'barra'       => '',
		);
		$c = get_option( 'km_promo' );
		return is_array( $c ) ? array_merge( $def, $c ) : $def;
	}
}

// ¿Está vigente hoy (hora de Lima)?
if ( ! function_exists( 'km_promo_vigente' ) ) {
	function km_promo_vigente() {
		$c = km_promo_cfg();
		if ( empty( $c['activo'] ) || empty( $c['packs'] ) ) {
			return false;
		}
		$hoy = wp_date( 'Y-m-d', null, new DateTimeZone( 'America/Lima' ) );
		if ( $c['desde'] && $hoy < $c['desde'] ) {
			return false;
		}
		if ( $c['hasta'] && $hoy > $c['hasta'] ) {
			return false;
		}
		return true;
	}
}

if ( ! function_exists( 'km_promo_packs' ) ) {
	// Packs ordenados de mayor a menor cantidad: [[n, precio], ...]
	function km_promo_packs() {
		$p = array();
		foreach ( (array) km_promo_cfg()['packs'] as $x ) {
			$n = (int) ( $x['n'] ?? 0 );
			$pr = (float) ( $x['precio'] ?? 0 );
			if ( $n >= 2 && $pr > 0 ) {
				$p[] = array( $n, $pr );
			}
		}
		usort( $p, function ( $a, $b ) { return $b[0] - $a[0]; } );
		return $p;
	}
}

if ( ! function_exists( 'km_promo_es_lente' ) ) {
	function km_promo_es_lente( $product_id ) {
		return has_term( array( 148, 307 ), 'product_cat', $product_id );
	}
}

// Un artículo del carrito entra en la promo si es lente y su precio regular no pasa del precio base.
if ( ! function_exists( 'km_promo_elegible' ) ) {
	function km_promo_elegible( $product ) {
		if ( ! $product ) {
			return false;
		}
		$padre = $product->get_parent_id() ? $product->get_parent_id() : $product->get_id();
		if ( ! km_promo_es_lente( $padre ) ) {
			return false;
		}
		$reg = (float) $product->get_regular_price();
		if ( $reg <= 0 ) {
			$reg = (float) $product->get_price();
		}
		return $reg > 0 && $reg <= (float) km_promo_cfg()['precio_base'] + 0.001;
	}
}

// Para la ficha: un producto variable entra si alguna variante entra (los FreshLady sin medida sí, con medida no).
if ( ! function_exists( 'km_promo_producto_aplica' ) ) {
	function km_promo_producto_aplica( $product ) {
		if ( ! $product || ! km_promo_es_lente( $product->get_id() ) ) {
			return false;
		}
		if ( $product->is_type( 'variable' ) ) {
			$min = (float) $product->get_variation_regular_price( 'min' );
			return $min > 0 && $min <= (float) km_promo_cfg()['precio_base'] + 0.001;
		}
		return km_promo_elegible( $product );
	}
}

// Calcula el descuento: arma packs con los pares más caros primero (lo más favorable para el cliente).
if ( ! function_exists( 'km_promo_calculo' ) ) {
	function km_promo_calculo( $cart ) {
		$unidades = array();
		foreach ( $cart->get_cart() as $item ) {
			if ( km_promo_elegible( $item['data'] ) ) {
				for ( $i = 0; $i < (int) $item['quantity']; $i++ ) {
					$unidades[] = (float) $item['data']->get_price();
				}
			}
		}
		rsort( $unidades );
		$desc   = 0;
		$usados = array();
		$resto  = $unidades;
		foreach ( km_promo_packs() as $pk ) {
			list( $n, $precio ) = $pk;
			while ( count( $resto ) >= $n ) {
				$grupo = array_splice( $resto, 0, $n );
				$ahorro = array_sum( $grupo ) - $precio;
				if ( $ahorro > 0 ) {
					$desc += $ahorro;
					$usados[] = $n;
				}
			}
		}
		return array( 'pares' => count( $unidades ), 'descuento' => round( $desc, 2 ), 'packs' => $usados );
	}
}

add_action( 'woocommerce_cart_calculate_fees', function ( $cart ) {
	if ( ( is_admin() && ! defined( 'DOING_AJAX' ) ) || ! km_promo_vigente() ) {
		return;
	}
	$r = km_promo_calculo( $cart );
	if ( $r['descuento'] > 0 ) {
		// Los precios incluyen IGV y WooCommerce le suma impuesto proporcional a las tarifas negativas:
		// se pasa el monto sin IGV para que el descuento final sea exacto (2 pares = S/99 justo).
		$sub   = (float) $cart->get_subtotal();
		$tax   = (float) $cart->get_subtotal_tax();
		$monto = ( $sub > 0 && $tax > 0 ) ? $r['descuento'] * $sub / ( $sub + $tax ) : $r['descuento'];
		$cart->add_fee( km_promo_cfg()['nombre'] . ' (packs de lentes)', -$monto, false );
	}
} );

if ( ! function_exists( 'km_promo_s' ) ) {
	function km_promo_s( $v ) {
		$v = (float) $v;
		return 'S/' . ( floor( $v ) == $v ? number_format( $v, 0 ) : number_format( $v, 2 ) );
	}
}

// Recuadro en la ficha de producto, sobre el botón "Añadir al carrito".
add_filter( 'render_block_woocommerce/add-to-cart-form', function ( $html ) {
	if ( ! function_exists( 'is_product' ) || ! is_product() || did_action( 'km_promo_ficha' ) || ! km_promo_vigente() ) {
		return $html;
	}
	$product = wc_get_product( get_queried_object_id() );
	if ( ! km_promo_producto_aplica( $product ) ) {
		return $html;
	}
	do_action( 'km_promo_ficha' );
	$c     = km_promo_cfg();
	$packs = array_reverse( km_promo_packs() );
	$items = '<li><strong>1 par</strong> ' . esc_html( km_promo_s( $c['precio_base'] ) ) . '</li>';
	foreach ( $packs as $pk ) {
		$items .= '<li><strong>' . (int) $pk[0] . ' pares</strong> ' . esc_html( km_promo_s( $pk[1] ) ) . '</li>';
	}
	$nota = $product->is_type( 'variable' ) ? ' Aplica a los lentes sin medida de ' . esc_html( km_promo_s( $c['precio_base'] ) ) . '.' : '';
	$box  = '<div class="km-promo-ficha"><div class="km-promo-ficha__t">' . esc_html( $c['nombre'] ) . '</div><ul>' . $items . '</ul>'
		. '<p>Combina modelos y colores: el descuento se aplica solo en el carrito.' . $nota
		. ( $c['regalo'] ? ' ' . esc_html( $c['regalo'] ) . '.' : '' ) . ' <a href="/promociones/">Ver promo</a></p></div>';
	return $box . $html;
}, 9 );

// Aviso en el carrito: cuánto falta para el siguiente pack.
add_filter( 'render_block_woocommerce/proceed-to-checkout-block', function ( $html ) {
	if ( ! km_promo_vigente() || ! function_exists( 'WC' ) || ! WC()->cart ) {
		return $html;
	}
	$r     = km_promo_calculo( WC()->cart );
	$packs = array_reverse( km_promo_packs() );
	$msg   = '';
	foreach ( $packs as $pk ) {
		if ( $r['pares'] < $pk[0] ) {
			$falta = $pk[0] - $r['pares'];
			$msg   = 'Agrega ' . $falta . ( 1 === $falta ? ' par más' : ' pares más' ) . ' de lentes de ' . km_promo_s( km_promo_cfg()['precio_base'] )
				. ' y llévate ' . $pk[0] . ' por ' . km_promo_s( $pk[1] ) . '.';
			break;
		}
	}
	if ( ! $msg && $r['descuento'] > 0 ) {
		$msg = 'Ya tienes tu ' . km_promo_cfg()['nombre'] . ': ahorras ' . km_promo_s( $r['descuento'] ) . '.';
	}
	if ( ! $msg || 0 === $r['pares'] ) {
		return $html;
	}
	return '<p class="km-promo-carrito">' . esc_html( $msg ) . '</p>' . $html;
}, 9 );

// Barra superior: con la promo vigente, el primer aviso ("Envío gratis…") pasa a anunciar la promo.
add_filter( 'render_block_core/paragraph', function ( $html, $block ) {
	if ( false === strpos( $block['attrs']['className'] ?? '', 'km-tb-envio' ) || ! km_promo_vigente() ) {
		return $html;
	}
	$t = trim( (string) km_promo_cfg()['barra'] );
	if ( '' === $t ) {
		return $html;
	}
	return '<p class="km-tb-envio"><a href="/promociones/">' . esc_html( $t ) . '</a></p>';
}, 10, 2 );

// [km_promo]: bloque principal de la página /promociones/ (se arma con la configuración del panel).
add_shortcode( 'km_promo', function () {
	$c = km_promo_cfg();
	if ( ! km_promo_vigente() ) {
		return '<div class="km-promo"><p>La promoción terminó. Mira nuestros <a href="/lentes-naturales/">lentes naturales</a> desde '
			. esc_html( km_promo_s( $c['precio_base'] ) ) . ' con envío gratis a todo el Perú.</p></div>';
	}
	$base  = (float) $c['precio_base'];
	$cards = '<div class="km-promo__card"><span class="km-promo__n">1 par</span><span class="km-promo__p">' . esc_html( km_promo_s( $base ) ) . '</span><span class="km-promo__a">Envío gratis</span></div>';
	foreach ( array_reverse( km_promo_packs() ) as $pk ) {
		$ahorro = $base * $pk[0] - $pk[1];
		$cards .= '<div class="km-promo__card km-promo__card--top"><span class="km-promo__n">' . (int) $pk[0] . ' pares</span><span class="km-promo__p">'
			. esc_html( km_promo_s( $pk[1] ) ) . '</span><span class="km-promo__a">' . ( $ahorro > 0 ? 'Ahorras ' . esc_html( km_promo_s( $ahorro ) ) : 'Envío gratis' ) . '</span></div>';
	}
	$hasta = '';
	if ( $c['hasta'] ) {
		$meses = array( 1 => 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre' );
		$ts    = strtotime( $c['hasta'] );
		$hasta = '<p class="km-promo__hasta">Válido hasta el ' . (int) gmdate( 'j', $ts ) . ' de ' . $meses[ (int) gmdate( 'n', $ts ) ] . ' o hasta agotar stock.</p>';
	}
	return '<div class="km-promo"><div class="km-promo__cards">' . $cards . '</div>' . $hasta . '</div>';
} );

add_action( 'wp_head', function () {
	if ( ! km_promo_vigente() ) {
		return;
	}
	echo '<style id="km-promo">'
		. '.km-promo-ficha{border:2px solid #c0392b;border-radius:12px;padding:12px 16px;margin:0 0 16px;background:#fff7f5}'
		. '.km-promo-ficha__t{font-weight:800;color:#c0392b;margin-bottom:6px}'
		. '.km-promo-ficha ul{display:flex;flex-wrap:wrap;gap:6px 18px;list-style:none;margin:0 0 6px;padding:0}'
		. '.km-promo-ficha p{margin:0;font-size:14px}'
		. '.km-promo-carrito{background:#fff7f5;border:1px solid #f1c4bd;border-radius:10px;padding:10px 14px;font-weight:600;color:#9b2c1f;margin:0 0 12px}'
		. '.km-promo__cards{display:flex;flex-wrap:wrap;gap:16px;justify-content:center}'
		. '.km-promo__card{flex:1 1 180px;max-width:260px;background:#fff;color:#0d2a3a;border:1px solid #e3e8ee;border-radius:14px;padding:22px 16px;text-align:center;display:flex;flex-direction:column;gap:4px}'
		. '.km-promo__card--top{border:2px solid #c0392b;background:#fff7f5}'
		. '.km-promo__n{font-weight:700;font-size:18px}.km-promo__p{font-size:2.2rem;font-weight:800;color:#c0392b;line-height:1.1}'
		. '.km-promo__a{font-size:14px;color:#2e7d32;font-weight:600}.km-promo__hasta{text-align:center;font-size:14px;color:#555;margin-top:12px}.km-hero .km-promo__hasta{color:#e3edf3}'
		. '</style>' . "\n";
}, 30 );

add_action( 'rest_api_init', function () {
	register_rest_route( 'km/v1', '/promo', array(
		'methods'             => 'POST',
		'permission_callback' => function () { return current_user_can( 'manage_woocommerce' ); },
		'callback'            => function ( WP_REST_Request $r ) {
			$fecha = function ( $v ) {
				$v = (string) $v;
				return preg_match( '/^\d{4}-\d{2}-\d{2}$/', $v ) ? $v : '';
			};
			$packs = array();
			foreach ( (array) $r->get_param( 'packs' ) as $p ) {
				$n  = (int) ( $p['n'] ?? 0 );
				$pr = round( (float) ( $p['precio'] ?? 0 ), 2 );
				if ( $n >= 2 && $n <= 10 && $pr > 0 ) {
					$packs[] = array( 'n' => $n, 'precio' => $pr );
				}
			}
			$c = array(
				'activo'      => (bool) $r->get_param( 'activo' ),
				'nombre'      => sanitize_text_field( (string) $r->get_param( 'nombre' ) ) ?: 'Promo',
				'desde'       => $fecha( $r->get_param( 'desde' ) ),
				'hasta'       => $fecha( $r->get_param( 'hasta' ) ),
				'precio_base' => max( 1, round( (float) $r->get_param( 'precio_base' ), 2 ) ),
				'packs'       => $packs,
				'regalo'      => sanitize_text_field( (string) $r->get_param( 'regalo' ) ),
				'barra'       => sanitize_text_field( (string) $r->get_param( 'barra' ) ),
			);
			update_option( 'km_promo', $c, true );
			update_option( 'km_promo_estado', km_promo_vigente() ? 'si' : 'no', false );
			do_action( 'litespeed_purge_all' );
			return array( 'ok' => true, 'vigente' => km_promo_vigente(), 'config' => $c );
		},
	) );
} );

// Cuando la promo empieza o termina por fecha, se purga la caché para que no queden páginas con el recuadro viejo.
add_action( 'init', function () {
	$ahora = km_promo_vigente() ? 'si' : 'no';
	if ( get_option( 'km_promo_estado' ) !== $ahora ) {
		update_option( 'km_promo_estado', $ahora, false );
		do_action( 'litespeed_purge_all' );
	}
}, 99 );
