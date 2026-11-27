<?php
// KM: temas de temporada — estilo visual del sitio ligado a la promo del panel (Campañas → Promociones).
// Se activa solo mientras la promo está vigente (km_promo_vigente, snippet 'KM: promociones') y su campo
// 'tema' no está vacío. Al terminar la promo (por fecha o apagándola) el sitio vuelve solo a su estilo normal:
// no se toca ninguna página ni plantilla, todo es CSS + un cambio de imagen al vuelo en el hero de la portada.
// Para añadir otra temporada (p. ej. Fiestas Patrias) basta con agregar una entrada en km_tema_estilos().
// Mantenido desde el repo panel-control (web_snippets/km_temas.php).

// Imagen normal del hero de la portada (la que vuelve cuando no hay tema).
if ( ! defined( 'KM_HERO_NORMAL' ) ) {
	define( 'KM_HERO_NORMAL', '/wp-content/uploads/2025/03/lentes-de-contacto.webp' );
}

// Tema activo hoy: 'navidad', … o '' (estilo normal). Vista previa: ?km_tema=navidad (solo administradores).
if ( ! function_exists( 'km_tema_activo' ) ) {
	function km_tema_activo() {
		static $t = null;
		if ( null !== $t ) {
			return $t;
		}
		$t = '';
		if ( isset( $_GET['km_tema'] ) && current_user_can( 'manage_woocommerce' ) ) {
			$t = sanitize_key( wp_unslash( $_GET['km_tema'] ) );
			return $t;
		}
		if ( function_exists( 'km_promo_vigente' ) && km_promo_vigente() ) {
			$t = sanitize_key( (string) ( km_promo_cfg()['tema'] ?? '' ) );
		}
		return $t;
	}
}

// Banner que reemplaza a la imagen del hero de la portada mientras hay tema (configurado en el panel).
if ( ! function_exists( 'km_tema_banner' ) ) {
	function km_tema_banner() {
		if ( '' === km_tema_activo() || ! function_exists( 'km_promo_cfg' ) ) {
			return null;
		}
		$c = km_promo_cfg();
		if ( empty( $c['banner'] ) ) {
			return null;
		}
		return array(
			'src'    => $c['banner'],
			'alt'    => $c['banner_alt'] ?? '',
			'enlace' => $c['banner_enlace'] ?? '',
			'w'      => (int) ( $c['banner_w'] ?? 0 ),
			'h'      => (int) ( $c['banner_h'] ?? 0 ),
		);
	}
}

// Imagen a precargar en la portada (la usa el snippet 'banner optimi').
if ( ! function_exists( 'km_hero_imagen' ) ) {
	function km_hero_imagen() {
		$b = km_tema_banner();
		return $b ? $b['src'] : KM_HERO_NORMAL;
	}
}

// Portada: la imagen del hero (bloque con clase km-hero__img) pasa a ser el banner de la temporada.
add_filter( 'render_block_core/image', function ( $html, $block ) {
	if ( false === strpos( $block['attrs']['className'] ?? '', 'km-hero__img' ) || ! is_front_page() ) {
		return $html;
	}
	$b = km_tema_banner();
	if ( ! $b ) {
		return $html;
	}
	$dim = ( $b['w'] && $b['h'] ) ? ' width="' . $b['w'] . '" height="' . $b['h'] . '" style="aspect-ratio:' . $b['w'] . '/' . $b['h'] . '"' : '';
	$img = '<img src="' . esc_url( $b['src'] ) . '" alt="' . esc_attr( $b['alt'] ) . '"' . $dim . ' fetchpriority="high" decoding="async"/>';
	if ( $b['enlace'] ) {
		$img = '<a href="' . esc_url( $b['enlace'] ) . '">' . $img . '</a>';
	}
	return '<figure class="wp-block-image size-full km-hero__img km-hero__img--tema">' . $img . '</figure>';
}, 10, 2 );

// Estilos por tema. Cada uno redefine los colores del sitio (variables --km-*) y añade adornos.
if ( ! function_exists( 'km_tema_estilos' ) ) {
	function km_tema_estilos() {
		$copo = "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23000' stroke-width='2' stroke-linecap='round'%3E%3Cpath d='M12 2v20M4.9 7l14.2 10M4.9 17L19.1 7M9 4l3 2 3-2M9 20l3-2 3 2'/%3E%3C/svg%3E\")";
		return array(
			'navidad' => array(
				'nieve' => true,
				'css'   =>
					':root{--km-p:#b3202a;--km-pd:#8a141d;--km-dark:#3a0d12;--km-soft:#fdf6ee;--km-line:#f0e1d2;--kmn-rojo:#b3202a;--kmn-verde:#17603f;--kmn-oro:#e0b25a;--ico-copo:' . $copo . '}'
					// Franja superior: roja, con íconos dorados.
					. '.km-topbar{background:linear-gradient(90deg,#8a141d,#b3202a 50%,#8a141d)}'
					. '.km-topbar p:before{background:var(--kmn-oro)}'
					. '.km-topbar .km-tb-envio:before{-webkit-mask:var(--ico-copo) center/contain no-repeat;mask:var(--ico-copo) center/contain no-repeat}'
					// Cabecera: guirnalda de bastón de caramelo bajo la barra.
					. '.km-header{border-bottom:0;position:relative}'
					. '@media(min-width:782px){.km-header{position:sticky}}'
					. '.km-header:after{content:"";position:absolute;left:0;right:0;bottom:-5px;height:5px;z-index:2;background:repeating-linear-gradient(-45deg,var(--kmn-rojo) 0 10px,#fff 10px 16px,var(--kmn-verde) 16px 26px,#fff 26px 32px)}'
					// Botones principales en rojo (los de borde se mantienen).
					. '.wp-block-button:not(.is-style-outline)>.wp-block-button__link,button.single_add_to_cart_button,.wc-block-components-button:not(.is-link),.wp-block-search__button,.wc-block-cart__submit-button{background-color:var(--kmn-rojo)!important;color:#fff!important}'
					. '.wp-block-button:not(.is-style-outline)>.wp-block-button__link:hover,button.single_add_to_cart_button:hover,.wc-block-components-button:not(.is-link):hover{background-color:var(--km-pd)!important}'
					// Hero de la portada y de las páginas de categoría: rojo profundo con nieve suave.
					. '.km-hero{background:radial-gradient(circle at 12% 20%,rgba(255,255,255,.18) 0 2px,transparent 3px) 0 0/90px 90px,radial-gradient(circle at 70% 60%,rgba(255,255,255,.12) 0 1.5px,transparent 2.5px) 0 0/60px 60px,linear-gradient(135deg,#4a0a10 0%,#8a141d 55%,#b3202a 100%)}'
					. '.km-hero .km-eyebrow{background:rgba(224,178,90,.18);border-color:rgba(224,178,90,.55);color:#f6dfae}'
					. '.km-hero .km-hero__desc{color:#f6e6e3}'
					. '.km-hero .km-btn-claro .wp-block-button__link{background:#fff!important;color:#8a141d!important}'
					. '.km-hero .km-btn-claro .wp-block-button__link:hover{background:#fbeee0!important;color:#8a141d!important}'
					. '.km-hero__checks li:before{background:var(--kmn-oro);color:#5c0b12}'
					. '.km-hero__img--tema img{border-radius:var(--km-r);box-shadow:0 18px 40px rgba(0,0,0,.35)}'
					// Tarjetas de producto: borde dorado al pasar el mouse; etiqueta de oferta verde.
					. 'li.wc-block-product:hover{border-color:var(--kmn-oro)}'
					. '.wc-block-components-product-sale-badge{background:var(--kmn-verde)!important}'
					// Títulos de sección con un adorno dorado.
					. '.entry-content>h2.wp-block-heading:after,.wp-block-post-content>h2.wp-block-heading:after{content:"";display:block;width:56px;height:3px;margin:10px auto 0;border-radius:3px;background:linear-gradient(90deg,var(--kmn-rojo),var(--kmn-oro),var(--kmn-verde))}'
					. '.entry-content>h2.wp-block-heading.has-text-align-left:after,.wp-block-post-content>h2.wp-block-heading.has-text-align-left:after{margin-left:0}'
					// Pie de página: verde pino con borde nevado.
					. '.km-footer{background:linear-gradient(180deg,#0f3d2a,#0a2c1e)!important;position:relative}'
					. '.km-footer:before{content:"";position:absolute;left:0;right:0;top:-1px;height:14px;background:radial-gradient(circle at 10px 0,#fff 0 9px,transparent 10px) 0 0/20px 14px repeat-x}'
					. '.km-footer a:hover{color:var(--kmn-oro)!important}'
					. '.km-footer .km-social .wp-social-link:hover{background:var(--kmn-rojo)!important}'
					. '.km-pagos .km-pagos__txt:before{background:var(--kmn-oro)}'
					// Ficha y carrito.
					. '.km-confianza li:before{background:var(--kmn-verde)}'
					. '.km-promo-ficha{border-color:var(--kmn-rojo);background:#fff7f1 linear-gradient(90deg,var(--kmn-rojo),var(--kmn-oro),var(--kmn-verde)) top/100% 4px no-repeat}',
			),
		);
	}
}

add_action( 'wp_head', function () {
	$t = km_tema_activo();
	$e = km_tema_estilos();
	if ( '' === $t || empty( $e[ $t ] ) ) {
		return;
	}
	$css = $e[ $t ]['css'];
	$nieve = $e[ $t ]['nieve'] && ( ! function_exists( 'km_promo_cfg' ) || ! isset( km_promo_cfg()['tema_nieve'] ) || km_promo_cfg()['tema_nieve'] );
	if ( $nieve ) {
		// Nieve: dos capas de puntos que bajan con transform (no repinta la página). Sin animación si el
		// visitante pidió reducir movimiento; en móvil, una sola capa.
		$capa = 'radial-gradient(circle at 20% 30%,#fff 0 2px,transparent 2.5px),radial-gradient(circle at 75% 10%,#fff 0 1.5px,transparent 2px),radial-gradient(circle at 50% 70%,#fff 0 2.5px,transparent 3px),radial-gradient(circle at 90% 85%,#fff 0 1.5px,transparent 2px)';
		$css .= '.km-nieve{position:fixed;inset:0;pointer-events:none;z-index:9990;overflow:hidden}'
			. '.km-nieve:before,.km-nieve:after{content:"";position:absolute;left:0;right:0;top:-600px;height:calc(100% + 600px);background-image:' . $capa . ';background-size:300px 300px;opacity:.5;filter:drop-shadow(0 0 1px rgba(150,170,195,.35));animation:km-nieve 22s linear infinite;will-change:transform}'
			. '.km-nieve:after{background-size:220px 220px;background-position:110px 40px;opacity:.35;animation-duration:34s;top:-660px;height:calc(100% + 660px)}'
			. '@keyframes km-nieve{to{transform:translateY(600px)}}'
			. '.km-nieve:after{animation-name:km-nieve2}@keyframes km-nieve2{to{transform:translateY(660px)}}'
			. '@media(max-width:781px){.km-nieve:after{display:none}.km-nieve:before{opacity:.4}}'
			. '@media(prefers-reduced-motion:reduce){.km-nieve{display:none}}';
	}
	echo '<style id="km-tema-' . esc_attr( $t ) . '">' . $css . '</style>' . "\n";
}, 40 );

add_action( 'wp_body_open', function () {
	$t = km_tema_activo();
	$e = km_tema_estilos();
	if ( '' === $t || empty( $e[ $t ]['nieve'] ) ) {
		return;
	}
	if ( function_exists( 'km_promo_cfg' ) && isset( km_promo_cfg()['tema_nieve'] ) && ! km_promo_cfg()['tema_nieve'] ) {
		return;
	}
	echo '<div class="km-nieve" aria-hidden="true"></div>';
} );

// La clase en <body> permite ajustes finos por tema desde otros snippets (body.km-tema-navidad …).
add_filter( 'body_class', function ( $c ) {
	$t = km_tema_activo();
	if ( '' !== $t ) {
		$c[] = 'km-tema-' . $t;
	}
	return $c;
} );
