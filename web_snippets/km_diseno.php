/* KM: diseño profesional y señales de confianza (pedido del usuario, 22-nov-2026).
   - Estilo general: tipografía, botones, tarjetas de producto, cabecera fija y pie de página.
   - Producto: franja de confianza bajo "Añadir al carrito" y guía de uso seguro en los lentes.
   - Textos que el tema mostraba en inglés ("Category:", "Search").
   - Shortcode [km_anio] para el año del pie de página.
   La cabecera y el pie (partes de plantilla del tema) se editaron por REST; respaldo en respaldos_web/.
   Mantenido desde el repo panel-control (web_snippets/km_diseno.php). */

// Solo FreshLady y Meetone tienen registro sanitario (confirmado por el usuario el 22-nov-2026).
// El panel escribe la meta km_registro_sanitario: "no" o la marca ("FreshLady", "Meetone", "FreshLady y Meetone")
// según la marca del stock que hay (un lente puede venir de varias marcas). Sin meta: se decide por la marca.
if ( ! function_exists( 'km_registro_sanitario_marca' ) ) {
	function km_registro_sanitario_marca( $product_id ) {
		if ( ! $product_id ) {
			return '';
		}
		$meta = get_post_meta( $product_id, 'km_registro_sanitario', true );
		if ( '' !== $meta ) {
			return 'no' === $meta ? '' : $meta;
		}
		if ( ! has_term( array( 'freshlady', 'meetone' ), 'product_brand', $product_id ) ) {
			return '';
		}
		return has_term( 'meetone', 'product_brand', $product_id ) ? 'Meetone' : 'FreshLady';
	}
}
if ( ! function_exists( 'km_tiene_registro_sanitario' ) ) {
	function km_tiene_registro_sanitario( $product_id ) {
		return '' !== km_registro_sanitario_marca( $product_id );
	}
}

add_shortcode( 'km_anio', function () {
	return esc_html( wp_date( 'Y' ) );
} );

// La fuente del sitio (Roboto normal, un solo archivo) se precarga en la portada, donde el titular es lo
// primero que se ve. En producto no: competiría con la descarga de la foto principal.
add_action( 'wp_head', function () {
	if ( ! is_front_page() ) {
		return;
	}
	echo '<link rel="preload" href="/wp-content/uploads/fonts/Roboto-normal-latin.woff2" as="font" type="font/woff2" crossorigin>' . "\n";
}, 2 );

add_action( 'wp_head', function () {
	// Íconos de línea (trazo) usados como máscara: heredan el color del texto.
	$ico = function ( $paths ) {
		$svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#000" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' . $paths . '</svg>';
		return 'url("data:image/svg+xml,' . rawurlencode( $svg ) . '")';
	};
	$i = array(
		'truck'  => $ico( '<path d="M1 3h15v13H1z"/><path d="M16 8h4l3 3v5h-7z"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/>' ),
		'shield' => $ico( '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/>' ),
		'lock'   => $ico( '<rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>' ),
		'chat'   => $ico( '<path d="M21 11.5a8.4 8.4 0 0 1-9 8.4 8.6 8.6 0 0 1-3.8-.9L3 21l1.9-5.2A8.4 8.4 0 1 1 21 11.5z"/>' ),
		'eye'    => $ico( '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>' ),
		'box'    => $ico( '<path d="M21 16V8l-9-5-9 5v8l9 5 9-5z"/><path d="M3.3 7 12 12l8.7-5M12 22V12"/>' ),
		'cash'   => $ico( '<rect x="2" y="6" width="20" height="12" rx="2"/><circle cx="12" cy="12" r="2.5"/><path d="M6 12h.01M18 12h.01"/>' ),
		'star'   => $ico( '<path d="m12 2 3.1 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.8 21l1.2-6.8-5-4.9 6.9-1z"/>' ),
	);

	$css = '
:root{--ico-shield:' . $i['shield'] . ';--km-p:#096385;--km-pd:#074d68;--km-dark:#0d2b45;--km-soft:#f2f7f9;--km-line:#e2e9ee;--km-text:#2f3b45;--km-muted:#5f6d78;--km-r:14px;--km-sh:0 6px 24px rgba(13,43,69,.08)}
body{color:var(--km-text);-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
:root :where(p),li{letter-spacing:.005em}
h1,h2,h3,h4{letter-spacing:-.01em;line-height:1.2}
h1.wp-block-post-title,h1.wp-block-query-title,.wp-block-post-title.has-x-large-font-size{color:var(--km-dark)}
a{text-underline-offset:3px}
img{max-width:100%}

/* Botones */
.wp-element-button,.wp-block-button__link,.button,button.single_add_to_cart_button{border-radius:999px!important;font-weight:600!important;letter-spacing:.01em;transition:background-color .2s,transform .15s,box-shadow .2s}
.wp-element-button:hover,.wp-block-button__link:hover,.button:hover{background-color:var(--km-pd)!important;color:#fff!important;box-shadow:0 6px 16px rgba(9,99,133,.25)}
.wp-element-button:active,.wp-block-button__link:active{transform:translateY(1px)}
.single_add_to_cart_button{padding:.95em 2.2em!important;font-size:1rem!important}

/* Franja superior de confianza */
.km-topbar{background:var(--km-dark);color:#fff;font-size:13px;padding:8px 16px;gap:6px 32px;margin:0!important}
.km-topbar p{margin:0;display:flex;align-items:center;gap:7px;color:#fff}
.km-topbar p:before{content:"";width:16px;height:16px;background:#7fd3e6;flex:none;-webkit-mask:var(--ico) center/contain no-repeat;mask:var(--ico) center/contain no-repeat}
.km-topbar a{color:#fff;text-decoration:none}
.km-tb-envio{--ico:' . $i['truck'] . '}.km-tb-garantia{--ico:' . $i['shield'] . '}.km-tb-pago{--ico:' . $i['cash'] . '}.km-tb-wsp{--ico:' . $i['chat'] . '}
@media(max-width:781px){.km-topbar{font-size:12px;gap:4px 16px}.km-topbar .km-tb-garantia,.km-topbar .km-tb-wsp{display:none}}

/* Cabecera */
.km-header{background:#fff;border-bottom:1px solid var(--km-line);margin:0!important;padding:0!important}
.km-header__row{max-width:1240px;margin:0 auto!important;padding:10px 24px!important;gap:28px!important}
.km-header .wp-block-site-logo{flex:none}
.km-header .wp-block-site-logo img{width:76px!important;height:auto}
.km-header .wp-block-navigation{flex:1;font-size:15px;font-weight:500}
.km-header .wp-block-navigation a:hover{color:var(--km-p)}
.km-header .wp-block-navigation__submenu-container{border:1px solid var(--km-line)!important;border-radius:10px;box-shadow:var(--km-sh);padding:6px 0}
.km-header .wp-block-search{width:300px;flex:none;margin:0}
.km-header .wp-block-search__inside-wrapper{border:1px solid var(--km-line)!important;border-radius:999px;overflow:hidden;padding:0!important;background:var(--km-soft)}
.km-header .wp-block-search__input{border:0!important;background:transparent;padding:10px 16px;font-size:14px}
.km-header .wp-block-search__button{margin:0!important;border-radius:999px!important;background:var(--km-p)!important;color:#fff!important;padding:8px 14px!important}
.km-header__iconos{flex:none;gap:6px!important}
@media(min-width:782px){.km-header{position:sticky;top:0;z-index:50}.admin-bar .km-header{top:32px}}
@media(max-width:781px){
 .km-header__row{flex-wrap:wrap!important;gap:10px 14px!important;padding:8px 16px!important}
 .km-header .wp-block-site-logo{margin-right:auto}
 .km-header .wp-block-site-logo img{width:60px!important}
 .km-header .wp-block-navigation{flex:none;order:3}
 .km-header__iconos{order:2}
 .km-header .wp-block-search{order:4;width:100%}
}

/* Pie de página */
.km-footer{background:var(--km-dark)!important;color:#d5e1ea;padding:56px 24px 24px!important;margin-top:64px!important}
.km-footer a{color:#fff;text-decoration:none}
.km-footer a:hover{text-decoration:underline}
.km-footer h2{color:#fff!important;font-size:16px!important;font-weight:700;letter-spacing:.06em;text-transform:uppercase;margin:0 0 14px}
.km-footer p,.km-footer li{font-size:14px;line-height:1.6;color:#d5e1ea}
.km-footer .wp-block-navigation{font-size:14px;gap:8px!important}
.km-footer .wp-block-navigation .wp-block-navigation-item__content{color:#d5e1ea}
.km-footer__logo img{width:72px;height:72px;border-radius:50%;background:#fff;padding:6px}
.km-footer .km-social{gap:10px!important;margin-top:16px}
.km-footer .km-social .wp-social-link{background:rgba(255,255,255,.1)!important;color:#fff!important;border-radius:50%;transition:background .2s}
.km-footer .km-social .wp-social-link:hover{background:var(--km-p)!important}
.km-footer .km-social .wp-social-link a{padding:9px!important}
.km-footer .km-social svg{fill:#fff;width:20px;height:20px}
.km-pagos{gap:8px!important;margin-top:36px!important;align-items:center}
.km-pagos p{margin:0}
.km-pagos .km-chip{background:#fff;color:var(--km-dark)!important;border-radius:6px;padding:3px 10px;font-size:12px!important;font-weight:700;letter-spacing:.02em}
.km-pagos .km-pagos__txt{display:flex;align-items:center;gap:6px;font-size:13px!important}
.km-pagos .km-pagos__txt:before{content:"";width:15px;height:15px;background:#7fd3e6;-webkit-mask:' . $i['lock'] . ' center/contain no-repeat;mask:' . $i['lock'] . ' center/contain no-repeat}
.km-footer__base{border-top:1px solid rgba(255,255,255,.12);margin-top:28px!important;padding-top:20px!important;gap:6px 20px!important}
.km-footer__base p{font-size:13px;margin:0;color:#a9bccb}
.km-footer__base a{color:#a9bccb}

/* Tarjetas de producto (tienda, categorías, relacionados) */
li.wc-block-product{background:#fff;border:1px solid var(--km-line);border-radius:var(--km-r);padding:0 0 16px!important;overflow:hidden;transition:box-shadow .25s,transform .25s;display:flex;flex-direction:column}
li.wc-block-product:hover{box-shadow:var(--km-sh);transform:translateY(-3px)}
li.wc-block-product .wc-block-components-product-image{margin:0 0 12px!important}
li.wc-block-product .wc-block-components-product-image img{transition:transform .4s;aspect-ratio:1/1;object-fit:cover}
li.wc-block-product:hover .wc-block-components-product-image img{transform:scale(1.04)}
li.wc-block-product .wp-block-post-title{padding:0 12px;font-size:15px!important;line-height:1.35;font-weight:600}
li.wc-block-product .wp-block-post-title a{color:var(--km-dark);text-decoration:none}
li.wc-block-product .wc-block-components-product-price{font-size:16px!important;font-weight:700;color:var(--km-dark)}
li.wc-block-product .wc-block-components-product-price del{color:#8a98a3;font-weight:400;font-size:.85em}
li.wc-block-product .wc-block-components-product-price ins{text-decoration:none;color:#c2410c}
li.wc-block-product .wp-block-woocommerce-product-button{margin-top:auto!important}
.wc-block-components-product-sale-badge{background:#c2410c!important;color:#fff!important;border:0!important;border-radius:999px!important;font-weight:700;letter-spacing:.04em;font-size:11px!important;padding:4px 10px!important}
.woocommerce-price-suffix{font-size:.7em;color:var(--km-muted);font-weight:400}

@media(max-width:600px){
 ul.wc-block-product-template{display:grid!important;grid-template-columns:1fr 1fr!important;gap:12px!important}
 ul.wc-block-product-template>li.wc-block-product{width:auto!important;max-width:none!important;margin:0!important}
 li.wc-block-product .wp-block-post-title{font-size:13px!important;padding:0 8px}
 li.wc-block-product .wc-block-components-product-price{font-size:14px!important}
 li.wc-block-product .wp-block-button__link,li.wc-block-product .wp-element-button{font-size:12px!important;padding:8px 12px!important}
}

/* Página de producto */
.single-product .wp-block-post-title{color:var(--km-dark);font-size:clamp(1.6rem,2.6vw,2.2rem)!important;font-weight:700}
.single-product .wp-block-woocommerce-product-price{font-size:1.6rem!important;font-weight:700;color:var(--km-dark)}
.single-product .wp-block-post-excerpt{color:var(--km-muted)}
.single-product .woocommerce-product-gallery__image img,.single-product .flex-viewport{border-radius:var(--km-r)}
.single-product .taxonomy-product_tag{display:none}
.single-product .wp-block-woocommerce-product-meta{font-size:14px;color:var(--km-muted)}
.single-product .wp-block-woocommerce-product-meta .wp-block-group{flex-wrap:wrap!important;gap:4px 18px}
.single-product .quantity .qty{border:1px solid var(--km-line);border-radius:999px;min-height:48px;width:72px;text-align:center}
.single-product .stock{color:#15803d;font-weight:600}
.woocommerce-tabs ul.tabs{border-bottom:1px solid var(--km-line)!important}
.woocommerce-tabs ul.tabs li.active a{color:var(--km-p)!important}
.woocommerce-Tabs-panel h2,.woocommerce-Tabs-panel h3{color:var(--km-dark)}

.km-confianza{margin:22px 0 8px;border:1px solid var(--km-line);border-radius:var(--km-r);background:var(--km-soft);padding:6px 18px}
.km-confianza ul{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:1fr 1fr;gap:0 20px}
.km-confianza li{display:flex;gap:10px;align-items:flex-start;padding:12px 0;font-size:14px;line-height:1.4;color:var(--km-muted)}
.km-confianza li strong{display:block;color:var(--km-dark);font-size:14px}
.km-confianza li:before{content:"";flex:none;width:22px;height:22px;margin-top:2px;background:var(--km-p);-webkit-mask:var(--ico) center/contain no-repeat;mask:var(--ico) center/contain no-repeat}
.km-aviso-contra{background:var(--km-soft);border:1px solid var(--km-line);border-left:4px solid #2bb3a3;border-radius:10px;padding:10px 14px;margin:0 0 14px;font-size:14px;line-height:1.45}.km-aviso-contra:before{content:"";display:inline-block;vertical-align:-3px;width:18px;height:18px;margin-right:6px;background:var(--km-p);-webkit-mask:' . $i['cash'] . ' center/contain no-repeat;mask:' . $i['cash'] . ' center/contain no-repeat}
.km-c-envio{--ico:' . $i['truck'] . '}.km-c-garantia{--ico:' . $i['shield'] . '}.km-c-pago{--ico:' . $i['lock'] . '}.km-c-contra{--ico:' . $i['cash'] . '}.km-c-wsp{--ico:' . $i['chat'] . '}
.km-uso{border-top:1px solid var(--km-line);padding:10px 0 8px;font-size:14px}
.km-uso summary{cursor:pointer;font-weight:600;color:var(--km-dark);display:flex;align-items:center;gap:10px;list-style:none}
.km-uso summary::-webkit-details-marker{display:none}
.km-uso summary:before{content:"";width:22px;height:22px;background:var(--km-p);-webkit-mask:' . $i['eye'] . ' center/contain no-repeat;mask:' . $i['eye'] . ' center/contain no-repeat}
.km-uso summary:after{content:"+";margin-left:auto;font-size:18px;color:var(--km-p)}
.km-uso[open] summary:after{content:"–"}
.km-uso ul{margin:10px 0 4px;padding-left:20px;color:var(--km-muted);line-height:1.55}
@media(max-width:600px){.km-confianza ul{grid-template-columns:1fr}}

/* Registro sanitario (solo FreshLady y Meetone) */
.km-rs{display:flex;flex-direction:column;gap:2px;margin:12px 0 4px;padding:12px 14px 12px 50px;border-radius:10px;background:#e8f7f4;border:1px solid #b9e6dd;color:#24564e;font-size:14px;line-height:1.4;position:relative}
.km-rs strong{color:#0f5a4f;font-size:15px}
.km-rs:before{content:"";position:absolute;left:14px;top:50%;width:24px;height:24px;margin-top:-12px;background:#15937f;-webkit-mask:var(--ico-shield) center/contain no-repeat;mask:var(--ico-shield) center/contain no-repeat}
li.wc-block-product .wc-block-components-product-image{position:relative}
.km-rs-sello{position:absolute;left:8px;bottom:8px;z-index:2;display:inline-flex;align-items:center;gap:5px;background:rgba(255,255,255,.95);color:#0f5a4f;border:1px solid #b9e6dd;border-radius:999px;padding:3px 9px 3px 6px;font-size:11px;font-weight:700;line-height:1.3;box-shadow:0 2px 6px rgba(0,0,0,.08);pointer-events:none}
.km-rs-sello:before{content:"✓";display:inline-grid;place-items:center;width:15px;height:15px;border-radius:50%;background:#15937f;color:#fff;font-size:10px}
@media(max-width:600px){.km-rs-sello{font-size:10px;padding:2px 7px 2px 4px}}

/* Imágenes de categoría (fotos reales con el nombre abajo): mostrarlas completas, sin recortar el nombre */
img[src*="/km-cat-"],img[src*="/km-col-"],img[src*="/km-sub-"],img[src*="/km-marca-"]{object-position:50% 100%!important}

/* Portada: testimonios, preguntas frecuentes, cifras */
.uagb-ifb-title-prefix{font-size:15px!important;line-height:1.6!important}
.uagb-ifb-title-prefix:before{content:"★★★★★";display:block;color:#f5a524;letter-spacing:2px;font-size:16px;font-style:normal;margin-bottom:8px}
.wp-block-uagb-info-box .uagb-ifb-title a[href*="maps.app"]:after{content:" · Reseña en Google";font-weight:400;color:var(--km-muted);font-size:13px}
.wp-block-uagb-container:has(> .uagb-container-inner-blocks-wrap > .wp-block-uagb-info-box .uagb-ifb-title a[href*="maps.app"]),
.wp-block-uagb-container:has(> .wp-block-uagb-info-box .uagb-ifb-title a[href*="maps.app"]){border-radius:var(--km-r)!important;border-color:var(--km-line)!important;box-shadow:var(--km-sh)}
.wp-block-uagb-faq .uagb-faq-item{border:1px solid var(--km-line)!important;border-radius:10px!important;margin-bottom:10px!important;overflow:hidden;background:#fff}
.wp-block-uagb-faq .uagb-faq-questions-button{padding:16px 18px!important;font-weight:600}
.wp-block-uagb-faq .uagb-faq-content{padding:0 18px 16px!important;color:var(--km-muted)}
.km-social-home{gap:10px!important}
.km-social-home .wp-social-link{background:var(--km-p)!important;color:#fff!important}
.km-social-home .wp-social-link a{padding:10px!important}

/* Portada: bloque principal */
.km-hero{background:linear-gradient(135deg,#0d2b45 0%,#0b4a66 60%,#096385 100%);color:#fff;padding:56px clamp(16px,4vw,32px) 64px!important;margin-top:0!important;margin-bottom:0!important}
.km-hero__cols{gap:48px!important;margin-top:0!important;margin-bottom:0!important}
.km-hero .km-eyebrow{display:inline-block;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.2);color:#cdeef6;border-radius:999px;padding:5px 14px;font-size:13px;font-weight:600;letter-spacing:.04em;margin:0 0 18px}
.km-hero h1{color:#fff!important;font-size:clamp(2rem,4.2vw,3.2rem)!important;font-weight:800;line-height:1.1;margin:0 0 18px;letter-spacing:-.02em}
.km-hero .km-hero__desc{color:#d9e8f0;font-size:1.1rem;line-height:1.6;margin:0 0 26px}
.km-hero__botones{gap:12px!important;margin-bottom:26px!important}
.km-hero .km-btn-claro .wp-block-button__link{background:#fff!important;color:var(--km-dark)!important;padding:14px 26px!important}
.km-hero .km-btn-claro .wp-block-button__link:hover{background:#e6f4f8!important;color:var(--km-dark)!important}
.km-hero .km-btn-borde .wp-block-button__link{background:transparent!important;color:#fff!important;border:2px solid rgba(255,255,255,.7)!important;padding:12px 24px!important}
.km-hero .km-btn-borde .wp-block-button__link:hover{background:rgba(255,255,255,.12)!important;box-shadow:none}
.km-hero__checks{list-style:none;padding:0!important;margin:0;display:flex;flex-wrap:wrap;gap:8px 22px}
.km-hero__checks li{display:flex;align-items:center;gap:8px;color:#fff;font-size:14px;font-weight:500}
.km-hero__checks li:before{content:"✓";display:inline-grid;place-items:center;width:20px;height:20px;border-radius:50%;background:#2bb3a3;color:#fff;font-size:12px;font-weight:700}
.km-hero__img img{aspect-ratio:1120/576;border-radius:18px;box-shadow:0 20px 50px rgba(0,0,0,.35);width:100%;height:auto}
@media(max-width:781px){.km-hero{padding:32px 16px 40px!important}.km-hero__cols{gap:28px!important}.km-hero__botones .wp-block-button{flex:1 1 100%}.km-hero__botones .wp-block-button__link{display:block;text-align:center}}

/* Tablas de las páginas de políticas */
.wp-block-post-content table{border-collapse:collapse;width:100%;font-size:15px;border:1px solid var(--km-line);border-radius:10px;overflow:hidden}
.wp-block-post-content table td,.wp-block-post-content table th{border-bottom:1px solid var(--km-line);padding:10px 14px;text-align:left}
.wp-block-post-content table thead th,.wp-block-post-content table thead td{background:var(--km-soft);color:var(--km-dark);font-weight:700}
.wp-block-post-content table tbody tr:nth-child(even){background:#fafcfd}
.wp-block-post-content figure.wp-block-table{margin:12px 0 20px}

/* Página Nosotros */
.km-eyebrow-oscuro{color:var(--km-p);font-weight:700;font-size:13px!important;letter-spacing:.08em;text-transform:uppercase;margin-bottom:4px}
.home main#wp--skip-link--target{padding-top:0!important;margin-top:0!important}
.home .wp-site-blocks>main{margin-block-start:0}
.km-nos-cifras{gap:16px!important}
.km-nos-cifras>.wp-block-group{flex:1 1 160px;background:var(--km-soft);border-radius:var(--km-r);padding:22px 16px!important;text-align:center}
.km-nos-cifras .km-cifra{font-size:2rem;font-weight:800;color:var(--km-p);margin:0;line-height:1.1}
.km-nos-cifras p{margin:4px 0 0}
.km-nos-card{background:#fff;border:1px solid var(--km-line);border-radius:var(--km-r);padding:24px!important;height:100%}
';
	echo '<style id="km-diseno">' . preg_replace( '/\s*\n\s*/', '', $css ) . '</style>' . "\n";
}, 20 );

// Franja de confianza y guía de uso seguro bajo el botón "Añadir al carrito".
add_filter( 'render_block_woocommerce/add-to-cart-form', function ( $html ) {
	if ( ! function_exists( 'is_product' ) || ! is_product() || did_action( 'km_confianza_mostrada' ) ) {
		return $html;
	}
	do_action( 'km_confianza_mostrada' );
	$es_lente = has_term( array( 'lentes-de-contacto-fantasia', 'lentes-de-contacto-naturales' ), 'product_cat', get_queried_object_id() );
	$wsp      = 'https://wa.me/51936041531?text=' . rawurlencode( 'Hola, tengo una consulta sobre ' . get_the_title( get_queried_object_id() ) );
	$pid  = get_queried_object_id();
	$out  = '<div class="km-confianza">';
	if ( km_tiene_registro_sanitario( $pid ) ) {
		$marca = km_registro_sanitario_marca( $pid );
		// Números de registro tal como figuran en el empaque (solo los que el usuario confirmó con foto).
		$numeros = array( 'FreshLady' => 'R.S. DM23897E · Importado por Droguería Zigma S.A.C.' );
		$detalle = array();
		foreach ( $numeros as $m => $txt ) {
			if ( false !== stripos( $marca, $m ) ) {
				$detalle[] = ( count( $numeros ) > 1 || $marca !== $m ? $m . ': ' : '' ) . $txt;
			}
		}
		$out .= '<div class="km-rs"><strong>Con Registro Sanitario</strong>Los lentes ' . esc_html( $marca ) . ' cuentan con registro sanitario en el Perú.'
			. ( $detalle ? '<br><small>' . esc_html( implode( ' | ', $detalle ) ) . '</small>' : '' ) . '</div>';
	}
	$out .= '<ul>';
	$out .= $es_lente
		? '<li class="km-c-envio"><span><strong>Envío gratis a todo el Perú</strong>En lentes de contacto. Despachamos el siguiente día hábil.</span></li>'
		: '<li class="km-c-envio"><span><strong>Envíos a todo el Perú</strong>Gratis si tu pedido incluye lentes de contacto. Despachamos el siguiente día hábil.</span></li>';
	$out .= '<li class="km-c-garantia"><span><strong>Garantía de fábrica</strong>Tienes 24 h desde que lo recibes para reportar cualquier problema.</span></li>';
	$out .= '<li class="km-c-contra"><span><strong>En Lima pagas al recibir</strong>Contraentrega sin adelanto dentro de nuestra <a href="/envios/">cobertura</a>. También Yape, Plin, tarjeta o Mercado Pago.</span></li>';
	$out .= '<li class="km-c-wsp"><span><strong>Asesoría personalizada</strong><a href="' . esc_url( $wsp ) . '" target="_blank" rel="noopener">Escríbenos por WhatsApp</a> antes y después de tu compra.</span></li>';
	$out .= '</ul>';
	if ( $es_lente ) {
		$out .= '<details class="km-uso"><summary>Uso seguro de tus lentes de contacto</summary><ul>'
			. '<li>Lávate y sécate bien las manos antes de ponértelos o quitártelos.</li>'
			. '<li>Guárdalos en su estuche con solución multipropósito nueva; nunca uses agua del caño ni saliva.</li>'
			. '<li>No duermas con ellos puestos y no los compartas con nadie.</li>'
			. '<li>Respeta las horas de uso diario y la duración indicada del lente.</li>'
			. '<li>Si sientes molestia, enrojecimiento o visión borrosa, retíralos y consulta con un oftalmólogo.</li>'
			. '</ul></details>';
	}
	$out .= '</div>';
	return $html . $out;
} );

// Carrito: aviso de pago contra entrega en Lima justo encima del botón "Finalizar compra".
add_filter( 'render_block_woocommerce/proceed-to-checkout-block', function ( $html ) {
	return '<p class="km-aviso-contra"><strong>¿Estás en Lima?</strong> Elige «Delivery a domicilio» y paga al recibir, sin adelanto (efectivo o Yape). <a href="/envios/">Ver cobertura</a></p>' . $html;
} );

// Portada: descripción para Google con envío gratis y contraentrega en Lima.
add_filter( 'rank_math/frontend/description', function ( $desc ) {
	return is_front_page()
		? 'Lentes de contacto naturales y de fantasía: FreshLady y Meetone con registro sanitario. Envío gratis a todo el Perú y pago contra entrega en Lima.'
		: $desc;
}, 20 );

// Sello "Registro sanitario" sobre la foto de los productos FreshLady y Meetone en listados.
add_filter( 'render_block_woocommerce/product-image', function ( $html, $block, $instance = null ) {
	$pid = ( $instance && isset( $instance->context['postId'] ) ) ? (int) $instance->context['postId'] : 0;
	if ( ! km_tiene_registro_sanitario( $pid ) ) {
		return $html;
	}
	$sello = '<span class="km-rs-sello" title="Producto con registro sanitario">Registro sanitario</span>';
	$pos   = strrpos( $html, '</div>' );
	return false === $pos ? $html : substr_replace( $html, $sello . '</div>', $pos, 6 );
}, 10, 3 );

// Índice de contenidos de Rank Math: título en español.
add_filter( 'gettext_rank-math', function ( $t, $orig ) {
	return 'Table of Contents' === $orig ? 'Contenido' : $t;
}, 10, 2 );
add_filter( 'render_block', function ( $html ) {
	return false === strpos( $html, '<h2>Table of Contents</h2>' ) ? $html : str_replace( '<h2>Table of Contents</h2>', '<h2>Contenido</h2>', $html );
}, 99 );

// Textos de la plantilla de producto que salían en inglés.
add_filter( 'render_block_core/post-terms', function ( $html, $block ) {
	if ( isset( $block['attrs']['term'] ) && 'product_cat' === $block['attrs']['term'] ) {
		$html = str_replace( '>Category: <', '>Categoría: <', $html );
		$html = str_replace( '>Categories: <', '>Categorías: <', $html );
	}
	return $html;
}, 10, 2 );
