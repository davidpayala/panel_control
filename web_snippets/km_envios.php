/* KM: envíos de kmlentes.pe (Lima con delivery, agencias Shalom, Olva y otras a todo el Perú).
   Mantenido desde el repo panel-control (web_snippets/km_envios.php). Sustituye a los plugins de envío.

   Reglas (definidas por el dueño, nov-2026):
   - "Lente" = el carrito tiene al menos un producto de Lentes Naturales o de Fantasía. Si no, es "solo accesorios".
   - Lima Metropolitana y Callao (zonas CALL, LIM1-LIM4): delivery al día siguiente dentro de la cobertura de eservice
     (gratis con lente, S/ 10 solo accesorios; desde el 24-nov-2026 SIN adelanto: pago contra entrega completo, salvo clientes
     de la lista interna "km_lista_adelanto" que mantiene el panel) y Express el mismo día (InDriver desde SMP,
     productos adelantados, taxi se paga al motorizado).
   - Todo el Perú: Shalom (normal, contraentrega, express, express aéreo), Olva, agencias de Plaza Norte y otras agencias. */

if ( ! defined( 'ABSPATH' ) ) {
	return;
}

const KM_ENV_CATS_LENTE = array( 148, 307 ); // Lentes de Contacto Naturales, Lentes de Contacto de Fantasía.
const KM_ENV_ZONAS_LIMA = array( 'CALL', 'LIM1', 'LIM2', 'LIM3', 'LIM4' );
const KM_ENV_WHATSAPP   = '+51 936 041 531';
const KM_ENV_HORA_CORTE = 17;
const KM_ENV_ADELANTO   = 10; // Adelanto que se sigue pidiendo a los clientes de la lista interna (no recibieron un pedido antes). // Envíos que salen el mismo día (Express Lima y Shalom Express/aéreo): pedidos hasta las 5 pm (hora de Lima).

function km_env_express_abierto() {
	$ahora = new DateTime( 'now', new DateTimeZone( 'America/Lima' ) );
	return (int) $ahora->format( 'G' ) < KM_ENV_HORA_CORTE;
}
/* La hora entra en el paquete para que WooCommerce recalcule las opciones al pasar las 5 pm. */
add_filter( 'woocommerce_cart_shipping_packages', function ( $packages ) {
	foreach ( $packages as $i => $pkg ) {
		$packages[ $i ]['km_express_abierto'] = km_env_express_abierto() ? 'si' : 'no';
	}
	return $packages;
} );

/** Tarifas: [clave => [etiqueta lente, etiqueta accesorios, costo lente, costo accesorios, tipo de pago]] */
function km_env_tarifas() {
	return array(
		'lima_dia'       => array( 'Delivery a domicilio gratis · llega mañana', 'Delivery a domicilio · llega mañana', 0, 10, 'contraentrega' ),
		'lima_express'   => array( 'Delivery Express · llega hoy', 'Delivery Express · llega hoy', 0, 0, 'productos' ),
		'shalom'         => array( 'Agencia Shalom gratis · sale mañana', 'Agencia Shalom · sale mañana', 0, 10, 'total' ),
		'shalom_contra'  => array( 'Agencia Shalom contraentrega · sale mañana', 'Agencia Shalom contraentrega · sale mañana', 10, 15, 'envio' ),
		'shalom_express' => array( 'Agencia Shalom Express · sale hoy', 'Agencia Shalom Express · sale hoy', 5, 15, 'total' ),
		'shalom_avion'   => array( 'Agencia Shalom Express aéreo · sale hoy', 'Agencia Shalom Express aéreo · sale hoy', 15, 25, 'total' ),
		'olva'           => array( 'Olva Courier · sale mañana', 'Olva Courier · sale mañana', 10, 20, 'total' ),
		'plaza_norte'    => array( 'Otra agencia en el terminal Plaza Norte · sale mañana', 'Otra agencia en el terminal Plaza Norte · sale mañana', 10, 20, 'total' ),
		'otra_agencia'   => array( 'Otra agencia de transporte · sale mañana', 'Otra agencia de transporte · sale mañana', 15, 25, 'total' ),
	);
}

function km_env_descripcion( $clave, $lente, $nota_distrito ) {
	switch ( $clave ) {
		case 'lima_dia':
			return 'Sin adelanto: pagas todo al motorizado al recibir (efectivo o Yape). Te escribimos por WhatsApp para confirmar tu pedido. Entrega mañana de 12:30 pm a 8:00 pm, sin hora exacta. Solo dentro de nuestra zona de cobertura.' . $nota_distrito;
		case 'lima_express':
			return 'Pedidos hasta las 5 pm. Pagas los productos por adelantado; el taxi (InDriver desde San Martín de Porres) se lo pagas al motorizado al recibir.';
		case 'shalom':
			return 'Pagas todo por adelantado. Sale mañana; lo que tarde en llegar a tu agencia de destino depende de Shalom.';
		case 'shalom_contra':
			return 'Adelantas solo el envío y pagas los productos al recoger en tu agencia Shalom. Sale mañana.';
		case 'shalom_express':
			return 'Pedidos hasta las 5 pm. Pagas el envío y los productos por adelantado y lo enviamos hoy mismo.';
		case 'shalom_avion':
			return 'Pedidos hasta las 5 pm. Pagas el envío y los productos por adelantado y sale hoy por avión, solo si Shalom tiene envío aéreo a tu ciudad (te lo confirmamos por WhatsApp).';
		default:
			return 'Pagas los productos y el envío por adelantado. Sale mañana; lo que tarde en llegar depende de la agencia.';
	}
}

/* ---------- Carrito: ¿lleva lente de contacto? ---------- */
function km_env_es_lente( $product_id ) {
	return has_term( KM_ENV_CATS_LENTE, 'product_cat', $product_id );
}
function km_env_lleva_lente( $items ) {
	foreach ( $items as $item ) {
		if ( km_env_es_lente( $item['product_id'] ) ) {
			return true;
		}
	}
	return false;
}

/* ---------- Cobertura de eservice por distrito (calculada del mapa oficial, nov-2026) ---------- */
function km_env_normalizar( $texto ) {
	$texto = strtolower( remove_accents( (string) $texto ) );
	return preg_replace( '/[^a-z]/', '', $texto );
}
/** 'fuera' = el distrito entero está fuera de la cobertura; 'parcial' = tiene zonas altas o alejadas sin cobertura. */
function km_env_cobertura( $ciudad ) {
	$c = km_env_normalizar( $ciudad );
	if ( '' === $c ) {
		return '';
	}
	$fuera = array( 'ancon', 'santarosa', 'pucusana', 'santamariadelmar', 'sanbartolo', 'puntanegra', 'puntahermosa',
		'pachacamac', 'manchay', 'cieneguilla', 'chaclacayo', 'lurigancho', 'chosica', 'lurin', 'huachipa', 'jicamarca', 'carapongo' );
	$parcial = array( 'ate', 'vitarte', 'huaycan', 'carabayllo', 'comas', 'independencia', 'puentepiedra', 'sanjuandelurigancho',
		'sjl', 'villamariadeltriunfo', 'vmt', 'ventanilla', 'rimac', 'lamolina', 'callao', 'sanjuandemiraflores', 'sjm',
		'chorrillos', 'elagustino', 'villaelsalvador', 'ves', 'sanmartindeporres', 'smp', 'santiagodesurco', 'surco' );
	foreach ( $fuera as $d ) {
		if ( $c === $d || 0 === strpos( $c, $d ) ) {
			return 'fuera';
		}
	}
	foreach ( $parcial as $d ) {
		if ( $c === $d || 0 === strpos( $c, $d ) ) {
			return 'parcial';
		}
	}
	return 'dentro';
}

/* ---------- Método de envío ---------- */
add_action( 'woocommerce_shipping_init', function () {
	if ( class_exists( 'KM_Envio_Metodo' ) ) {
		return;
	}
	class KM_Envio_Metodo extends WC_Shipping_Method {
		public function __construct( $instance_id = 0 ) {
			$this->id                 = 'km_envio';
			$this->instance_id        = absint( $instance_id );
			$this->method_title       = 'Envíos KM Lentes';
			$this->method_description = 'Delivery en Lima, agencias Shalom, Olva y otras. Reglas en el snippet "KM: envíos".';
			$this->supports           = array( 'shipping-zones', 'instance-settings' );
			$this->title              = 'Envíos KM Lentes';
			$this->enabled            = 'yes';
			$this->tax_status         = 'none';
		}

		public function calculate_shipping( $package = array() ) {
			$estado = isset( $package['destination']['state'] ) ? $package['destination']['state'] : '';
			$pais   = isset( $package['destination']['country'] ) ? $package['destination']['country'] : '';
			if ( 'PE' !== $pais || '' === $estado || in_array( $estado, array( 'REGE', 'REGA' ), true ) ) {
				return; // Sin zona elegida no se ofrece nada.
			}
			$lente  = km_env_lleva_lente( isset( $package['contents'] ) ? $package['contents'] : array() );
			$lima   = in_array( $estado, KM_ENV_ZONAS_LIMA, true );
			$ciudad = isset( $package['destination']['city'] ) ? $package['destination']['city'] : '';
			$cob    = $lima ? km_env_cobertura( $ciudad ) : '';
			$nota   = 'parcial' === $cob ? ' En tu distrito algunas zonas altas o alejadas no tienen cobertura: si es tu caso, te escribimos para pasarlo a Express o agencia.' : '';

			foreach ( km_env_tarifas() as $clave => $t ) {
				if ( ! $lima && in_array( $clave, array( 'lima_dia', 'lima_express' ), true ) ) {
					continue;
				}
				if ( in_array( $clave, array( 'lima_express', 'shalom_express', 'shalom_avion' ), true ) && ! km_env_express_abierto() ) {
					continue; // Después de las 5 pm ya no sale el mismo día.
				}
				if ( 'lima_dia' === $clave && 'fuera' === $cob ) {
					continue; // Distrito fuera de la cobertura del delivery: quedan Express y agencias.
				}
				if ( 'shalom_avion' === $clave && ( $lima || 'LIPO' === $estado ) ) {
					continue; // No hay vuelo a Lima.
				}
				$this->add_rate( array(
					'id'        => 'km_envio:' . $clave,
					'label'     => $lente ? $t[0] : $t[1],
					'cost'      => $lente ? $t[2] : $t[3],
					'taxes'     => false,
					'package'   => $package,
					'description' => km_env_descripcion( $clave, $lente, $nota ),
					'meta_data' => array( '_km_clave' => $clave, '_km_desc' => km_env_descripcion( $clave, $lente, $nota ) ),
				) );
			}
		}
	}
} );
add_filter( 'woocommerce_shipping_methods', function ( $metodos ) {
	if ( class_exists( 'KM_Envio_Metodo' ) ) {
		$metodos['km_envio'] = 'KM_Envio_Metodo';
	}
	return $metodos;
} );

/* La descripción de cada opción se muestra debajo de ella (carrito/checkout de bloques). */
add_filter( 'woocommerce_package_rates', function ( $rates ) {
	foreach ( $rates as $rate ) {
		if ( 'km_envio' === $rate->get_method_id() && method_exists( $rate, 'set_description' ) ) {
			$meta = $rate->get_meta_data();
			if ( ! empty( $meta['_km_desc'] ) ) {
				$rate->set_description( $meta['_km_desc'] );
			}
		}
	}
	return $rates;
}, 20 );
/* ---------- Zonas: quitar los títulos separadores y hacer más claras las zonas de Lima ---------- */
add_filter( 'woocommerce_states', function ( $states ) {
	if ( empty( $states['PE'] ) || ( is_admin() && ! wp_doing_ajax() ) ) {
		return $states; // En el admin se conservan para que los pedidos antiguos se vean bien.
	}
	unset( $states['PE']['REGE'], $states['PE']['REGA'] );
	return $states;
}, 20 );
/* El código postal no se usa en Perú y muchos clientes no lo saben: se oculta y deja de ser obligatorio. */
add_filter( 'woocommerce_get_country_locale', function ( $locale ) {
	$locale['PE']['postcode'] = array( 'required' => false, 'hidden' => true );
	return $locale;
}, 20 );

/* ---------- Campos de agencia (solo se piden si se elige una agencia) ---------- */
function km_env_es_agencia( $clave ) {
	return in_array( $clave, array( 'shalom', 'shalom_contra', 'shalom_express', 'shalom_avion', 'olva', 'plaza_norte', 'otra_agencia' ), true );
}
function km_env_ids_agencia() {
	$ids = array();
	foreach ( array_keys( km_env_tarifas() ) as $clave ) {
		if ( km_env_es_agencia( $clave ) ) {
			$ids[] = 'km_envio:' . $clave;
		}
	}
	return $ids;
}
add_action( 'woocommerce_init', function () {
	if ( ! function_exists( 'woocommerce_register_additional_checkout_field' ) ) {
		return;
	}
	$con_agencia = array(
		'type'       => 'object',
		'properties' => array(
			'cart' => array(
				'properties' => array(
					'shipping_rates' => array( 'contains' => array( 'enum' => km_env_ids_agencia() ) ),
				),
			),
		),
	);
	$sin_agencia = array(
		'type'       => 'object',
		'properties' => array(
			'cart' => array(
				'properties' => array(
					'shipping_rates' => array( 'not' => array( 'contains' => array( 'enum' => km_env_ids_agencia() ) ) ),
				),
			),
		),
	);
	$ids_lima = array( 'km_envio:lima_dia', 'km_envio:lima_express' );
	$con_lima = array(
		'type'       => 'object',
		'properties' => array(
			'cart' => array(
				'properties' => array(
					'shipping_rates' => array( 'contains' => array( 'enum' => $ids_lima ) ),
				),
			),
		),
	);
	$sin_lima = array(
		'type'       => 'object',
		'properties' => array(
			'cart' => array(
				'properties' => array(
					'shipping_rates' => array( 'not' => array( 'contains' => array( 'enum' => $ids_lima ) ) ),
				),
			),
		),
	);
	woocommerce_register_additional_checkout_field( array(
		'id'       => 'km/gps',
		'label'    => 'Ubicación en Google Maps (enlace, ayuda al motorizado)',
		'location' => 'order',
		'type'     => 'text',
		'required' => false,
		'hidden'   => $sin_lima,
	) );
	woocommerce_register_additional_checkout_field( array(
		'id'                => 'km/telefono2',
		'label'             => 'Otro celular de contacto (por si el motorizado no te ubica en el primero)',
		'location'          => 'order',
		'type'              => 'text',
		'required'          => $con_lima,
		'hidden'            => $sin_lima,
		'sanitize_callback' => function ( $v ) { return km_env_celular( $v ); },
		'validate_callback' => function ( $v ) {
			if ( '' !== $v && ! preg_match( '/^9[0-9]{8}$/', $v ) ) {
				return new WP_Error( 'km_telefono2', 'Revisa el otro celular de contacto: deben ser 9 dígitos y empezar con 9.' );
			}
		},
	) );
	woocommerce_register_additional_checkout_field( array(
		'id'       => 'km/agencia',
		'label'    => 'Nombre y dirección de la agencia de destino',
		'location' => 'order',
		'type'     => 'text',
		'required' => $con_agencia,
		'hidden'   => $sin_agencia,
	) );
	woocommerce_register_additional_checkout_field( array(
		'id'                => 'km/dni',
		'label'             => 'DNI de quien recoge en la agencia',
		'location'          => 'order',
		'type'              => 'text',
		'required'          => $con_agencia,
		'hidden'            => $sin_agencia,
		'sanitize_callback' => function ( $v ) { return preg_replace( '/[^0-9A-Za-z]/', '', (string) $v ); },
		'validate_callback' => function ( $v ) {
			if ( '' !== $v && ! preg_match( '/^[0-9A-Za-z]{8,12}$/', $v ) ) {
				return new WP_Error( 'km_dni', 'Revisa el DNI (8 dígitos) o carné de extranjería.' );
			}
		},
	) );
} );

/* ---------- Condición de pago del pedido ---------- */
function km_env_clave_pedido( $order ) {
	foreach ( $order->get_shipping_methods() as $m ) {
		if ( 'km_envio' === $m->get_method_id() ) {
			$clave = $m->get_meta( '_km_clave' );
			if ( $clave && isset( km_env_tarifas()[ $clave ] ) ) {
				return $clave;
			}
		}
	}
	return '';
}

/* ---------- Lista interna de clientes que deben adelantar (la mantiene el panel por REST) ---------- */
function km_env_celular( $v ) {
	$d = preg_replace( '/[^0-9]/', '', (string) $v );
	if ( 11 === strlen( $d ) && 0 === strpos( $d, '51' ) ) {
		$d = substr( $d, 2 );
	}
	return $d;
}
function km_env_en_lista_adelanto( $order ) {
	$lista = get_option( 'km_lista_adelanto', array() );
	$tels  = isset( $lista['telefonos'] ) ? (array) $lista['telefonos'] : array();
	$dnis  = isset( $lista['dnis'] ) ? (array) $lista['dnis'] : array();
	if ( ! $tels && ! $dnis ) {
		return false;
	}
	$mios = array_filter( array(
		km_env_celular( $order->get_billing_phone() ),
		km_env_celular( $order->get_shipping_phone() ),
		km_env_celular( $order->get_meta( '_wc_other/km/telefono2' ) ),
	) );
	if ( array_intersect( $mios, $tels ) ) {
		return true;
	}
	$dni = preg_replace( '/[^0-9A-Za-z]/', '', (string) $order->get_meta( '_wc_other/km/dni' ) );
	return '' !== $dni && in_array( $dni, $dnis, true );
}
add_action( 'rest_api_init', function () {
	register_rest_route( 'km/v1', '/lista-adelanto', array(
		'methods'             => 'POST',
		'permission_callback' => function () { return current_user_can( 'manage_woocommerce' ); },
		'callback'            => function ( WP_REST_Request $r ) {
			$tels = array();
			foreach ( (array) $r->get_param( 'telefonos' ) as $t ) {
				$t = km_env_celular( $t );
				if ( preg_match( '/^[0-9]{7,9}$/', $t ) ) {
					$tels[] = $t;
				}
			}
			$dnis = array();
			foreach ( (array) $r->get_param( 'dnis' ) as $d ) {
				$d = preg_replace( '/[^0-9A-Za-z]/', '', (string) $d );
				if ( strlen( $d ) >= 8 ) {
					$dnis[] = $d;
				}
			}
			$lista = array( 'telefonos' => array_values( array_unique( $tels ) ), 'dnis' => array_values( array_unique( $dnis ) ) );
			update_option( 'km_lista_adelanto', $lista, false );
			return array( 'ok' => true, 'telefonos' => count( $lista['telefonos'] ), 'dnis' => count( $lista['dnis'] ) );
		},
	) );
} );

function km_env_texto_pago( $order ) {
	$clave = km_env_clave_pedido( $order );
	if ( ! $clave ) {
		return '';
	}
	$tipo     = km_env_tarifas()[ $clave ][4];
	if ( 'contraentrega' === $tipo && km_env_en_lista_adelanto( $order ) ) {
		$tipo = 'adelanto10';
	}
	$total    = (float) $order->get_total();
	$envio    = (float) $order->get_shipping_total();
	$prod     = max( 0, $total - $envio );
	$f        = function ( $n ) { return 'S/ ' . number_format( $n, 2, '.', ',' ); };
	switch ( $tipo ) {
		case 'contraentrega':
			$como = 'cod' === $order->get_payment_method()
				? 'Sin adelanto: pagas el total (' . $f( $total ) . ') al motorizado al recibir, en efectivo o Yape.'
				: 'No necesitas adelantar: puedes pagar el total (' . $f( $total ) . ') por Yape, Plin o transferencia, o al motorizado al recibir.';
			return $como . ' Te escribiremos por WhatsApp para confirmar tu pedido; respóndenos para que podamos programarlo. Entrega el día siguiente entre 12:30 pm y 8:00 pm.';
		case 'adelanto10':
			$resto = max( 0, $total - KM_ENV_ADELANTO );
			return 'Adelanta como mínimo ' . $f( KM_ENV_ADELANTO ) . ' por Yape, Plin o transferencia para confirmar tu pedido. El resto (' . $f( $resto ) . ') se lo pagas al motorizado al recibir. Entrega el día siguiente entre 12:30 pm y 8:00 pm.';
		case 'productos':
			return 'Paga por adelantado los productos (' . $f( $total ) . '). Lo enviamos hoy por InDriver desde San Martín de Porres y el costo del taxi se lo pagas al motorizado al recibir. Te confirmamos la hora de salida por WhatsApp.';
		case 'envio':
			return 'Adelanta solo el envío (' . $f( $envio ) . '). Los productos (' . $f( $prod ) . ') los pagas al recoger en tu agencia Shalom.';
		default:
			return 'Paga el total por adelantado (' . $f( $total ) . ') para que despachemos tu pedido.';
	}
}

add_action( 'woocommerce_checkout_order_created', 'km_env_guardar_condicion' );
add_action( 'woocommerce_store_api_checkout_order_processed', 'km_env_guardar_condicion' );
function km_env_guardar_condicion( $order ) {
	$texto = km_env_texto_pago( $order );
	if ( $texto && $order->get_meta( '_km_condicion_pago' ) !== $texto ) {
		$order->update_meta_data( '_km_condicion_pago', $texto );
		$order->save();
		$order->add_order_note( 'Condición de pago según el envío elegido: ' . $texto );
	}
}

function km_env_mostrar_condicion( $order, $html = true ) {
	static $mostrado = array();
	if ( ! $order instanceof WC_Order ) {
		$order = wc_get_order( $order );
	}
	if ( ! $order || ! in_array( $order->get_payment_method(), array( 'bacs', 'cod' ), true ) ) {
		return; // Con tarjeta / Mercado Pago ya se cobró el total.
	}
	$texto = $order->get_meta( '_km_condicion_pago' );
	if ( ! $texto ) {
		$texto = km_env_texto_pago( $order );
	}
	if ( ! $texto ) {
		return;
	}
	$ws = 0 === strpos( $texto, 'Adelanta' ) || 0 === strpos( $texto, 'Paga' )
		? 'Envíanos la captura del pago por WhatsApp al ' . KM_ENV_WHATSAPP . '.'
		: 'Te escribiremos desde el WhatsApp ' . KM_ENV_WHATSAPP . '.';
	if ( ! $html ) {
		echo "\nCÓMO PAGAR TU PEDIDO\n" . $texto . ' ' . $ws . "\n\n";
		return;
	}
	if ( isset( $mostrado[ $order->get_id() ] ) ) {
		return; // Ya se mostró arriba en la página de gracias.
	}
	$mostrado[ $order->get_id() ] = true;
	echo '<section class="km-condicion-pago" style="border:2px solid #0b7a75;border-radius:10px;padding:14px 18px;margin:0 0 24px;background:#f2faf9">'
		. '<h2 style="margin:0 0 6px;font-size:1.15em">Cómo pagar tu pedido</h2>'
		. '<p style="margin:0 0 6px">' . esc_html( $texto ) . '</p>'
		. '<p style="margin:0">' . esc_html( $ws ) . '</p></section>';
}
/* En la página de gracias (de bloques) el recuadro va justo debajo del resumen del pedido, antes de los datos bancarios. */
add_filter( 'render_block_woocommerce/order-confirmation-summary', function ( $html ) {
	$id  = absint( get_query_var( 'order-received' ) );
	$key = isset( $_GET['key'] ) ? wc_clean( wp_unslash( $_GET['key'] ) ) : '';
	$order = $id ? wc_get_order( $id ) : false;
	if ( ! $order || ! hash_equals( $order->get_order_key(), $key ) ) {
		return $html;
	}
	ob_start();
	km_env_mostrar_condicion( $order );
	return $html . ob_get_clean();
} );
add_action( 'woocommerce_thankyou', function ( $order_id ) { km_env_mostrar_condicion( $order_id ); }, 5 );
add_action( 'woocommerce_view_order', function ( $order_id ) { km_env_mostrar_condicion( $order_id ); }, 5 );
add_action( 'woocommerce_email_order_details', function ( $order, $sent_to_admin, $plain_text ) {
	km_env_mostrar_condicion( $order, ! $plain_text );
}, 5, 3 );
add_action( 'woocommerce_admin_order_data_after_shipping_address', function ( $order ) {
	$texto = $order->get_meta( '_km_condicion_pago' );
	if ( $texto ) {
		echo '<p><strong>Condición de pago:</strong><br>' . esc_html( $texto ) . '</p>';
	}
} );

/* ---------- Pago contra entrega: solo con el delivery de Lima al día siguiente ----------
   WooCommerce no deja guardar "km_envio:lima_dia" en "Activar para métodos de envío" (solo instancias), así que se fuerza
   aquí: con eso el checkout de bloques muestra el pago contra entrega solo con esa opción de envío. En el servidor
   WooCommerce compara con "km_envio:<instancia>" y lo descartaría: el filtro de abajo decide a mano. */
add_filter( 'option_woocommerce_cod_settings', function ( $opciones ) {
	if ( is_array( $opciones ) ) {
		$opciones['enable_for_methods'] = array( 'km_envio:lima_dia' );
	}
	return $opciones;
} );
add_filter( 'woocommerce_available_payment_gateways', function ( $gateways ) {
	if ( is_admin() && ! wp_doing_ajax() ) {
		return $gateways;
	}
	if ( ! function_exists( 'WC' ) || ! WC()->session ) {
		return $gateways;
	}
	$elegidos = (array) WC()->session->get( 'chosen_shipping_methods', array() );
	$lima_dia = in_array( 'km_envio:lima_dia', $elegidos, true );
	$todos    = WC()->payment_gateways() ? WC()->payment_gateways()->payment_gateways() : array();
	if ( $lima_dia && ! isset( $gateways['cod'] ) && isset( $todos['cod'] ) && 'yes' === $todos['cod']->enabled ) {
		$gateways['cod'] = $todos['cod'];
	} elseif ( ! $lima_dia && isset( $gateways['cod'] ) ) {
		unset( $gateways['cod'] );
	}
	return $gateways;
}, 20 );
/* Pedido contra entrega: queda "En espera" hasta que el cliente confirme por WhatsApp (no "Procesando"). */
add_filter( 'woocommerce_cod_process_payment_order_status', function () {
	return 'on-hold';
} );

/* ---------- Estilos del checkout ---------- */
add_action( 'wp_head', function () {
	if ( ! function_exists( 'is_checkout' ) || ! ( is_checkout() || is_cart() ) ) {
		return;
	}
	?>
<style id="km-envios">
/* Express: el taxi se paga al recibir, no mostrar "Gratis". */
.wc-block-components-radio-control__option:has(input[value="km_envio:lima_express"]) .wc-block-components-radio-control__secondary-label{visibility:hidden;position:relative}
.wc-block-components-radio-control__option:has(input[value="km_envio:lima_express"]) .wc-block-components-radio-control__secondary-label::after{content:"Taxi al recibir";visibility:visible;position:absolute;right:0;white-space:nowrap}
.wc-block-components-shipping-rates-control .wc-block-components-radio-control__description{font-size:.85em;line-height:1.4;opacity:.85}
.km-env-enlace{display:block;margin-top:6px;font-size:.9em;font-weight:600}
</style>
<script>
/* El checkout de bloques no deja cambiar la etiqueta "Ciudad": se cambia en pantalla para que en Lima pongan el distrito. */
(function(){
	var zonas={
		LIM1:'Lima Norte (Los Olivos, SMP, Comas, Independencia, Puente Piedra, Carabayllo...)',
		LIM2:'Lima Este (SJL, Ate, Santa Anita, El Agustino, La Molina, Chosica...)',
		LIM3:'Lima Centro (Cercado, Rímac, Breña, Jesús María, Lince, Miraflores, Surco, San Borja...)',
		LIM4:'Lima Sur (SJM, VMT, VES, Chorrillos, Barranco, Lurín...)',
		CALL:'Callao (Bellavista, La Perla, Ventanilla, Carmen de la Legua...)',
		LIPO:'Lima Provincias (Huaral, Huacho, Cañete, Barranca...)'
	};
	var enlaces={
		lima:['https://www.google.com/maps/d/viewer?mid=1co_XXj-wqILxR70VXtbn8wnIYFORyNo','Ver el mapa de cobertura del delivery'],
		shalom:['https://shalom.com.pe/agencias','Ver las agencias Shalom del Perú']
	};
	/* Enlace de ayuda debajo de la opción de envío elegida (cobertura en Lima, agencias en Shalom). */
	function enlace(){
		document.querySelectorAll('.wc-block-components-shipping-rates-control .wc-block-components-radio-control__option').forEach(function(op){
			var inp=op.querySelector('input'); var v=inp?inp.value:''; var tipo='';
			if(inp&&inp.checked){
				if(v.indexOf('km_envio:lima_')===0){tipo='lima';}
				else if(v.indexOf('km_envio:shalom')===0){tipo='shalom';}
			}
			var a=op.querySelector('.km-env-enlace');
			if(a&&a.getAttribute('data-tipo')!==tipo){a.remove();a=null;}
			if(tipo&&!a){
				a=document.createElement('a'); a.className='km-env-enlace'; a.setAttribute('data-tipo',tipo);
				a.href=enlaces[tipo][0]; a.target='_blank'; a.rel='noopener'; a.textContent=enlaces[tipo][1]+' ↗';
				a.addEventListener('click',function(e){e.stopPropagation();});
				(op.querySelector('.wc-block-components-radio-control__option-layout')||op).appendChild(a);
			}
		});
	}
	function fijar(){
		enlace();
		document.querySelectorAll('label[for="billing-city"],label[for="shipping-city"]').forEach(function(l){
			if(l.textContent!=='Distrito o ciudad'){l.textContent='Distrito o ciudad';}
		});
		/* Ejemplos de distritos solo en la lista; en el pedido queda el nombre corto de la zona. */
		document.querySelectorAll('#billing-state option,#shipping-state option').forEach(function(o){
			var t=zonas[o.value]; if(t&&o.textContent!==t){o.textContent=t;}
		});
	}
	new MutationObserver(fijar).observe(document.documentElement,{childList:true,subtree:true});
	document.addEventListener('DOMContentLoaded',fijar);
})();
</script>
	<?php
} );
