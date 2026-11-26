/**
 * KM: redirecciones 301 de páginas que ya no corresponden.
 * Agregar líneas al arreglo: 'ruta-vieja' => 'ruta-nueva'.
 */
add_action('template_redirect', function () {
    $mapa = array(
        'rosado-morado' => '/lentes-fantasia/',   // 25-nov-2026: rosados y morados pasaron a fantasía
    );
    $ruta = trim(parse_url($_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH), '/');
    if (isset($mapa[$ruta])) {
        wp_safe_redirect(home_url($mapa[$ruta]), 301);
        exit;
    }
}, 1);
