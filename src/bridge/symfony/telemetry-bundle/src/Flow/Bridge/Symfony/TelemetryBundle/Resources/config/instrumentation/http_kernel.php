<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\ControllerSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\RouteNamePathMap;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.telemetry.http_kernel.route_name_path_map', RouteNamePathMap::class)
        ->args([
            service('router')->ignoreOnInvalid(),
            '%kernel.build_dir%',
            '%kernel.debug%',
        ])
        ->tag('kernel.cache_warmer');

    $services
        ->set('flow.telemetry.http_kernel.span_subscriber', HttpKernelSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.http_kernel.exclude_paths%',
            service('flow.telemetry.context_storage'),
            service('flow.telemetry.propagator'),
            '%flow.telemetry.http_kernel.context_propagation%',
            '%flow.telemetry.http_kernel.context_propagation_query%',
            service('flow.telemetry.http_kernel.route_name_path_map'),
            // arg $routeNaming (RouteNaming enum) is set in FlowTelemetryBundle::registerInstrumentation.
        ])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.http_kernel.flush_subscriber', HttpKernelFlushSubscriber::class)
        ->args([
            service(Telemetry::class),
            tagged_iterator('flow.telemetry.async_curl_transport'),
        ])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.http_kernel.controller_span_subscriber', ControllerSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.http_kernel.trace_controller%',
        ])
        ->tag('kernel.event_subscriber');
};
