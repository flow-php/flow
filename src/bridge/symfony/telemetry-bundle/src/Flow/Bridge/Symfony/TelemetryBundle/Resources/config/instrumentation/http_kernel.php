<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\ControllerSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.telemetry.http_kernel.span_subscriber', HttpKernelSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.http_kernel.exclude_paths%',
            service('flow.telemetry.context_storage'),
            service('flow.telemetry.propagator'),
            '%flow.telemetry.http_kernel.context_propagation%',
            '%flow.telemetry.http_kernel.context_propagation_query%',
            service('router')->ignoreOnInvalid(),
            // arg $routeNaming (RouteNaming enum) is set in FlowTelemetryBundle::registerInstrumentation.
        ])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.http_kernel.flush_subscriber', HttpKernelFlushSubscriber::class)
        ->args([service(Telemetry::class)])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.http_kernel.controller_span_subscriber', ControllerSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.http_kernel.trace_controller%',
        ])
        ->tag('kernel.event_subscriber');
};
