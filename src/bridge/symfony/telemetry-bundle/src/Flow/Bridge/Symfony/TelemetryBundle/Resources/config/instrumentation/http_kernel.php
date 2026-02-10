<?php

declare(strict_types=1);

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\{HttpKernelFlushSubscriber, HttpKernelSpanSubscriber};
use Flow\Telemetry\Telemetry;

use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services->set('flow.telemetry.http_kernel.span_subscriber', HttpKernelSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.http_kernel.exclude_paths%',
            service('flow.telemetry.context_storage'),
            service('flow.telemetry.propagator'),
            '%flow.telemetry.http_kernel.context_propagation%',
        ])
        ->tag('kernel.event_subscriber');

    $services->set('flow.telemetry.http_kernel.flush_subscriber', HttpKernelFlushSubscriber::class)
        ->args([service(Telemetry::class)])
        ->tag('kernel.event_subscriber');
};
