<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.telemetry.trace_context_provider', TraceContextProvider::class)->args([
        service('flow.telemetry.propagator'),
        service('flow.telemetry.context_storage'),
    ]);

    $services->alias(TraceContextProvider::class, 'flow.telemetry.trace_context_provider')->public();
};
