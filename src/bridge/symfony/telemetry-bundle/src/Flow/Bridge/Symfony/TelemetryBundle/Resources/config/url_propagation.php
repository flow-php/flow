<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.telemetry.trace_context_url_generator', TraceContextUrlGenerator::class)->args([
        service('router'),
        service('flow.telemetry.trace_context_provider'),
    ]);

    $services->alias(TraceContextUrlGenerator::class, 'flow.telemetry.trace_context_url_generator')->public();
};
