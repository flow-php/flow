<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Twig\TelemetryPropagationExtension;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $container
        ->services()
        ->set('flow.telemetry.twig.propagation_extension', TelemetryPropagationExtension::class)
        ->args([service('flow.telemetry.trace_context_provider')])
        ->tag('twig.extension');
};
