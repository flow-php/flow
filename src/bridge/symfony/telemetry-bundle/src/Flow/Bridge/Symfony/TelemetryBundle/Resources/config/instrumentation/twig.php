<?php

declare(strict_types=1);

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TracingTwigExtension;
use Flow\Telemetry\Telemetry;

use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services->set('flow.telemetry.twig.extension', TracingTwigExtension::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.twig.trace_templates%',
            '%flow.telemetry.twig.trace_blocks%',
            '%flow.telemetry.twig.trace_macros%',
            '%flow.telemetry.twig.exclude_templates%',
        ])
        ->tag('twig.extension');
};
