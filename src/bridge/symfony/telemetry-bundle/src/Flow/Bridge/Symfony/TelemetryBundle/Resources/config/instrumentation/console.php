<?php

declare(strict_types=1);

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\{ConsoleFlushSubscriber, ConsoleSpanSubscriber};
use Flow\Telemetry\Telemetry;

use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services->set('flow.telemetry.console.span_subscriber', ConsoleSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.console.exclude_commands%',
        ])
        ->tag('kernel.event_subscriber');

    $services->set('flow.telemetry.console.flush_subscriber', ConsoleFlushSubscriber::class)
        ->args([service(Telemetry::class)])
        ->tag('kernel.event_subscriber');
};
