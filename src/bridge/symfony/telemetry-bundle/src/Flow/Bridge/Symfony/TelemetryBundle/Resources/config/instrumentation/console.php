<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleSpanSubscriber;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.telemetry.console.span_subscriber', ConsoleSpanSubscriber::class)
        ->args([
            service(Telemetry::class),
            '%flow.telemetry.console.exclude_commands%',
        ])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.console.flush_subscriber', ConsoleFlushSubscriber::class)
        ->args([
            service(Telemetry::class),
            tagged_iterator('flow.telemetry.async_curl_transport'),
        ])
        ->tag('kernel.event_subscriber');
};
