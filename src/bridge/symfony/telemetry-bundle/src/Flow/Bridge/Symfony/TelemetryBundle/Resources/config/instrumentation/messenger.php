<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\CurlTransportTickSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.telemetry.messenger.middleware', TracingMiddleware::class)->args([
        service(Telemetry::class),
    ]);

    $services
        ->set('flow.telemetry.messenger.flush_subscriber', MessengerFlushSubscriber::class)
        ->args([service(Telemetry::class)])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.messenger.curl_transport_tick_subscriber', CurlTransportTickSubscriber::class)
        ->args([tagged_iterator('flow.telemetry.curl_transport')])
        ->tag('kernel.event_subscriber');
};
