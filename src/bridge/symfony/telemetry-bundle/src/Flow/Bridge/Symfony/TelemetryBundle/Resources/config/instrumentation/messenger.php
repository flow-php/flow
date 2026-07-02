<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\AsyncCurlTransportTickSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\ConsumeCommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerReceiveCycleSubscriber;
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
        ->set('flow.telemetry.messenger.async_curl_transport_tick_subscriber', AsyncCurlTransportTickSubscriber::class)
        ->args([tagged_iterator('flow.telemetry.async_curl_transport')])
        ->tag('kernel.event_subscriber');

    $services
        ->set('flow.telemetry.messenger.worker_receive_cycle_subscriber', WorkerReceiveCycleSubscriber::class)
        ->args([service(Telemetry::class)])
        ->tag('kernel.event_subscriber');

    $services
        ->set(
            'flow.telemetry.messenger.consume_command_suppression_subscriber',
            ConsumeCommandSuppressionSubscriber::class,
        )
        ->args([service('flow.telemetry.context_storage')])
        ->tag('kernel.event_subscriber');
};
