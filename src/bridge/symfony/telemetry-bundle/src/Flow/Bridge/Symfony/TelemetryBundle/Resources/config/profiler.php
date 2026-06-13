<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Profiler\FlowTelemetryDataCollector;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.telemetry.profiler.store', MemoryExporter::class)->public();

    $services->set('flow.telemetry.profiler.collector', FlowTelemetryDataCollector::class)->args([
        service(Telemetry::class),
        service('flow.telemetry.profiler.store'),
    ])->tag('data_collector', [
        'id' => 'flow_telemetry',
        'template' => '@FlowTelemetry/Collector/telemetry.html.twig',
        'priority' => 250,
    ]);
};
