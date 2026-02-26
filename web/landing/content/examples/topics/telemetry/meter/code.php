<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use function Flow\Telemetry\DSL\{
    console_metric_exporter,
    memory_metric_processor,
    meter_provider,
    resource_detector,
    telemetry
};

require __DIR__ . '/vendor/autoload.php';

$clock = new SystemClock();

$resource = resource_detector()->detect();

$telemetry = telemetry(
    $resource,
    null,
    meter_provider(
        memory_metric_processor(console_metric_exporter(colors: false)),
        $clock,
    ),
);

$telemetry->registerShutdownFunction();

$meter = $telemetry->meter('order-service');

$requestCounter = $meter->createCounter('http.requests', 'requests', 'Total HTTP requests');
$requestCounter->add(1, ['method' => 'GET', 'path' => '/api/users']);
$requestCounter->add(1, ['method' => 'POST', 'path' => '/api/orders']);

$durationHistogram = $meter->createHistogram('http.duration', 'ms', 'Request duration');
$durationHistogram->record(45.2, ['method' => 'GET', 'status' => 200]);
$durationHistogram->record(123.8, ['method' => 'POST', 'status' => 201]);

$queueSize = $meter->createUpDownCounter('queue.size', 'items', 'Queue size');
$queueSize->add(10);
$queueSize->add(-3);

$memoryGauge = $meter->createGauge('process.memory', 'bytes', 'Memory usage');
$memoryGauge->record(memory_get_usage());
