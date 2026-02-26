<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use function Flow\Telemetry\DSL\{
    console_span_exporter,
    memory_context_storage,
    memory_span_processor,
    resource,
    resource_detector,
    telemetry,
    tracer_provider
};
use function Flow\ETL\DSL\clock;

require __DIR__ . '/vendor/autoload.php';

// Detect system resource attributes (OS, host, process, service)
$detected = resource_detector()->detect();

// Add custom attributes that override or extend detected values
$custom = resource([
    'deployment.environment.name' => 'production',
    'service.instance.id' => 'worker-01',
    'service.namespace' => 'order-processing',
]);

// Merge resources (custom attributes take precedence)
$combinedResource = $detected->merge($custom);

$telemetry = telemetry(
    $combinedResource,
    tracer_provider(
        memory_span_processor(console_span_exporter(colors: false)),
        clock(),
        memory_context_storage(),
    ),
)->registerShutdownFunction();

$tracer = $telemetry->tracer('order-service');

$span = $tracer->span('process-order', SpanKind::INTERNAL);
$span->setAttribute('order.id', 'ORD-12345');
$span->setStatus(SpanStatus::ok());
$tracer->complete($span);
