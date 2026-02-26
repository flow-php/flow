<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use function Flow\Telemetry\DSL\{
    console_span_exporter,
    memory_context_storage,
    memory_span_processor,
    resource_detector,
    telemetry,
    tracer_provider
};
use function Flow\ETL\DSL\clock;

require __DIR__ . '/vendor/autoload.php';

$telemetry = telemetry(
    resource_detector()->detect(),
    tracer_provider(
        memory_span_processor(console_span_exporter(colors: false)),
        clock(),
        memory_context_storage(),
    ),
)->registerShutdownFunction();

$tracer = $telemetry->tracer('order-service');

$span = $tracer->span('process-order', SpanKind::INTERNAL);
$span->setAttribute('order.id', 'ORD-12345');
$span->setAttribute('customer.id', 42);

$childSpan = $tracer->span('validate-payment', SpanKind::INTERNAL);
$childSpan->setAttribute('payment.method', 'credit_card');
$childSpan->setStatus(SpanStatus::ok());
$tracer->complete($childSpan);

$span->setStatus(SpanStatus::ok());
$tracer->complete($span);
