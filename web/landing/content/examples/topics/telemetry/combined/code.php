<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use function Flow\Telemetry\DSL\{
    console_log_exporter,
    console_metric_exporter,
    console_span_exporter,
    logger_provider,
    memory_context_storage,
    memory_log_processor,
    memory_metric_processor,
    memory_span_processor,
    meter_provider,
    resource_detector,
    telemetry,
    tracer_provider
};
use function Flow\ETL\DSL\clock;

require __DIR__ . '/vendor/autoload.php';

$clock = clock();
$contextStorage = memory_context_storage();

$telemetry = telemetry(
    resource_detector()->detect(),
    tracer_provider(
        memory_span_processor(console_span_exporter(colors: false)),
        $clock,
        $contextStorage,
    ),
    meter_provider(
        memory_metric_processor(console_metric_exporter(colors: false)),
        $clock,
    ),
    logger_provider(
        memory_log_processor(console_log_exporter(colors: false, maxBodyLength: 200)),
        $clock,
        $contextStorage,
    ),
)->registerShutdownFunction();

$tracer = $telemetry->tracer('order-service');
$logger = $telemetry->logger('order-service');
$meter = $telemetry->meter('order-service');

$requestCounter = $meter->createCounter('orders.processed', 'orders', 'Orders processed');
$durationHistogram = $meter->createHistogram('order.duration', 'ms', 'Order processing time');

$span = $tracer->span('process-order', SpanKind::INTERNAL);
$span->setAttribute('order.id', 'ORD-12345');

$logger->info('Processing order', ['order.id' => 'ORD-12345']);

$childSpan = $tracer->span('validate-inventory', SpanKind::INTERNAL);
$logger->debug('Checking inventory', ['items.count' => 3]);
$childSpan->setStatus(SpanStatus::ok());
$tracer->complete($childSpan);

$requestCounter->add(1, ['status' => 'success']);
$durationHistogram->record(125.5, ['order.type' => 'standard']);

$logger->info('Order completed', ['order.id' => 'ORD-12345', 'duration_ms' => 125.5]);

$span->setStatus(SpanStatus::ok());
$tracer->complete($span);
