<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use function Flow\Telemetry\DSL\{
    array_carrier,
    composite_propagator,
    console_span_exporter,
    memory_context_storage,
    memory_span_processor,
    resource_detector,
    telemetry,
    tracer_provider,
    w3c_baggage,
    w3c_trace_context
};

require __DIR__ . '/vendor/autoload.php';

$clock = new SystemClock();
$contextStorage = memory_context_storage();

$telemetry = telemetry(
    resource_detector()->detect(),
    tracer_provider(
        memory_span_processor(console_span_exporter(colors: false)),
        $clock,
        $contextStorage,
    ),
);

$telemetry->registerShutdownFunction();

// Create composite propagator (W3C Trace Context + Baggage)
$propagator = composite_propagator(
    w3c_trace_context(),
    w3c_baggage(),
);

// Simulate incoming request headers from upstream service
$incomingHeaders = [
    'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
    'baggage' => 'userId=alice,requestId=req-123',
];

// Extract context from incoming request
$carrier = array_carrier($incomingHeaders);
$extractedContext = $propagator->extract($carrier);

// Access extracted data
if ($extractedContext->spanContext !== null) {
    echo "Trace ID: " . $extractedContext->spanContext->traceId->toHex() . "\n";
    echo "Parent Span ID: " . $extractedContext->spanContext->spanId->toHex() . "\n";
    echo "Is Sampled: " . ($extractedContext->spanContext->traceFlags->isSampled() ? 'yes' : 'no') . "\n";
}

if ($extractedContext->baggage !== null) {
    echo "User ID: " . $extractedContext->baggage->get('userId') . "\n";
    echo "Request ID: " . $extractedContext->baggage->get('requestId') . "\n";
}

// Create a span as child of the extracted context
$tracer = $telemetry->tracer('order-service');

// Use extracted span context as explicit parent
$span = $tracer->span('handle-request', SpanKind::SERVER, parentContext: $extractedContext->spanContext);
$span->setAttribute('http.method', 'POST');
$span->setAttribute('http.route', '/api/orders');

// Process request...

$span->setStatus(SpanStatus::ok());
$tracer->complete($span);

// Inject context into outgoing response headers
$outgoingCarrier = array_carrier();
$propagator->inject($extractedContext, $outgoingCarrier);
$outgoingHeaders = $outgoingCarrier->unwrap();

echo "\nOutgoing headers:\n";
foreach ($outgoingHeaders as $name => $value) {
    echo "  {$name}: {$value}\n";
}
