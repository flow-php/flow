<?php

declare(strict_types=1);

namespace Flow\Telemetry\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Telemetry\{Attributes, Resource, Telemetry};
use Flow\Telemetry\Context\{Baggage, Context, ContextStorage, MemoryContextStorage, SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\{LogExporter, LogProcessor, LoggerProvider};
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, PassThroughLogProcessor};
use Flow\Telemetry\Meter\{AggregationTemporality, MeterProvider, MetricExporter, MetricProcessor};
use Flow\Telemetry\Meter\Exemplar\{AlwaysOffExemplarFilter, AlwaysOnExemplarFilter, ExemplarFilter, TraceBasedExemplarFilter};
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Propagation\{ArrayCarrier, CompositePropagator, PropagationContext, Propagator, SuperglobalCarrier, W3CBaggage, W3CTraceContext};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleMetricExporter, ConsoleSpanExporter};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Tracer\{GenericEvent, SpanContext, SpanExporter, SpanLink, SpanProcessor, TracerProvider};
use Flow\Telemetry\Tracer\Processor\{BatchingSpanProcessor, PassThroughSpanProcessor};
use Flow\Telemetry\Tracer\Sampler\{AlwaysOnSampler, Sampler};
use Psr\Clock\ClockInterface;

/**
 * Create a TraceId.
 *
 * If a hex string is provided, creates a TraceId from it.
 * Otherwise, generates a new random TraceId.
 *
 * @param null|string $hex Optional 32-character hexadecimal string
 *
 * @throws \InvalidArgumentException if the hex string is invalid
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function trace_id(?string $hex = null) : TraceId
{
    if ($hex !== null) {
        return TraceId::fromHex($hex);
    }

    return TraceId::generate();
}

/**
 * Create a SpanId.
 *
 * If a hex string is provided, creates a SpanId from it.
 * Otherwise, generates a new random SpanId.
 *
 * @param null|string $hex Optional 16-character hexadecimal string
 *
 * @throws \InvalidArgumentException if the hex string is invalid
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_id(?string $hex = null) : SpanId
{
    if ($hex !== null) {
        return SpanId::fromHex($hex);
    }

    return SpanId::generate();
}

/**
 * Create a Baggage.
 *
 * @param array<string, string> $entries Initial key-value entries
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function baggage(array $entries = []) : Baggage
{
    return new Baggage($entries);
}

/**
 * Create a Context.
 *
 * If no TraceId is provided, generates a new one.
 * If no Baggage is provided, creates an empty one.
 *
 * @param null|TraceId $traceId Optional TraceId to use
 * @param null|Baggage $baggage Optional Baggage to use
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function context(?TraceId $traceId = null, ?Baggage $baggage = null) : Context
{
    $traceId ??= TraceId::generate();
    $baggage ??= new Baggage();

    return new Context($traceId, $baggage);
}

/**
 * Create a MemoryContextStorage.
 *
 * In-memory context storage for storing and retrieving the current context.
 * A single instance should be shared across all providers within a request lifecycle.
 *
 * @param null|Context $context Optional initial context
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_context_storage(?Context $context = null) : MemoryContextStorage
{
    return new MemoryContextStorage($context);
}

/**
 * Create a Resource.
 *
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Resource attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function resource(array $attributes = []) : Resource
{
    return Resource::create($attributes);
}

/**
 * Create a SpanContext.
 *
 * @param TraceId $traceId The trace ID
 * @param SpanId $spanId The span ID
 * @param null|SpanId $parentSpanId Optional parent span ID
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_context(TraceId $traceId, SpanId $spanId, ?SpanId $parentSpanId = null) : SpanContext
{
    return SpanContext::create($traceId, $spanId, $parentSpanId);
}

/**
 * Create a SpanEvent (GenericEvent) with an explicit timestamp.
 *
 * @param string $name Event name
 * @param \DateTimeImmutable $timestamp Event timestamp
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Event attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_event(string $name, \DateTimeImmutable $timestamp, array $attributes = []) : GenericEvent
{
    return GenericEvent::create($name, $timestamp, $attributes);
}

/**
 * Create a SpanLink.
 *
 * @param SpanContext $context The linked span context
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Link attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_link(SpanContext $context, array $attributes = []) : SpanLink
{
    return SpanLink::create($context, $attributes);
}

/**
 * Create a VoidSpanProcessor.
 *
 * No-op span processor that discards all data.
 * Use this when tracing is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_span_processor() : VoidSpanProcessor
{
    return new VoidSpanProcessor();
}

/**
 * Create a VoidMetricProcessor.
 *
 * No-op metric processor that discards all data.
 * Use this when metrics collection is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_metric_processor() : VoidMetricProcessor
{
    return new VoidMetricProcessor();
}

/**
 * Create a VoidLogProcessor.
 *
 * No-op log processor that discards all data.
 * Use this when logging is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_log_processor() : VoidLogProcessor
{
    return new VoidLogProcessor();
}

/**
 * Create a VoidSpanExporter.
 *
 * No-op span exporter that discards all data.
 * Use this when telemetry export is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_span_exporter() : VoidSpanExporter
{
    return new VoidSpanExporter();
}

/**
 * Create a VoidMetricExporter.
 *
 * No-op metric exporter that discards all data.
 * Use this when telemetry export is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_metric_exporter() : VoidMetricExporter
{
    return new VoidMetricExporter();
}

/**
 * Create a VoidLogExporter.
 *
 * No-op log exporter that discards all data.
 * Use this when telemetry export is disabled to minimize overhead.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_log_exporter() : VoidLogExporter
{
    return new VoidLogExporter();
}

/**
 * Create a MemorySpanExporter.
 *
 * Span exporter that stores data in memory.
 * Provides direct getter access to exported spans.
 * Useful for testing and inspection without serialization.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_span_exporter() : MemorySpanExporter
{
    return new MemorySpanExporter();
}

/**
 * Create a MemoryMetricExporter.
 *
 * Metric exporter that stores data in memory.
 * Provides direct getter access to exported metrics.
 * Useful for testing and inspection without serialization.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_metric_exporter() : MemoryMetricExporter
{
    return new MemoryMetricExporter();
}

/**
 * Create a MemoryLogExporter.
 *
 * Log exporter that stores data in memory.
 * Provides direct getter access to exported log entries.
 * Useful for testing and inspection without serialization.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_log_exporter() : MemoryLogExporter
{
    return new MemoryLogExporter();
}

/**
 * Create a MemorySpanProcessor.
 *
 * Span processor that stores spans in memory and exports via configured exporter.
 * Useful for testing.
 *
 * @param SpanExporter $exporter The exporter to send spans to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_span_processor(SpanExporter $exporter) : MemorySpanProcessor
{
    return new MemorySpanProcessor($exporter);
}

/**
 * Create a MemoryMetricProcessor.
 *
 * Metric processor that stores metrics in memory and exports via configured exporter.
 * Useful for testing.
 *
 * @param MetricExporter $exporter The exporter to send metrics to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_metric_processor(MetricExporter $exporter) : MemoryMetricProcessor
{
    return new MemoryMetricProcessor($exporter);
}

/**
 * Create a MemoryLogProcessor.
 *
 * Log processor that stores log records in memory and exports via configured exporter.
 * Useful for testing.
 *
 * @param LogExporter $exporter The exporter to send logs to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_log_processor(LogExporter $exporter) : MemoryLogProcessor
{
    return new MemoryLogProcessor($exporter);
}

/**
 * Create a TracerProvider.
 *
 * Creates a provider that uses a SpanProcessor for processing spans.
 * For void/disabled tracing, pass void_processor().
 * For memory-based testing, pass memory_processor() with exporters.
 *
 * @param SpanProcessor $processor The processor for spans
 * @param ClockInterface $clock The clock for timestamps
 * @param ContextStorage $contextStorage Storage for context propagation
 * @param Sampler $sampler Sampling strategy for spans
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function tracer_provider(
    SpanProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
    Sampler $sampler = new AlwaysOnSampler(),
) : TracerProvider {
    return new TracerProvider(
        $processor,
        $clock,
        $contextStorage,
        $sampler,
    );
}

/**
 * Create a LoggerProvider.
 *
 * Creates a provider that uses a LogProcessor for processing logs.
 * For void/disabled logging, pass void_processor().
 * For memory-based testing, pass memory_processor() with exporters.
 *
 * @param LogProcessor $processor The processor for logs
 * @param ClockInterface $clock The clock for timestamps
 * @param ContextStorage $contextStorage Storage for span correlation
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function logger_provider(
    LogProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
) : LoggerProvider {
    return new LoggerProvider(
        $processor,
        $clock,
        $contextStorage,
    );
}

/**
 * Create a MeterProvider.
 *
 * Creates a provider that uses a MetricProcessor for processing metrics.
 * For void/disabled metrics, pass void_processor().
 * For memory-based testing, pass memory_processor() with exporters.
 *
 * @param MetricProcessor $processor The processor for metrics
 * @param ClockInterface $clock The clock for timestamps
 * @param AggregationTemporality $temporality Aggregation temporality for metrics
 * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling (default: TraceBasedExemplarFilter)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function meter_provider(
    MetricProcessor $processor,
    ClockInterface $clock,
    AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
    ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
) : MeterProvider {
    return new MeterProvider(
        $processor,
        $clock,
        $temporality,
        $exemplarFilter,
    );
}

/**
 * Create a new Telemetry instance with the given providers.
 *
 * If providers are not specified, void providers (no-op) are used.
 *
 * @param resource $resource The resource describing the entity producing telemetry
 * @param null|TracerProvider $tracerProvider The tracer provider (null for void/disabled)
 * @param null|MeterProvider $meterProvider The meter provider (null for void/disabled)
 * @param null|LoggerProvider $loggerProvider The logger provider (null for void/disabled)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function telemetry(
    Resource $resource,
    ?TracerProvider $tracerProvider = null,
    ?MeterProvider $meterProvider = null,
    ?LoggerProvider $loggerProvider = null,
) : Telemetry {
    $clock = new SystemClock();
    $contextStorage = new MemoryContextStorage();

    return new Telemetry(
        $resource,
        $tracerProvider ?? new TracerProvider(new VoidSpanProcessor(), $clock, $contextStorage),
        $meterProvider ?? new MeterProvider(new VoidMetricProcessor(), $clock),
        $loggerProvider ?? new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
    );
}

/**
 * Create an InstrumentationScope.
 *
 * @param string $name The instrumentation scope name
 * @param string $version The instrumentation scope version
 * @param null|string $schemaUrl Optional schema URL
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function instrumentation_scope(
    string $name,
    string $version = 'unknown',
    ?string $schemaUrl = null,
    Attributes $attributes = new Attributes(),
) : InstrumentationScope {
    return new InstrumentationScope($name, $version, $schemaUrl, $attributes);
}

/**
 * Create a BatchingSpanProcessor.
 *
 * Collects spans in memory and exports them in batches for efficiency.
 * Spans are exported when batch size is reached, flush() is called, or shutdown().
 *
 * @param SpanExporter $exporter The exporter to send spans to
 * @param int $batchSize Number of spans to collect before exporting (default 512)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_span_processor(SpanExporter $exporter, int $batchSize = 512) : BatchingSpanProcessor
{
    return new BatchingSpanProcessor($exporter, $batchSize);
}

/**
 * Create a PassThroughSpanProcessor.
 *
 * Exports each span immediately when it ends.
 * Useful for debugging where immediate visibility is more important than performance.
 *
 * @param SpanExporter $exporter The exporter to send spans to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_span_processor(SpanExporter $exporter) : PassThroughSpanProcessor
{
    return new PassThroughSpanProcessor($exporter);
}

/**
 * Create a BatchingMetricProcessor.
 *
 * Collects metrics in memory and exports them in batches for efficiency.
 * Metrics are exported when batch size is reached, flush() is called, or shutdown().
 *
 * @param MetricExporter $exporter The exporter to send metrics to
 * @param int $batchSize Number of metrics to collect before exporting (default 512)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_metric_processor(MetricExporter $exporter, int $batchSize = 512) : BatchingMetricProcessor
{
    return new BatchingMetricProcessor($exporter, $batchSize);
}

/**
 * Create a PassThroughMetricProcessor.
 *
 * Exports each metric immediately when processed.
 * Useful for debugging where immediate visibility is more important than performance.
 *
 * @param MetricExporter $exporter The exporter to send metrics to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_metric_processor(MetricExporter $exporter) : PassThroughMetricProcessor
{
    return new PassThroughMetricProcessor($exporter);
}

/**
 * Create a BatchingLogProcessor.
 *
 * Collects log records in memory and exports them in batches for efficiency.
 * Logs are exported when batch size is reached, flush() is called, or shutdown().
 *
 * @param LogExporter $exporter The exporter to send logs to
 * @param int $batchSize Number of logs to collect before exporting (default 512)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_log_processor(LogExporter $exporter, int $batchSize = 512) : BatchingLogProcessor
{
    return new BatchingLogProcessor($exporter, $batchSize);
}

/**
 * Create a PassThroughLogProcessor.
 *
 * Exports each log record immediately when processed.
 * Useful for debugging where immediate visibility is more important than performance.
 *
 * @param LogExporter $exporter The exporter to send logs to
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_log_processor(LogExporter $exporter) : PassThroughLogProcessor
{
    return new PassThroughLogProcessor($exporter);
}

/**
 * Create a ConsoleSpanExporter.
 *
 * Outputs spans to the console with ASCII table formatting.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_exporter(bool $colors = true) : ConsoleSpanExporter
{
    return new ConsoleSpanExporter($colors);
}

/**
 * Create a ConsoleMetricExporter.
 *
 * Outputs metrics to the console with ASCII table formatting.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_exporter(bool $colors = true) : ConsoleMetricExporter
{
    return new ConsoleMetricExporter($colors);
}

/**
 * Create a ConsoleLogExporter.
 *
 * Outputs log records to the console with severity-based coloring.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 * @param null|int $maxBodyLength Maximum length for body+attributes column (null = no limit, default: 100)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_exporter(bool $colors = true, ?int $maxBodyLength = 100) : ConsoleLogExporter
{
    return new ConsoleLogExporter($colors, $maxBodyLength);
}

/**
 * Create an AlwaysOnExemplarFilter.
 *
 * Records exemplars whenever a span context is present.
 * Use this filter for debugging or when complete trace context is important.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function always_on_exemplar_filter() : AlwaysOnExemplarFilter
{
    return new AlwaysOnExemplarFilter();
}

/**
 * Create an AlwaysOffExemplarFilter.
 *
 * Never records exemplars. Use this filter to disable exemplar collection
 * entirely for performance optimization.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function always_off_exemplar_filter() : AlwaysOffExemplarFilter
{
    return new AlwaysOffExemplarFilter();
}

/**
 * Create a TraceBasedExemplarFilter.
 *
 * Records exemplars only when the span is sampled (has SAMPLED trace flag).
 * This is the default filter, balancing exemplar collection with performance.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function trace_based_exemplar_filter() : TraceBasedExemplarFilter
{
    return new TraceBasedExemplarFilter();
}

/**
 * Create a PropagationContext.
 *
 * Value object containing both trace context (SpanContext) and application
 * data (Baggage) that can be propagated across process boundaries.
 *
 * @param null|SpanContext $spanContext Optional span context
 * @param null|Baggage $baggage Optional baggage
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function propagation_context(?SpanContext $spanContext = null, ?Baggage $baggage = null) : PropagationContext
{
    return new PropagationContext($spanContext, $baggage);
}

/**
 * Create an ArrayCarrier.
 *
 * Carrier backed by an associative array with case-insensitive key lookup.
 *
 * @param array<string, string> $data Initial carrier data
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function array_carrier(array $data = []) : ArrayCarrier
{
    return new ArrayCarrier($data);
}

/**
 * Create a SuperglobalCarrier.
 *
 * Read-only carrier that extracts context from PHP superglobals
 * ($_SERVER, $_GET, $_POST, $_COOKIE).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function superglobal_carrier() : SuperglobalCarrier
{
    return new SuperglobalCarrier();
}

/**
 * Create a W3CTraceContext propagator.
 *
 * Implements W3C Trace Context specification for propagating trace context
 * using traceparent and tracestate headers.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function w3c_trace_context() : W3CTraceContext
{
    return new W3CTraceContext();
}

/**
 * Create a W3CBaggage propagator.
 *
 * Implements W3C Baggage specification for propagating application-specific
 * key-value pairs using the baggage header.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function w3c_baggage() : W3CBaggage
{
    return new W3CBaggage();
}

/**
 * Create a CompositePropagator.
 *
 * Combines multiple propagators into one. On extract, all propagators are
 * invoked and their contexts are merged. On inject, all propagators are invoked.
 *
 * @param Propagator ...$propagators The propagators to combine
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function composite_propagator(Propagator ...$propagators) : CompositePropagator
{
    return new CompositePropagator($propagators);
}
