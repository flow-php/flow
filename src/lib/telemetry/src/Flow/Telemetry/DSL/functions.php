<?php

declare(strict_types=1);

namespace Flow\Telemetry\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Telemetry\{Attributes, Resource, Telemetry};
use Flow\Telemetry\Context\{Baggage, Context, ContextStorage, MemoryContextStorage, SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\{LogExporter, LogProcessor, LogRecordLimits, LoggerProvider, Severity};
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, PassThroughLogProcessor, SeverityFilteringLogProcessor};
use Flow\Telemetry\Meter\{AggregationTemporality, MeterProvider, MetricExporter, MetricLimits, MetricProcessor};
use Flow\Telemetry\Meter\Exemplar\{AlwaysOffExemplarFilter, AlwaysOnExemplarFilter, ExemplarFilter, TraceBasedExemplarFilter};
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Propagation\{ArrayCarrier, CompositePropagator, PropagationContext, Propagator, SuperglobalCarrier, W3CBaggage, W3CTraceContext};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleLogOptions, ConsoleMetricExporter, ConsoleMetricOptions, ConsoleSpanExporter, ConsoleSpanOptions};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Resource\Detector\{CachingDetector, ChainDetector, ComposerDetector, EnvironmentDetector, HostDetector, ManualDetector, OsDetector, ProcessDetector};
use Flow\Telemetry\Resource\ResourceDetector;
use Flow\Telemetry\Tracer\{GenericEvent, SpanContext, SpanExporter, SpanLimits, SpanLink, SpanProcessor, TracerProvider};
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
 * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Resource attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function resource(array|Attributes $attributes = []) : Resource
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
 * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Event attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_event(string $name, \DateTimeImmutable $timestamp, array|Attributes $attributes = []) : GenericEvent
{
    return GenericEvent::create($name, $timestamp, $attributes);
}

/**
 * Create a SpanLink.
 *
 * @param SpanContext $context The linked span context
 * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Link attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function span_link(SpanContext $context, array|Attributes $attributes = []) : SpanLink
{
    return SpanLink::create($context, $attributes);
}

/**
 * Create SpanLimits configuration.
 *
 * SpanLimits controls the maximum amount of data a span can collect,
 * preventing unbounded memory growth and ensuring reasonable span sizes.
 *
 * @param int $attributeCountLimit Maximum number of attributes per span
 * @param int $eventCountLimit Maximum number of events per span
 * @param int $linkCountLimit Maximum number of links per span
 * @param int $attributePerEventCountLimit Maximum number of attributes per event
 * @param int $attributePerLinkCountLimit Maximum number of attributes per link
 * @param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function span_limits(
    int $attributeCountLimit = 128,
    int $eventCountLimit = 128,
    int $linkCountLimit = 128,
    int $attributePerEventCountLimit = 128,
    int $attributePerLinkCountLimit = 128,
    ?int $attributeValueLengthLimit = null,
) : SpanLimits {
    return new SpanLimits(
        $attributeCountLimit,
        $eventCountLimit,
        $linkCountLimit,
        $attributePerEventCountLimit,
        $attributePerLinkCountLimit,
        $attributeValueLengthLimit,
    );
}

/**
 * Create LogRecordLimits configuration.
 *
 * LogRecordLimits controls the maximum amount of data a log record can collect,
 * preventing unbounded memory growth and ensuring reasonable log record sizes.
 *
 * @param int $attributeCountLimit Maximum number of attributes per log record
 * @param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function log_record_limits(
    int $attributeCountLimit = 128,
    ?int $attributeValueLengthLimit = null,
) : LogRecordLimits {
    return new LogRecordLimits(
        $attributeCountLimit,
        $attributeValueLengthLimit,
    );
}

/**
 * Create MetricLimits configuration.
 *
 * MetricLimits controls the maximum cardinality (unique attribute combinations)
 * per metric instrument, preventing memory exhaustion from high-cardinality attributes.
 *
 * When the cardinality limit is exceeded, new attribute combinations are aggregated
 * into an overflow data point with `otel.metric.overflow: true` attribute.
 *
 * Note: Unlike spans and logs, metrics are EXEMPT from attribute count and value
 * length limits per the OpenTelemetry specification. Only cardinality is limited.
 *
 * @param int $cardinalityLimit Maximum number of unique attribute combinations per instrument
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function metric_limits(
    int $cardinalityLimit = 2000,
) : MetricLimits {
    return new MetricLimits(
        $cardinalityLimit,
    );
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
 * @param SpanLimits $limits Limits for span attributes, events, and links
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function tracer_provider(
    SpanProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
    Sampler $sampler = new AlwaysOnSampler(),
    SpanLimits $limits = new SpanLimits(),
) : TracerProvider {
    return new TracerProvider(
        $processor,
        $clock,
        $contextStorage,
        $sampler,
        $limits,
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
 * @param LogRecordLimits $limits Limits for log record attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function logger_provider(
    LogProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
    LogRecordLimits $limits = new LogRecordLimits(),
) : LoggerProvider {
    return new LoggerProvider(
        $processor,
        $clock,
        $contextStorage,
        $limits,
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
 * @param MetricLimits $limits Cardinality limits for metric instruments
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function meter_provider(
    MetricProcessor $processor,
    ClockInterface $clock,
    AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
    ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
    MetricLimits $limits = new MetricLimits(),
) : MeterProvider {
    return new MeterProvider(
        $processor,
        $clock,
        $temporality,
        $exemplarFilter,
        $limits,
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
 * Create a SeverityFilteringLogProcessor.
 *
 * Filters log entries based on minimum severity level. Only entries at or above
 * the configured threshold are passed to the wrapped processor.
 *
 * @param LogProcessor $processor The processor to wrap
 * @param Severity $minimumSeverity Minimum severity level (default: INFO)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function severity_filtering_log_processor(
    LogProcessor $processor,
    Severity $minimumSeverity = Severity::INFO,
) : SeverityFilteringLogProcessor {
    return new SeverityFilteringLogProcessor($processor, $minimumSeverity);
}

/**
 * Create a ConsoleSpanExporter.
 *
 * Outputs spans to the console with ASCII table formatting.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 * @param ConsoleSpanOptions $options Display options for the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_exporter(bool $colors = true, ConsoleSpanOptions $options = new ConsoleSpanOptions()) : ConsoleSpanExporter
{
    return new ConsoleSpanExporter($colors, null, $options);
}

/**
 * Create a ConsoleMetricExporter.
 *
 * Outputs metrics to the console with ASCII table formatting.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 * @param ConsoleMetricOptions $options Display options for the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_exporter(bool $colors = true, ConsoleMetricOptions $options = new ConsoleMetricOptions()) : ConsoleMetricExporter
{
    return new ConsoleMetricExporter($colors, null, $options);
}

/**
 * Create a ConsoleLogExporter.
 *
 * Outputs log records to the console with severity-based coloring.
 * Useful for debugging and development.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 * @param null|int $maxBodyLength Maximum length for body+attributes column (null = no limit, default: 100)
 * @param ConsoleLogOptions $options Display options for the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_exporter(bool $colors = true, ?int $maxBodyLength = 100, ConsoleLogOptions $options = new ConsoleLogOptions()) : ConsoleLogExporter
{
    return new ConsoleLogExporter($colors, $maxBodyLength, null, $options);
}

/**
 * Create ConsoleSpanOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_options() : ConsoleSpanOptions
{
    return ConsoleSpanOptions::default();
}

/**
 * Create ConsoleSpanOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_options_minimal() : ConsoleSpanOptions
{
    return ConsoleSpanOptions::minimal();
}

/**
 * Create ConsoleLogOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_options() : ConsoleLogOptions
{
    return ConsoleLogOptions::default();
}

/**
 * Create ConsoleLogOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_options_minimal() : ConsoleLogOptions
{
    return ConsoleLogOptions::minimal();
}

/**
 * Create ConsoleMetricOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_options() : ConsoleMetricOptions
{
    return ConsoleMetricOptions::default();
}

/**
 * Create ConsoleMetricOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_options_minimal() : ConsoleMetricOptions
{
    return ConsoleMetricOptions::minimal();
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

/**
 * Create a ChainDetector.
 *
 * Combines multiple resource detectors into a chain. Detectors are executed
 * in order and their results are merged. Later detectors take precedence
 * over earlier ones when there are conflicting attribute keys.
 *
 * @param ResourceDetector ...$detectors The detectors to chain
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function chain_detector(ResourceDetector ...$detectors) : ChainDetector
{
    return new ChainDetector(...$detectors);
}

/**
 * Create an OsDetector.
 *
 * Detects operating system information including os.type, os.name, os.version,
 * and os.description using PHP's php_uname() function.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function os_detector() : OsDetector
{
    return new OsDetector();
}

/**
 * Create a HostDetector.
 *
 * Detects host information including host.name, host.arch, and host.id
 * (from /etc/machine-id on Linux or IOPlatformUUID on macOS).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function host_detector() : HostDetector
{
    return new HostDetector();
}

/**
 * Create a ProcessDetector.
 *
 * Detects process information including process.pid, process.executable.path,
 * process.runtime.name (PHP), process.runtime.version, process.command,
 * and process.owner (on POSIX systems).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function process_detector() : ProcessDetector
{
    return new ProcessDetector();
}

/**
 * Create an EnvironmentDetector.
 *
 * Detects resource attributes from OpenTelemetry standard environment variables:
 * - OTEL_SERVICE_NAME: Sets service.name attribute
 * - OTEL_RESOURCE_ATTRIBUTES: Sets additional attributes in key=value,key2=value2 format
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function environment_detector() : EnvironmentDetector
{
    return new EnvironmentDetector();
}

/**
 * Create a ComposerDetector.
 *
 * Detects service.name and service.version from Composer's InstalledVersions
 * using the root package information.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function composer_detector() : ComposerDetector
{
    return new ComposerDetector();
}

/**
 * Create a ManualDetector.
 *
 * Returns manually specified resource attributes. Use this when you need
 * to set attributes explicitly rather than detecting them automatically.
 *
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Resource attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function manual_detector(array $attributes) : ManualDetector
{
    return new ManualDetector($attributes);
}

/**
 * Create a CachingDetector.
 *
 * Wraps another detector and caches its results to a file. On subsequent
 * calls, returns the cached resource instead of running detection again.
 *
 * @param ResourceDetector $detector The detector to wrap
 * @param null|string $cachePath Cache file path (default: sys_get_temp_dir()/flow_telemetry_resource.cache)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function caching_detector(ResourceDetector $detector, ?string $cachePath = null) : CachingDetector
{
    return new CachingDetector($detector, $cachePath);
}

/**
 * Create a resource detector chain.
 *
 * When no detectors are provided, uses the default detector chain:
 * 1. OsDetector - Operating system information
 * 2. HostDetector - Host information
 * 3. ProcessDetector - Process information
 * 4. ComposerDetector - Service information from Composer
 * 5. EnvironmentDetector - Environment variable overrides (highest precedence)
 *
 * When detectors are provided, uses only those detectors.
 *
 * @param array<ResourceDetector> $detectors Optional custom detectors (empty = use defaults)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function resource_detector(array $detectors = []) : ChainDetector
{
    if (\count($detectors) === 0) {
        return new ChainDetector(
            new OsDetector(),
            new HostDetector(),
            new ProcessDetector(),
            new ComposerDetector(),
            new EnvironmentDetector(),
        );
    }

    return new ChainDetector(...$detectors);
}
