<?php

declare(strict_types=1);

namespace Flow\Telemetry\DSL;

use DateTimeImmutable;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\ErrorHandler\CompositeErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\ErrorHandler\StreamHandler;
use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogHandler;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;
use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\LogRecordLimits;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Processor\PassThroughLogProcessor;
use Flow\Telemetry\Logger\Processor\SeverityFilteringLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Exemplar\AlwaysOffExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\AlwaysOnExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\ExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\TraceBasedExemplarFilter;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricLimits;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Meter\Processor\PassThroughMetricProcessor;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Propagation\SuperglobalCarrier;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\ConsoleExporter;
use Flow\Telemetry\Provider\Console\ConsoleLogOptions;
use Flow\Telemetry\Provider\Console\ConsoleMetricOptions;
use Flow\Telemetry\Provider\Console\ConsoleSpanOptions;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Detector\CachingDetector;
use Flow\Telemetry\Resource\Detector\ChainDetector;
use Flow\Telemetry\Resource\Detector\ComposerDetector;
use Flow\Telemetry\Resource\Detector\EnvironmentDetector;
use Flow\Telemetry\Resource\Detector\HostDetector;
use Flow\Telemetry\Resource\Detector\ManualDetector;
use Flow\Telemetry\Resource\Detector\OsDetector;
use Flow\Telemetry\Resource\Detector\ProcessDetector;
use Flow\Telemetry\Resource\ResourceDetector;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\Processor\PassThroughSpanProcessor;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\Sampler;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanLimits;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;

use function count;

use const LOG_PID;

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
function trace_id(?string $hex = null): TraceId
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
function span_id(?string $hex = null): SpanId
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
function baggage(array $entries = []): Baggage
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
function context(?TraceId $traceId = null, ?Baggage $baggage = null): Context
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
function memory_context_storage(?Context $context = null): MemoryContextStorage
{
    return new MemoryContextStorage($context);
}

/**
 * Create a Resource.
 *
 * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Resource attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function resource(array|Attributes $attributes = []): Resource
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
function span_context(TraceId $traceId, SpanId $spanId, ?SpanId $parentSpanId = null): SpanContext
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
function span_event(string $name, DateTimeImmutable $timestamp, array|Attributes $attributes = []): GenericEvent
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
function span_link(SpanContext $context, array|Attributes $attributes = []): SpanLink
{
    return SpanLink::create($context, $attributes);
}

/**
 * Create SpanLimits configuration.
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
): SpanLimits {
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
 * @param int $attributeCountLimit Maximum number of attributes per log record
 * @param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function log_record_limits(int $attributeCountLimit = 128, ?int $attributeValueLengthLimit = null): LogRecordLimits
{
    return new LogRecordLimits($attributeCountLimit, $attributeValueLengthLimit);
}

/**
 * Create MetricLimits configuration.
 *
 * @param int $cardinalityLimit Maximum number of unique attribute combinations per instrument
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function metric_limits(int $cardinalityLimit = 2000): MetricLimits
{
    return new MetricLimits($cardinalityLimit);
}

/**
 * Create a VoidSpanProcessor.
 *
 * No-op span processor that discards all data.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_span_processor(): VoidSpanProcessor
{
    return new VoidSpanProcessor();
}

/**
 * Create a VoidMetricProcessor.
 *
 * No-op metric processor that discards all data.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_metric_processor(): VoidMetricProcessor
{
    return new VoidMetricProcessor();
}

/**
 * Create a VoidLogProcessor.
 *
 * No-op log processor that discards all data.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_log_processor(): VoidLogProcessor
{
    return new VoidLogProcessor();
}

/**
 * Create a VoidExporter.
 *
 * No-op unified exporter that discards logs, metrics, and spans.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function void_exporter(): VoidExporter
{
    return new VoidExporter();
}

/**
 * Create a MemoryExporter.
 *
 * Unified exporter that stores logs, metrics, and spans in memory for direct access.
 * Useful for testing and inspection without serialization.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_exporter(): MemoryExporter
{
    return new MemoryExporter();
}

/**
 * Create a MemorySpanProcessor.
 *
 * @param Exporter $exporter The exporter to send spans to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_span_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): MemorySpanProcessor {
    return new MemorySpanProcessor($exporter, $errorHandler);
}

/**
 * Create a MemoryMetricProcessor.
 *
 * @param Exporter $exporter The exporter to send metrics to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_metric_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): MemoryMetricProcessor {
    return new MemoryMetricProcessor($exporter, $errorHandler);
}

/**
 * Create a MemoryLogProcessor.
 *
 * @param Exporter $exporter The exporter to send logs to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function memory_log_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): MemoryLogProcessor {
    return new MemoryLogProcessor($exporter, $errorHandler);
}

/**
 * Create a TracerProvider.
 *
 * @param SpanProcessor $processor The processor for spans
 * @param ClockInterface $clock The clock for timestamps
 * @param ContextStorage $contextStorage Storage for context propagation
 * @param Sampler $sampler Sampling strategy for spans
 * @param SpanLimits $limits Limits for span attributes, events, and links
 * @param ErrorHandler $errorHandler Handler for runtime Throwables raised by the processor
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function tracer_provider(
    SpanProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
    Sampler $sampler = new AlwaysOnSampler(),
    SpanLimits $limits = new SpanLimits(),
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): TracerProvider {
    return new TracerProvider($processor, $clock, $contextStorage, $sampler, $limits, $errorHandler);
}

/**
 * Create a LoggerProvider.
 *
 * @param LogProcessor $processor The processor for logs
 * @param ClockInterface $clock The clock for timestamps
 * @param ContextStorage $contextStorage Storage for span correlation
 * @param LogRecordLimits $limits Limits for log record attributes
 * @param ErrorHandler $errorHandler Handler for runtime Throwables raised by the processor
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function logger_provider(
    LogProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage,
    LogRecordLimits $limits = new LogRecordLimits(),
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): LoggerProvider {
    return new LoggerProvider($processor, $clock, $contextStorage, $limits, $errorHandler);
}

/**
 * Create a MeterProvider.
 *
 * @param MetricProcessor $processor The processor for metrics
 * @param ClockInterface $clock The clock for timestamps
 * @param AggregationTemporality $temporality Aggregation temporality for metrics
 * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling (default: TraceBasedExemplarFilter)
 * @param MetricLimits $limits Cardinality limits for metric instruments
 * @param ErrorHandler $errorHandler Handler for runtime Throwables raised by the processor
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function meter_provider(
    MetricProcessor $processor,
    ClockInterface $clock,
    AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
    ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
    MetricLimits $limits = new MetricLimits(),
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): MeterProvider {
    return new MeterProvider($processor, $clock, $temporality, $exemplarFilter, $limits, $errorHandler);
}

/**
 * Create a new Telemetry instance with the given providers.
 *
 * If providers are not specified, void providers (no-op) are used.
 *
 * @param \Flow\Telemetry\Resource $resource The resource describing the entity producing telemetry
 * @param null|TracerProvider $tracerProvider The tracer provider (null for void/disabled)
 * @param null|MeterProvider $meterProvider The meter provider (null for void/disabled)
 * @param null|LoggerProvider $loggerProvider The logger provider (null for void/disabled)
 * @param ErrorHandler $errorHandler Handler propagated to default void providers when explicit ones are not supplied
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function telemetry(
    Resource $resource,
    ?TracerProvider $tracerProvider = null,
    ?MeterProvider $meterProvider = null,
    ?LoggerProvider $loggerProvider = null,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): Telemetry {
    $clock = new SystemClock();
    $contextStorage = new MemoryContextStorage();

    return new Telemetry(
        $resource,
        $tracerProvider ?? new TracerProvider(
            new VoidSpanProcessor(),
            $clock,
            $contextStorage,
            errorHandler: $errorHandler,
        ),
        $meterProvider ?? new MeterProvider(new VoidMetricProcessor(), $clock, errorHandler: $errorHandler),
        $loggerProvider ?? new LoggerProvider(
            new VoidLogProcessor(),
            $clock,
            $contextStorage,
            errorHandler: $errorHandler,
        ),
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
): InstrumentationScope {
    return new InstrumentationScope($name, $version, $schemaUrl, $attributes);
}

/**
 * Create a BatchingSpanProcessor.
 *
 * @param Exporter $exporter The exporter to send spans to
 * @param int $batchSize Number of spans to collect before exporting (default 512)
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_span_processor(
    Exporter $exporter,
    int $batchSize = 512,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): BatchingSpanProcessor {
    return new BatchingSpanProcessor($exporter, $batchSize, $errorHandler);
}

/**
 * Create a PassThroughSpanProcessor.
 *
 * @param Exporter $exporter The exporter to send spans to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_span_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): PassThroughSpanProcessor {
    return new PassThroughSpanProcessor($exporter, $errorHandler);
}

/**
 * Create a BatchingMetricProcessor.
 *
 * @param Exporter $exporter The exporter to send metrics to
 * @param int $batchSize Number of metrics to collect before exporting (default 512)
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_metric_processor(
    Exporter $exporter,
    int $batchSize = 512,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): BatchingMetricProcessor {
    return new BatchingMetricProcessor($exporter, $batchSize, $errorHandler);
}

/**
 * Create a PassThroughMetricProcessor.
 *
 * @param Exporter $exporter The exporter to send metrics to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_metric_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): PassThroughMetricProcessor {
    return new PassThroughMetricProcessor($exporter, $errorHandler);
}

/**
 * Create a BatchingLogProcessor.
 *
 * @param Exporter $exporter The exporter to send logs to
 * @param int $batchSize Number of logs to collect before exporting (default 512)
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function batching_log_processor(
    Exporter $exporter,
    int $batchSize = 512,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): BatchingLogProcessor {
    return new BatchingLogProcessor($exporter, $batchSize, $errorHandler);
}

/**
 * Create a PassThroughLogProcessor.
 *
 * @param Exporter $exporter The exporter to send logs to
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the exporter
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function pass_through_log_processor(
    Exporter $exporter,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): PassThroughLogProcessor {
    return new PassThroughLogProcessor($exporter, $errorHandler);
}

/**
 * Create a SeverityFilteringLogProcessor.
 *
 * @param LogProcessor $processor The processor to wrap
 * @param Severity $minimumSeverity Minimum severity level (default: INFO)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function severity_filtering_log_processor(
    LogProcessor $processor,
    Severity $minimumSeverity = Severity::INFO,
): SeverityFilteringLogProcessor {
    return new SeverityFilteringLogProcessor($processor, $minimumSeverity);
}

/**
 * Create a unified ConsoleExporter for logs, metrics, and spans.
 *
 * Outputs telemetry to the console with ASCII table formatting and optional ANSI colors.
 *
 * @param bool $colors Whether to use ANSI colors (default: true)
 * @param null|int $maxLogBodyLength Maximum length for log body+attributes column (null = no limit)
 * @param ConsoleLogOptions $logOptions Display options for log records
 * @param ConsoleMetricOptions $metricOptions Display options for metrics
 * @param ConsoleSpanOptions $spanOptions Display options for spans
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_exporter(
    bool $colors = true,
    ?int $maxLogBodyLength = 100,
    ConsoleLogOptions $logOptions = new ConsoleLogOptions(),
    ConsoleMetricOptions $metricOptions = new ConsoleMetricOptions(),
    ConsoleSpanOptions $spanOptions = new ConsoleSpanOptions(),
): ConsoleExporter {
    return new ConsoleExporter($colors, $maxLogBodyLength, null, $logOptions, $metricOptions, $spanOptions);
}

/**
 * Create ConsoleSpanOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_options(): ConsoleSpanOptions
{
    return ConsoleSpanOptions::default();
}

/**
 * Create ConsoleSpanOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_span_options_minimal(): ConsoleSpanOptions
{
    return ConsoleSpanOptions::minimal();
}

/**
 * Create ConsoleLogOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_options(): ConsoleLogOptions
{
    return ConsoleLogOptions::default();
}

/**
 * Create ConsoleLogOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_log_options_minimal(): ConsoleLogOptions
{
    return ConsoleLogOptions::minimal();
}

/**
 * Create ConsoleMetricOptions with all display options enabled (default behavior).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_options(): ConsoleMetricOptions
{
    return ConsoleMetricOptions::default();
}

/**
 * Create ConsoleMetricOptions with minimal display (legacy compact format).
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function console_metric_options_minimal(): ConsoleMetricOptions
{
    return ConsoleMetricOptions::minimal();
}

/**
 * Create an AlwaysOnExemplarFilter.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function always_on_exemplar_filter(): AlwaysOnExemplarFilter
{
    return new AlwaysOnExemplarFilter();
}

/**
 * Create an AlwaysOffExemplarFilter.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function always_off_exemplar_filter(): AlwaysOffExemplarFilter
{
    return new AlwaysOffExemplarFilter();
}

/**
 * Create a TraceBasedExemplarFilter.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function trace_based_exemplar_filter(): TraceBasedExemplarFilter
{
    return new TraceBasedExemplarFilter();
}

/**
 * Create a PropagationContext.
 *
 * @param null|SpanContext $spanContext Optional span context
 * @param null|Baggage $baggage Optional baggage
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::TYPE)]
function propagation_context(?SpanContext $spanContext = null, ?Baggage $baggage = null): PropagationContext
{
    return new PropagationContext($spanContext, $baggage);
}

/**
 * Create an ArrayCarrier.
 *
 * @param array<string, string> $data Initial carrier data
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function array_carrier(array $data = []): ArrayCarrier
{
    return new ArrayCarrier($data);
}

/**
 * Create a SuperglobalCarrier.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function superglobal_carrier(): SuperglobalCarrier
{
    return new SuperglobalCarrier();
}

/**
 * Create a W3CTraceContext propagator.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function w3c_trace_context(): W3CTraceContext
{
    return new W3CTraceContext();
}

/**
 * Create a W3CBaggage propagator.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function w3c_baggage(): W3CBaggage
{
    return new W3CBaggage();
}

/**
 * Create a CompositePropagator.
 *
 * @param Propagator ...$propagators The propagators to combine
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function composite_propagator(Propagator ...$propagators): CompositePropagator
{
    return new CompositePropagator($propagators);
}

/**
 * Create a ChainDetector.
 *
 * @param ResourceDetector ...$detectors The detectors to chain
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function chain_detector(ResourceDetector ...$detectors): ChainDetector
{
    return new ChainDetector(...$detectors);
}

/**
 * Create an OsDetector.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function os_detector(): OsDetector
{
    return new OsDetector();
}

/**
 * Create a HostDetector.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function host_detector(): HostDetector
{
    return new HostDetector();
}

/**
 * Create a ProcessDetector.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function process_detector(): ProcessDetector
{
    return new ProcessDetector();
}

/**
 * Create an EnvironmentDetector.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function environment_detector(): EnvironmentDetector
{
    return new EnvironmentDetector();
}

/**
 * Create a ComposerDetector.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function composer_detector(): ComposerDetector
{
    return new ComposerDetector();
}

/**
 * Create a ManualDetector.
 *
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Resource attributes
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function manual_detector(array $attributes): ManualDetector
{
    return new ManualDetector($attributes);
}

/**
 * Create a CachingDetector.
 *
 * @param ResourceDetector $detector The detector to wrap
 * @param null|string $cachePath Cache file path (default: sys_get_temp_dir()/flow_telemetry_resource.cache)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function caching_detector(ResourceDetector $detector, ?string $cachePath = null): CachingDetector
{
    return new CachingDetector($detector, $cachePath);
}

/**
 * Create a resource detector chain.
 *
 * @param array<ResourceDetector> $detectors Optional custom detectors (empty = use defaults)
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function resource_detector(array $detectors = []): ChainDetector
{
    if (count($detectors) === 0) {
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

/**
 * Create the default ErrorLogHandler. Writes via PHP's error_log().
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function error_log_handler(
    ErrorLogMessageType $messageType = ErrorLogMessageType::OperatingSystem,
    bool $expandNewlines = false,
    string $messagePrefix = '[flow-telemetry]',
): ErrorLogHandler {
    return new ErrorLogHandler($messageType, $expandNewlines, $messagePrefix);
}

/**
 * Create a StreamHandler. Appends formatted Throwables (one per line) to a file
 * path or php:// stream wrapper.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function stream_error_handler(
    string $destination,
    int $filePermissions = 0644,
    bool $createDirectories = true,
    string $messagePrefix = '[flow-telemetry]',
): StreamHandler {
    return new StreamHandler($destination, $filePermissions, $createDirectories, $messagePrefix);
}

/**
 * Create a SyslogHandler. Writes via openlog/syslog/closelog.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function syslog_error_handler(
    string $ident = 'flow-telemetry',
    SyslogFacility $facility = SyslogFacility::User,
    int $logOpts = LOG_PID,
    SyslogSeverity $severity = SyslogSeverity::Error,
): SyslogHandler {
    return new SyslogHandler($ident, $facility, $logOpts, $severity);
}

/**
 * Create a UdpSyslogHandler. Sends RFC 5424-style syslog frames over UDP.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function udp_syslog_error_handler(
    string $host,
    int $port = 514,
    string $ident = 'flow-telemetry',
    SyslogFacility $facility = SyslogFacility::User,
    SyslogSeverity $severity = SyslogSeverity::Error,
): UdpSyslogHandler {
    return new UdpSyslogHandler($host, $port, $ident, $facility, $severity);
}

/**
 * Fan errors out to multiple handlers.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function composite_error_handler(ErrorHandler ...$handlers): CompositeErrorHandler
{
    return new CompositeErrorHandler(...$handlers);
}

/**
 * Discard every error. Use only in tests or for explicit silence.
 */
#[DocumentationDSL(module: Module::TELEMETRY, type: DSLType::HELPER)]
function null_error_handler(): NullErrorHandler
{
    return new NullErrorHandler();
}
