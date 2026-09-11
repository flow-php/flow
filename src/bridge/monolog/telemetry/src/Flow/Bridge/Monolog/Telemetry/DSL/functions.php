<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\DSL;

use Flow\Bridge\Monolog\Telemetry\LogRecordConverter;
use Flow\Bridge\Monolog\Telemetry\SeverityMapper;
use Flow\Bridge\Monolog\Telemetry\TelemetryHandler;
use Flow\Bridge\Monolog\Telemetry\ValueNormalizer;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\Severity;
use Monolog\Level;

/**
 * Create a ValueNormalizer for converting arbitrary PHP values to Telemetry attribute types.
 *
 * The normalizer handles:
 * - null → 'null' string
 * - scalars (string, int, float, bool) → unchanged
 * - DateTimeInterface → unchanged
 * - Throwable → unchanged
 * - arrays → recursively normalized
 * - objects with __toString() → string cast
 * - objects without __toString() → class name
 * - other types → get_debug_type() result
 *
 * Example usage:
 * ```php
 * $normalizer = value_normalizer();
 * $normalized = $normalizer->normalize($value);
 * ```
 */
#[DocumentationDSL(module: Module::MONOLOG_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function value_normalizer(): ValueNormalizer
{
    return new ValueNormalizer();
}

/**
 * Create a SeverityMapper for mapping Monolog levels to Telemetry severities.
 *
 * @param null|array<int, Severity> $customMapping Optional custom mapping (Monolog Level value => Telemetry Severity)
 *
 * Example with default mapping:
 * ```php
 * $mapper = severity_mapper();
 * ```
 *
 * Example with custom mapping:
 * ```php
 * use Monolog\Level;
 * use Flow\Telemetry\Logger\Severity;
 *
 * $mapper = severity_mapper([
 *     Level::Debug->value => Severity::DEBUG,
 *     Level::Info->value => Severity::INFO,
 *     Level::Notice->value => Severity::WARN,  // Custom: NOTICE → WARN instead of INFO
 *     Level::Warning->value => Severity::WARN,
 *     Level::Error->value => Severity::ERROR,
 *     Level::Critical->value => Severity::FATAL,
 *     Level::Alert->value => Severity::FATAL,
 *     Level::Emergency->value => Severity::FATAL,
 * ]);
 * ```
 */
#[DocumentationDSL(module: Module::MONOLOG_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function severity_mapper(?array $customMapping = null): SeverityMapper
{
    return new SeverityMapper($customMapping);
}

/**
 * Create a LogRecordConverter for converting Monolog LogRecord to Telemetry LogRecord.
 *
 * The converter handles:
 * - Severity mapping from Monolog Level to Telemetry Severity
 * - Message body conversion
 * - Channel and level name as monolog.* attributes
 * - Context values as context.* attributes (Throwables use setException())
 * - Extra values as extra.* attributes
 *
 * @param null|SeverityMapper $severityMapper Custom severity mapper (defaults to standard mapping)
 * @param null|ValueNormalizer $valueNormalizer Custom value normalizer (defaults to standard normalizer)
 *
 * Example usage:
 * ```php
 * $converter = log_record_converter();
 * $telemetryRecord = $converter->convert($monologRecord);
 * ```
 *
 * Example with custom mapper:
 * ```php
 * $converter = log_record_converter(
 *     severityMapper: severity_mapper([
 *         Level::Debug->value => Severity::TRACE,
 *     ])
 * );
 * ```
 */
#[DocumentationDSL(module: Module::MONOLOG_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function log_record_converter(
    ?SeverityMapper $severityMapper = null,
    ?ValueNormalizer $valueNormalizer = null,
): LogRecordConverter {
    return new LogRecordConverter($severityMapper ?? new SeverityMapper(), $valueNormalizer ?? new ValueNormalizer());
}

/**
 * Create a TelemetryHandler for forwarding Monolog logs to Flow Telemetry.
 *
 * @param Logger $logger The Flow Telemetry logger to forward logs to
 * @param LogRecordConverter $converter Converter to transform Monolog LogRecord to Telemetry LogRecord
 * @param Level $level The minimum logging level at which this handler will be triggered
 * @param bool $bubble Whether messages handled by this handler should bubble up to other handlers
 *
 * Example usage:
 * ```php
 * use Monolog\Logger as MonologLogger;
 * use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;
 * use function Flow\Telemetry\DSL\telemetry;
 *
 * $telemetry = telemetry();
 * $logger = $telemetry->logger('my-app');
 *
 * $monolog = new MonologLogger('channel');
 * $monolog->pushHandler(telemetry_handler($logger));
 *
 * $monolog->info('User logged in', ['user_id' => 123]);
 * // → Forwarded to Flow Telemetry with INFO severity
 * ```
 *
 * Example with custom converter:
 * ```php
 * $converter = log_record_converter(
 *     severityMapper: severity_mapper([
 *         Level::Debug->value => Severity::TRACE,
 *     ])
 * );
 * $monolog->pushHandler(telemetry_handler($logger, $converter));
 * ```
 */
#[DocumentationDSL(module: Module::MONOLOG_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function telemetry_handler(
    Logger $logger,
    LogRecordConverter $converter = new LogRecordConverter(),
    Level $level = Level::Debug,
    bool $bubble = true,
    ErrorHandler $errorHandler = new ErrorLogHandler(),
): TelemetryHandler {
    return new TelemetryHandler($logger, $converter, $level, $bubble, $errorHandler);
}
