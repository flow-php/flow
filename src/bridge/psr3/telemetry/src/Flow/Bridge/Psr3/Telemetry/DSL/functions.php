<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\DSL;

use Flow\Bridge\Psr3\Telemetry\{LogRecordConverter, SeverityMapper, TelemetryLogger, ValueNormalizer};
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Telemetry\Logger\{Logger, Severity};

/**
 * Create a TelemetryLogger that exposes a PSR-3 LoggerInterface backed by a Flow Telemetry Logger.
 *
 * @param Logger $logger Flow Telemetry logger to forward log records to
 * @param LogRecordConverter $converter Converter from PSR-3 calls to Telemetry LogRecords
 *
 * Example:
 * ```php
 * use function Flow\Bridge\Psr3\Telemetry\DSL\psr3_telemetry_logger;
 *
 * $psrLogger = psr3_telemetry_logger($telemetry->logger('my-service'));
 * $psrLogger->info('User {user_id} logged in', ['user_id' => 123]);
 * ```
 */
#[DocumentationDSL(module: Module::PSR3_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr3_telemetry_logger(
    Logger $logger,
    LogRecordConverter $converter = new LogRecordConverter(),
) : TelemetryLogger {
    return new TelemetryLogger($logger, $converter);
}

/**
 * Create a SeverityMapper for mapping PSR-3 LogLevel strings to Telemetry Severity.
 *
 * @param null|array<string, Severity> $customMapping Optional override (PSR-3 LogLevel string => Severity)
 *
 * Example with custom mapping:
 * ```php
 * use Psr\Log\LogLevel;
 * use Flow\Telemetry\Logger\Severity;
 *
 * $mapper = psr3_severity_mapper([
 *     LogLevel::DEBUG     => Severity::TRACE,
 *     LogLevel::INFO      => Severity::INFO,
 *     LogLevel::NOTICE    => Severity::WARN,
 *     LogLevel::WARNING   => Severity::WARN,
 *     LogLevel::ERROR     => Severity::ERROR,
 *     LogLevel::CRITICAL  => Severity::FATAL,
 *     LogLevel::ALERT     => Severity::FATAL,
 *     LogLevel::EMERGENCY => Severity::FATAL,
 * ]);
 * ```
 */
#[DocumentationDSL(module: Module::PSR3_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr3_severity_mapper(?array $customMapping = null) : SeverityMapper
{
    return new SeverityMapper($customMapping);
}

/**
 * Create a LogRecordConverter that turns PSR-3 calls into Telemetry LogRecords.
 *
 * The converter:
 * - Maps the PSR-3 level to a {@see Severity} via the provided mapper.
 * - Substitutes `{placeholder}` tokens in the message body using context entries
 *   (scalars and Stringable objects only).
 * - Stores every context entry as an attribute under its raw key.
 * - Routes a `Throwable` under the `exception` key through `setException()`.
 */
#[DocumentationDSL(module: Module::PSR3_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr3_log_record_converter(
    ?SeverityMapper $severityMapper = null,
    ?ValueNormalizer $valueNormalizer = null,
) : LogRecordConverter {
    return new LogRecordConverter(
        $severityMapper ?? psr3_severity_mapper(),
        $valueNormalizer ?? psr3_value_normalizer(),
    );
}

/**
 * Create a ValueNormalizer for converting arbitrary PHP values into Telemetry attribute types.
 */
#[DocumentationDSL(module: Module::PSR3_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function psr3_value_normalizer() : ValueNormalizer
{
    return new ValueNormalizer();
}
