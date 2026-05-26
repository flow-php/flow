<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use Flow\Telemetry\Logger\LogRecord as TelemetryLogRecord;
use Monolog\LogRecord;
use Throwable;

/**
 * Convert Monolog LogRecord to Telemetry LogRecord with proper attribute mapping.
 *
 * This class handles the conversion of Monolog log records to Telemetry log records,
 * mapping Monolog's severity levels and applying appropriate prefixes to context
 * and extra attributes.
 */
final readonly class LogRecordConverter
{
    public function __construct(
        private SeverityMapper $severityMapper = new SeverityMapper(),
        private ValueNormalizer $valueNormalizer = new ValueNormalizer(),
    ) {}

    public function convert(LogRecord $record): TelemetryLogRecord
    {
        $telemetryRecord = new TelemetryLogRecord(
            severity: $this->severityMapper->map($record->level),
            body: $record->message,
        );

        return $this->applyAttributes($telemetryRecord, $record);
    }

    private function applyAttributes(TelemetryLogRecord $telemetryRecord, LogRecord $record): TelemetryLogRecord
    {
        $telemetryRecord = $telemetryRecord->setAttribute('monolog.channel', $record->channel)->setAttribute(
            'monolog.level_name',
            $record->level->name,
        );

        $context = $record->context;

        foreach (array_keys($context) as $key) {
            if ($context[$key] instanceof Throwable) {
                $telemetryRecord = $telemetryRecord->setException($context[$key]);

                continue;
            }

            $telemetryRecord = $telemetryRecord->setAttribute(
                "context.{$key}",
                $this->valueNormalizer->normalize($context[$key]),
            );
        }

        $extra = $record->extra;

        foreach (array_keys($extra) as $key) {
            $telemetryRecord = $telemetryRecord->setAttribute(
                "extra.{$key}",
                $this->valueNormalizer->normalize($extra[$key]),
            );
        }

        return $telemetryRecord;
    }
}
