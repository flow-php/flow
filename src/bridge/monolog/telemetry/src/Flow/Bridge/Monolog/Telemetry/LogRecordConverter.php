<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use Flow\Telemetry\Logger\LogRecord as TelemetryLogRecord;
use Monolog\LogRecord;

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

        foreach ($record->context as $key => $value) {
            if ($value instanceof \Throwable) {
                $telemetryRecord = $telemetryRecord->setException($value);

                continue;
            }

            $telemetryRecord = $telemetryRecord->setAttribute(
                "context.{$key}",
                $this->valueNormalizer->normalize($value),
            );
        }

        foreach ($record->extra as $key => $value) {
            $telemetryRecord = $telemetryRecord->setAttribute(
                "extra.{$key}",
                $this->valueNormalizer->normalize($value),
            );
        }

        return $telemetryRecord;
    }
}
