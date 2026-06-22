<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use BackedEnum;
use DateTimeInterface;
use Flow\Telemetry\Logger\LogRecord as TelemetryLogRecord;
use Monolog\LogRecord;
use Throwable;
use UnitEnum;

use function array_keys;
use function gettype;
use function is_array;
use function is_object;
use function is_scalar;
use function json_encode;
use function method_exists;
use function str_contains;
use function strtr;

/**
 * Convert Monolog LogRecord to Telemetry LogRecord with proper attribute mapping.
 */
final readonly class LogRecordConverter
{
    private const string INTERPOLATION_DATE_FORMAT = 'Y-m-d\TH:i:s.uP';

    public function __construct(
        private SeverityMapper $severityMapper = new SeverityMapper(),
        private ValueNormalizer $valueNormalizer = new ValueNormalizer(),
    ) {}

    public function convert(LogRecord $record): TelemetryLogRecord
    {
        $telemetryRecord = new TelemetryLogRecord(
            severity: $this->severityMapper->map($record->level),
            body: $this->interpolate($record->message, $record->context),
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

    /**
     * @param array<array-key, mixed> $context
     */
    private function interpolate(string $message, array $context): string
    {
        if (!str_contains($message, '{')) {
            return $message;
        }

        $replacements = [];

        foreach (array_keys($context) as $key) {
            $placeholder = '{' . $key . '}';

            if (!str_contains($message, $placeholder)) {
                continue;
            }

            $replacements[$placeholder] = $this->renderForInterpolation($context[$key]);
        }

        return strtr($message, $replacements);
    }

    private function renderForInterpolation(mixed $value): string
    {
        if ($value === null) {
            return '';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        if ($value instanceof DateTimeInterface) {
            return $value->format(self::INTERPOLATION_DATE_FORMAT);
        }

        if ($value instanceof BackedEnum) {
            return (string) $value->value;
        }

        if ($value instanceof UnitEnum) {
            return $value->name;
        }

        if (is_object($value)) {
            return '[object ' . $value::class . ']';
        }

        if (is_array($value)) {
            return 'array' . (json_encode($value) ?: '');
        }

        return '[' . gettype($value) . ']';
    }
}
