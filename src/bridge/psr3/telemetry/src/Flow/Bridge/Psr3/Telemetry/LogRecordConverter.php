<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry;

use BackedEnum;
use DateTimeInterface;
use Flow\Telemetry\Logger\LogRecord;
use Stringable;
use Throwable;
use UnitEnum;

use function array_keys;
use function array_walk;
use function gettype;
use function is_array;
use function is_object;
use function is_scalar;
use function json_encode;
use function method_exists;
use function str_contains;
use function strtr;

/**
 * Convert a PSR-3 log call (level + message + context) into a Telemetry LogRecord.
 */
final readonly class LogRecordConverter
{
    private const string INTERPOLATION_DATE_FORMAT = 'Y-m-d\TH:i:s.uP';

    public function __construct(
        private SeverityMapper $severityMapper = new SeverityMapper(),
        private ValueNormalizer $valueNormalizer = new ValueNormalizer(),
    ) {}

    /**
     * @param array<array-key, mixed> $context
     */
    public function convert(string|Stringable $level, string|Stringable $message, array $context = []): LogRecord
    {
        $record = new LogRecord(
            severity: $this->severityMapper->map($level),
            body: $this->interpolate((string) $message, $context),
        );

        return $this->applyContext($record, $context);
    }

    /**
     * @param array<array-key, mixed> $context
     */
    private function applyContext(LogRecord $record, array $context): LogRecord
    {
        array_walk($context, function (mixed $value, int|string $key) use (&$record): void {
            $stringKey = (string) $key;

            if ($stringKey === 'exception' && $value instanceof Throwable) {
                $record = $record->setException($value);

                return;
            }

            $record = $record->setAttribute($stringKey, $this->valueNormalizer->normalize($value));
        });

        return $record;
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
