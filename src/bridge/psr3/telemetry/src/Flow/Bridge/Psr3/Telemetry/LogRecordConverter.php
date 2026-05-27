<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry;

use Flow\Telemetry\Logger\LogRecord;
use Stringable;
use Throwable;

use function array_walk;
use function is_bool;
use function is_object;
use function is_scalar;
use function method_exists;
use function str_contains;
use function strtr;

/**
 * Convert a PSR-3 log call (level + message + context) into a Telemetry LogRecord.
 *
 * Behavior:
 * - Severity mapped via {@see SeverityMapper}.
 * - Message body has `{placeholder}` tokens substituted from context per PSR-3 §1.2.
 *   Only scalars and Stringable objects participate in interpolation; arrays,
 *   Throwables, and objects without __toString are left in the template.
 * - Every context entry is recorded as an attribute under its raw key.
 * - A `Throwable` under the `exception` key is routed through
 *   {@see LogRecord::setException()} (populates exception.type/message/stacktrace)
 *   and NOT also recorded under the `exception` attribute.
 */
final readonly class LogRecordConverter
{
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
        if ($message === '' || !str_contains($message, '{')) {
            return $message;
        }

        $replacements = [];

        array_walk($context, function (mixed $value, int|string $key) use (&$replacements): void {
            $rendered = $this->renderForInterpolation($value);

            if ($rendered === null) {
                return;
            }

            $replacements['{' . $key . '}'] = $rendered;
        });

        if ($replacements === []) {
            return $message;
        }

        return strtr($message, $replacements);
    }

    private function renderForInterpolation(mixed $value): ?string
    {
        if ($value === null) {
            return '';
        }

        if (is_bool($value)) {
            return $value ? '1' : '';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        if ($value instanceof Throwable) {
            return null;
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        return null;
    }
}
