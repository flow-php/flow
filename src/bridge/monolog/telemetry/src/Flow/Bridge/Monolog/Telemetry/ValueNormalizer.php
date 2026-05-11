<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

/**
 * Normalize arbitrary PHP values to types acceptable by Telemetry attributes.
 *
 * This class handles the conversion of various PHP types to the subset of types
 * that can be stored as Telemetry log record attributes.
 */
final readonly class ValueNormalizer
{
    /**
     * Normalize a value to a type acceptable by Telemetry attributes.
     *
     * @return array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable
     */
    public function normalize(mixed $value): string|int|float|bool|\DateTimeInterface|\Throwable|array
    {
        if ($value === null) {
            return 'null';
        }

        if (\is_scalar($value)) {
            return $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value;
        }

        if ($value instanceof \Throwable) {
            return $value;
        }

        if (\is_array($value)) {
            /** @phpstan-ignore return.type */
            return \array_map(fn($v) => $this->normalize($v), $value);
        }

        if (\is_object($value)) {
            if (\method_exists($value, '__toString')) {
                return (string) $value;
            }

            return $value::class;
        }

        return \get_debug_type($value);
    }
}
