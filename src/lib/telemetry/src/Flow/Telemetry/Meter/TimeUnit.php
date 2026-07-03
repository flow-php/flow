<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * Backing values are UCUM time unit codes — they are appended to metric unit strings
 * (e.g. Throughput exports "{row}/s") and must stay UCUM-conformant.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
enum TimeUnit: string
{
    case MICROSECONDS = 'us';
    case MILLISECONDS = 'ms';
    case MINUTES = 'min';
    case NANOSECONDS = 'ns';
    case SECONDS = 's';

    public function fromNanoseconds(int|float $nanoseconds): float
    {
        return match ($this) {
            self::NANOSECONDS => (float) $nanoseconds,
            self::MICROSECONDS => $nanoseconds / 1_000,
            self::MILLISECONDS => $nanoseconds / 1_000_000,
            self::SECONDS => $nanoseconds / 1_000_000_000,
            self::MINUTES => $nanoseconds / 60_000_000_000,
        };
    }
}
