<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

enum TimeUnit : string
{
    case MICROSECONDS = 'µs';
    case MILLISECONDS = 'ms';
    case MINUTES = 'min';
    case NANOSECONDS = 'ns';
    case SECONDS = 'sec';

    public function fromNanoseconds(int $nanoseconds) : float
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
