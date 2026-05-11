<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Clock;

use Psr\Clock\ClockInterface;

/**
 * @internal
 */
final readonly class SystemClock implements ClockInterface
{
    public function now(): \DateTimeImmutable
    {
        return new \DateTimeImmutable();
    }
}
