<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;

final class SteppingClock implements ClockInterface
{
    public function __construct(
        private readonly int $seconds = 1,
        private DateTimeImmutable $now = new DateTimeImmutable('2026-01-01 00:00:00'),
    ) {}

    public function now(): DateTimeImmutable
    {
        $this->now = $this->now->modify("+{$this->seconds} seconds");

        return $this->now;
    }
}
