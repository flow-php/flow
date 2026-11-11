<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;

final class SteppingClock implements ClockInterface
{
    private DateTimeImmutable $now;

    public function __construct()
    {
        $this->now = new DateTimeImmutable('2026-01-01 00:00:00');
    }

    public function now(): DateTimeImmutable
    {
        $this->now = $this->now->modify('+1 seconds');

        return $this->now;
    }
}
