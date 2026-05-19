<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;

final class ClockMother
{
    public static function frozen(DateTimeImmutable $now = new DateTimeImmutable()): ClockInterface
    {
        return new readonly class($now) implements ClockInterface {
            public function __construct(
                private DateTimeImmutable $now,
            ) {}

            public function now(): DateTimeImmutable
            {
                return $this->now;
            }
        };
    }
}
