<?php

declare(strict_types=1);

namespace Flow\Clock;

use Psr\Clock\ClockInterface;

final class FakeClock implements ClockInterface
{
    public function __construct(private \DateTimeImmutable $dateTime = new \DateTimeImmutable('now'))
    {
    }

    public function modify(string $modify) : void
    {
        $this->dateTime = $this->dateTime->modify($modify);
    }

    public function now() : \DateTimeImmutable
    {
        return $this->dateTime;
    }

    public function set(\DateTimeImmutable $dateTime) : void
    {
        $this->dateTime = $dateTime;
    }
}
