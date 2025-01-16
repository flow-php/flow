<?php

declare(strict_types=1);

namespace Flow\Clock;

use Psr\Clock\ClockInterface;

final readonly class SystemClock implements ClockInterface
{
    public function __construct(private \DateTimeZone $timezone)
    {
    }

    public static function system() : self
    {
        return new self(new \DateTimeZone(date_default_timezone_get()));
    }

    public static function utc() : self
    {
        return new self(new \DateTimeZone('UTC'));
    }

    public function now() : \DateTimeImmutable
    {
        return new \DateTimeImmutable('now', $this->timezone);
    }
}
