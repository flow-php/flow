<?php

declare(strict_types=1);

namespace Flow\ETL\Retry;

use Psr\Clock\ClockInterface;

final readonly class FailedRetry
{
    private function __construct(
        public \DateTimeImmutable $timestamp,
        public \Throwable $exception,
        public int $attemptNumber,
    ) {
    }

    public static function create(ClockInterface $clock, \Throwable $exception, int $attemptNumber) : self
    {
        return new self($clock->now(), $exception, $attemptNumber);
    }
}
