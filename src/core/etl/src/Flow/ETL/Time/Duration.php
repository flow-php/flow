<?php

declare(strict_types=1);

namespace Flow\ETL\Time;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class Duration
{
    private int $microseconds;

    private function __construct(int $microseconds)
    {
        if ($microseconds < 0) {
            throw new InvalidArgumentException('Duration cannot be negative');
        }

        $this->microseconds = $microseconds;
    }

    public static function fromMicroseconds(int $microseconds) : self
    {
        return new self($microseconds);
    }

    public static function fromMilliseconds(int $milliseconds) : self
    {
        return new self($milliseconds * 1000);
    }

    public static function fromMinutes(int $minutes) : self
    {
        return new self($minutes * 60 * 1_000_000);
    }

    public static function fromSeconds(int $seconds) : self
    {
        return new self($seconds * 1_000_000);
    }

    public function microseconds() : int
    {
        return $this->microseconds;
    }

    public function milliseconds() : int
    {
        return (int) ($this->microseconds / 1000);
    }

    public function minutes() : int
    {
        return (int) ($this->microseconds / 60_000_000);
    }

    public function seconds() : int
    {
        return (int) ($this->microseconds / 1_000_000);
    }
}
