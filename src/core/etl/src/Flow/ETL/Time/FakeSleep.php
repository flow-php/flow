<?php

declare(strict_types=1);

namespace Flow\ETL\Time;

final class FakeSleep implements Sleep
{
    /**
     * @var array<Duration>
     */
    private array $sleepDurations = [];

    private int $totalMicroseconds = 0;

    public function for(Duration $duration) : void
    {
        $this->sleepDurations[] = $duration;
        $this->totalMicroseconds += $duration->microseconds();
    }

    public function reset() : void
    {
        $this->totalMicroseconds = 0;
        $this->sleepDurations = [];
    }

    public function sleepCount() : int
    {
        return \count($this->sleepDurations);
    }

    /**
     * @return array<Duration>
     */
    public function sleepDurations() : array
    {
        return $this->sleepDurations;
    }

    public function totalMicroseconds() : int
    {
        return $this->totalMicroseconds;
    }

    public function totalMilliseconds() : int
    {
        return (int) ($this->totalMicroseconds / 1000);
    }

    public function totalSeconds() : int
    {
        return (int) ($this->totalMicroseconds / 1_000_000);
    }
}
