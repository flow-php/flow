<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics\Statistics;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;

final readonly class ExecutionTime
{
    public function __construct(
        public DateTimeImmutable $startedAt,
        public DateTimeImmutable $finishedAt,
        public HighResolutionTime $highResolutionTime,
    ) {
        if ($startedAt > $finishedAt) {
            throw new InvalidArgumentException('Execution start date must be before finish date');
        }
    }

    public function duration(): DateInterval
    {
        return $this->startedAt->diff($this->finishedAt);
    }

    public function inSeconds(): int
    {
        return $this->finishedAt->getTimestamp() - $this->startedAt->getTimestamp();
    }

    /**
     * @return array{started_at: string, finished_at: string, duration_ns: int}
     */
    public function normalize(): array
    {
        return [
            'started_at' => $this->startedAt->format(DateTimeInterface::RFC3339_EXTENDED),
            'finished_at' => $this->finishedAt->format(DateTimeInterface::RFC3339_EXTENDED),
            'duration_ns' => $this->highResolutionTime->toNanoseconds(),
        ];
    }
}
