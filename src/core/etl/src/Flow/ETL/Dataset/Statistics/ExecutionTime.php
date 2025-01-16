<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class ExecutionTime
{
    public function __construct(public \DateTimeImmutable $startedAt, public \DateTimeImmutable $finishedAt)
    {
        if ($startedAt > $finishedAt) {
            throw new InvalidArgumentException('Execution start date must be before finish date');
        }
    }

    public function duration() : \DateInterval
    {
        return $this->startedAt->diff($this->finishedAt);
    }

    public function inSeconds() : int
    {
        return $this->finishedAt->getTimestamp() - $this->startedAt->getTimestamp();
    }
}
