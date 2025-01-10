<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

final readonly class DatePeriodSequenceGenerator implements SequenceGenerator
{
    /**
     * @param \DatePeriod<\DateTimeInterface, \DateTimeInterface, null>|\DatePeriod<\DateTimeInterface, null, int> $period
     */
    public function __construct(private \DatePeriod $period)
    {
    }

    public function generate() : \Generator
    {
        foreach ($this->period->getIterator() as $item) {
            yield $item;
        }
    }
}
