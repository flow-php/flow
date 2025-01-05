<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

final class DatePeriodSequenceGenerator implements SequenceGenerator
{
    /**
     * @param \DatePeriod<\DateTimeImmutable, \DateTimeImmutable, null> $period
     */
    public function __construct(private readonly \DatePeriod $period)
    {
    }

    public function generate() : \Generator
    {
        foreach ($this->period->getIterator() as $item) {
            yield $item;
        }
    }
}
