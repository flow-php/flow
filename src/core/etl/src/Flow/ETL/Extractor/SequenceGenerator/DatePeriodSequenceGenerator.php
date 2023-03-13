<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

final class DatePeriodSequenceGenerator implements SequenceGenerator
{
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
