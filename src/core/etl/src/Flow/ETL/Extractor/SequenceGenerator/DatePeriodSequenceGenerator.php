<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

use DatePeriod;
use Flow\ETL\Cardinality;
use Generator;

use function iterator_count;

final readonly class DatePeriodSequenceGenerator implements SequenceGenerator
{
    /**
     * @param \DatePeriod<\DateTimeInterface, \DateTimeInterface, null>|\DatePeriod<\DateTimeInterface, null, int> $period
     */
    public function __construct(
        private DatePeriod $period,
    ) {}

    public function generate(): Generator
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($this->period->getIterator() as $item) {
            yield $item;
        }
    }

    public function rows(): Cardinality
    {
        return Cardinality::exact(iterator_count($this->period));
    }
}
