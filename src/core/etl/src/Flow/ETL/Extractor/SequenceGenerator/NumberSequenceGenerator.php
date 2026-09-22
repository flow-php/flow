<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

use Flow\ETL\Cardinality;
use Generator;

use function abs;
use function count;
use function intdiv;
use function is_int;
use function range;

final readonly class NumberSequenceGenerator implements SequenceGenerator
{
    public function __construct(
        private string|int|float $start,
        private string|int|float $end,
        private int|float $step = 1,
    ) {}

    public function generate(): Generator
    {
        foreach (range($this->start, $this->end, $this->step) as $item) {
            yield $item;
        }
    }

    /**
     * Integer bounds with a positive step no wider than the span are counted arithmetically; everything else counts
     * the same range() generate() walks, because PHP rounds float ranges its own way and throws on a step that does
     * not fit - a formula would diverge from what extract() yields.
     */
    public function rows(): Cardinality
    {
        if (
            is_int($this->start)
            && is_int($this->end)
            && is_int($this->step)
            && $this->step > 0
            && abs($this->end - $this->start) >= $this->step
        ) {
            return Cardinality::exact(intdiv(abs($this->end - $this->start), $this->step) + 1);
        }

        return Cardinality::exact(count(range($this->start, $this->end, $this->step)));
    }
}
