<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

final readonly class NumberSequenceGenerator implements SequenceGenerator
{
    public function __construct(private string|int|float $start, private string|int|float $end, private int|float $step = 1)
    {
    }

    public function generate() : \Generator
    {
        foreach (\range($this->start, $this->end, $this->step) as $item) {
            yield $item;
        }
    }
}
