<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

final class NumberSequenceGenerator implements SequenceGenerator
{
    public function __construct(private readonly string|int|float $start, private readonly string|int|float $end, private readonly int|float $step = 1)
    {
    }

    public function generate() : \Generator
    {
        foreach (\range($this->start, $this->end, $this->step) as $item) {
            yield $item;
        }
    }
}
