<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Rows;

use function max;
use function min;

final readonly class RowsFrame implements WindowFrame
{
    public function __construct(
        private FrameBound $start,
        private FrameBound $end,
    ) {}

    public function bounds(int $index, Rows $partition): array
    {
        $lastIndex = $partition->count() - 1;

        $start = $this->start->resolve($index, $lastIndex);
        $end = $this->end->resolve($index, $lastIndex);

        if ($start > $lastIndex || $end < 0 || $start > $end) {
            return [1, 0];
        }

        return [max(0, $start), min($lastIndex, $end)];
    }
}
