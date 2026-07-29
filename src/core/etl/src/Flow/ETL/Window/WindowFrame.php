<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Rows;

interface WindowFrame
{
    /**
     * Inclusive indexes into the sorted partition.
     * An empty frame is expressed as [1, 0] (start > end).
     *
     * @return array{0: int, 1: int}
     */
    public function bounds(int $index, Rows $partition): array;
}
