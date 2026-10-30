<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Values\Rows;

interface WindowFrame extends Describable
{
    /**
     * Inclusive indexes into the sorted partition.
     * An empty frame is expressed as [1, 0] (start > end).
     *
     * @return array{0: int, 1: int}
     */
    public function bounds(int $index, Rows $partition): array;
}
