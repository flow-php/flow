<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

/**
 * Asserts the result depends only on frame contents, never on the current row or index.
 */
interface FrameAccumulating
{
    /**
     * @param list<array{0: int, 1: int}> $bounds per row of the sorted $partition, inclusive; [1, 0] is an empty frame
     *
     * @return list<mixed> one value per bound
     */
    public function frameValues(Rows $partition, array $bounds, Backend $backend, Progress $progress): array;
}
