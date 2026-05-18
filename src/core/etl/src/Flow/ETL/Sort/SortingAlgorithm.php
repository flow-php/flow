<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\FlowContext;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

/**
 * @internal
 */
interface SortingAlgorithm
{
    /**
     * Sort a generator of Rows batches.
     *
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator;
}
