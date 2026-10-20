<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader\Pruning;

use Flow\Parquet\Filter\Binding\BoundFilter;

interface Pruner
{
    /**
     * Selects a subset of $rows->selected, evaluating only the units that still select a row; matched is the selected
     * rows of $rows->matched and of the units it proves to match.
     */
    public function prune(BoundFilter $filter, PrunedRows $rows): PrunedRows;
}
