<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedTotals;

interface AggregatesNumericTotal extends AggregatingFunction
{
    /**
     * @return list<Reference>
     */
    public function references(): array;

    public function exact(): bool;

    public function groupValues(GroupedTotals $totals, Backend $backend): Column;
}
