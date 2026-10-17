<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class Bound implements BoundLeaf
{
    public function __construct(
        public FlatColumn $column,
        public PhysicalOrder $order,
        public PhysicalIntervals $intervals,
    ) {}
}
