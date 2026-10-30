<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Functions\Reference\Reference;

final readonly class Pivot
{
    public function __construct(
        public Reference $column,
        public DeclaredPivotValues $values,
    ) {}
}
