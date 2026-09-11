<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Row\Reference;

final readonly class Pivot
{
    public function __construct(
        public Reference $column,
        public DeclaredPivotValues $values,
    ) {}
}
