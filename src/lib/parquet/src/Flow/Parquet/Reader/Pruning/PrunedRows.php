<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader\Pruning;

use Flow\Parquet\Reader\RowSelection;

final readonly class PrunedRows
{
    public function __construct(
        public RowSelection $selected,
        public RowSelection $matched,
    ) {}
}
