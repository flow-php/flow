<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\GroupedDataFrame;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Operators\GroupBy\PivotValues;
use NoDiscard;

final readonly class Pivot
{
    public function __construct(
        private Reference $ref,
        private PivotValues $values,
    ) {}

    #[NoDiscard]
    public function __invoke(GroupedDataFrame $groupedDataFrame): GroupedDataFrame
    {
        return $groupedDataFrame->pivot($this->ref, $this->values);
    }
}
