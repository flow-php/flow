<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotValues;
use Flow\ETL\Row\Reference;

final readonly class GroupedDataFrame
{
    public function __construct(
        private DataFrame $df,
        private DataFrame $input,
        private GroupBy $groupBy,
    ) {}

    public function aggregate(AggregatingFunction ...$aggregations): DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        return $this->df;
    }

    public function pivot(Reference $ref, PivotValues $values): self
    {
        $this->groupBy->pivot($ref, $values->resolve($this->input, $ref));

        return $this;
    }
}
