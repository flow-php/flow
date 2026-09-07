<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
use Flow\ETL\DataFrame;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotValues;
use Flow\ETL\Row\Reference;

final readonly class GroupedDataFrame
{
    public function __construct(
        private DataFrame $df,
        private GroupBy $groupBy,
        private ?GroupByAlgorithmBuilder $algorithm = null,
    ) {}

    public function aggregate(AggregatingFunction ...$aggregations): DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);
        $this->df->registerGroupBy($this->groupBy, $this->algorithm);

        return $this->df;
    }

    public function pivot(Reference $ref, PivotValues $values): self
    {
        // a discovering form scans here, above the plan and over this frame, so the processor only
        // ever holds concrete literals
        $this->groupBy->pivot($ref, $values->resolve($this->df, $ref));

        return $this;
    }
}
