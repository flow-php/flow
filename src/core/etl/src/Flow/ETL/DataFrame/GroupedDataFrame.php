<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\{DataFrame, GroupBy, Pipeline};

final class GroupedDataFrame
{
    public function __construct(private readonly DataFrame $df, private readonly GroupBy $groupBy)
    {
    }

    public function aggregate(AggregatingFunction ...$aggregations) : DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        $pipelineSetter = function (GroupBy $groupBy) : void {
            /**
             * @phpstan-ignore-next-line
             */
            $this->pipeline = new Pipeline\LinkedPipeline(new Pipeline\GroupByPipeline($groupBy, $this->pipeline));
        };

        $pipelineSetter->bindTo($this->df, $this->df)($this->groupBy);

        return $this->df;
    }

    public function pivot(Reference $ref) : self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }
}
