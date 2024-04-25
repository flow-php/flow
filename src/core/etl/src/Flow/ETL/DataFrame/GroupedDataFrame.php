<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\{DataFrame, FlowContext, GroupBy, Pipeline};

final class GroupedDataFrame
{
    public function __construct(private readonly DataFrame $df, private readonly GroupBy $groupBy)
    {
    }

    public function aggregate(AggregatingFunction ...$aggregations) : DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        return $this->toDataFrame();
    }

    public function pivot(Reference $ref) : self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }

    public function toDataFrame(): DataFrame
    {
        return $this->df->rebuild(function (Pipeline $pipeline, FlowContext $context): DataFrame {
            return new DataFrame(
                new Pipeline\LinkedPipeline(new Pipeline\GroupByPipeline($this->groupBy, $pipeline), new Pipeline\SynchronousPipeline()),
                $context
            );
        });
    }

    public function toDF(): DataFrame
    {
        return $this->toDataFrame();
    }
}
