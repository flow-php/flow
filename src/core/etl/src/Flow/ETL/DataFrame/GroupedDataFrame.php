<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\{DataFrame, GroupBy};
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Pipeline\{GroupByPipeline, LinkedPipeline};
use Flow\ETL\Row\Reference;

final readonly class GroupedDataFrame
{
    public function __construct(private DataFrame $df, private GroupBy $groupBy)
    {
    }

    public function aggregate(AggregatingFunction ...$aggregations) : DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        $pipelineSetter = function (GroupBy $groupBy) : void {
            /**
             * @phpstan-ignore-next-line
             */
            $this->pipeline = new LinkedPipeline(new GroupByPipeline($groupBy, $this->pipeline));
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
