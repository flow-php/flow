<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByProcessor;
use Flow\ETL\Row\Reference;

final readonly class GroupedDataFrame
{
    public function __construct(
        private DataFrame $df,
        private GroupBy $groupBy,
    ) {}

    public function aggregate(AggregatingFunction ...$aggregations): DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        $pipelineAdder = function (GroupBy $groupBy): void {
            // @mago-ignore analysis:non-existent-property,method-access-on-null
            $this->pipeline->add(new GroupByProcessor($groupBy));
        };

        // @mago-ignore analysis:invalid-method-access
        $pipelineAdder->bindTo($this->df, $this->df)($this->groupBy);

        return $this->df;
    }

    public function pivot(Reference $ref): self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }
}
