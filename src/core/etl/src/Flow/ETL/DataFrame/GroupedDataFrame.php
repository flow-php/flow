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

        return $this->addProcessor(new GroupByProcessor($this->groupBy));
    }

    public function pivot(Reference $ref): self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }

    private function addProcessor(GroupByProcessor $processor): DataFrame
    {
        $pipelineAdder = function (GroupByProcessor $processor): void {
            // @mago-ignore analysis:non-existent-property,method-access-on-null
            $this->pipeline->add($processor);
        };

        // @mago-ignore analysis:invalid-method-access
        $pipelineAdder->bindTo($this->df, $this->df)($processor);

        return $this->df;
    }
}
