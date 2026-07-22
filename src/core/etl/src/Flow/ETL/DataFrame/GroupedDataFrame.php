<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupBySteps;
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

        $register = function (GroupBy $groupBy): void {
            // @mago-ignore analysis:non-existent-property,null-property-access,null-argument
            foreach (GroupBySteps::of($groupBy, $this->context->config) as $step) {
                // @mago-ignore analysis:non-existent-property,method-access-on-null
                $this->pipeline->add($step);
            }
        };

        // @mago-ignore analysis:invalid-method-access
        $register->bindTo($this->df, $this->df)($this->groupBy);

        return $this->df;
    }

    public function pivot(Reference $ref): self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }
}
