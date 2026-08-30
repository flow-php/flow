<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
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
        private ?GroupByAlgorithmBuilder $algorithm = null,
    ) {}

    public function aggregate(AggregatingFunction ...$aggregations): DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        $register = function (GroupBy $groupBy, ?GroupByAlgorithmBuilder $algorithm): void {
            // @mago-ignore analysis:non-existent-property,null-property-access,null-argument
            foreach (GroupBySteps::of($groupBy, $this->context->config, $algorithm) as $step) {
                // @mago-ignore analysis:non-existent-property,method-access-on-null
                $this->pipeline->add($step);
            }
        };

        $register->bindTo($this->df, $this->df)($this->groupBy, $this->algorithm);

        return $this->df;
    }

    public function pivot(Reference $ref): self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }
}
