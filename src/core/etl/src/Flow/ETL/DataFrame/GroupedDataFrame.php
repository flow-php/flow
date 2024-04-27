<?php

declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\{DataFrame, GroupBy, Pipeline};

final class GroupedDataFrame
{
    /**
     * @var \ReflectionClass<DataFrame>
     */
    private \ReflectionClass $dataFrameReflection;

    public function __construct(private readonly DataFrame $df, private readonly GroupBy $groupBy)
    {
        $this->dataFrameReflection = new \ReflectionClass($this->df);
    }

    public function aggregate(AggregatingFunction ...$aggregations) : DataFrame
    {
        $this->groupBy->aggregate(...$aggregations);

        $pipelineProperty = $this->dataFrameReflection->getProperty('pipeline');
        $currentPipeline = $pipelineProperty->getValue($this->df);
        $pipelineProperty->setValue($this->df, new Pipeline\LinkedPipeline(new Pipeline\GroupByPipeline($this->groupBy, $currentPipeline)));

        return $this->df;
    }

    public function pivot(Reference $ref) : self
    {
        $this->groupBy->pivot($ref);

        return $this;
    }
}
