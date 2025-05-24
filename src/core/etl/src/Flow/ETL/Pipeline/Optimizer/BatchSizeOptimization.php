<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\{Loader, Loader\BatchingLoader, Pipeline, Transformer};
use Flow\ETL\Pipeline\{BatchingPipeline, CollectingPipeline, OverridingPipeline, PartitioningPipeline};
use Flow\ETL\Pipeline\LinkedPipeline;
use Flow\ETL\Transformer\BatchingTransformer;

/**
 * The goal of this optimizer is to detect if there is a loader that supports batching and optimize a pipeline to use it.
 * This optimization is only applicable for the pipelines with a default batch size (1).
 *
 * By default, all extractors are yielding rows one by one, in that case loaders like-for-example, DbalLoader
 * would become a bottleneck because it would execute a single query for each row.
 * This optimization will detect that and will wrap the pipeline with a BatchingPipeline.
 */
final class BatchSizeOptimization implements Optimization
{
    /**
     * @var array<class-string<Pipeline>>
     */
    private array $batchingPipelines = [
        BatchingPipeline::class,
        CollectingPipeline::class,
        PartitioningPipeline::class,
    ];

    public function __construct()
    {
    }

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        // Pipeline is already batching, so we don't need to optimize it
        if (\in_array($pipeline::class, $this->batchingPipelines, true)) {
            return false;
        }

        foreach ($this->allPipelines($pipeline) as $subPipeline) {
            if (\in_array($subPipeline::class, $this->batchingPipelines, true)) {
                return false;
            }
        }

        if ($element instanceof BatchingLoader || $element instanceof BatchingTransformer) {
            return true;
        }

        return false;
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        if ($pipeline instanceof BatchingPipeline) {
            return $pipeline;
        }

        if ($element instanceof BatchingLoader || $element instanceof BatchingTransformer) {
            $pipeline = new LinkedPipeline(new BatchingPipeline($pipeline, $element->defaultBatchSize()));
            $pipeline->add($element);
        }

        return $pipeline;
    }

    /**
     * @return array<Pipeline>
     */
    private function allPipelines(Pipeline $pipeline) : array
    {
        $pipelines = [$pipeline];

        if ($pipeline instanceof OverridingPipeline) {
            foreach ($pipeline->pipelines() as $nextPipeline) {
                $pipelines = [...$pipelines, ...$this->allPipelines($nextPipeline)];
            }
        }

        return $pipelines;
    }
}
