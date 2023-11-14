<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\BatchingPipeline;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\OverridingPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Transformer;

/**
 * The goal of this optimizer is to detect if there is a loader that supports batching and optimize pipeline to use it.
 * This optimization is only applicable for the pipelines with a default batch size (1).
 *
 * Be default all extractors are yielding rows one by one, in that case loaders like for example DbalLoader
 * would become a bottleneck because it would execute a single query for each row.
 * This optimization will detect that and will wrap the pipeline with a BatchingPipeline.
 */
final class BatchSizeOptimization implements Optimization
{
    /**
     * @psalm-suppress DeprecatedClass
     *
     * @var array<class-string<Pipeline>>
     */
    private array $batchingPipelines = [
        BatchingPipeline::class,
        CollectingPipeline::class,
        ParallelizingPipeline::class,
    ];

    /**
     * We can't use DbalLoader::class here because that would create a circular dependency between ETL and Adapters.
     * All adapters requires ETL, but ELT does not require a single adapter to be present.
     *
     * @psalm-suppress PropertyTypeCoercion
     *
     * @var array<class-string<Loader>>
     */
    private array $supportedLoaders = [
        'Flow\ETL\Adapter\Doctrine\DbalLoader',
        'Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader',
        'Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchLoader',
    ];

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(private int $batchSize = 1000)
    {
    }

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        // Pipeline is already batching so we don't need to optimize it
        if (\in_array($pipeline::class, $this->batchingPipelines, true)) {
            return false;
        }

        if ($pipeline instanceof OverridingPipeline) {
            foreach ($pipeline->pipelines() as $subPipeline) {
                if (\in_array($subPipeline::class, $this->batchingPipelines, true)) {
                    return false;
                }
            }
        }

        if (\in_array($element::class, $this->supportedLoaders, true)) {
            return true;
        }

        return false;
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        if ($pipeline instanceof BatchingPipeline) {
            return $pipeline;
        }

        $pipeline = new BatchingPipeline($pipeline, $this->batchSize);
        $pipeline->add($element);

        return $pipeline;
    }
}
