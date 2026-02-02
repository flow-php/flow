<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\{Loader, Pipeline, Processor, Transformer};
use Flow\ETL\Processor\{BatchingProcessor, CollectingProcessor, PartitioningProcessor};

/**
 * The goal of this optimizer is to detect if there is a loader that supports batching and optimize pipeline to use it.
 * This optimization is only applicable for the pipelines with a default batch size (1).
 *
 * Be default all extractors are yielding rows one by one, in that case loaders like for example DbalLoader
 * would become a bottleneck because it would execute a single query for each row.
 * This optimization will detect that and will add a BatchingProcessor to the pipeline.
 */
final class BatchSizeOptimization implements Optimization
{
    /**
     * Processor classes that already provide batching behavior.
     *
     * @var array<class-string<Processor>>
     */
    private array $batchingProcessors = [
        BatchingProcessor::class,
        CollectingProcessor::class,
        PartitioningProcessor::class,
    ];

    /**
     * We can't use DbalLoader::class here because that would create a circular dependency between ETL and Adapters.
     * All adapters requires ETL, but ELT does not require a single adapter to be present.
     *
     * @var array<class-string<Loader>>
     */
    private array $supportedLoaders = [
        'Flow\ETL\Adapter\Doctrine\DbalLoader',
        'Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader',
        'Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchLoader',
        'Flow\ETL\Adapter\PostgreSql\PostgreSqlLoader',
    ];

    /**
     * @param int<1, max> $batchSize
     * @param null|array<int, class-string<Loader>> $supportedLoaders
     */
    public function __construct(private readonly int $batchSize = 1000, ?array $supportedLoaders = null)
    {
        if ($supportedLoaders !== null) {
            $this->supportedLoaders = $supportedLoaders;
        }
    }

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        if ($this->hasBatchingProcessor($pipeline)) {
            return false;
        }

        if (\in_array($element::class, $this->supportedLoaders, true)) {
            return true;
        }

        return false;
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        if ($this->hasBatchingProcessor($pipeline)) {
            return $pipeline->add($element);
        }

        $pipeline->add(new BatchingProcessor($this->batchSize));
        $pipeline->add($element);

        return $pipeline;
    }

    private function hasBatchingProcessor(Pipeline $pipeline) : bool
    {
        foreach ($pipeline->stages()->steps() as $step) {
            if ($step instanceof Processor) {
                foreach ($this->batchingProcessors as $batchingProcessor) {
                    if ($step instanceof $batchingProcessor) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
