<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Extractor\ChainExtractor;
use Flow\ETL\Extractor\CollectingExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Partition;
use Flow\ETL\Pipeline;
use Flow\ETL\Row\Reference;
use Flow\ETL\Transformer;

final class PartitioningPipeline implements OverridingPipeline, Pipeline
{
    private Pipeline $nextPipeline;

    /**
     * @param Pipeline $pipeline
     * @param array<Reference> $orderBy
     */
    public function __construct(private readonly Pipeline $pipeline, private readonly array $orderBy = [])
    {
        $this->nextPipeline = $this->pipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
    }

    public function closure(FlowContext $context) : void
    {
        $this->pipeline->closure($context);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass) || $this->nextPipeline->has($transformerClass);
    }

    /**
     * @return array<Pipeline>
     */
    public function pipelines() : array
    {
        $pipelines = [];

        if ($this->pipeline instanceof OverridingPipeline) {
            $pipelines = $this->pipeline->pipelines();
        }
        $pipelines[] = $this->pipeline;

        if ($this->nextPipeline instanceof OverridingPipeline) {
            $pipelines = $this->nextPipeline->pipelines();
        }
        $pipelines[] = $this->nextPipeline;

        return $pipelines;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes()->merge($this->nextPipeline->pipes());
    }

    public function process(FlowContext $context) : \Generator
    {
        $partitionIds = [];

        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows->partitionBy(...$context->partitionEntries()->all()) as $partitionedRows) {

                $rows = $partitionedRows->sortBy(...$this->orderBy);

                $partitionId = \hash('xxh128', $context->config->id() . '_' . \implode('_', \array_map(
                    static fn (Partition $partition) : string => $partition->id(),
                    $partitionedRows->partitions()
                )));

                $partitionIds[] = $partitionId;
                $context->cache()->add($partitionId, $rows);
            }
        }

        $this->nextPipeline->setSource(new ChainExtractor(...\array_map(
            static fn (string $id) : Extractor => new CollectingExtractor(new CacheExtractor($id, null, true)),
            \array_unique($partitionIds)
        )));

        foreach ($this->nextPipeline->process($context) as $rows) {
            yield $rows;
        }
    }

    public function setSource(Extractor $extractor) : Pipeline
    {
        $this->pipeline->setSource($extractor);

        return $this;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
