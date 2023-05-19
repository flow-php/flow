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
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\WindowFunctionTransformer;
use Flow\ETL\Window;

final class WindowPartitioningPipeline implements Pipeline
{
    private Pipeline $nextPipeline;

    public function __construct(private readonly Pipeline $pipeline, private readonly Window $window, private readonly string $entryName)
    {
        $this->nextPipeline = $this->pipeline->cleanCopy();
        $this->nextPipeline->add(new WindowFunctionTransformer($this->entryName, $this->window));
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

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass) || $this->nextPipeline->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return false;
    }

    public function process(FlowContext $context) : \Generator
    {
        $window = $this->window;
        $partitionIds = [];

        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows->partitionBy(...$this->window->partitions()) as $partitionedRows) {
                $orderedRows = $partitionedRows->orderBy(...$window->order());

                $partitionId = \hash('xxh128', \implode(',', \array_map(
                    static fn (Partition $partition) : string => $partition->id(),
                    $partitionedRows->partitions
                )));

                $partitionIds[] = $partitionId;
                $context->cache()->add($partitionId, $orderedRows);
            }
        }

        $this->nextPipeline->source(new ChainExtractor(...\array_map(
            static fn (string $id) : Extractor => new CollectingExtractor(new CacheExtractor($id, null, true)),
            \array_unique($partitionIds)
        )));

        foreach ($this->nextPipeline->process($context) as $rows) {
            yield $rows;
        }
    }

    public function source(Extractor $extractor) : Pipeline
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
