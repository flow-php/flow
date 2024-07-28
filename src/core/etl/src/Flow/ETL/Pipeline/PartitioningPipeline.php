<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{from_all, from_cache};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CollectingExtractor;
use Flow\ETL\Row\Reference;
use Flow\ETL\{Extractor, FlowContext, Hash\Algorithm, Hash\NativePHPHash, Loader, Pipeline, Transformer};
use Flow\Filesystem\Partition;

final class PartitioningPipeline implements Pipeline
{
    private readonly Algorithm $hashAlgorithm;

    /**
     * @param Pipeline $pipeline
     * @param array<Reference> $partitionBy
     * @param array<Reference> $orderBy
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly array $partitionBy = [],
        private readonly array $orderBy = []
    ) {
        if (!\count($this->partitionBy)) {
            throw new InvalidArgumentException('PartitioningPipeline requires at least one partitionBy entry');
        }
        $this->hashAlgorithm = new NativePHPHash();
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        $partitionIds = [];

        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows->partitionBy(...$this->partitionBy) as $partitionedRows) {

                $rows = $partitionedRows->sortBy(...$this->orderBy);

                $partitionId = $this->hashAlgorithm->hash($context->config->id() . '_' . \implode('_', \array_map(
                    static fn (Partition $partition) : string => $partition->id(),
                    $partitionedRows->partitions()->toArray()
                )));

                $partitionIds[] = $partitionId;
                $context->rowsCache()->append($partitionId, $rows);
            }
        }

        return from_all(
            ...\array_map(
                static fn (string $id) : Extractor => new CollectingExtractor(from_cache($id, null, true)),
                \array_unique($partitionIds)
            )
        )->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
