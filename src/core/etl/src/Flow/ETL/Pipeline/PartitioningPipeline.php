<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{from_all, from_cache};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CollectingExtractor;
use Flow\ETL\Row\Reference;
use Flow\ETL\{Cache\CacheIndex,
    Extractor,
    FlowContext,
    Hash\Algorithm,
    Hash\NativePHPHash,
    Loader,
    Pipeline,
    Transformer};
use Flow\Filesystem\Partition;

final readonly class PartitioningPipeline implements Pipeline
{
    private Algorithm $hashAlgorithm;

    /**
     * @param Pipeline $pipeline
     * @param array<Reference> $partitionBy
     * @param array<Reference> $orderBy
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Pipeline $pipeline,
        private array $partitionBy = [],
        private array $orderBy = [],
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
        /**
         * @var array<CacheIndex> $partitionIndexes
         */
        $partitionIndexes = [];

        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows->partitionBy(...$this->partitionBy) as $partitionedRows) {

                $rows = $partitionedRows->sortBy(...$this->orderBy);

                $partitionId = $this->hashAlgorithm->hash($context->config->id() . '_' . \implode('_', \array_map(
                    static fn (Partition $partition) : string => $partition->id(),
                    $partitionedRows->partitions()->toArray()
                )));

                if (!\array_key_exists($partitionId, $partitionIndexes)) {
                    $partitionIndexes[$partitionId] = new CacheIndex($partitionId);
                }

                $context->cache()->set($rowsCacheId = \bin2hex(\random_bytes(16)), $rows);
                $partitionIndexes[$partitionId]->add($rowsCacheId);
            }
        }

        foreach ($partitionIndexes as $partitionIndex) {
            $context->cache()->set($partitionIndex->key, $partitionIndex);
        }

        return from_all(
            ...\array_map(
                static fn (string $id) : Extractor => new CollectingExtractor(from_cache($id, clear: true)),
                \array_keys($partitionIndexes)
            )
        )->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
