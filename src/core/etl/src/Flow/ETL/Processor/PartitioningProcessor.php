<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use function Flow\ETL\DSL\{from_all, from_cache};
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\CollectingExtractor;
use Flow\ETL\{Extractor, FlowContext, Processor, Rows};
use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\ETL\Row\Reference;
use Flow\Filesystem\Partition;

/**
 * Partitions rows by column values and caches each partition.
 *
 * @internal
 */
final readonly class PartitioningProcessor implements Processor
{
    private Algorithm $hashAlgorithm;

    /**
     * @param array<Reference> $partitionBy
     * @param array<Reference> $orderBy
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private array $partitionBy = [],
        private array $orderBy = [],
    ) {
        if (!\count($this->partitionBy)) {
            throw new InvalidArgumentException('PartitioningProcessor requires at least one partitionBy entry');
        }
        $this->hashAlgorithm = new NativePHPHash();
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        /** @var array<string, CacheIndex> $partitionIndexes */
        $partitionIndexes = [];

        foreach ($rows as $batch) {
            foreach ($batch->partitionBy(...$this->partitionBy) as $partitionedRows) {

                $sortedRows = $partitionedRows->sortBy(...$this->orderBy);

                $partitionId = $this->hashAlgorithm->hash($context->config->id() . '_' . \implode('_', \array_map(
                    static fn (Partition $partition) : string => $partition->id(),
                    $partitionedRows->partitions()->toArray()
                )));

                if (!\array_key_exists($partitionId, $partitionIndexes)) {
                    $partitionIndexes[$partitionId] = new CacheIndex($partitionId);
                }

                $context->cache()->set($rowsCacheId = \bin2hex(\random_bytes(16)), $sortedRows);
                $partitionIndexes[$partitionId]->add($rowsCacheId);
            }
        }

        foreach ($partitionIndexes as $partitionIndex) {
            $context->cache()->set($partitionIndex->key, $partitionIndex);
        }

        yield from from_all(
            ...\array_map(
                static fn (string $id) : Extractor => new CollectingExtractor(from_cache($id, clear: true)),
                \array_keys($partitionIndexes)
            )
        )->extract($context);
    }
}
