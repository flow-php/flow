<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Sort\MemorySort;
use Generator;

/**
 * Sorts all rows by the specified columns.
 *
 * Uses memory sort when possible, falls back to external sort for large datasets.
 *
 * @internal
 */
final readonly class SortingProcessor implements Processor
{
    public function __construct(
        private References $refs,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $minMemoryForMemorySort = Unit::fromMb(1);

        if (
            $context->config->sort->algorithm->useMemory()
            && $context->config->sort->memoryLimit->isGreaterThan($minMemoryForMemorySort)
        ) {
            yield from (new MemorySort($context->config->sort->memoryLimit))->sortGenerator(
                $rows,
                $context,
                $this->refs,
            );
        } else {
            yield from $this->externalSort($rows, $context);
        }
    }

    /**
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    private function externalSort(Generator $rows, FlowContext $context): Generator
    {
        return (new ExternalSort(
            new FilesystemBucketsCache(
                $context->filesystem($context->config->sort->filesystemProtocol),
                cacheDir: $context->config->cache->localFilesystemCacheDir->suffix('/flow-php-external-sort/'),
                batchSize: $context->config->cache->externalSortBatchSize,
            ),
            $context->config->cache->externalSortBucketsCount,
        ))->sortGenerator($rows, $context, $this->refs);
    }
}
