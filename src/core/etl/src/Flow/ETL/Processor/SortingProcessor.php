<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Sort\MemorySort;
use Flow\ETL\Sort\SortAlgorithms;
use Generator;

/**
 * Sorts all rows by the specified columns using the algorithm selected in the sort config.
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
        yield from match ($context->config->sort->algorithm) {
            SortAlgorithms::MEMORY_SORT => (new MemorySort())->sortGenerator($rows, $context, $this->refs),
            SortAlgorithms::EXTERNAL_SORT => (new ExternalSort(
                new FilesystemBucketsCache(
                    $context->filesystem($context->config->sort->filesystemProtocol),
                    cacheDir: $context->config->cache->localFilesystemCacheDir->suffix('/flow-php-external-sort/'),
                    batchSize: $context->config->cache->externalSortBatchSize,
                ),
                $context->config->cache->externalSortBucketsCount,
                $context->config->cache->externalSortBucketSize,
            ))->sortGenerator($rows, $context, $this->refs),
        };
    }
}
