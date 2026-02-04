<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\{FlowContext, Processor, Rows};
use Flow\ETL\Row\References;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Sort\{ExternalSort, MemorySort};

/**
 * Sorts all rows by the specified columns.
 *
 * Uses memory sort when possible, falls back to external sort for large datasets.
 *
 * @internal
 */
final readonly class SortingProcessor implements Processor
{
    public function __construct(private References $refs)
    {
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        $minMemoryForMemorySort = Unit::fromMb(1);

        if ($context->config->sort->algorithm->useMemory() && $context->config->sort->memoryLimit->isGreaterThan($minMemoryForMemorySort)) {
            yield from (new MemorySort($context->config->sort->memoryLimit))
                ->sortGenerator($rows, $context, $this->refs);
        } else {
            yield from $this->externalSort($rows, $context);
        }
    }

    /**
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    private function externalSort(\Generator $rows, FlowContext $context) : \Generator
    {
        return (new ExternalSort(
            new FilesystemBucketsCache(
                $context->filesystem(protocol('file')),
                $context->config->serializer(),
                100,
                $context->config->cache->localFilesystemCacheDir->suffix('/flow-php-external-sort/')
            ),
            $context->config->cache->externalSortBucketsCount
        ))->sortGenerator($rows, $context, $this->refs);
    }
}
