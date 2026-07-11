<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\OutOfMemoryException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Sort\MemorySort;
use Flow\ETL\Sort\SortAlgorithms;
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
            try {
                yield from (new MemorySort($context->config->sort->memoryLimit))->sortGenerator(
                    $rows,
                    $context,
                    $this->refs,
                );

                return;
            } catch (OutOfMemoryException $exception) {
                if ($context->config->sort->algorithm !== SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT) {
                    throw $exception;
                }

                $rows->next();

                yield from $this->externalSort(self::resume($exception->collectedRows, $rows), $context);

                return;
            }
        }

        yield from $this->externalSort($rows, $context);
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
            $context->config->cache->externalSortBatchSize,
        ))->sortGenerator($rows, $context, $this->refs);
    }

    /**
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    private static function resume(?Rows $collectedRows, Generator $rows): Generator
    {
        if ($collectedRows !== null && !$collectedRows->empty()) {
            yield $collectedRows;
        }

        yield from $rows;
    }
}
