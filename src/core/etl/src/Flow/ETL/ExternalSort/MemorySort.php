<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Cache;
use Flow\ETL\ExternalSort;
use Flow\ETL\Extractor;
use Flow\ETL\Monitoring\Memory\Configuration;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;

/**
 * This implementation of external sort will try to read from cache until it reaches memory limit.
 * Memory limit must be lower by at least 10% from value in php.ini memory_limit,
 * if provided maximum memory is greater than maximum_memory it will get reduced to 90% of maximum_memory.
 * If memory limit get exceeded, sort will get back to CacheExternalSort algorithm.
 *
 * Technically speaking, reading from cache is redundant but it was easier to implement first version this way.
 * Ideally cache should be avoided as long as possible.
 */
final class MemorySort implements ExternalSort
{
    private readonly Configuration $configuration;

    public function __construct(
        private readonly string $cacheId,
        private readonly Cache $cache,
        private Unit $maximumMemory
    ) {
        $this->configuration = new Configuration(10);

        if ($this->configuration->isLessThan($maximumMemory) && !$this->configuration->isInfinite()) {
            /**
             * @psalm-suppress PossiblyNullReference
             *
             * @phpstan-ignore-next-line
             */
            $this->maximumMemory = $this->configuration->limit()->percentage(90);
        }
    }

    public function sortBy(EntryReference ...$refs) : Extractor
    {
        $memoryConsumption = new Consumption();

        $mergedRows = new Rows();
        $maxSize = 1;

        foreach ($this->cache->read($this->cacheId) as $rows) {
            $maxSize = \max($rows->count(), $maxSize);
            $mergedRows = $mergedRows->merge($rows);

            if ($memoryConsumption->currentDiff()->isGreaterThan($this->maximumMemory)) {
                // Reset already merged rows and fallback to Cache based External Sort
                unset($mergedRows);

                return (new CacheExternalSort($this->cacheId, $this->cache))->sortBy(...$refs);
            }
        }

        $this->cache->clear($this->cacheId);

        return new Extractor\ProcessExtractor(...\iterator_to_array($mergedRows->sortBy(...$refs)->chunks($maxSize)));
    }
}
