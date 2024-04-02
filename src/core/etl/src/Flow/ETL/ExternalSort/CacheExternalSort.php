<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Row\Reference;
use Flow\ETL\{Cache, ExternalSort, Extractor, Rows};

/**
 * External sorting is explained here:.
 *
 * https://medium.com/outco/how-to-merge-k-sorted-arrays-c35d87aa298e
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 *
 * There is still much space for optimization, for example currently heap is created from all parts that for
 * massive datasets with millions of small Rows might become a potential memory leak.
 * Ideally in that case, Rows should be merged in multiple runs with a limited heap.
 */
final class CacheExternalSort implements ExternalSort
{
    public function __construct(
        private readonly string $id,
        private readonly Cache $cache
    ) {
    }

    public function sortBy(Reference ...$refs) : Extractor
    {
        /** @var array<string, \Generator<Rows>> $cachedPartsArray */
        $cachedPartsArray = [];
        $maxRowsSize = 1;

        /** @var int $i */
        foreach ($this->cache->read($this->id) as $i => $rows) {
            $maxRowsSize = \max($maxRowsSize, $rows->count());
            $partId = $this->id . '_tmp_' . $i;
            $this->cache->add($partId, $rows->sortBy(...$refs));
            $cachedPartsArray[$partId] = $this->cache->read($partId);
        }

        $this->cache->clear($this->id);

        $cachedParts = new CachedParts($cachedPartsArray);

        $minHeap = $cachedParts->createHeap(...$refs);

        $bufferCache = new BufferCache($this->cache, $maxRowsSize);

        while ($cachedParts->notEmpty() || !$minHeap->isEmpty()) {
            $cachedParts->takeNext($minHeap, $this->id, $bufferCache);
        }

        $bufferCache->close();

        foreach ($cachedParts->cacheIds() as $cacheId) {
            $this->cache->clear($cacheId);
        }

        return new CacheExtractor($this->id, fallbackExtractor: null, clear: true);
    }
}
