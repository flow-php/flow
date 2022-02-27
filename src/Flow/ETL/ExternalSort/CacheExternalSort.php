<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Cache;
use Flow\ETL\ExternalSort;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Row\Sort;
use Flow\ETL\Rows;

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
    private Cache $cache;

    private string $id;

    public function __construct(string $id, Cache $cache)
    {
        $this->cache = $cache;
        $this->id = $id;
    }

    public function sortBy(Sort ...$entries) : Extractor
    {
        /** @var array<string, \Generator<int, Rows, mixed, void>> $cachedPartsArray */
        $cachedPartsArray = [];
        $maxRowsSize = 0;

        foreach ($this->cache->read($this->id) as $i => $rows) {
            $maxRowsSize = \max($maxRowsSize, $rows->count());
            /** @var Rows $singleRowRows */
            $partId = $this->id . '_tmp_' . $i;

            foreach ($rows->sortBy(...$entries)->chunks(1) as $singleRowRows) {
                $this->cache->add($partId, $singleRowRows);
            }

            $cachedPartsArray[$partId] = $this->cache->read($partId);
        }

        $this->cache->clear($this->id);

        $cachedParts = new CachedParts($cachedPartsArray);

        $minHeap = $cachedParts->createHeap(...$entries);

        $bufferCache = new ExternalSort\BufferCache($this->cache, $maxRowsSize);

        while ($cachedParts->notEmpty() || !$minHeap->isEmpty()) {
            $cachedParts->takeNext($minHeap, $this->id, $bufferCache);
        }

        $bufferCache->close();

        foreach ($cachedParts->cacheIds() as $cacheId) {
            $this->cache->clear($cacheId);
        }

        return new CacheExtractor($this->id, $this->cache, $clear = true);
    }
}
