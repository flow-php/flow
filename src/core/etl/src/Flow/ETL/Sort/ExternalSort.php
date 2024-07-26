<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\{Cache, Extractor, FlowContext, Row, Row\References, Rows, Sort\ExternalSort\SortRowCache};

/**
 * External sorting is explained here:.
 *
 * https://medium.com/outco/how-to-merge-k-sorted-arrays-c35d87aa298e
 * https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
 *
 * There is still much space for optimization, for example currently heap is created from all parts that for
 * massive datasets with millions of small Rows might become a potential memory leak.
 * Ideally in that case, Rows should be merged in multiple runs with a limited heap.
 */
final class ExternalSort implements SortingAlgorithm
{
    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly Extractor $extractor,
        private readonly SortRowCache $rowCache,
        private int $batchSize
    ) {
    }

    public function sortBy(FlowContext $context, References $refs) : Extractor
    {
        /** @var array<string, \Generator<Row>> $sortedChunks */
        $sortedChunks = [];

        foreach ($this->extractor->extract($context) as $rows) {
            $partId = $this->id . \bin2hex(\random_bytes(16));
            $sortedRows = $rows->sortBy(...$refs);

            $this->rowCache->set($partId, $sortedRows);
            $sortedChunks[$partId] = $this->rowCache->get($partId);
        }

        $cachedParts = new CacheSort($sortedChunks);

        $rows = new Rows();

        foreach ($cachedParts->sort(...$refs->all()) as $row) {
            $rows = $rows->add($row);

            if ($rows->count() >= $this->batchSize) {
                $this->cache->add($this->id, $rows);
                $rows = new Rows();
            }
        }

        if ($rows->count() > 0) {
            $this->cache->add($this->id, $rows);
        }

        foreach ($sortedChunks as $partId => $generator) {
            $this->rowCache->remove($partId);
        }

        return new CacheExtractor($this->id, fallbackExtractor: null, clear: true);
    }
}
