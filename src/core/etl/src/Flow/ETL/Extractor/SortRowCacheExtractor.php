<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Sort\ExternalSort\SortRowCache;
use Flow\ETL\{Extractor, FlowContext, Row, Rows};

final class SortRowCacheExtractor implements Extractor
{
    /**
     * @param array<string, \Generator<Row>> $sortBuckets
     */
    public function __construct(
        private readonly array $sortBuckets,
        private readonly int $batchSize,
        private readonly SortRowCache $cache
    ) {

    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->sortBuckets as $bucketId => $bucket) {
            $rows = new Rows();

            foreach ($bucket as $row) {
                $rows = $rows->add($row);

                if ($rows->count() >= $this->batchSize) {
                    yield $rows;
                    $rows = new Rows();
                }
            }

            if ($rows->count() > 0) {
                yield $rows;
            }

            $this->cache->remove($bucketId);
        }
    }
}
