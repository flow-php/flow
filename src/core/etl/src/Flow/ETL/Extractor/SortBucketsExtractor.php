<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cache\RowCache;
use Flow\ETL\{Extractor, FlowContext, Rows, Sort\ExternalSort\Bucket};

final class SortBucketsExtractor implements Extractor
{
    /**
     * @param array<Bucket> $sortBuckets
     */
    public function __construct(
        private readonly array $sortBuckets,
        private readonly int $batchSize,
        private readonly RowCache $cache
    ) {

    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->sortBuckets as $bucket) {
            $rows = new Rows();

            foreach ($bucket->rows as $row) {
                $rows = $rows->add($row);

                if ($rows->count() >= $this->batchSize) {
                    yield $rows;
                    $rows = new Rows();
                }
            }

            if ($rows->count() > 0) {
                yield $rows;
            }

            $this->cache->remove($bucket->id);
        }
    }
}
