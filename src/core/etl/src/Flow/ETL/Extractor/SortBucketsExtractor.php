<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\Bucket;
use Flow\ETL\Sort\ExternalSort\BucketsCache;

/**
 * @internal created and used by ExternalSort algorithm
 */
final readonly class SortBucketsExtractor implements Extractor
{
    /**
     * @param array<Bucket> $sortBuckets
     */
    public function __construct(
        private array $sortBuckets,
        private int $batchSize,
        private BucketsCache $cache,
    ) {}

    /**
     * @return \Generator<int, Rows, mixed, mixed>
     */
    public function extract(FlowContext $context): \Generator
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
