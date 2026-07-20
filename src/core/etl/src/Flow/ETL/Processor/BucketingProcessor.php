<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\BucketingStrategy;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketStatsCollector;
use Flow\ETL\Bucketing\BucketYield;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Generator;

use function Flow\ETL\DSL\str_entry;

/**
 * @internal
 */
final class BucketingProcessor implements Processor
{
    public const BUCKET_ENTRY = '_bucket_id';

    public function __construct(
        private readonly BucketingStrategy $strategy,
        private readonly Buckets $buckets,
        private readonly BucketYield $yield = BucketYield::rows,
        private readonly int $batchSize = 1000,
    ) {
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $sortedBy = $this->strategy->sortedBy();

        /** @var array<string, BucketStatsCollector> $collectors */
        $collectors = [];

        foreach ($this->strategy->bucketize($rows) as $chunk) {
            if ($sortedBy !== null) {
                $this->buckets->storage()->set($chunk->bucketId, $chunk->rows);
            } else {
                $this->buckets->storage()->append($chunk->bucketId, $chunk->rows);
            }

            ($collectors[$chunk->bucketId] ??= new BucketStatsCollector($this->strategy->by()))->collect(
                $chunk->values,
                $chunk->hashes,
            );
        }

        foreach ($collectors as $id => $collector) {
            $this->buckets->add($collector->bucket($id, $sortedBy));
        }

        if ($this->yield === BucketYield::none) {
            return;
        }

        foreach ($this->buckets->all() as $bucket) {
            foreach ($this->buckets->rows($bucket->id, $this->batchSize) as $batch) {
                yield $batch->map(static fn(Row $r): Row => $r->add(str_entry(self::BUCKET_ENTRY, $bucket->id)));
            }
        }
    }
}
