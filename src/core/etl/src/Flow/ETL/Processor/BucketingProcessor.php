<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\BucketingStrategy;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Generator;

/**
 * @internal
 */
final class BucketingProcessor implements Processor
{
    public function __construct(
        private readonly BucketingStrategy $strategy,
        private readonly Buckets $buckets,
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        foreach ($this->strategy->bucketize($rows, $this->buckets->storage()) as $bucket) {
            $this->buckets->add($bucket);

            yield new Rows(Bucket::schema(), $bucket->toRow());
        }
    }
}
