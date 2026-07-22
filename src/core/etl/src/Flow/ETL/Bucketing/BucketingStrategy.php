<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Rows;
use Generator;

interface BucketingStrategy
{
    /**
     * Consumes the row stream, spills bucket data into the storage and yields each Bucket
     * once it is complete - the strategy alone decides when and how buckets are stored.
     *
     * @param Generator<Rows> $rows
     *
     * @return Generator<Bucket>
     */
    public function bucketize(Generator $rows, BucketsStorage $storage): Generator;
}
