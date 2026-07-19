<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketStatsMother;

use function Flow\ETL\DSL\refs;

final class BucketTest extends FlowTestCase
{
    public function test_exposes_identity_bucket_by_columns_and_stats(): void
    {
        $stats = BucketStatsMother::with(rowsCount: 7);
        $bucket = new Bucket('bucket-1', refs('id', 'name'), $stats);

        static::assertSame('bucket-1', $bucket->id);
        static::assertEquals(refs('id', 'name'), $bucket->by);
        static::assertSame($stats, $bucket->stats);
    }
}
