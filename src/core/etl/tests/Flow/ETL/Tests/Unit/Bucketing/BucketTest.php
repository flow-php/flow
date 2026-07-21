<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Tests\FlowTestCase;

final class BucketTest extends FlowTestCase
{
    public function test_exposes_identity_total_rows_and_index(): void
    {
        $bucket = new Bucket('bucket-1', 7, 3);

        static::assertSame('bucket-1', $bucket->id);
        static::assertSame(7, $bucket->totalRows);
        static::assertSame(3, $bucket->index);
    }

    public function test_to_row_carries_id_and_total_rows(): void
    {
        $row = (new Bucket('bucket-1', 4, 0))->toRow();

        static::assertSame('bucket-1', $row->valueOf(BucketShape::id->value));
        static::assertSame(4, $row->valueOf(BucketShape::totalRows->value));
        static::assertCount(2, $row->entries());
    }
}
