<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Tests\FlowTestCase;

final class BucketShapeTest extends FlowTestCase
{
    public function test_column_names_are_stable(): void
    {
        static::assertSame('_bucket_id', BucketShape::id->value);
        static::assertSame('_bucket_total_rows', BucketShape::totalRows->value);
    }
}
