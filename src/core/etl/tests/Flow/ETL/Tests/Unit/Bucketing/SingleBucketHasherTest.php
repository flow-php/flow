<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\SingleBucketHasher;
use Flow\ETL\Tests\FlowTestCase;

final class SingleBucketHasherTest extends FlowTestCase
{
    public function test_empty_input_produces_no_hashes(): void
    {
        static::assertSame([], (new SingleBucketHasher())->hash([]));
    }

    public function test_every_row_gets_the_same_constant_hash(): void
    {
        static::assertSame(['0', '0', '0'], (new SingleBucketHasher())->hash([[1], [2], []]));
    }
}
