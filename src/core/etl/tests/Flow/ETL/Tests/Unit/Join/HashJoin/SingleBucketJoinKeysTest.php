<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\SingleBucketJoinKeys;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SingleBucketJoinKeysTest extends FlowTestCase
{
    public function test_every_row_lands_in_the_same_bucket(): void
    {
        $keys = new SingleBucketJoinKeys();

        static::assertSame('', $keys->leftHash(row(int_entry('id', 1))));
        static::assertSame('', $keys->leftHash(row(str_entry('name', 'anything'))));
        static::assertSame('', $keys->rightHash(row(int_entry('id', 42))));
        static::assertSame('', $keys->rightHash(row(str_entry('name', 'other'))));
    }
}
