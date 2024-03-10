<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Join\Comparison\LessThanEqual;
use Flow\ETL\Row;

final class LessThanEqualTest extends TestCase
{
    public function test_failure() : void
    {
        self::assertFalse(
            (new LessThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 2)),
                Row::create(int_entry('id', 1)),
            )
        );
    }

    public function test_success() : void
    {
        self::assertTrue(
            (new LessThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 1)),
                Row::create(int_entry('id', 5)),
            )
        );

        self::assertTrue(
            (new LessThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 1)),
                Row::create(int_entry('id', 1)),
            )
        );
    }
}
