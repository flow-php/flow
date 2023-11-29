<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Join\Comparison\GreaterThanEqual;
use Flow\ETL\Row;

final class GreaterThanEqualTest extends TestCase
{
    public function test_failure() : void
    {
        $this->assertFalse(
            (new GreaterThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 1)),
                Row::create(int_entry('id', 2)),
            )
        );
    }

    public function test_success() : void
    {
        $this->assertTrue(
            (new GreaterThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 5)),
                Row::create(int_entry('id', 1)),
            )
        );

        $this->assertTrue(
            (new GreaterThanEqual('id', 'id'))->compare(
                Row::create(int_entry('id', 1)),
                Row::create(int_entry('id', 1)),
            )
        );
    }
}
