<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Join\Comparison\LessThan;
use Flow\ETL\Row;

final class LessTest extends TestCase
{
    public function test_failure() : void
    {
        $this->assertFalse(
            (new LessThan('id', 'id'))->compare(
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 1)),
            )
        );
        $this->assertFalse(
            (new LessThan('id', 'id'))->compare(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 1)),
            )
        );
    }

    public function test_success() : void
    {
        $this->assertTrue(
            (new LessThan('id', 'id'))->compare(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 5)),
            )
        );
    }
}
