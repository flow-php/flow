<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\Filter\Filter\Opposite;
use PHPUnit\Framework\TestCase;

final class OppositeTest extends TestCase
{
    public function test_that_filter_is_matched() : void
    {
        $filter = new Opposite(new Callback(fn (Row $row) : bool => true));

        $this->assertFalse($filter->keep(Row::create(Entry::float('test-entry', 1.02))));
    }

    public function test_that_filter_is_not_matched() : void
    {
        $filter = new Opposite(new Callback(fn (Row $row) : bool => false));

        $this->assertTrue($filter->keep(Row::create(Entry::integer('test-entry', 1))));
    }
}
