<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\EntryNotEqualsTo;
use PHPUnit\Framework\TestCase;

final class EntryNotEqualsToTest extends TestCase
{
    public function test_float_entry_not_equals() : void
    {
        $filter = new EntryNotEqualsTo('float-entry', 1.0001003);

        $this->assertFalse($filter->keep(Row::create(Entry::float('float-entry', 1.0001003))));
        $this->assertTrue($filter->keep(Row::create(Entry::float('float-entry', 1.0002003))));
    }

    public function test_float_entry_not_equals_to_not_numeric() : void
    {
        $filter = new EntryNotEqualsTo('float-entry', 'not_numeric');

        $this->assertFalse($filter->keep(Row::create(Entry::float('float-entry', 1.0001003))));
    }

    public function test_that_integer_entry_is_not_equals_to_other_integer() : void
    {
        $filter = new EntryNotEqualsTo('integer-entry', 200);

        $this->assertTrue($filter->keep(Row::create(Entry::integer('integer-entry', 404))));
        $this->assertFalse($filter->keep(Row::create(Entry::integer('integer-entry', 200))));
    }

    public function test_that_string_entry_is_not_equals_to_other_string() : void
    {
        $filter = new EntryNotEqualsTo('test-entry', 'test-value');

        $this->assertTrue($filter->keep(Row::create(Entry::string_lower('test-entry', 'not-same-value'))));
        $this->assertFalse($filter->keep(Row::create(Entry::string_lower('test-entry', 'test-value'))));
    }
}
