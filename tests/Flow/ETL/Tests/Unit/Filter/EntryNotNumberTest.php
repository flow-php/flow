<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNumber;
use PHPUnit\Framework\TestCase;

final class EntryNotNumberTest extends TestCase
{
    public function test_that_float_entry_is_number() : void
    {
        $filter = new EntryNotNumber('test-entry');

        $this->assertFalse($filter->keep(Row::create(Entry::float('test-entry', 1.02))));
    }

    public function test_that_integer_entry_is_number() : void
    {
        $filter = new EntryNotNumber('test-entry');

        $this->assertFalse($filter->keep(Row::create(Entry::integer('test-entry', 1))));
    }

    public function test_that_string_entry_is_not_number() : void
    {
        $filter = new EntryNotNumber('test-entry');

        $this->assertTrue($filter->keep(Row::create(Entry::string_lower('test-entry', 'test-value'))));
    }

    public function test_that_string_number_entry_is_number() : void
    {
        $filter = new EntryNotNumber('test-entry');

        $this->assertFalse($filter->keep(Row::create(Entry::string('test-entry', '1.02'))));
    }
}
