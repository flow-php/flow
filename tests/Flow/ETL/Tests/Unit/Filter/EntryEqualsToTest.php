<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\EntryEqualsTo;
use PHPUnit\Framework\TestCase;

final class EntryEqualsToTest extends TestCase
{
    public function test_that_string_entry_is_equals_to() : void
    {
        $filter = new EntryEqualsTo('test-entry', 'test-value');

        $this->assertTrue($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }

    public function test_that_integer_entry_is_equals_to_other_integer() : void
    {
        $filter = new EntryEqualsTo('integer-entry', 200);

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\IntegerEntry('integer-entry', 404))));
        $this->assertTrue($filter->keep(Row::create(new Row\Entry\IntegerEntry('integer-entry', 200))));
    }

    public function test_float_entry_not_equals() : void
    {
        $filter = new EntryEqualsTo('float-entry', 1.0001003);

        $this->assertTrue($filter->keep(Row::create(new Row\Entry\FloatEntry('float-entry', 1.0001003))));
        $this->assertFalse($filter->keep(Row::create(new Row\Entry\FloatEntry('float-entry', 1.0002003))));
    }

    public function test_that_string_entry_is_not_equals_to() : void
    {
        $filter = new EntryEqualsTo('test-entry', 'test-value');

        $this->assertFalse($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value-random'))));
    }

    public function test_that_entry_is_not_a_string() : void
    {
        $filter = new EntryEqualsTo('test-entry', 'test-value');

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\JsonEntry('test-entry', []))));
    }

    public function test_float_entry_equals_to_not_numeric() : void
    {
        $filter = new EntryEqualsTo('float-entry', 'something');

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\FloatEntry('float-entry', 1.0001003))));
    }

    public function test_float_entry_equals_to_numeric() : void
    {
        $filter = new EntryEqualsTo('float-entry', 10.01);

        $this->assertTrue($filter->keep(Row::create(new Row\Entry\StringEntry('float-entry', '10.01'))));
    }

    public function test_float_entry_equals_to_integer() : void
    {
        $filter = new EntryEqualsTo('float-entry', 10.00);

        $this->assertTrue($filter->keep(Row::create(new Row\Entry\IntegerEntry('float-entry', 10))));
    }

    public function test_float_entry_equals_float() : void
    {
        $filter = new EntryEqualsTo('float-entry', 10.01);

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\StringEntry('float-entry', 'test'))));
    }
}
