<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\EntryNumber;
use Flow\ETL\Transformer\Filter\Filter\Opposite;
use PHPUnit\Framework\TestCase;

final class OppositeTest extends TestCase
{
    public function test_that_string_entry_is_not_number() : void
    {
        $filter = new Opposite(new EntryNumber('test-entry'));

        $this->assertTrue($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }

    public function test_that_integer_entry_is_number() : void
    {
        $filter = new Opposite(new EntryNumber('test-entry'));

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\IntegerEntry('test-entry', 1))));
    }

    public function test_that_float_entry_is_number() : void
    {
        $filter = new Opposite(new EntryNumber('test-entry'));

        $this->assertFalse($filter->keep(Row::create(new Row\Entry\FloatEntry('test-entry', 1.02))));
    }
}
