<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\Any;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNumber;
use Flow\ETL\Transformer\Filter\Filter\EntryNumber;
use PHPUnit\Framework\TestCase;

final class AnyTest extends TestCase
{
    public function test_that_all_filters_are_satisfied() : void
    {
        $filter = new Any(new EntryNotNumber('test-entry'), new Callback(fn (Row $row) : bool => true));

        $this->assertTrue($filter->keep(Row::create(Entry::string_lower('test-entry', 'test-value'))));
    }

    public function test_that_none_filter_is_satisfied() : void
    {
        $filter = new Any(new EntryNumber('test-entry'), new Callback(fn (Row $row) : bool => false));

        $this->assertFalse($filter->keep(Row::create(Entry::string_lower('test-entry', 'test-value'))));
    }

    public function test_that_not_all_filters_are_satisfied() : void
    {
        $filter = new Any(new EntryNotNumber('test-entry'), new Callback(fn (Row $row) : bool => false));

        $this->assertTrue($filter->keep(Row::create(Entry::string_lower('test-entry', 'test-value'))));
    }
}
