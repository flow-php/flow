<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\All;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNumber;
use Flow\ETL\Transformer\Filter\Filter\EntryNumber;
use PHPUnit\Framework\TestCase;

final class AllTest extends TestCase
{
    public function test_that_all_filters_are_satisfied() : void
    {
        $filter = new All(new EntryNotNumber('test-entry'), new Callback(fn (Row $row) : bool => true));

        $this->assertTrue($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }

    public function test_that_not_all_filters_are_satisfied() : void
    {
        $filter = new All(new EntryNotNumber('test-entry'), new Callback(fn (Row $row) : bool => false));

        $this->assertFalse($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }

    public function test_that_none_filter_is_satisfied() : void
    {
        $filter = new All(new EntryNumber('test-entry'), new Callback(fn (Row $row) : bool => false));

        $this->assertFalse($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }
}
