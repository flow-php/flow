<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\EntryExists;
use PHPUnit\Framework\TestCase;

final class EntryExistsTest extends TestCase
{
    public function test_entry_exists() : void
    {
        $filter = new EntryExists('test-entry');

        $this->assertTrue($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('test-entry', 'test-value'))));
    }

    public function test_entry_not_exists() : void
    {
        $filter = new EntryExists('test-entry');

        $this->assertFalse($filter->keep(Row::create(Row\Entry\StringEntry::lowercase('production-entry', 'test-value'))));
    }
}
