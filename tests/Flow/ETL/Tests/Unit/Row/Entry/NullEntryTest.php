<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\NullEntry;
use PHPUnit\Framework\TestCase;

final class NullEntryTest extends TestCase
{
    public function test_renames_entry() : void
    {
        $entry = new NullEntry('entry-name');
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertNull($newEntry->value());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, NullEntry $entry, NullEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new NullEntry('name'), new NullEntry('name')];
        yield 'different names characters and equal values' => [true, new NullEntry('NAME'), new NullEntry('name')];
    }
}
