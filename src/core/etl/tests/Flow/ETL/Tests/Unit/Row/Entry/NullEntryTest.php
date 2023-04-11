<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\NullEntry;
use PHPUnit\Framework\TestCase;

final class NullEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new NullEntry('name'), new NullEntry('name')];
        yield 'different names characters and equal values' => [false, new NullEntry('NAME'), new NullEntry('name')];
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new NullEntry('0'))->name());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, NullEntry $entry, NullEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new NullEntry('');
    }

    public function test_renames_entry() : void
    {
        $entry = new NullEntry('entry-name');
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertNull($newEntry->value());
    }

    public function test_serialization() : void
    {
        $string = new NullEntry('name');

        $serialized = \serialize($string);
        /** @var NullEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
