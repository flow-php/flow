<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class MapEntryEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [
            true,
            new MapEntry('name', ['a' => 'b']),
            new MapEntry('name', ['a' => 'b']),
        ];

        yield 'different names and values' => [
            false,
            new MapEntry('name', ['a' => 'b']),
            new MapEntry('different_name', ['c' => 'd']),
        ];

        yield 'equal names and different values' => [
            false,
            new MapEntry('name', ['a' => 'b']),
            new MapEntry('name', ['a', 'b']),
        ];

        yield 'different names characters and equal values' => [
            false,
            new MapEntry('NAME', ['a' => 'b']),
            new MapEntry('name', ['c', 'd']),
        ];
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new MapEntry('0', ['a' => 'b']))->name());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, MapEntry $entry, MapEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new MapEntry('entry-name', ['a' => 'b', 'c' => 'd']);

        $this->assertEquals(
            $entry,
            $entry->map(fn ($value) => $value)
        );
    }

    public function test_metadata() : void
    {
        $entry = new MapEntry('name', ['a', 'b']);

        $this->assertSame(['key_type' => 'int', 'value_type' => 'string'], $entry->metadata());

        $entry = new MapEntry('name', [new StringEntry('a', 'a'), new StringEntry('b', 'b')]);

        $this->assertSame(['key_type' => 'int', 'value_type' => 'object<' . StringEntry::class . '>'], $entry->metadata());
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        new MapEntry('', ['a' => 'b']);
    }

    public function test_prevents_from_creating_entry_with_mixed_key_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All keys in map must have the same type.');

        new MapEntry('name', ['a' => 'b', 0 => 'c']);
    }

    public function test_prevents_from_creating_entry_with_mixed_value_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All values in map must have the same type.');

        new MapEntry('name', ['a' => 'b', 'c' => 0]);
    }

    public function test_renames_entry() : void
    {
        $entry = new MapEntry('entry-name', ['a', 'b']);
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals(['a', 'b'], $newEntry->value());
    }
}
