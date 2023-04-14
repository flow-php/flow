<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class CollectionEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple same integer entries' => [
            true,
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries' => [
            false,
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2))),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries reversed' => [
            false,
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2))),
            new CollectionEntry('name', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))),
        ];
        yield 'equal names and equal simple same array entries' => [
            true,
            new CollectionEntry('name', new Entries(new ArrayEntry('json', ['foo' => ['bar' => 'baz']]))),
            new CollectionEntry('name', new Entries(new ArrayEntry('json', ['foo' => ['bar' => 'baz']]))),
        ];
        yield 'equal names and equal simple same collection entries' => [
            true,
            new CollectionEntry('name', new Entries(new CollectionEntry('json', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))))),
            new CollectionEntry('name', new Entries(new CollectionEntry('json', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))))),
        ];
        yield 'equal names and equal simple different collection entries' => [
            false,
            new CollectionEntry('name', new Entries(new CollectionEntry('json', new Entries(new IntegerEntry('5', 5), new IntegerEntry('2', 2), new IntegerEntry('3', 3))))),
            new CollectionEntry('name', new Entries(new CollectionEntry('json', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3))))),
        ];
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new CollectionEntry('0', new Entries(new IntegerEntry('id', 1), new StringEntry('name', 'one'))))->name());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, CollectionEntry $entry, CollectionEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new CollectionEntry('entry-name', new Entries(new IntegerEntry('id', 1234)));

        $this->assertEquals(
            $entry,
            $entry->map(fn (array $entries) => $entries)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new CollectionEntry(
            '',
            new Entries(new IntegerEntry('id', 1), new StringEntry('name', 'one'))
        );
    }

    public function test_renames_entry() : void
    {
        $entry = new CollectionEntry('entry-name', new Entries(new IntegerEntry('id', 1234)));
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_array_as_value() : void
    {
        $entry = new CollectionEntry(
            'items',
            new Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
            new Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
            new Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three'))
        );

        $this->assertEquals(
            [
                new Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
                new Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
                new Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three')),
            ],
            $entry->value()
        );
    }

    public function test_serialization() : void
    {
        $string = new CollectionEntry('name', new Entries(new CollectionEntry('json', new Entries(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3)))));

        $serialized = \serialize($string);
        /** @var CollectionEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
