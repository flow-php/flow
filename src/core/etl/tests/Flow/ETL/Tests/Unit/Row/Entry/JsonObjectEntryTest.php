<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{integer_entry, json_object_entry};
use Flow\ETL\Row\Entry\{JsonEntry};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class JsonObjectEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal multi dimensional array with the same order' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with different order' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['baz' => 2, 'bar' => ['bar' => 'bar', 'foo' => 'foo'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with missing entry' => [
            false,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['baz' => 2, 'bar' => ['bar' => 'bar'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with different characters size' => [
            false,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'BAR'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with object same entries' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with object different entries' => [
            false,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-05 00:00:00'), 'bar' => 'bar'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries 1' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'], 'baz' => 2]),
        ];
    }

    public function test_duplicating_entry() : void
    {
        $entry = json_object_entry('entry-name', ['id' => 1, 'name' => 'one']);
        $duplicated = $entry->duplicate();

        self::assertNotSame($entry, $duplicated);
        self::assertEquals($entry, $duplicated);
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, JsonEntry $entry, JsonEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $item = ['item-id' => 1, 'name' => 'one'];
        $entry = JsonEntry::object('item', $item);
        $mappedEntry = $entry->map(fn (?array $value) : array => ['item-id' => 1, 'name' => 'ONE']);

        self::assertEquals(JsonEntry::object('item', ['item-id' => 1, 'name' => 'ONE']), $mappedEntry);
    }

    public function test_renames_entry() : void
    {
        $entry = JsonEntry::object('entry-name', ['id' => 1, 'name' => 'one']);
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_json_as_value() : void
    {
        $item = ['item-id' => 1, 'name' => 'one'];
        $entry = JsonEntry::object('item', $item);

        self::assertEquals(\json_encode($item), $entry->toString());
    }
}
