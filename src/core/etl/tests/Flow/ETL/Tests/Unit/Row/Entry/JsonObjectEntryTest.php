<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use DateTimeImmutable;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use stdClass;

use function Flow\ETL\DSL\integer_entry;
use function json_encode;

final class JsonObjectEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
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
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => new DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'],
                'baz' => 2,
            ]),
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => new DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'],
                'baz' => 2,
            ]),
        ];
        yield 'equal names and equal multi dimensional array with object different entries' => [
            false,
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => new DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'],
                'baz' => 2,
            ]),
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => new DateTimeImmutable('2020-01-05 00:00:00'), 'bar' => 'bar'],
                'baz' => 2,
            ]),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries' => [
            true,
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new stdClass(), 'bar' => 'bar'], 'baz' => 2]),
            JsonEntry::object('name', ['foo' => 1, 'bar' => ['foo' => new stdClass(), 'bar' => 'bar'], 'baz' => 2]),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries 1' => [
            true,
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'],
                'baz' => 2,
            ]),
            JsonEntry::object('name', [
                'foo' => 1,
                'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'],
                'baz' => 2,
            ]),
        ];
    }

    /**
     * @param JsonEntry<\Flow\Types\Value\Json|null> $entry
     * @param JsonEntry<\Flow\Types\Value\Json|null> $nextEntry
     */
    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, JsonEntry $entry, JsonEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_renames_entry(): void
    {
        $entry = JsonEntry::object('entry-name', ['id' => 1, 'name' => 'one']);
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_json_as_value(): void
    {
        $item = ['item-id' => 1, 'name' => 'one'];
        $entry = JsonEntry::object('item', $item);

        static::assertEquals(json_encode($item), $entry->toString());
    }
}
