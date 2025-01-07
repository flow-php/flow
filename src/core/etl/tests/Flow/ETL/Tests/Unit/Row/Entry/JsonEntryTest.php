<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{integer_entry, json_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\{JsonEntry};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class JsonEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple integer arrays with the same order' => [
            true,
            json_entry('name', [1, 2, 3]),
            json_entry('name', [1, 2, 3]),
        ];
        yield 'equal names and equal simple integer arrays with different order' => [
            true,
            json_entry('name', [1, 2, 3]),
            json_entry('name', [2, 1, 3]),
        ];
        yield 'equal names and equal simple string arrays with the same order' => [
            true,
            json_entry('name', ['aaa', 'bbb', 'ccc']),
            json_entry('name', ['aaa', 'bbb', 'ccc']),
        ];
        yield 'equal names and equal simple string arrays with the same order but different characters size' => [
            false,
            json_entry('name', ['aaa', 'bbb', 'ccc']),
            json_entry('name', ['aaa', 'BBB', 'ccc']),
        ];
        yield 'equal names and equal multi dimensional array with the same order' => [
            true,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with different order' => [
            true,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            json_entry('name', ['baz', 'bar' => ['bar' => 'bar', 'foo' => 'foo'], 'foo' => 1]),
        ];
        yield 'equal names and equal simple integerrish arrays with the same order' => [
            false,
            json_entry('name', [1, 2, 3]),
            json_entry('name', ['1', '2', '3']),
        ];
        yield 'equal names and equal multi dimensional array with missing entry' => [
            false,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            json_entry('name', ['baz', 'bar' => ['bar' => 'bar'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with different characters size' => [
            false,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'BAR'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object same entries' => [
            true,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object different entries' => [
            false,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-05 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries' => [
            true,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries 1' => [
            true,
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'], 'baz']),
            json_entry('name', ['foo' => 1, 'bar' => ['foo' => integer_entry('test', 1), 'bar' => 'bar'], 'baz']),
        ];
    }

    public function test_empty_entry() : void
    {
        $jsonEntry = json_entry('empty', []);
        $jsonObjectEntry = JsonEntry::object('empty', []);

        self::assertEquals([], $jsonEntry->value());
        self::assertEquals([], $jsonObjectEntry->value());
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (json_entry('0', [1]))->name());
    }

    public function test_invalid_json() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Invalid value given: 'random string', reason: Syntax error");

        json_entry('a', 'random string');
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, JsonEntry $entry, JsonEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $items = [
            ['item-id' => 1, 'name' => 'one', 'address' => ['line1' => "NO. 47 HENGSHAN ROAD, ECONOMIC TECHNOLOGICAL DEVELOPMENT ZONE, WUHU, ANHUI, 241000, CHINA\t\t\t\t\t\t\t\t\t\t \t\t\t\t\t\t\t\t\t\t"]],
            ['item-id' => 2, 'name' => 'two'],
            ['item-id' => 3, 'name' => 'three'],
        ];
        $entry = (json_entry('items', $items))->map(function (array $value) {
            \array_walk_recursive($value, function (&$v) : void {
                if (\is_string($v)) {
                    $v = \trim($v);
                }
            });

            return \json_encode($value, JSON_THROW_ON_ERROR);
        });

        self::assertEquals(
            $items = [
                ['item-id' => 1, 'name' => 'one', 'address' => ['line1' => 'NO. 47 HENGSHAN ROAD, ECONOMIC TECHNOLOGICAL DEVELOPMENT ZONE, WUHU, ANHUI, 241000, CHINA']],
                ['item-id' => 2, 'name' => 'two'],
                ['item-id' => 3, 'name' => 'three'],
            ],
            $entry->value()
        );
    }

    public function test_prevent_from_creating_object_with_integers_as_keys_in_entry() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('All keys for JsonEntry object must be strings');

        JsonEntry::object('entry-name', [1 => 'one', 'id' => 1, 'name' => 'one']);
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        json_entry('', [1, 2, 3]);
    }

    public function test_renames_entry() : void
    {
        $entry = json_entry('entry-name', ['id' => 1, 'name' => 'one']);
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_json_as_value() : void
    {
        $items = [
            ['item-id' => 1, 'name' => 'one'],
            ['item-id' => 2, 'name' => 'two'],
            ['item-id' => 3, 'name' => 'three'],
        ];
        $entry = json_entry('items', $items);

        self::assertEquals(\json_encode($items), $entry->toString());
    }

    public function test_serialization() : void
    {
        $entry = json_entry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']);

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        self::assertTrue($entry->isEqual($unserialized));
    }

    public function test_serialization_of_json_objects() : void
    {
        $entry = JsonEntry::object('entry-name', ['id' => 1, 'name' => 'one']);

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        self::assertTrue($entry->isEqual($unserialized));
    }
}
