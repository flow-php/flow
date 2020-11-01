<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use PHPUnit\Framework\TestCase;

final class JsonEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new JsonEntry('', [1, 2, 3]);
    }

    public function test_prevent_from_creating_object_with_integers_as_keys_in_entry() : void
    {
        $this->expectExceptionMessage('All keys for JsonEntry object must be strings');

        JsonEntry::object('entry-name', [1 => 'one', 'id' => 1, 'name' => 'one']);
    }

    public function test_empty_entry() : void
    {
        $jsonEntry = new JsonEntry('empty', []);
        $jsonObjectEntry = JsonEntry::object('empty', []);

        $this->assertEquals('[]', $jsonEntry->value());
        $this->assertEquals('{}', $jsonObjectEntry->value());
    }

    public function test_renames_entry() : void
    {
        $entry = new JsonEntry('entry-name', ['id' => 1, 'name' => 'one']);
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_json_as_value() : void
    {
        $items = [
            ['item-id' => 1, 'name' => 'one'],
            ['item-id' => 2, 'name' => 'two'],
            ['item-id' => 3, 'name' => 'three'],
        ];
        $entry = new JsonEntry('items', $items);

        $this->assertEquals(\json_encode($items), $entry->value());
    }

    public function test_map() : void
    {
        $items = [
            ['item-id' => 1, 'name' => 'one', 'address' => ['line1' => "NO. 47 HENGSHAN ROAD, ECONOMIC TECHNOLOGICAL DEVELOPMENT ZONE, WUHU, ANHUI, 241000, CHINA\t\t\t\t\t\t\t\t\t\t \t\t\t\t\t\t\t\t\t\t"]],
            ['item-id' => 2, 'name' => 'two'],
            ['item-id' => 3, 'name' => 'three'],
        ];
        $entry = (new JsonEntry('items', $items))->map(function (array $value) {
            $trimValue = $value;

            \array_walk_recursive($trimValue, function (&$v) : void {
                if (\is_string($v)) {
                    $v = \trim($v);
                }
            });

            return $trimValue;
        });

        $this->assertEquals(
            \json_encode(
                $items = [
                    ['item-id' => 1, 'name' => 'one', 'address' => ['line1' => 'NO. 47 HENGSHAN ROAD, ECONOMIC TECHNOLOGICAL DEVELOPMENT ZONE, WUHU, ANHUI, 241000, CHINA']],
                    ['item-id' => 2, 'name' => 'two'],
                    ['item-id' => 3, 'name' => 'three'],
                ]
            ),
            $entry->value()
        );
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, JsonEntry $entry, JsonEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple integer arrays with the same order' => [
            true,
            new JsonEntry('name', [1, 2, 3]),
            new JsonEntry('name', [1, 2, 3]),
        ];
        yield 'equal names and equal simple integerrish arrays with the same order' => [
            true,
            new JsonEntry('name', [1, 2, 3]),
            new JsonEntry('name', ['1', '2', '3']),
        ];
        yield 'equal names and equal simple integer arrays with different order' => [
            true,
            new JsonEntry('name', [1, 2, 3]),
            new JsonEntry('name', [2, 1, 3]),
        ];
        yield 'equal names and equal simple string arrays with the same order' => [
            true,
            new JsonEntry('name', ['aaa', 'bbb', 'ccc']),
            new JsonEntry('name', ['aaa', 'bbb', 'ccc']),
        ];
        yield 'equal names and equal simple string arrays with the same order but different characters size' => [
            false,
            new JsonEntry('name', ['aaa', 'bbb', 'ccc']),
            new JsonEntry('name', ['aaa', 'BBB', 'ccc']),
        ];
        yield 'equal names and equal multi dimensional array with the same order' => [
            true,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with different order' => [
            true,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['baz', 'bar' => ['bar' => 'bar', 'foo' => 'foo'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with missing entry' => [
            false,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['baz', 'bar' => ['bar' => 'bar'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with different characters size' => [
            false,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'BAR'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object same entries' => [
            true,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object different entries' => [
            false,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-05 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries' => [
            true,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries 1' => [
            true,
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new IntegerEntry('test', 1), 'bar' => 'bar'], 'baz']),
            new JsonEntry('name', ['foo' => 1, 'bar' => ['foo' => new IntegerEntry('test', 1), 'bar' => 'bar'], 'baz']),
        ];
    }
}
