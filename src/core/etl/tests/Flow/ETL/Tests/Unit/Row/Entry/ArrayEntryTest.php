<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\{ArrayEntry, IntegerEntry};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ArrayEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple integer arrays with the same order' => [
            true,
            new ArrayEntry('name', [1, 2, 3]),
            new ArrayEntry('name', [1, 2, 3]),
        ];
        yield 'equal names and equal simple integerrish arrays with the same order' => [
            false,
            new ArrayEntry('name', [1, 2, 3]),
            new ArrayEntry('name', ['1', '2', '3']),
        ];
        yield 'equal names and equal simple integer arrays with different order' => [
            true,
            new ArrayEntry('name', [1, 2, 3]),
            new ArrayEntry('name', [2, 1, 3]),
        ];
        yield 'equal names and equal simple string arrays with the same order' => [
            true,
            new ArrayEntry('name', ['aaa', 'bbb', 'ccc']),
            new ArrayEntry('name', ['aaa', 'bbb', 'ccc']),
        ];
        yield 'equal names and equal simple string arrays with the same order but different characters size' => [
            false,
            new ArrayEntry('name', ['aaa', 'bbb', 'ccc']),
            new ArrayEntry('name', ['aaa', 'BBB', 'ccc']),
        ];
        yield 'equal names and equal multi dimensional array with the same order' => [
            true,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with different order' => [
            true,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['baz', 'bar' => ['bar' => 'bar', 'foo' => 'foo'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with missing entry' => [
            false,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['baz', 'bar' => ['bar' => 'bar'], 'foo' => 1]),
        ];
        yield 'equal names and equal multi dimensional array with different characters size' => [
            false,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => 'foo', 'bar' => 'BAR'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object same entries' => [
            true,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => $date = new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with object different entries' => [
            false,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-01 00:00:00'), 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new \DateTimeImmutable('2020-01-05 00:00:00'), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries' => [
            true,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new \stdClass(), 'bar' => 'bar'], 'baz']),
        ];
        yield 'equal names and equal multi dimensional array with equals different entries 1' => [
            true,
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new IntegerEntry('test', 1), 'bar' => 'bar'], 'baz']),
            new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new IntegerEntry('test', 1), 'bar' => 'bar'], 'baz']),
        ];
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (new ArrayEntry('0', ['id' => 1]))->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, ArrayEntry $entry, ArrayEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new ArrayEntry('entry-name', ['id' => 1, 'name' => 'one']);

        self::assertEquals(
            $entry,
            $entry->map(fn (array $value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new ArrayEntry('', ['id' => 1]);
    }

    public function test_renames_entry() : void
    {
        $entry = new ArrayEntry('entry-name', ['id' => 1, 'name' => 'one']);
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals(['id' => 1, 'name' => 'one'], $newEntry->value());
    }

    public function test_returns_array_as_value() : void
    {
        $items = [
            ['item-id' => 1, 'name' => 'one'],
            ['item-id' => 2, 'name' => 'two'],
            ['item-id' => 3, 'name' => 'three'],
        ];
        $entry = new ArrayEntry('items', $items);

        self::assertEquals($items, $entry->value());
    }

    public function test_serialization() : void
    {
        $string = new ArrayEntry('name', ['foo' => 1, 'bar' => ['foo' => new IntegerEntry('test', 1), 'bar' => 'bar'], 'baz']);

        $serialized = \serialize($string);
        /** @var ArrayEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }
}
