<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{struct_element, struct_entry, struct_type, type_array, type_int, type_string};
use function Flow\ETL\DSL\{structure_entry, structure_schema, type_map};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StructureEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple same integer entries' => [
            true,
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], struct_type([struct_element('1', type_int()), struct_element('2', type_int()), struct_element('3', type_int())])),
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], struct_type([struct_element('1', type_int()), struct_element('2', type_int()), struct_element('3', type_int())])),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries' => [
            false,
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], struct_type([struct_element('1', type_int()), struct_element('2', type_string()), struct_element('3', type_string())])),
            structure_entry('name', ['1' => 1, '2' => 2], struct_type([struct_element('1', type_int()), struct_element('2', type_string())])),
        ];
        yield 'equal names and equal simple same integer entries with different number of entries reversed' => [
            false,
            structure_entry('name', ['1' => 1, '2' => 2], struct_type([struct_element('1', type_int()), struct_element('2', type_string())])),
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], struct_type([struct_element('1', type_int()), struct_element('2', type_string()), struct_element('3', type_string())])),
        ];
        yield 'equal names and equal simple same array entries' => [
            true,
            structure_entry('name', ['json' => ['foo' => ['bar' => 'baz']]], struct_type([struct_element('json', type_map(type_string(), type_map(type_string(), type_string())))])),
            structure_entry('name', ['json' => ['foo' => ['bar' => 'baz']]], struct_type([struct_element('json', type_map(type_string(), type_map(type_string(), type_string())))])),
        ];
        yield 'equal names and equal simple same collection entries' => [
            true,
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], struct_type([struct_element('json', type_array())])),
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], struct_type([struct_element('json', type_array())])),
        ];
        yield 'equal names and equal simple different collection entries' => [
            false,
            structure_entry('name', ['json' => ['5' => 5, '2' => 2, '1' => 1]], struct_type([struct_element('json', type_array())])),
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], struct_type([struct_element('json', type_array())])),
        ];
    }

    public function test_creating_string_structure_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected structure{id: integer, name: string} got different types: list<integer>');

        structure_entry('test', [1, 2, 3], struct_type([struct_element('id', type_int()), struct_element('name', type_string())]));
    }

    public function test_definition() : void
    {
        $entry = struct_entry(
            'items',
            [
                'id' => 1,
                'name' => 'one',
                'address' => [
                    'street' => 'foo',
                    'city' => 'bar',
                ],
            ],
            struct_type([
                struct_element('id', type_int()),
                struct_element('name', type_string()),
                struct_element(
                    'address',
                    struct_type([
                        struct_element('street', type_string()),
                        struct_element('city', type_string()),
                    ])
                ),
            ]),
        );

        self::assertEquals(
            structure_schema('items', struct_type([
                struct_element('id', type_int()),
                struct_element('name', type_string()),
                struct_element(
                    'address',
                    struct_type([
                        struct_element('street', type_string()),
                        struct_element('city', type_string()),
                    ])
                ),
            ])),
            $entry->definition()
        );
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame(
            '0',
            (
                structure_entry('0', ['id' => 1, 'name' => 'one'], struct_type([struct_element('id', type_int()), struct_element('name', type_string())]))
            )->name()
        );
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, StructureEntry $entry, StructureEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = structure_entry('entry-name', ['id' => 1234], struct_type([struct_element('id', type_int())]));

        self::assertEquals(
            $entry,
            $entry->map(fn (array $entries) => $entries)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        structure_entry('', ['id' => 1, 'name' => 'one'], struct_type([struct_element('id', type_int()), struct_element('name', type_string())]));
    }

    public function test_renames_entry() : void
    {
        $entry = structure_entry('entry-name', ['id' => 1234], struct_type([struct_element('id', type_int())]));
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_array_as_value() : void
    {
        $entry = structure_entry('items', ['item-id' => 1, 'name' => 'one'], struct_type([struct_element('id', type_int()), struct_element('name', type_string())]));

        self::assertEquals(
            [
                'item-id' => 1,
                'name' => 'one',
            ],
            $entry->value()
        );
    }

    public function test_serialization() : void
    {
        $string = structure_entry('name', ['json' => ['5' => 5, '2' => 2, '3' => 3]], struct_type([struct_element('json', type_array())]));

        $serialized = \serialize($string);
        /** @var StructureEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }
}
