<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{struct_entry, type_array, type_int, type_string};
use function Flow\ETL\DSL\{structure_entry, structure_schema, type_map, type_structure};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StructureEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and equal simple same array entries' => [
            true,
            structure_entry('name', ['json' => ['foo' => ['bar' => 'baz']]], type_structure([
                'json' => type_map(type_string(), type_map(type_string(), type_string())),
            ])),
            structure_entry('name', ['json' => ['foo' => ['bar' => 'baz']]], type_structure([
                'json' => type_map(type_string(), type_map(type_string(), type_string())),
            ])),
        ];
        yield 'equal names and equal simple same collection entries' => [
            true,
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], type_structure([
                'json' => type_array(),
            ])),
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], type_structure([
                'json' => type_array(),
            ])),
        ];
        yield 'equal names and equal simple different collection entries' => [
            false,
            structure_entry('name', ['json' => ['5' => 5, '2' => 2, '1' => 1]], type_structure([
                'json' => type_array(),
            ])),
            structure_entry('name', ['json' => ['1' => 1, '2' => 2, '3' => 3]], type_structure([
                'json' => type_array(),
            ])),
        ];
    }

    public function test_creating_string_structure_from_wrong_value_types() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected structure{id: integer, name: string} got different types: list<integer>');

        structure_entry('test', [1, 2, 3], type_structure([
            'id' => type_int(),
            'name' => type_string(),
        ]));
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
            type_structure([
                'id' => type_int(),
                'name' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ]),
        );

        self::assertEquals(
            structure_schema('items', type_structure([
                'id' => type_int(),
                'name' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ])),
            $entry->definition()
        );
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame(
            '0',
            (
                structure_entry('0', ['id' => 1, 'name' => 'one'], type_structure(['id' => type_int(), 'name' => type_string()]))
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
        $entry = structure_entry('entry-name', ['id' => 1234], type_structure(['id' => type_int()]));

        self::assertEquals(
            $entry,
            /** @phpstan-ignore-next-line */
            $entry->map(fn (array $entries) : array => $entries)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        structure_entry('', ['id' => 1, 'name' => 'one'], type_structure(['id' => type_int(), 'name' => type_string()]));
    }

    public function test_renames_entry() : void
    {
        $entry = structure_entry('entry-name', ['id' => 1234], type_structure(['id' => type_int()]));
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_array_as_value() : void
    {
        $entry = structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure(['item-id' => type_int(), 'name' => type_string()]));

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
        $string = structure_entry('name', ['json' => ['5' => 5, '2' => 2, '3' => 3]], type_structure(['json' => type_array()]));

        $serialized = \serialize($string);
        /** @var StructureEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }

    public function test_structure_element_names_as_numbers() : void
    {
        self::assertNotEquals(
            /** @phpstan-ignore-next-line */
            structure_entry('name', ['1' => 1, '2' => '2'], type_structure([
                '1' => type_int(),
                '2' => type_string(),
            ])),
            /** @phpstan-ignore-next-line */
            structure_entry('name', ['1' => 1, '2' => '2', '3' => '3'], type_structure([
                '1' => type_int(),
                '2' => type_string(),
                '3' => type_string(),
            ])),
        );
        self::assertEquals(
            /** @phpstan-ignore-next-line */
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], type_structure([
                '1' => type_int(),
                '2' => type_int(),
                '3' => type_int(),
            ])),
            /** @phpstan-ignore-next-line */
            structure_entry('name', ['1' => 1, '2' => 2, '3' => 3], type_structure(['1' => type_int(), '2' => type_int(), '3' => type_int()])),
        );
    }
}
