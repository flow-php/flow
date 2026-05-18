<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function serialize;
use function unserialize;

final class StructureEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
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

    public function test_creating_string_structure_from_wrong_value_types(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Expected structure{id: integer, name: string} got different types: list<integer>',
        );

        /**
         * @phpstan-ignore argument.type
         */
        structure_entry('test', [1, 2, 3], type_structure([
            'id' => type_integer(),
            'name' => type_string(),
        ]));
    }

    public function test_definition(): void
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
                'id' => type_integer(),
                'name' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ]),
        );

        static::assertEquals(
            structure_schema('items', type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ])),
            $entry->definition(),
        );
    }

    public function test_duplicating_entry(): void
    {
        $entry = structure_entry('name', ['a1' => 1, 'a2' => 2, 'a3' => 3], type_structure([
            'a1' => type_integer(),
            'a2' => type_integer(),
            'a3' => type_integer(),
        ]));
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    public function test_entry_name_can_be_zero(): void
    {
        static::assertSame('0', structure_entry('0', ['id' => 1, 'name' => 'one'], type_structure([
            'id' => type_integer(),
            'name' => type_string(),
        ]))->name());
    }

    /**
     * @param StructureEntry<array<mixed>> $entry
     * @param StructureEntry<array<mixed>> $nextEntry
     */
    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, StructureEntry $entry, StructureEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map(): void
    {
        $entry = structure_entry('entry-name', ['id' => 1234], type_structure(['id' => type_integer()]));

        static::assertEquals($entry, $entry->map(static fn(?array $entries): ?array => $entries));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        structure_entry('', ['id' => 1, 'name' => 'one'], type_structure([
            'id' => type_integer(),
            'name' => type_string(),
        ]));
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = structure_entry('old_name', ['id' => 1234], type_structure(['id' => type_integer()]), $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals(['id' => 1234], $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = structure_entry('entry-name', ['id' => 1234], type_structure(['id' => type_integer()]));
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_returns_array_as_value(): void
    {
        $entry = structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([
            'item-id' => type_integer(),
            'name' => type_string(),
        ]));

        static::assertEquals(
            [
                'item-id' => 1,
                'name' => 'one',
            ],
            $entry->value(),
        );
    }

    public function test_serialization(): void
    {
        $string = structure_entry('name', ['json' => ['5' => 5, '2' => 2, '3' => 3]], type_structure([
            'json' => type_array(),
        ]));

        $serialized = serialize($string);
        /** @var StructureEntry<array<array-key, mixed>> $unserialized */
        $unserialized = unserialize($serialized);

        static::assertTrue($string->isEqual($unserialized));
    }

    public function test_structure_element_names_as_numbers(): void
    {
        static::assertNotEquals(
            structure_entry(
                'name',
                /** @phpstan-ignore-next-line */
                ['1' => 1, '2' => '2'],
                /** @phpstan-ignore-next-line */
                type_structure([
                    '1' => type_integer(),
                    '2' => type_string(),
                ]),
            ),
            structure_entry(
                'name',
                /** @phpstan-ignore-next-line */
                ['1' => 1, '2' => '2', '3' => '3'],
                /** @phpstan-ignore-next-line */
                type_structure([
                    '1' => type_integer(),
                    '2' => type_string(),
                    '3' => type_string(),
                ]),
            ),
        );
        static::assertEquals(
            structure_entry(
                'name',
                /** @phpstan-ignore-next-line */
                ['1' => 1, '2' => 2, '3' => 3],
                /** @phpstan-ignore-next-line */
                type_structure([
                    '1' => type_integer(),
                    '2' => type_integer(),
                    '3' => type_integer(),
                ]),
            ),
            structure_entry(
                'name',
                /** @phpstan-ignore-next-line */
                ['1' => 1, '2' => 2, '3' => 3],
                /** @phpstan-ignore-next-line */
                type_structure([
                    '1' => type_integer(),
                    '2' => type_integer(),
                    '3' => type_integer(),
                ]),
            ),
        );
    }
}
