<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry,
    datetime_entry,
    float_entry,
    generate_random_int,
    int_entry,
    json_entry,
    list_entry,
    map_entry,
    row,
    str_entry,
    string_entry,
    struct_element,
    struct_entry,
    struct_type,
    type_int,
    type_list,
    type_map,
    type_string};
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{ListType, MapType, StructureType};
use Flow\ETL\Row\Entry\{
    BooleanEntry,
    DateTimeEntry,
    IntegerEntry,
    JsonEntry,
    MapEntry,
    StructureEntry};
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RowTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal simple same integer entries' => [
            true,
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3)),
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3)),
        ];
        yield 'same integer entries with different number of entries' => [
            false,
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3)),
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2)),
        ];
        yield 'simple same integer entries with different number of entries reversed' => [
            false,
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2)),
            row(new IntegerEntry('1', 1), new IntegerEntry('2', 2), new IntegerEntry('3', 3)),
        ];
        yield 'simple same array entries' => [
            true,
            row(new JsonEntry('json', ['foo' => ['bar' => 'baz']])),
            row(new JsonEntry('json', ['foo' => ['bar' => 'baz']])),
        ];
        yield 'simple same collection entries' => [
            true,
            row(
                new StructureEntry(
                    'json',
                    ['json' => [1, 2, 3]],
                    new StructureType([new StructureElement('json', new ListType(ListElement::integer()))])
                )
            ),
            row(
                new StructureEntry(
                    'json',
                    ['json' => [1, 2, 3]],
                    new StructureType([new StructureElement('json', new ListType(ListElement::integer()))])
                )
            ),
        ];
        yield 'simple different collection entries' => [
            false,
            row(
                new StructureEntry(
                    'json',
                    ['json' => ['5', '2', '1']],
                    new StructureType([new StructureElement('json', new ListType(ListElement::string()))])
                )
            ),
            row(
                new StructureEntry(
                    'json',
                    ['json' => ['1', '2', '3']],
                    new StructureType([new StructureElement('json', new ListType(ListElement::string()))])
                )
            ),
        ];
    }

    public function test_getting_schema_from_row() : void
    {
        $row = row(
            int_entry('id', generate_random_int(100, 100000)),
            float_entry('price', generate_random_int(100, 100000) / 100),
            bool_entry('deleted', false),
            datetime_entry('created-at', new \DateTimeImmutable('now')),
            str_entry('phase', null),
            json_entry(
                'array',
                [
                    ['id' => 1, 'status' => 'NEW'],
                    ['id' => 2, 'status' => 'PENDING'],
                ]
            ),
            struct_entry(
                'items',
                ['item-id' => 1, 'name' => 'one'],
                struct_type([
                    struct_element('item-id', type_int()),
                    struct_element('name', type_string()),
                ])
            ),
            list_entry('list', [1, 2, 3], type_list(type_int())),
            map_entry(
                'statuses',
                ['NEW', 'PENDING'],
                type_map(type_int(), type_string())
            ),
        );

        self::assertEquals(
            new Schema(
                Definition::integer('id'),
                Definition::float('price'),
                Definition::boolean('deleted'),
                Definition::dateTime('created-at'),
                Definition::string('phase', nullable: true),
                Definition::json('array'),
                Definition::structure(
                    'items',
                    new StructureType([
                        new StructureElement('item-id', type_int()),
                        new StructureElement('name', type_string()),
                    ])
                ),
                Definition::map(
                    'statuses',
                    new MapType(MapKey::integer(), MapValue::string())
                ),
                Definition::list('list', new ListType(ListElement::integer())),
            ),
            $row->schema()
        );
    }

    public function test_hash() : void
    {
        $row = row(
            int_entry('id', 1),
            str_entry('string', 'string'),
            bool_entry('bool', false),
            list_entry('list', [1, 2, 3], type_list(type_int()))
        );

        self::assertSame(
            $row->hash(),
            row(
                int_entry('id', 1),
                bool_entry('bool', false),
                str_entry('string', 'string'),
                list_entry('list', [1, 2, 3], type_list(type_int()))
            )->hash()
        );
    }

    public function test_hash_different_rows() : void
    {
        self::assertNotSame(
            row(list_entry('list', [1, 2, 3], type_list(type_int())))->hash(),
            row(list_entry('list', [3, 2, 1], type_list(type_int())))->hash()
        );
    }

    public function test_hash_empty_row() : void
    {
        self::assertSame(
            row()->hash(),
            row()->hash()
        );
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, \Flow\ETL\Row $row, \Flow\ETL\Row $nextRow) : void
    {
        self::assertSame($equals, $row->isEqual($nextRow));
    }

    public function test_keep() : void
    {
        $row = row(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        );

        self::assertEquals(
            row(
                int_entry('id', 1),
                bool_entry('active', true)
            ),
            $row->keep('id', 'active')
        );
    }

    public function test_keep_non_existing_entry() : void
    {
        $this->expectExceptionMessage('Entry "something" does not exist.');

        $row = row(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        );

        self::assertEquals(
            row(),
            $row->keep('something')
        );
    }

    public function test_merge_row_with_another_row_using_prefix() : void
    {
        self::assertSame(
            [
                'id' => 1,
                '_id' => 2,
            ],
            row(new IntegerEntry('id', 1))
                ->merge(row(new IntegerEntry('id', 2)), $prefix = '_')
                ->toArray()
        );
    }

    public function test_remove() : void
    {
        $row = row(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        );

        self::assertEquals(
            row(
                int_entry('id', 1),
                str_entry('name', 'test')
            ),
            $row->remove('active')
        );
    }

    public function test_remove_non_existing_entry() : void
    {
        $row = row(
            int_entry('id', 1),
            str_entry('name', 'test'),
            bool_entry('active', true)
        );

        self::assertEquals(
            row(
                int_entry('id', 1),
                str_entry('name', 'test'),
                bool_entry('active', true)
            ),
            $row->remove('something')
        );
    }

    public function test_renames_entry() : void
    {
        $row = row(
            string_entry('name', 'just a string'),
            new BooleanEntry('active', true)
        );
        $newRow = $row->rename('name', 'new-name');

        self::assertEquals(
            row(
                new BooleanEntry('active', true),
                string_entry('new-name', 'just a string')
            ),
            $newRow
        );
    }

    public function test_transforms_row_to_array() : void
    {
        $row = row(
            new IntegerEntry('id', 1234),
            new BooleanEntry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            string_entry('phase', null),
            new StructureEntry(
                'items',
                ['item-id' => 1, 'name' => 'one'],
                new StructureType([new StructureElement('id', type_int()), new StructureElement('name', type_string())])
            ),
            new MapEntry(
                'statuses',
                ['NEW', 'PENDING'],
                new MapType(MapKey::integer(), MapValue::string())
            )
        );

        self::assertEquals(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt,
                'phase' => null,
                'items' => [
                    'item-id' => 1,
                    'name' => 'one',
                ],
                'statuses' => ['NEW', 'PENDING'],
            ],
            $row->toArray(),
        );
    }
}
