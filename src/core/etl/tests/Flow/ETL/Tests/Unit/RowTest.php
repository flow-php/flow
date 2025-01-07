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
    struct_entry,
    type_int,
    type_list,
    type_map,
    type_string};
use function Flow\ETL\DSL\{bool_schema, boolean_entry, datetime_schema, float_schema, integer_entry, integer_schema, json_schema, list_schema, map_schema, schema, string_schema, structure_entry, structure_schema, type_integer, type_structure};
use Flow\ETL\Row\Entry\{
    DateTimeEntry
};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class RowTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal simple same integer entries' => [
            true,
            row(integer_entry('1', 1), integer_entry('2', 2), integer_entry('3', 3)),
            row(integer_entry('1', 1), integer_entry('2', 2), integer_entry('3', 3)),
        ];
        yield 'same integer entries with different number of entries' => [
            false,
            row(integer_entry('1', 1), integer_entry('2', 2), integer_entry('3', 3)),
            row(integer_entry('1', 1), integer_entry('2', 2)),
        ];
        yield 'simple same integer entries with different number of entries reversed' => [
            false,
            row(integer_entry('1', 1), integer_entry('2', 2)),
            row(integer_entry('1', 1), integer_entry('2', 2), integer_entry('3', 3)),
        ];
        yield 'simple same array entries' => [
            true,
            row(json_entry('json', ['foo' => ['bar' => 'baz']])),
            row(json_entry('json', ['foo' => ['bar' => 'baz']])),
        ];
        yield 'simple same collection entries' => [
            true,
            row(
                structure_entry('json', ['json' => [1, 2, 3]], type_structure(['json' => type_list(type_integer())]))
            ),
            row(
                structure_entry('json', ['json' => [1, 2, 3]], type_structure(['json' => type_list(type_integer())]))
            ),
        ];
        yield 'simple different collection entries' => [
            false,
            row(
                structure_entry('json', ['json' => ['5', '2', '1']], type_structure(['json' => type_list(type_string())]))
            ),
            row(
                structure_entry('json', ['json' => ['1', '2', '3']], type_structure(['json' => type_list(type_string())]))
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
                type_structure([
                    'item-id' => type_int(),
                    'name' => type_string(),
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
            schema(integer_schema('id'), float_schema('price'), bool_schema('deleted'), datetime_schema('created-at'), string_schema('phase', nullable: true), json_schema('array'), structure_schema('items', type_structure([
                'item-id' => type_int(),
                'name' => type_string(),
            ])), map_schema('statuses', type_map(type_integer(), type_string())), list_schema('list', type_list(type_integer()))),
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
            row(integer_entry('id', 1))
                ->merge(row(integer_entry('id', 2)), $prefix = '_')
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
            boolean_entry('active', true)
        );
        $newRow = $row->rename('name', 'new-name');

        self::assertEquals(
            row(
                boolean_entry('active', true),
                string_entry('new-name', 'just a string')
            ),
            $newRow
        );
    }

    public function test_transforms_row_to_array() : void
    {
        $row = row(
            integer_entry('id', 1234),
            boolean_entry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            string_entry('phase', null),
            structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure(['item-id' => type_int(), 'name' => type_string()])),
            map_entry('statuses', ['NEW', 'PENDING'], type_map(type_integer(), type_string()))
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
