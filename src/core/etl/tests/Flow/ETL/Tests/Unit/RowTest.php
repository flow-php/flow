<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\boolean_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function number_format;

final class RowTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
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
            row(structure_entry('json', ['json' => [1, 2, 3]], type_structure(['json' => type_list(type_integer())]))),
            row(structure_entry('json', ['json' => [1, 2, 3]], type_structure(['json' => type_list(type_integer())]))),
        ];
        yield 'simple different collection entries' => [
            false,
            row(structure_entry('json', ['json' => ['5', '2', '1']], type_structure([
                'json' => type_list(type_string()),
            ]))),
            row(structure_entry('json', ['json' => ['1', '2', '3']], type_structure([
                'json' => type_list(type_string()),
            ]))),
        ];
    }

    public function test_getting_schema_from_row(): void
    {
        $row = row(
            int_entry('id', generate_random_int(100, 100000)),
            float_entry('price', number_format(generate_random_int(100, 100000) / 100, 2, '.', '')),
            bool_entry('deleted', false),
            datetime_entry('created-at', new DateTimeImmutable('now')),
            str_entry('phase', null),
            json_entry('array', [
                ['id' => 1, 'status' => 'NEW'],
                ['id' => 2, 'status' => 'PENDING'],
            ]),
            struct_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([
                'item-id' => type_integer(),
                'name' => type_string(),
            ])),
            list_entry('list', [1, 2, 3], type_list(type_integer())),
            map_entry('statuses', ['NEW', 'PENDING'], type_map(type_integer(), type_string())),
        );

        static::assertEquals(
            schema(
                integer_schema('id'),
                float_schema('price'),
                bool_schema('deleted'),
                datetime_schema('created-at'),
                string_schema('phase', nullable: true),
                json_schema('array'),
                structure_schema('items', type_structure([
                    'item-id' => type_integer(),
                    'name' => type_string(),
                ])),
                map_schema('statuses', type_map(type_integer(), type_string())),
                list_schema('list', type_list(type_integer())),
            ),
            $row->schema(),
        );
    }

    public function test_hash(): void
    {
        $row = row(
            int_entry('id', 1),
            str_entry('string', 'string'),
            bool_entry('bool', false),
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        static::assertSame(
            $row->hash(),
            row(
                int_entry('id', 1),
                bool_entry('bool', false),
                str_entry('string', 'string'),
                list_entry('list', [1, 2, 3], type_list(type_integer())),
            )->hash(),
        );
    }

    public function test_hash_different_rows(): void
    {
        static::assertNotSame(
            row(list_entry('list', [1, 2, 3], type_list(type_integer())))->hash(),
            row(list_entry('list', [3, 2, 1], type_list(type_integer())))->hash(),
        );
    }

    public function test_hash_empty_row(): void
    {
        static::assertSame(row()->hash(), row()->hash());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, Row $row, Row $nextRow): void
    {
        static::assertSame($equals, $row->isEqual($nextRow));
    }

    public function test_keep(): void
    {
        $row = row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true));

        static::assertEquals(row(int_entry('id', 1), bool_entry('active', true)), $row->keep('id', 'active'));
    }

    public function test_keep_non_existing_entry(): void
    {
        $this->expectExceptionMessage('Entry "something" does not exist.');

        $row = row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true));

        static::assertEquals(row(), $row->keep('something'));
    }

    public function test_merge_row_with_another_row_using_prefix(): void
    {
        static::assertSame(
            [
                'id' => 1,
                '_id' => 2,
            ],
            row(integer_entry('id', 1))->merge(row(integer_entry('id', 2)), '_')->toArray(),
        );
    }

    public function test_remove(): void
    {
        $row = row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true));

        static::assertEquals(row(int_entry('id', 1), str_entry('name', 'test')), $row->remove('active'));
    }

    public function test_remove_non_existing_entry(): void
    {
        $row = row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true));

        static::assertEquals(
            row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true)),
            $row->remove('something'),
        );
    }

    public function test_rename_many_entries(): void
    {
        $row = row(string_entry('a', 'value_a'), string_entry('b', 'value_b'), string_entry('c', 'value_c'));

        $renamed = $row->renameMany(['a' => 'x', 'b' => 'y']);

        static::assertEquals(
            row(string_entry('x', 'value_a'), string_entry('y', 'value_b'), string_entry('c', 'value_c')),
            $renamed,
        );
    }

    public function test_rename_many_entries_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test']);
        $row = row(string_entry('a', 'value_a', $metadata), string_entry('b', 'value_b'));

        $renamed = $row->renameMany(['a' => 'x']);

        static::assertTrue($renamed->get('x')->definition()->metadata()->isEqual($metadata));
    }

    public function test_rename_many_entries_with_empty_array_returns_same_instance(): void
    {
        $row = row(string_entry('name', 'value'));

        $renamed = $row->renameMany([]);

        static::assertSame($row, $renamed);
    }

    public function test_renames_entry(): void
    {
        $row = row(string_entry('name', 'just a string'), boolean_entry('active', true));
        $newRow = $row->rename('name', 'new-name');

        static::assertEquals(row(boolean_entry('active', true), string_entry('new-name', 'just a string')), $newRow);
    }

    public function test_transforms_row_to_array(): void
    {
        $row = row(
            integer_entry('id', 1234),
            boolean_entry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new DateTimeImmutable('2020-07-13 15:00')),
            string_entry('phase', null),
            structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([
                'item-id' => type_integer(),
                'name' => type_string(),
            ])),
            map_entry('statuses', ['NEW', 'PENDING'], type_map(type_integer(), type_string())),
        );

        static::assertEquals(
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
