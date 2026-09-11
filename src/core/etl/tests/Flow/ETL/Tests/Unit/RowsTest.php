<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;
use function serialize;
use function unserialize;

final class RowsTest extends FlowTestCase
{
    public static function rows_diff_left_provider(): Generator
    {
        yield 'one entry identical row' => [
            rows(schema()),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];

        yield 'one entry right different - missing entry' => [
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema()),
        ];

        yield 'one entry left different - missing entry' => [
            rows(schema()),
            rows(schema()),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];

        yield 'one entry right different - different entry' => [
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 2])),
        ];

        yield 'one entry left different - different entry' => [
            rows(schema(int_schema('number')), row(['number' => 2])),
            rows(schema(int_schema('number')), row(['number' => 2])),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];
    }

    public static function rows_diff_right_provider(): Generator
    {
        yield 'one entry identical row' => [
            rows(schema()),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];

        yield 'one entry right different - missing entry' => [
            rows(schema()),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema()),
        ];

        yield 'one entry left different - missing entry' => [
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema()),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];

        yield 'one entry right different - different entry' => [
            rows(schema(int_schema('number')), row(['number' => 2])),
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 2])),
        ];

        yield 'one entry left different - different entry' => [
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 2])),
            rows(schema(int_schema('number')), row(['number' => 1])),
        ];
    }

    public static function unique_rows_provider(): Generator
    {
        yield 'simple identical rows' => [
            rows(schema(int_schema('number')), row(['number' => 1])),
            rows(schema(int_schema('number')), row(['number' => 1]), row(['number' => 1])),
            new NativeComparator(),
        ];
    }

    public function test_add_checks_only_the_added_rows(): void
    {
        // the existing rows are not re-checked, so one that never passed a door survives add()
        $rows = Rows::trusted(schema(int_schema('number')), [row(['number' => 'x'])])->add(row(['number' => 2]));

        static::assertSame([['number' => 'x'], ['number' => 2]], $rows->toArray());
    }

    public function test_add_reports_the_violating_row_at_its_position_in_the_batch(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "number" (row 1): could not convert \'x\' (string) to integer');

        rows(schema(int_schema('number')), row(['number' => 1]))->add(row(['number' => 'x']));
    }

    public function test_adding_multiple_rows(): void
    {
        $one = row(['number' => 1, 'name' => 'one']);
        $two = row(['number' => 2, 'name' => 'two']);
        $schema = schema(int_schema('number'), str_schema('name'));

        static::assertEquals(rows($schema, $one, $two), rows($schema)->add($one, $two));
    }

    public function test_match_to_adopts_a_wider_schema_and_pads_the_new_nullable_column(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => null]],
            rows(schema(int_schema('id')), row(['id' => 1]))
                ->matchTo(schema(int_schema('id'), str_schema('name', true)))
                ->toArray(),
        );
    }

    public function test_match_to_reports_the_violating_row_at_its_position_in_the_batch(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 1) declared by the schema is missing from the row');

        Rows::trusted(schema(int_schema('id')), [
            row(['id' => 1]),
            row(['other' => 2]),
        ])->matchTo(schema(int_schema('id')));
    }

    public function test_project_drops_the_columns_the_schema_does_not_declare(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'a']),
                row(['id' => 2, 'name' => 'b']),
            )
                ->project(schema(int_schema('id')))
                ->toArray(),
        );
    }

    public function test_project_rejects_a_schema_that_widens_the_batch(): void
    {
        // project() drops, it never invents - the padded column would leave the batch off its schema
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "name" (row 0) declared by the schema is missing from the row');

        rows(schema(int_schema('id')), row(['id' => 1]))->project(schema(int_schema('id'), str_schema('name')));
    }

    public function test_project_rejects_a_schema_that_retypes_a_column(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert 1 (integer) to string');

        rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a']))->project(schema(str_schema(
            'id',
        )));
    }

    public function test_project_does_not_recheck_columns_that_keep_their_definition(): void
    {
        // the rows passed a door under these definitions, and a subset of them needs no second check
        static::assertSame(
            [['id' => 'not-an-integer']],
            Rows::trusted(schema(int_schema('id'), str_schema('name')), [row([
                'id' => 'not-an-integer',
                'name' => 'a',
            ])])
                ->project(schema(int_schema('id')))
                ->toArray(),
        );
    }

    public function test_conformed_pads_the_shape_without_revalidating_values(): void
    {
        static::assertSame(
            [['id' => 'not-an-integer', 'name' => null]],
            Rows::conformed(schema(int_schema('id'), str_schema('name', true)), [row([
                'id' => 'not-an-integer',
            ])])->toArray(),
        );
    }

    public function test_conformed_reports_the_violating_row_at_its_position_in_the_batch(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 1)');

        Rows::conformed(schema(int_schema('id')), [row(['id' => 1]), row(['id' => null])]);
    }

    public function test_construct_reorders_row_storage_into_schema_order(): void
    {
        static::assertSame(
            ['c', 'a', 'b'],
            rows(schema(int_schema('c'), int_schema('a'), int_schema('b')), row(['a' => 1, 'b' => 2, 'c' => 3]))
                ->first()
                ->names(),
        );
    }

    public function test_construct_rejects_a_row_that_contradicts_the_schema(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "number" (row 0): could not convert \'x\' (string) to integer');

        rows(schema(int_schema('number')), row(['number' => 'x']));
    }

    public function test_the_epic_repro_is_unconstructible(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "code" (row 1): could not convert 1000 (integer) to string',
        );

        rows(schema(str_schema('code')), row(['code' => 'AB-01']), row(['code' => 1000]));
    }

    public function test_trusted_takes_a_matching_batch_verbatim(): void
    {
        static::assertSame([['number' => 1], ['number' => 2]], Rows::trusted(schema(int_schema('number')), [
            row(['number' => 1]),
            row(['number' => 2]),
        ])->toArray());
    }

    public function test_unserialize_rejects_a_payload_whose_rows_contradict_its_schema(): void
    {
        // a persisted payload is foreign input - cache reads and both serializers land here
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "number" (row 0) declared by the schema is missing from the row');

        rows(schema(int_schema('number')))->__unserialize([
            'schema' => schema(int_schema('number')),
            'rows' => [['other' => 1]],
        ]);
    }

    public function test_array_access_exists(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        static::assertTrue(isset($rows[0]));
        static::assertFalse(isset($rows[3]));
    }

    public function test_array_access_get(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        static::assertSame(1, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(3, $rows[2]->get('id'));
    }

    public function test_array_access_set(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Rows::add(Row $row) : self');
        $rows = rows(schema());
        $rows[0] = row(['id' => 1]);
    }

    public function test_array_access_unset(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to remove rows use Rows::remove(int $offset) : self');
        $rows = rows(schema(int_schema('id')), row(['id' => 1]));
        unset($rows[0]);
    }

    public function test_chunks_with_less(): void
    {
        $rows = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
            row(['id' => 6]),
            row(['id' => 7]),
        );

        $chunk = iterator_to_array($rows->chunks(10));

        static::assertCount(1, $chunk);
        static::assertSame([1, 2, 3, 4, 5, 6, 7], $chunk[0]->reduceToArray('id'));
    }

    public function test_chunks_with_more_than_expected_in_chunk_rows(): void
    {
        $rows = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
            row(['id' => 6]),
            row(['id' => 7]),
            row(['id' => 8]),
            row(['id' => 9]),
            row(['id' => 10]),
        );

        $chunk = iterator_to_array($rows->chunks(5));

        static::assertCount(2, $chunk);
        static::assertSame([1, 2, 3, 4, 5], $chunk[0]->reduceToArray('id'));
        static::assertSame([6, 7, 8, 9, 10], $chunk[1]->reduceToArray('id'));
    }

    public function test_drop(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->drop(1);

        static::assertCount(2, $rows);
        static::assertSame(2, $rows[0]->get('id'));
        static::assertSame(3, $rows[1]->get('id'));
    }

    public function test_drop_all(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->drop(3);

        static::assertCount(0, $rows);
    }

    public function test_drop_more_than_exists(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->drop(4);

        static::assertCount(0, $rows);
    }

    public function test_drop_right(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->dropRight(1);

        static::assertCount(2, $rows);
        static::assertSame(1, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
    }

    public function test_drop_right_all(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->dropRight(3);

        static::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_available(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->dropRight(5);

        static::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_exists(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->dropRight(4);

        static::assertCount(0, $rows);
    }

    public function test_empty_rows(): void
    {
        static::assertTrue(rows(schema())->empty());
        static::assertFalse(rows(schema(int_schema('id')), row(['id' => 1]))->empty());
    }

    public function test_first_on_empty_rows(): void
    {
        $this->expectException(RuntimeException::class);

        rows(schema())->first();
    }

    public function test_hash(): void
    {
        $rows = rows(
            schema(int_schema('id'), bool_schema('bool')),
            row(['id' => 1, 'bool' => false]),
            row(['id' => 2, 'bool' => false]),
            row(['id' => 3, 'bool' => false]),
            row(['id' => 4, 'bool' => false]),
        );

        static::assertSame(
            $rows->hash(),
            rows(
                schema(bool_schema('bool'), int_schema('id')),
                row(['bool' => false, 'id' => 1]),
                row(['bool' => false, 'id' => 2]),
                row(['bool' => false, 'id' => 3]),
                row(['bool' => false, 'id' => 4]),
            )->hash(),
        );
    }

    public function test_hash_empty_rows(): void
    {
        static::assertSame(rows(schema())->hash(), rows(schema())->hash());
    }

    public function test_hash_rows_with_different_columns(): void
    {
        $rows = rows(
            schema(int_schema('id'), bool_schema('bool')),
            row(['id' => 1, 'bool' => false]),
            row(['id' => 3, 'bool' => false]),
            row(['id' => 2, 'bool' => false]),
            row(['id' => 4, 'bool' => false]),
        );

        static::assertNotSame(
            $rows->hash(),
            rows(
                schema(bool_schema('bool')),
                row(['bool' => false]),
                row(['bool' => false]),
                row(['bool' => false]),
                row(['bool' => false]),
            )->hash(),
        );
    }

    public function test_hash_rows_with_different_order(): void
    {
        $rows = rows(
            schema(int_schema('id'), bool_schema('bool')),
            row(['id' => 1, 'bool' => false]),
            row(['id' => 3, 'bool' => false]),
            row(['id' => 2, 'bool' => false]),
            row(['id' => 4, 'bool' => false]),
        );

        static::assertNotSame(
            $rows->hash(),
            rows(
                schema(bool_schema('bool'), int_schema('id')),
                row(['bool' => false, 'id' => 1]),
                row(['bool' => false, 'id' => 2]),
                row(['bool' => false, 'id' => 3]),
                row(['bool' => false, 'id' => 4]),
            )->hash(),
        );
    }

    public function test_head(): void
    {
        $rows = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );

        $head = $rows->head(3);

        static::assertCount(3, $head);
        static::assertSame(1, $head[0]->get('id'));
        static::assertSame(2, $head[1]->get('id'));
        static::assertSame(3, $head[2]->get('id'));
    }

    public function test_head_on_empty_rows(): void
    {
        $head = rows(schema())->head(5);

        static::assertCount(0, $head);
    }

    public function test_head_with_count_larger_than_available(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $head = $rows->head(10);

        static::assertCount(3, $head);
        static::assertSame(1, $head[0]->get('id'));
        static::assertSame(2, $head[1]->get('id'));
        static::assertSame(3, $head[2]->get('id'));
    }

    public function test_head_with_negative_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Count must be greater than or equal to 0');

        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows->head(-1);
    }

    public function test_head_with_zero_count(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $head = $rows->head(0);

        static::assertCount(0, $head);
    }

    public function test_last(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $lastRow = $rows->last();

        static::assertNotNull($lastRow);
        static::assertSame(3, $lastRow->get('id'));
    }

    public function test_last_on_empty_rows(): void
    {
        $lastRow = rows(schema())->last();

        static::assertNull($lastRow);
    }

    public function test_last_on_single_row(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 42]));

        $lastRow = $rows->last();

        static::assertNotNull($lastRow);
        static::assertSame(42, $lastRow->get('id'));
    }

    public function test_merge_accepts_an_empty_side_with_any_schema(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]));

        // an empty batch has nothing to disagree about, so it short-circuits before the schema check
        static::assertEquals($rows, $rows->merge(rows(schema(str_schema('unrelated')))));
        static::assertEquals($rows, rows(schema(str_schema('unrelated')))->merge($rows));
    }

    public function test_merge_throws_when_schemas_disagree(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Cannot merge Rows with different schemas: [id: integer] and [id: integer, name: string]',
        );

        rows(schema(int_schema('id')), row(['id' => 1]))->merge(rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 2, 'name' => 'x']),
        ));
    }

    public function test_merges_collection_together(): void
    {
        $rowsOne = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $rowsTwo = rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4]), row(['id' => 5]));

        $rowsThree = rows(schema(int_schema('id')), row(['id' => 6]), row(['id' => 7]));

        $merged = $rowsOne->merge($rowsTwo)->merge($rowsThree);

        static::assertEquals(
            rows(
                schema(int_schema('id')),
                row(['id' => 1]),
                row(['id' => 2]),
                row(['id' => 3]),
                row(['id' => 4]),
                row(['id' => 5]),
                row(['id' => 6]),
                row(['id' => 7]),
            ),
            $merged,
        );
    }

    public function test_offset_exists_with_non_int_offset(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @mago-ignore analysis:invalid-argument
        rows(schema())->offsetExists('a');
    }

    public function test_offset_get_on_empty_rows(): void
    {
        $this->expectException(InvalidArgumentException::class);

        rows(schema())[5];
    }

    public function test_remove(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->remove(1);

        static::assertCount(2, $rows);
        static::assertSame(1, $rows[0]->get('id'));
        static::assertSame(3, $rows[1]->get('id'));
    }

    public function test_remove_on_empty_rows(): void
    {
        $this->expectException(InvalidArgumentException::class);

        rows(schema())->remove(1);
    }

    public function test_returns_first_row(): void
    {
        $rows = rows(
            schema(int_schema('number'), str_schema('name')),
            $first = row(['number' => 3, 'name' => 'three']),
            row(['number' => 1, 'name' => 'one']),
            row(['number' => 2, 'name' => 'two']),
        );

        static::assertEquals($first, $rows->first());
    }

    public function test_reverse(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->reverse();

        static::assertCount(3, $rows);
        static::assertSame(3, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(1, $rows[2]->get('id'));
    }

    #[DataProvider('rows_diff_left_provider')]
    public function test_rows_diff_left(Rows $expected, Rows $left, Rows $right): void
    {
        static::assertEquals($expected->toArray(), $left->diffLeft($right)->toArray());
    }

    #[DataProvider('rows_diff_right_provider')]
    public function test_rows_diff_right(Rows $expected, Rows $left, Rows $right): void
    {
        static::assertEquals($expected->toArray(), $left->diffRight($right)->toArray());
    }

    public function test_rows_diff_right_carries_the_right_sides_schema(): void
    {
        // the surviving rows come from the right side, so the left side's schema cannot describe them
        $left = rows(schema(int_schema('number')), row(['number' => 1]));
        $right = rows(schema(int_schema('number'), str_schema('name')), row(['number' => 2, 'name' => 'two']));

        static::assertTrue($left->diffRight($right)->schema()->isSame($right->schema()));
    }

    public function test_rows_schema(): void
    {
        $rows = rows(
            schema(
                int_schema('id'),
                str_schema('name', nullable: true),
                list_schema('list', type_list(type_integer()), nullable: true),
                list_schema('tags', type_list(type_string()), nullable: true),
            ),
            row(['id' => 1, 'name' => 'foo']),
            row(['id' => 1, 'name' => null, 'list' => [1, 2]]),
            row(['id' => 1, 'name' => 'bar', 'tags' => ['a', 'b']]),
            row(['id' => 1, 'name' => 'baz']),
        );

        static::assertEquals(
            schema(
                int_schema('id'),
                str_schema('name', nullable: true),
                list_schema('list', type_list(type_integer()), nullable: true),
                list_schema('tags', type_list(type_string()), nullable: true),
            ),
            $rows->schema(),
        );
    }

    public function test_rows_reject_a_row_whose_list_contradicts_the_declared_element_type(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "list" (row 1): could not convert array (list<integer>) to list<string>',
        );

        rows(
            schema(list_schema('list', type_list(type_string()))),
            row(['list' => ['one', 'two']]),
            row(['list' => [1, 2]]),
        );
    }

    public function test_rows_serialization(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $serialized = serialize($rows);

        /** @var Rows $unserialized */
        $unserialized = unserialize($serialized);

        static::assertSame($rows->toArray(), $unserialized->toArray());
        static::assertTrue($rows->schema()->isSame($unserialized->schema()));
    }

    #[DataProvider('unique_rows_provider')]
    public function test_rows_unique(
        Rows $expected,
        Rows $notUnique,
        Comparator $comparator = new NativeComparator(),
    ): void {
        static::assertEquals($expected, $notUnique->unique($comparator));
    }

    public function test_sort_rows_by_not_existing_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "c" does not exist');

        $rows = rows(
            schema(int_schema('a'), int_schema('b')),
            row(['a' => 3, 'b' => 2]),
            row(['a' => 1, 'b' => 5]),
            row(['a' => 1, 'b' => 4]),
            row(['a' => 2, 'b' => 7]),
            row(['a' => 3, 'b' => 10]),
            row(['a' => 2, 'b' => 4]),
        );

        $rows->sortBy(ref('c'), ref('b')->desc());
    }

    public function test_sort_rows_by_two_columns(): void
    {
        $rows = rows(
            schema(int_schema('a'), int_schema('b')),
            row(['a' => 3, 'b' => 2]),
            row(['a' => 1, 'b' => 5]),
            row(['a' => 1, 'b' => 4]),
            row(['a' => 2, 'b' => 7]),
            row(['a' => 3, 'b' => 10]),
            row(['a' => 2, 'b' => 4]),
        );

        $ascending = $rows->sortBy(ref('a'), ref('b')->desc());
        $descending = $rows->sortBy(ref('a')->desc(), ref('b'));

        static::assertSame(
            [
                ['a' => 1, 'b' => 5],
                ['a' => 1, 'b' => 4],
                ['a' => 2, 'b' => 7],
                ['a' => 2, 'b' => 4],
                ['a' => 3, 'b' => 10],
                ['a' => 3, 'b' => 2],
            ],
            $ascending->toArray(),
        );
        static::assertSame(
            [
                ['a' => 3, 'b' => 2],
                ['a' => 3, 'b' => 10],
                ['a' => 2, 'b' => 4],
                ['a' => 2, 'b' => 7],
                ['a' => 1, 'b' => 4],
                ['a' => 1, 'b' => 5],
            ],
            $descending->toArray(),
        );
    }

    public function test_sort_rows_without_changing_original_collection(): void
    {
        $rows = rows(
            schema(int_schema('number'), str_schema('name')),
            $three = row(['number' => 3, 'name' => 'three']),
            $one = row(['number' => 1, 'name' => 'one']),
            $five = row(['number' => 5, 'name' => 'five']),
            $two = row(['number' => 2, 'name' => 'two']),
            $four = row(['number' => 4, 'name' => 'four']),
        );

        $ascending = $rows->sortAscending(ref('number'));
        $descending = $rows->sortDescending(ref('number'));

        static::assertEquals(
            rows(schema(int_schema('number'), str_schema('name')), $one, $two, $three, $four, $five),
            $ascending,
        );
        static::assertEquals(
            rows(schema(int_schema('number'), str_schema('name')), $five, $four, $three, $two, $one),
            $descending,
        );
        static::assertNotEquals($ascending, $rows);
        static::assertNotEquals($descending, $rows);
    }

    public function test_tail(): void
    {
        $rows = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );

        $tail = $rows->tail(3);

        static::assertCount(3, $tail);
        static::assertSame(3, $tail[0]->get('id'));
        static::assertSame(4, $tail[1]->get('id'));
        static::assertSame(5, $tail[2]->get('id'));
    }

    public function test_tail_maintains_correct_order(): void
    {
        $rows = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );

        $tail = $rows->tail(2);

        static::assertCount(2, $tail);
        static::assertSame(4, $tail[0]->get('id'));
        static::assertSame(5, $tail[1]->get('id'));
    }

    public function test_tail_on_empty_rows(): void
    {
        $tail = rows(schema())->tail(5);

        static::assertCount(0, $tail);
    }

    public function test_tail_with_count_larger_than_available(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $tail = $rows->tail(10);

        static::assertCount(3, $tail);
        static::assertSame(1, $tail[0]->get('id'));
        static::assertSame(2, $tail[1]->get('id'));
        static::assertSame(3, $tail[2]->get('id'));
    }

    public function test_tail_with_negative_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Count must be greater than or equal to 0');

        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows->tail(-1);
    }

    public function test_tail_with_zero_count(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $tail = $rows->tail(0);

        static::assertCount(0, $tail);
    }

    public function test_take(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->take(1);

        static::assertCount(1, $rows);
        static::assertSame(1, $rows[0]->get('id'));
    }

    public function test_take_all(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->take(3);

        static::assertCount(3, $rows);
        static::assertSame(1, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(3, $rows[2]->get('id'));
    }

    public function test_take_more_than_exists(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->take(4);

        static::assertCount(3, $rows);
        static::assertSame(1, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(3, $rows[2]->get('id'));
    }

    public function test_take_right(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->takeRight(1);

        static::assertCount(1, $rows);
        static::assertSame(3, $rows[0]->get('id'));
    }

    public function test_take_right_all(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->takeRight(3);

        static::assertCount(3, $rows);
        static::assertSame(3, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(1, $rows[2]->get('id'));
    }

    public function test_take_right_more_than_exists(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $rows = $rows->takeRight(4);

        static::assertCount(3, $rows);
        static::assertSame(3, $rows[0]->get('id'));
        static::assertSame(2, $rows[1]->get('id'));
        static::assertSame(1, $rows[2]->get('id'));
    }

    public function test_transforms_rows_to_array(): void
    {
        $rows = rows(
            schema(int_schema('id'), bool_schema('deleted'), string_schema('phase', nullable: true)),
            row(['id' => 1234, 'deleted' => false, 'phase' => null]),
            row(['id' => 4321, 'deleted' => true, 'phase' => 'launch']),
        );

        static::assertEquals(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            $rows->toArray(),
        );
    }

    public function test_transforms_rows_to_array_without_keys(): void
    {
        $rows = rows(
            schema(int_schema('id'), bool_schema('deleted'), string_schema('phase', nullable: true)),
            row(['id' => 1234, 'deleted' => false, 'phase' => null]),
            row(['id' => 4321, 'deleted' => true, 'phase' => 'launch']),
        );

        static::assertEquals(
            [
                [1234, false, null],
                [4321, true,  'launch'],
            ],
            $rows->toArray(withKeys: false),
        );
    }

    public function test_structure_field_order_is_a_schema_difference_not_a_value_difference(): void
    {
        $left = rows(
            schema(structure_schema('s', type_structure([
                'a' => type_integer(),
                'b' => type_string(),
            ]))),
            row(['s' => ['a' => 1, 'b' => 'x']]),
        );
        $right = rows(
            schema(structure_schema('s', type_structure([
                'b' => type_string(),
                'a' => type_integer(),
            ]))),
            row(['s' => ['b' => 'x', 'a' => 1]]),
        );

        // Values are compared by content (ArrayComparison), so the two rows are equal ...
        static::assertCount(0, $left->diffLeft($right));
        static::assertCount(0, $left->diffRight($right));

        // ... and the field-order difference lives in the Schema, which 04c made positional, so R6
        // refuses the merge rather than emitting a batch whose declared order no row agrees with.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge Rows with different schemas');

        $left->merge($right);
    }
}
