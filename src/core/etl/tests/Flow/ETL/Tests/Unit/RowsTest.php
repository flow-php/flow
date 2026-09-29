<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Closure;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\Php\ValueColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;
use function serialize;
use function sprintf;
use function unserialize;

final class RowsTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Closure(Rows): Rows}>
     */
    public static function negative_sizes(): Generator
    {
        yield 'drop' => [static fn(Rows $rows): Rows => $rows->drop(-1)];
        yield 'dropRight' => [static fn(Rows $rows): Rows => $rows->dropRight(-1)];
        yield 'take' => [static fn(Rows $rows): Rows => $rows->take(-1)];
        yield 'takeRight' => [static fn(Rows $rows): Rows => $rows->takeRight(-1)];
    }

    /**
     * @param Closure(Rows): Rows $call
     */
    #[DataProvider('negative_sizes')]
    public function test_a_negative_size_is_refused(Closure $call): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Size must be greater than or equal to 0');

        $call(array_to_rows([['id' => 1]], schema(int_schema('id'))));
    }

    public static function rows_diff_left_provider(): Generator
    {
        yield 'one entry identical row' => [
            rows(schema()),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];

        yield 'one entry right different - missing entry' => [
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            rows(schema()),
        ];

        yield 'one entry left different - missing entry' => [
            rows(schema()),
            rows(schema()),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];

        yield 'one entry right different - different entry' => [
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
        ];

        yield 'one entry left different - different entry' => [
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];
    }

    public static function rows_diff_right_provider(): Generator
    {
        yield 'one entry identical row' => [
            rows(schema()),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];

        yield 'one entry right different - missing entry' => [
            rows(schema()),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            rows(schema()),
        ];

        yield 'one entry left different - missing entry' => [
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            rows(schema()),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];

        yield 'one entry right different - different entry' => [
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
        ];

        yield 'one entry left different - different entry' => [
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 2]], schema(int_schema('number'))),
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
        ];
    }

    public static function unique_rows_provider(): Generator
    {
        yield 'simple identical rows' => [
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 1], ['number' => 1]], schema(int_schema('number'))),
        ];
        yield 'rows equal in one column only' => [
            array_to_rows(
                [['number' => 1, 'name' => 'a'], ['number' => 1, 'name' => 'b']],
                schema(int_schema('number'), str_schema('name', true)),
            ),
            array_to_rows(
                [['number' => 1, 'name' => 'a'], ['number' => 1, 'name' => 'b'], ['number' => 1, 'name' => 'a']],
                schema(int_schema('number'), str_schema('name', true)),
            ),
        ];
        yield 'null and empty string are distinct' => [
            array_to_rows([['name' => null], ['name' => '']], schema(str_schema('name', true))),
            array_to_rows([['name' => null], ['name' => ''], ['name' => null]], schema(str_schema('name', true))),
        ];
    }

    public function test_match_to_adopts_a_wider_schema_and_pads_the_new_nullable_column(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => null]],
            array_to_rows([['id' => 1]], schema(int_schema('id')))
                ->matchTo(schema(int_schema('id'), str_schema('name', true)))
                ->toArray(),
        );
    }

    public function test_match_to_reports_the_violating_row_at_its_position_in_the_batch(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert 1 (integer) to string');

        array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))->matchTo(schema(str_schema('id')));
    }

    public function test_project_drops_the_columns_the_schema_does_not_declare(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            array_to_rows(
                [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
                schema(int_schema('id'), str_schema('name')),
            )
                ->project(schema(int_schema('id')))
                ->toArray(),
        );
        static::assertSame(
            ['name' => 'a', 'id' => 1],
            array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name')))
                ->project(schema(str_schema('name'), int_schema('id')))
                ->values(0),
        );
    }

    public function test_project_rejects_a_schema_that_widens_the_batch(): void
    {
        // project() drops, it never invents - the padded column would leave the batch off its schema
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "name" (row 0) declared by the schema is missing from the row');

        array_to_rows([['id' => 1]], schema(int_schema('id')))->project(schema(int_schema('id'), str_schema('name')));
    }

    public function test_project_rejects_a_schema_that_retypes_a_column(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert 1 (integer) to string');

        array_to_rows(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name')),
        )->project(schema(str_schema('id')));
    }

    public function test_construct_reorders_row_storage_into_schema_order(): void
    {
        static::assertSame(
            ['c' => 3, 'a' => 1, 'b' => 2],
            array_to_rows(
                [['a' => 1, 'b' => 2, 'c' => 3]],
                schema(int_schema('c'), int_schema('a'), int_schema('b')),
            )->values(0),
        );
    }

    public function test_construct_rejects_a_row_that_contradicts_the_schema(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "number" (row 0): could not convert \'x\' (string) to integer');

        array_to_rows([['number' => 'x']], schema(int_schema('number')));
    }

    public function test_chunks_with_less(): void
    {
        $rows = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
            ['id' => 6],
            ['id' => 7],
        ], schema(int_schema('id')));

        $chunk = iterator_to_array($rows->chunks(10));

        static::assertCount(1, $chunk);
        static::assertSame([1, 2, 3, 4, 5, 6, 7], $chunk[0]->reduceToArray('id'));
    }

    public function test_chunks_with_more_than_expected_in_chunk_rows(): void
    {
        $rows = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
            ['id' => 6],
            ['id' => 7],
            ['id' => 8],
            ['id' => 9],
            ['id' => 10],
        ], schema(int_schema('id')));

        $chunk = iterator_to_array($rows->chunks(5));

        static::assertCount(2, $chunk);
        static::assertSame([1, 2, 3, 4, 5], $chunk[0]->reduceToArray('id'));
        static::assertSame([6, 7, 8, 9, 10], $chunk[1]->reduceToArray('id'));
    }

    public function test_drop(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->drop(1);

        static::assertCount(2, $rows);
        static::assertSame(2, $rows->column('id')->value(0));
        static::assertSame(3, $rows->column('id')->value(1));
    }

    public function test_drop_all(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->drop(3);

        static::assertCount(0, $rows);
    }

    public function test_drop_more_than_exists(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->drop(4);

        static::assertCount(0, $rows);
    }

    public function test_drop_right(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->dropRight(1);

        static::assertCount(2, $rows);
        static::assertSame(1, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
    }

    public function test_drop_right_all(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->dropRight(3);

        static::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_available(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->dropRight(5);

        static::assertCount(0, $rows);
    }

    public function test_drop_right_more_than_exists(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->dropRight(4);

        static::assertCount(0, $rows);
    }

    public function test_empty_rows(): void
    {
        static::assertTrue(rows(schema())->isEmpty());
        static::assertFalse(array_to_rows([['id' => 1]], schema(int_schema('id')))->isEmpty());
    }

    public function test_hash(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'bool' => false],
                ['id' => 2, 'bool' => false],
                ['id' => 3, 'bool' => false],
                ['id' => 4, 'bool' => false],
            ],
            schema(int_schema('id'), bool_schema('bool')),
        );

        static::assertSame(
            $rows->hash(),
            array_to_rows(
                [
                    ['bool' => false, 'id' => 1],
                    ['bool' => false, 'id' => 2],
                    ['bool' => false, 'id' => 3],
                    ['bool' => false, 'id' => 4],
                ],
                schema(bool_schema('bool'), int_schema('id')),
            )->hash(),
        );
    }

    public function test_hash_empty_rows(): void
    {
        static::assertSame(rows(schema())->hash(), rows(schema())->hash());
    }

    public function test_hash_rows_with_different_columns(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'bool' => false],
                ['id' => 3, 'bool' => false],
                ['id' => 2, 'bool' => false],
                ['id' => 4, 'bool' => false],
            ],
            schema(int_schema('id'), bool_schema('bool')),
        );

        static::assertNotSame(
            $rows->hash(),
            array_to_rows([
                ['bool' => false],
                ['bool' => false],
                ['bool' => false],
                ['bool' => false],
            ], schema(bool_schema('bool')))->hash(),
        );
    }

    public function test_hash_rows_with_different_order(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'bool' => false],
                ['id' => 3, 'bool' => false],
                ['id' => 2, 'bool' => false],
                ['id' => 4, 'bool' => false],
            ],
            schema(int_schema('id'), bool_schema('bool')),
        );

        static::assertNotSame(
            $rows->hash(),
            array_to_rows(
                [
                    ['bool' => false, 'id' => 1],
                    ['bool' => false, 'id' => 2],
                    ['bool' => false, 'id' => 3],
                    ['bool' => false, 'id' => 4],
                ],
                schema(bool_schema('bool'), int_schema('id')),
            )->hash(),
        );
    }

    public function test_head(): void
    {
        $rows = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')));

        $head = $rows->head(3);

        static::assertCount(3, $head);
        static::assertSame(1, $head->column('id')->value(0));
        static::assertSame(2, $head->column('id')->value(1));
        static::assertSame(3, $head->column('id')->value(2));
    }

    public function test_head_on_empty_rows(): void
    {
        $head = rows(schema())->head(5);

        static::assertCount(0, $head);
    }

    public function test_head_with_count_larger_than_available(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $head = $rows->head(10);

        static::assertCount(3, $head);
        static::assertSame(1, $head->column('id')->value(0));
        static::assertSame(2, $head->column('id')->value(1));
        static::assertSame(3, $head->column('id')->value(2));
    }

    public function test_head_with_negative_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Count must be greater than or equal to 0');

        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows->head(-1);
    }

    public function test_head_with_zero_count(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $head = $rows->head(0);

        static::assertCount(0, $head);
    }

    public function test_merge_accepts_an_empty_side_with_any_schema(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

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

        array_to_rows([['id' => 1]], schema(int_schema('id')))->merge(array_to_rows(
            [['id' => 2, 'name' => 'x']],
            schema(int_schema('id'), str_schema('name')),
        ));
    }

    public function test_merges_collection_together(): void
    {
        $rowsOne = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        $rowsTwo = array_to_rows([['id' => 3], ['id' => 4], ['id' => 5]], schema(int_schema('id')));

        $rowsThree = array_to_rows([['id' => 6], ['id' => 7]], schema(int_schema('id')));

        $merged = $rowsOne->merge($rowsTwo)->merge($rowsThree);

        static::assertEquals(
            array_to_rows([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
            ], schema(int_schema('id'))),
            $merged,
        );
    }

    public function test_remove(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->remove(1);

        static::assertCount(2, $rows);
        static::assertSame(1, $rows->column('id')->value(0));
        static::assertSame(3, $rows->column('id')->value(1));
    }

    public function test_remove_on_empty_rows(): void
    {
        $this->expectException(InvalidArgumentException::class);

        rows(schema())->remove(1);
    }

    public function test_reverse(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->reverse();

        static::assertCount(3, $rows);
        static::assertSame(3, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
        static::assertSame(1, $rows->column('id')->value(2));
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
        $left = array_to_rows([['number' => 1]], schema(int_schema('number')));
        $right = array_to_rows([['number' => 2, 'name' => 'two']], schema(int_schema('number'), str_schema('name')));

        static::assertTrue($left->diffRight($right)->schema()->isSame($right->schema()));
    }

    public function test_rows_schema(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'name' => 'foo'],
                ['id' => 1, 'name' => null, 'list' => [1, 2]],
                ['id' => 1, 'name' => 'bar', 'tags' => ['a', 'b']],
                ['id' => 1, 'name' => 'baz'],
            ],
            schema(
                int_schema('id'),
                str_schema('name', nullable: true),
                list_schema('list', type_list(type_integer()), nullable: true),
                list_schema('tags', type_list(type_string()), nullable: true),
            ),
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

        array_to_rows([
            ['list' => []],
            ['list' => [1, 2]],
        ], schema(list_schema('list', type_list(type_integer()))))->matchTo(schema(list_schema(
            'list',
            type_list(type_string()),
        )));
    }

    public function test_rows_serialization(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $serialized = serialize($rows);

        /** @var Rows $unserialized */
        $unserialized = unserialize($serialized);

        static::assertSame($rows->toArray(), $unserialized->toArray());
        static::assertTrue($rows->schema()->isSame($unserialized->schema()));
    }

    #[DataProvider('unique_rows_provider')]
    public function test_rows_unique(Rows $expected, Rows $notUnique): void
    {
        static::assertSame($expected->toArray(), $notUnique->unique()->toArray());
    }

    public function test_sort_rows_by_not_existing_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "c" does not exist');

        $rows = array_to_rows(
            [
                ['a' => 3, 'b' => 2],
                ['a' => 1, 'b' => 5],
                ['a' => 1, 'b' => 4],
                ['a' => 2, 'b' => 7],
                ['a' => 3, 'b' => 10],
                ['a' => 2, 'b' => 4],
            ],
            schema(int_schema('a'), int_schema('b')),
        );

        $rows->sortBy(ref('c'), ref('b')->desc());
    }

    public function test_sort_rows_by_two_columns(): void
    {
        $rows = array_to_rows(
            [
                ['a' => 3, 'b' => 2],
                ['a' => 1, 'b' => 5],
                ['a' => 1, 'b' => 4],
                ['a' => 2, 'b' => 7],
                ['a' => 3, 'b' => 10],
                ['a' => 2, 'b' => 4],
            ],
            schema(int_schema('a'), int_schema('b')),
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
        $rows = array_to_rows(
            [
                ['number' => 3, 'name' => 'three'],
                ['number' => 1, 'name' => 'one'],
                ['number' => 5, 'name' => 'five'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 4, 'name' => 'four'],
            ],
            schema(int_schema('number'), str_schema('name')),
        );

        $ascending = $rows->sortAscending(ref('number'));
        $descending = $rows->sortDescending(ref('number'));

        static::assertSame([1, 2, 3, 4, 5], $ascending->column('number')->values());
        static::assertSame([5, 4, 3, 2, 1], $descending->column('number')->values());
        static::assertSame([3, 1, 5, 2, 4], $rows->column('number')->values());
    }

    public function test_tail(): void
    {
        $rows = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')));

        $tail = $rows->tail(3);

        static::assertCount(3, $tail);
        static::assertSame(3, $tail->column('id')->value(0));
        static::assertSame(4, $tail->column('id')->value(1));
        static::assertSame(5, $tail->column('id')->value(2));
    }

    public function test_tail_maintains_correct_order(): void
    {
        $rows = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')));

        $tail = $rows->tail(2);

        static::assertCount(2, $tail);
        static::assertSame(4, $tail->column('id')->value(0));
        static::assertSame(5, $tail->column('id')->value(1));
    }

    public function test_tail_on_empty_rows(): void
    {
        $tail = rows(schema())->tail(5);

        static::assertCount(0, $tail);
    }

    public function test_tail_with_count_larger_than_available(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $tail = $rows->tail(10);

        static::assertCount(3, $tail);
        static::assertSame(1, $tail->column('id')->value(0));
        static::assertSame(2, $tail->column('id')->value(1));
        static::assertSame(3, $tail->column('id')->value(2));
    }

    public function test_tail_with_negative_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Count must be greater than or equal to 0');

        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows->tail(-1);
    }

    public function test_tail_with_zero_count(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $tail = $rows->tail(0);

        static::assertCount(0, $tail);
    }

    public function test_take(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->take(1);

        static::assertCount(1, $rows);
        static::assertSame(1, $rows->column('id')->value(0));
    }

    public function test_take_all(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->take(3);

        static::assertCount(3, $rows);
        static::assertSame(1, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
        static::assertSame(3, $rows->column('id')->value(2));
    }

    public function test_take_more_than_exists(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->take(4);

        static::assertCount(3, $rows);
        static::assertSame(1, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
        static::assertSame(3, $rows->column('id')->value(2));
    }

    public function test_take_right(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->takeRight(1);

        static::assertCount(1, $rows);
        static::assertSame(3, $rows->column('id')->value(0));
    }

    public function test_take_right_all(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->takeRight(3);

        static::assertCount(3, $rows);
        static::assertSame(3, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
        static::assertSame(1, $rows->column('id')->value(2));
    }

    public function test_take_right_more_than_exists(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $rows = $rows->takeRight(4);

        static::assertCount(3, $rows);
        static::assertSame(3, $rows->column('id')->value(0));
        static::assertSame(2, $rows->column('id')->value(1));
        static::assertSame(1, $rows->column('id')->value(2));
    }

    public function test_transforms_rows_to_array(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema(int_schema('id'), bool_schema('deleted'), string_schema('phase', nullable: true)),
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
        $rows = array_to_rows(
            [
                ['id' => 1234, 'deleted' => false, 'phase' => null],
                ['id' => 4321, 'deleted' => true, 'phase' => 'launch'],
            ],
            schema(int_schema('id'), bool_schema('deleted'), string_schema('phase', nullable: true)),
        );

        static::assertEquals(
            [
                [1234, false, null],
                [4321, true,  'launch'],
            ],
            $rows->toArray(withKeys: false),
        );
    }

    public function test_values_of_a_row_are_keyed_in_schema_order(): void
    {
        $createdAt = new DateTimeImmutable('2020-07-13 15:00');

        static::assertEquals(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt,
                'phase' => null,
                'items' => ['item-id' => 1, 'name' => 'one'],
                'statuses' => [1 => 'NEW'],
            ],
            array_to_rows(
                [
                    [
                        'id' => 1,
                        'deleted' => true,
                        'created-at' => $createdAt,
                        'phase' => 'x',
                        'items' => [
                            'item-id' => 0,
                            'name' => 'zero',
                        ],
                        'statuses' => [],
                    ],
                    [
                        'statuses' => [1 => 'NEW'],
                        'phase' => null,
                        'items' => [
                            'item-id' => 1,
                            'name' => 'one',
                        ],
                        'id' => 1234,
                        'created-at' => $createdAt,
                        'deleted' => false,
                    ],
                ],
                schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('created-at'),
                    str_schema('phase', nullable: true),
                    structure_schema('items', type_structure(['item-id' => type_integer(), 'name' => type_string()])),
                    map_schema('statuses', type_map(type_integer(), type_string())),
                ),
            )->values(1),
        );
    }

    #[TestWith([-1])]
    #[TestWith([1])]
    public function test_values_outside_the_batch_are_refused(int $index): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Row {$index} does not exist in a batch of 1 rows");

        array_to_rows([['id' => 1]], schema(int_schema('id')))->values($index);
    }

    public function test_an_absent_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "missing" does not exist');

        array_to_rows([['id' => 1]], schema(int_schema('id')))->column('missing');
    }

    public function test_structure_field_order_is_a_schema_difference_not_a_value_difference(): void
    {
        $left = array_to_rows([['s' => [
            'a' => 1,
            'b' => 'x',
        ]]], schema(structure_schema('s', type_structure([
            'a' => type_integer(),
            'b' => type_string(),
        ]))));
        $right = array_to_rows([['s' => [
            'b' => 'x',
            'a' => 1,
        ]]], schema(structure_schema('s', type_structure([
            'b' => type_string(),
            'a' => type_integer(),
        ]))));

        // Values are compared by content (ArrayComparison), so the two rows are equal ...
        static::assertCount(0, $left->diffLeft($right));
        static::assertCount(0, $left->diffRight($right));

        // ... and the field-order difference lives in the Schema, which 04c made positional, so R6
        // refuses the merge rather than emitting a batch whose declared order no row agrees with.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge Rows with different schemas');

        $left->merge($right);
    }

    public function test_rows_hop_a_foreign_zone_at_ingest(): void
    {
        static::assertSame(
            '2026-01-01 22:04:05 UTC',
            type_datetime()
                ->assert(
                    array_to_rows([[
                        'at' => new DateTimeImmutable('2026-01-02 03:04:05+05:00'),
                    ]], schema(datetime_schema('at')))
                        ->column('at')
                        ->value(0),
                )
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_rows_hop_nested_datetimes(): void
    {
        static::assertSame(
            '2026-01-02 04:04:05 Europe/Warsaw',
            type_list(type_datetime())
                ->assert(
                    array_to_rows([['l' => [new DateTimeImmutable(
                        '2026-01-02 03:04:05',
                        new DateTimeZone('UTC'),
                    )]]], schema(list_schema('l', type_list(type_datetime('Europe/Warsaw')))))
                        ->column('l')
                        ->value(0),
                )[0]->format('Y-m-d H:i:s e'),
        );
    }

    public function test_rows_hop_nested_optional_datetimes(): void
    {
        static::assertSame(
            '2026-01-02 04:04:05 Europe/Warsaw',
            type_list(type_optional(type_datetime()))
                ->assert(
                    array_to_rows([['l' => [
                        new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('UTC')),
                        null,
                    ]]], schema(list_schema('l', type_list(type_optional(type_datetime('Europe/Warsaw'))))))
                        ->column('l')
                        ->value(0),
                )[0]?->format('Y-m-d H:i:s e'),
        );
    }

    /**
     * @return Generator<string, array{Schema, Rows, string}>
     */
    public static function match_to_refusals(): Generator
    {
        yield 'other column, same count' => [
            schema(int_schema('a'), int_schema('b')),
            array_to_rows([['a' => 1, 'z' => 2]], schema(int_schema('a'), int_schema('z'))),
            'Rows do not match their schema: column "z" (row 0) is not declared by the schema',
        ];
        yield 'null under not-null' => [
            schema(str_schema('a')),
            array_to_rows([['a' => null]], schema(int_schema('a', nullable: true))),
            'column "a" (row 0): could not convert null to string, column is not nullable',
        ];
        yield 'missing not-null column' => [
            schema(int_schema('a'), int_schema('b')),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            'Rows do not match their schema: column "b" (row 0) declared by the schema is missing from the row',
        ];
        yield 'unknown column' => [
            schema(int_schema('a')),
            array_to_rows([['a' => 1, 'b' => 2]], schema(int_schema('a'), int_schema('b'))),
            'Rows do not match their schema: column "b" (row 0) is not declared by the schema',
        ];
        yield 'any column against schema()' => [
            schema(),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            'Rows do not match their schema: column "a" (row 0) is not declared by the schema',
        ];
        yield 'same names, third value wrong' => [
            schema(int_schema('a'), str_schema('b'), str_schema('c')),
            array_to_rows(
                [['a' => 1, 'b' => 'x', 'c' => 3]],
                schema(int_schema('a'), str_schema('b'), int_schema('c')),
            ),
            'Rows do not match their schema: column "c" (row 0): could not convert 3 (integer) to string',
        ];
        yield 'reordered, wrong type' => [
            schema(int_schema('a'), int_schema('b')),
            array_to_rows([['b' => 'x', 'a' => 1]], schema(str_schema('b'), int_schema('a'))),
            'Rows do not match their schema: column "b" (row 0): could not convert \'x\' (string) to integer',
        ];
        yield 'null under not-null datetime' => [
            schema(datetime_schema('at')),
            array_to_rows([['at' => null]], schema(str_schema('at', nullable: true))),
            'column "at" (row 0): could not convert null to datetime, column is not nullable',
        ];
        yield 'string in a datetime column' => [
            schema(datetime_schema('at')),
            array_to_rows([['at' => '2026-01-02 03:04:05']], schema(str_schema('at'))),
            'column "at" (row 0): could not convert \'2026-01-02 03:04:05\' (string) to datetime',
        ];
    }

    #[DataProvider('match_to_refusals')]
    public function test_match_to_refuses(Schema $schema, Rows $rows, string $message): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage($message);

        $rows->matchTo($schema);
    }

    public function test_from_rows_refuses_null_under_not_null_in_row_1(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 1)');

        array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id')));
    }

    public function test_from_rows_reorders_and_hops_into_the_column_zone(): void
    {
        static::assertSame(
            '2026-01-02 04:04:05 Europe/Warsaw',
            type_datetime()
                ->assert(
                    array_to_rows(
                        [['at' => new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('UTC')), 'id' => 1]],
                        schema(int_schema('id'), datetime_schema('at', zone: 'Europe/Warsaw')),
                    )
                        ->column('at')
                        ->value(0),
                )
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_of_takes_columns_verbatim(): void
    {
        $schema = schema(int_schema('number'));
        $column = (new PhpBackend())->constant(int_schema('number'), 1, 2);
        $rows = Rows::fromColumns($schema, ['number' => $column], 2);

        static::assertSame($column, $rows->column('number'));
        static::assertSame([['number' => 1], ['number' => 1]], $rows->toArray());
    }

    public function test_of_checks_names_and_counts_only(): void
    {
        // a column of another type passes the structural tier - values are checked where they enter a column
        $rows = Rows::fromColumns(
            schema(str_schema('id')),
            ['id' => (new PhpBackend())->constant(int_schema('id'), 1, 1)],
            1,
        );

        static::assertSame(1, $rows->count());
    }

    public function test_of_refuses_other_column_names(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows::fromColumns() expects columns [id], got [other]');

        Rows::fromColumns(
            schema(int_schema('id')),
            ['other' => (new PhpBackend())->constant(int_schema('other'), 1, 1)],
            1,
        );
    }

    public function test_of_refuses_a_column_of_another_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "id" holds 2 rows, the batch 1');

        Rows::fromColumns(schema(int_schema('id')), ['id' => (new PhpBackend())->constant(int_schema('id'), 1, 2)], 1);
    }

    public function test_of_refuses_a_null_in_a_not_null_column(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "id" (row 1): could not convert null to integer, column is not nullable',
        );

        Rows::fromColumns(
            schema(int_schema('id')),
            [
                'id' => array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true)))->column(
                    'id',
                ),
            ],
            2,
        );
    }

    public function test_project_shares_kept_columns(): void
    {
        $rows = array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name')));

        static::assertSame($rows->column('id'), $rows->project(schema(int_schema('id')))->column('id'));
    }

    public function test_concat_of_a_constant_and_a_scalar_column_yields_default_backend_columns(): void
    {
        $schema = schema(int_schema('id'));
        $constant = Rows::fromColumns($schema, ['id' => (new PhpBackend())->constant(int_schema('id'), 7, 2)], 2);
        $concatenated = $constant->concat(array_to_rows([['id' => 1]], $schema));

        static::assertSame([['id' => 7], ['id' => 7], ['id' => 1]], $concatenated->toArray());
        static::assertInstanceOf(
            (new DefaultBackend())
                ->builder(int_schema('id'))
                ->finish()::class,
            $concatenated->column('id'),
        );
    }

    public function test_concat_refuses_a_zero_row_input_with_another_schema(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge Rows with different schemas');

        array_to_rows([['id' => 1]], schema(int_schema('id')))->concat(rows(schema(str_schema('id'))));
    }

    public function test_concat_of_empty_inputs_is_an_empty_batch(): void
    {
        $schema = schema(int_schema('id'));

        static::assertSame(0, rows($schema)->concat(rows($schema))->count());
    }

    public function test_with_columns_beside_a_constant_column(): void
    {
        $schema = schema(int_schema('id'), str_schema('tag'), int_schema('n'));
        $rows = Rows::fromColumns(
            schema(int_schema('id'), str_schema('tag')),
            [
                'id' => (new PhpBackend())->constant(int_schema('id'), 1, 2),
                'tag' => (new PhpBackend())->constant(str_schema('tag'), 'a', 2),
            ],
            2,
        );
        $builder = (new PhpBackend())->builder(str_schema('tag'));
        $builder->appendMany(['x', 'y']);
        $n = (new PhpBackend())->builder(int_schema('n'));
        $n->appendMany([5, 6]);

        static::assertSame(
            [['id' => 1, 'tag' => 'x', 'n' => 5], ['id' => 1, 'tag' => 'y', 'n' => 6]],
            $rows->withColumns($schema, ['n' => $n->finish(), 'tag' => $builder->finish()])->toArray(),
        );
    }

    public function test_empty_has_one_empty_column_per_definition(): void
    {
        $rows = Rows::empty(schema(int_schema('id'), str_schema('name', nullable: true)));

        static::assertSame(0, $rows->count());
        static::assertSame(['id', 'name'], array_keys($rows->columns()));
        static::assertSame(0, $rows->column('name')->count());
        static::assertEquals(schema(int_schema('id'), str_schema('name', nullable: true)), $rows->schema());
    }

    public function test_empty_of_an_empty_schema(): void
    {
        static::assertSame([], Rows::empty(schema())->columns());
    }

    public function test_from_columns_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "id": mixed cannot be a batch column');

        Rows::fromColumns(schema(int_schema('id')), ['id' => new ValueColumn([1])], 1);
    }

    public function test_with_columns_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "n": mixed cannot be a batch column');

        array_to_rows([['id' => 1]], schema(int_schema('id')))->withColumns(
            schema(int_schema('id'), int_schema('n')),
            ['n' => new ValueColumn([1])],
        );
    }

    public function test_with_columns_refuses_a_column_of_another_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "n" holds 1 rows, the batch 2');

        array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))->withColumns(
            schema(int_schema('id'), int_schema('n')),
            ['n' => (new PhpBackend())->constant(int_schema('n'), 1, 1)],
        );
    }

    public function test_with_schema_renames_and_retypes_positionally(): void
    {
        $renamed = array_to_rows([['id' => 1]], schema(int_schema('id', nullable: true)))->withSchema(schema(int_schema(
            'key',
        )));

        static::assertSame([['key' => 1]], $renamed->toArray());
    }

    public function test_with_schema_refuses_another_definition_count(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows::withSchema() expects 1 definitions, got 2');

        array_to_rows([['id' => 1]], schema(int_schema('id')))->withSchema(schema(int_schema('a'), int_schema('b')));
    }

    public function test_with_schema_refuses_nulls_under_not_null(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "key": could not convert null to integer, column is not nullable');

        array_to_rows([['id' => null]], schema(int_schema('id', nullable: true)))->withSchema(schema(int_schema(
            'key',
        )));
    }

    public function test_with_schema_refuses_another_column_kind(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'column "id": cannot retype the column to string, integer and string are different column kinds',
        );

        array_to_rows([['id' => 1]], schema(int_schema('id')))->withSchema(schema(str_schema('id')));
    }

    /**
     * @return Generator<string, array{Rows, int, int}>
     */
    public static function slices_out_of_range(): Generator
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        yield 'negative offset' => [$rows, -1, 1];
        yield 'negative length' => [$rows, 0, -1];
        yield 'past the end' => [$rows, 2, 2];
        yield 'past the end of a batch without columns' => [array_to_rows([[], [], []], schema()), 1, 3];
    }

    #[DataProvider('slices_out_of_range')]
    public function test_slice_refuses_out_of_range(Rows $rows, int $offset, int $length): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            'Rows::slice(%d, %d) is outside a batch of %d rows',
            $offset,
            $length,
            $rows->count(),
        ));

        $rows->slice($offset, $length);
    }

    /**
     * @return Generator<string, array{Rows, int}>
     */
    public static function gathers_out_of_range(): Generator
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        yield 'negative' => [$rows, -1];
        yield 'the count' => [$rows, 3];
        yield 'the count of a batch without columns' => [array_to_rows([[], [], []], schema()), 3];
    }

    #[DataProvider('gathers_out_of_range')]
    public function test_gather_refuses_out_of_range(Rows $rows, int $index): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            'Rows::gather() index %d is outside a batch of %d rows',
            $index,
            $rows->count(),
        ));

        $rows->gather([$index]);
    }

    public function test_gather_nothing_is_an_empty_batch(): void
    {
        static::assertSame(0, array_to_rows([['id' => 1]], schema(int_schema('id')))->gather([])->count());
    }

    public function test_a_batch_without_columns_keeps_its_rows(): void
    {
        $rows = array_to_rows([[], [], []], schema());

        static::assertSame(3, $rows->count());
        static::assertSame(2, $rows->slice(1, 2)->count());
        static::assertSame([[], []], $rows->gather([0, 2])->toArray());
    }

    public function test_dropping_nothing_keeps_the_batch(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        static::assertSame($rows, $rows->drop(0));
        static::assertSame($rows, $rows->dropRight(0));
    }

    public function test_an_empty_batch_sorts_to_itself_and_reduces_to_nothing(): void
    {
        $rows = rows(schema(int_schema('id')));

        static::assertSame($rows, $rows->sortAscending('id'));
        static::assertSame($rows, $rows->sortDescending('id'));
        static::assertSame([], $rows->reduceToArray('id'));
    }

    public function test_match_to_rebuilds_a_column_its_type_cannot_restamp(): void
    {
        // the child of a nullable structure carries null under the null parent row, so narrowing ?int to int cannot be a
        // restamp - the matching values are rebuilt under the new type instead
        $matched = array_to_rows([
            ['s' => ['a' => 1]],
            ['s' => null],
        ], schema(structure_schema('s', type_structure(['a' => type_optional(type_integer())]), nullable: true)))->matchTo(schema(structure_schema(
            's',
            type_structure(['a' => type_integer()]),
            nullable: true,
        )));

        static::assertSame([['s' => ['a' => 1]], ['s' => null]], $matched->toArray());
        static::assertSame('structure{a: integer}', $matched->column('s')->type()->toString());
    }

    public function test_match_to_adopts_a_not_null_column_an_empty_batch_lacks(): void
    {
        $matched = rows(schema(int_schema('id')))->matchTo(schema(int_schema('id'), str_schema('name')));

        static::assertSame(0, $matched->count());
        static::assertSame(['id', 'name'], $matched->schema()->references()->names());
    }

    public function test_match_to_refuses_a_midnight_outside_utc_in_a_date_column(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'column "at" (row 0): could not convert 2026-01-01T00:00:00.000000+01:00 (datetime) to date',
        );

        array_to_rows([['at' => new DateTimeImmutable(
            '2026-01-01 00:00:00',
            new DateTimeZone('Europe/Warsaw'),
        )]], schema(datetime_schema('at', zone: 'Europe/Warsaw')))->matchTo(schema(date_schema('at')));
    }
}
