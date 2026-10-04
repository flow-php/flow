<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowMergerTest extends FlowTestCase
{
    public function test_collision_between_left_and_right_names_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('Merged entries names must be unique');

        (new RowMerger())
            ->merge(
                array_to_rows([['id' => 1, 'name' => 'left']], schema(int_schema('id'), str_schema('name'))),
                array_to_rows([['name' => 'right']], schema(str_schema('name'))),
            )
            ->values(0);
    }

    public function test_drop_left_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropLeft: ['id']);

        static::assertSame(
            ['amount' => 100, 'id' => 1, 'name' => 'Alice'],
            $merger
                ->merge(
                    array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount'))),
                    array_to_rows([['id' => 1, 'name' => 'Alice']], schema(int_schema('id'), str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_drop_right_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropRight: ['id']);

        static::assertSame(
            ['id' => 1, 'amount' => 100, 'name' => 'Alice'],
            $merger
                ->merge(
                    array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount'))),
                    array_to_rows([['id' => 1, 'name' => 'Alice']], schema(int_schema('id'), str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_merge_without_prefix_keeps_right_names(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            (new RowMerger())
                ->merge(
                    array_to_rows([['id' => 1]], schema(int_schema('id'))),
                    array_to_rows([['name' => 'Alice']], schema(str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_prefix_collision_with_left_name_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);

        (new RowMerger('left_'))
            ->merge(
                array_to_rows([['left_name' => 'left']], schema(str_schema('left_name'))),
                array_to_rows([['name' => 'right']], schema(str_schema('name'))),
            )
            ->values(0);
    }

    public function test_prefix_renames_every_right_entry(): void
    {
        static::assertSame(
            ['id' => 1, 'right_id' => 1, 'right_name' => 'Alice'],
            (new RowMerger('right_'))
                ->merge(
                    array_to_rows([['id' => 1]], schema(int_schema('id'))),
                    array_to_rows([['id' => 1, 'name' => 'Alice']], schema(int_schema('id'), str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_renamed_entries_carry_renamed_names(): void
    {
        $merged = (new RowMerger('r_'))
            ->merge(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['name' => 'Alice']], schema(str_schema('name'))),
            )
            ->values(0);

        static::assertSame(['id' => 1, 'r_name' => 'Alice'], $merged);
    }

    public function test_reused_plan_renames_rows_with_identical_name_sets(): void
    {
        $merger = new RowMerger('r_');

        static::assertSame(
            ['id' => 1, 'r_x' => 'a'],
            $merger
                ->merge(
                    array_to_rows([['id' => 1]], schema(int_schema('id'))),
                    array_to_rows([['x' => 'a']], schema(str_schema('x'))),
                )
                ->values(0),
        );
        static::assertSame(
            ['id' => 2, 'r_x' => 5],
            $merger
                ->merge(
                    array_to_rows([['id' => 2]], schema(int_schema('id'))),
                    array_to_rows([['x' => 5]], schema(int_schema('x'))),
                )
                ->values(0),
        );
    }

    public function test_reused_merger_handles_rows_with_different_entry_sets(): void
    {
        $merger = new RowMerger();

        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            $merger
                ->merge(
                    array_to_rows([['id' => 1]], schema(int_schema('id'))),
                    array_to_rows([['name' => 'Alice']], schema(str_schema('name'))),
                )
                ->values(0),
        );
        static::assertSame(
            ['id' => 2, 'name' => 'Bob', 'age' => 30],
            $merger
                ->merge(
                    array_to_rows([['id' => 2]], schema(int_schema('id'))),
                    array_to_rows([['name' => 'Bob', 'age' => 30]], schema(str_schema('name'), int_schema('age'))),
                )
                ->values(0),
        );
        static::assertSame(
            ['id' => 3, 'name' => 'Cid'],
            $merger
                ->merge(
                    array_to_rows([['id' => 3]], schema(int_schema('id'))),
                    array_to_rows([['name' => 'Cid']], schema(str_schema('name'))),
                )
                ->values(0),
        );
    }

    public function test_merges_pair_aligned_batches_with_renamed_definitions(): void
    {
        $merged = (new RowMerger('r_'))->merge(
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            array_to_rows(
                [['id' => 10, 'name' => null], ['id' => 20, 'name' => 'b']],
                schema(int_schema('id'), str_schema('name', nullable: true)),
            ),
        );

        static::assertEquals(
            schema(int_schema('id'), int_schema('r_id'), str_schema('r_name', nullable: true)),
            $merged->schema(),
        );
        static::assertSame(
            [['id' => 1, 'r_id' => 10, 'r_name' => null], ['id' => 2, 'r_id' => 20, 'r_name' => 'b']],
            $merged->toArray(),
        );
    }
}
