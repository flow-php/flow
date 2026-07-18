<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class RowMergerTest extends FlowTestCase
{
    public function test_collision_between_left_and_right_names_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('Merged entries names must be unique');

        (new RowMerger())->merge(row(int_entry('id', 1), str_entry('name', 'left')), row(str_entry('name', 'right')));
    }

    public function test_drop_left_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropLeft: ['id']);

        static::assertSame(
            ['amount' => 100, 'id' => 1, 'name' => 'Alice'],
            $merger
                ->merge(
                    row(int_entry('id', 1), int_entry('amount', 100)),
                    row(int_entry('id', 1), str_entry('name', 'Alice')),
                )
                ->toArray(),
        );
    }

    public function test_drop_right_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropRight: ['id']);

        static::assertSame(
            ['id' => 1, 'amount' => 100, 'name' => 'Alice'],
            $merger
                ->merge(
                    row(int_entry('id', 1), int_entry('amount', 100)),
                    row(int_entry('id', 1), str_entry('name', 'Alice')),
                )
                ->toArray(),
        );
    }

    public function test_merge_without_prefix_keeps_right_names(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            (new RowMerger())
                ->merge(row(int_entry('id', 1)), row(str_entry('name', 'Alice')))
                ->toArray(),
        );
    }

    public function test_prefix_collision_with_left_name_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);

        (new RowMerger('left_'))->merge(row(str_entry('left_name', 'left')), row(str_entry('name', 'right')));
    }

    public function test_prefix_renames_every_right_entry(): void
    {
        static::assertSame(
            ['id' => 1, 'right_id' => 1, 'right_name' => 'Alice'],
            (new RowMerger('right_'))
                ->merge(row(int_entry('id', 1)), row(int_entry('id', 1), str_entry('name', 'Alice')))
                ->toArray(),
        );
    }

    public function test_renamed_definition_cache_survives_changing_definitions(): void
    {
        $merger = new RowMerger('r_');

        $first = $merger->merge(row(int_entry('id', 1)), row(str_entry('x', 'a')));
        $second = $merger->merge(row(int_entry('id', 2)), row(int_entry('x', 5)));

        static::assertSame('string', $first->get('r_x')->definition()->type()->toString());
        static::assertSame('integer', $second->get('r_x')->definition()->type()->toString());
    }

    public function test_renamed_entries_carry_renamed_definitions(): void
    {
        $merged = (new RowMerger('r_'))->merge(row(int_entry('id', 1)), row(str_entry('name', 'Alice')));

        static::assertSame('Alice', $merged->get('r_name')->value());
        static::assertSame('r_name', $merged->get('r_name')->name());
        static::assertSame('r_name', $merged->get('r_name')->definition()->entry()->name());
    }

    public function test_reused_merger_handles_rows_with_different_entry_sets(): void
    {
        $merger = new RowMerger();

        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            $merger->merge(row(int_entry('id', 1)), row(str_entry('name', 'Alice')))->toArray(),
        );
        static::assertSame(
            ['id' => 2, 'name' => 'Bob', 'age' => 30],
            $merger->merge(row(int_entry('id', 2)), row(str_entry('name', 'Bob'), int_entry('age', 30)))->toArray(),
        );
        static::assertSame(
            ['id' => 3, 'name' => 'Cid'],
            $merger->merge(row(int_entry('id', 3)), row(str_entry('name', 'Cid')))->toArray(),
        );
    }
}
