<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;

final class RowMergerTest extends FlowTestCase
{
    public function test_collision_between_left_and_right_names_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('Merged entries names must be unique');

        (new RowMerger())->merge(row(['id' => 1, 'name' => 'left']), row(['name' => 'right']));
    }

    public function test_drop_left_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropLeft: ['id']);

        static::assertSame(
            ['amount' => 100, 'id' => 1, 'name' => 'Alice'],
            $merger->merge(row(['id' => 1, 'amount' => 100]), row(['id' => 1, 'name' => 'Alice']))->toArray(),
        );
    }

    public function test_drop_right_skips_duplicated_join_columns(): void
    {
        $merger = new RowMerger('', dropRight: ['id']);

        static::assertSame(
            ['id' => 1, 'amount' => 100, 'name' => 'Alice'],
            $merger->merge(row(['id' => 1, 'amount' => 100]), row(['id' => 1, 'name' => 'Alice']))->toArray(),
        );
    }

    public function test_merge_without_prefix_keeps_right_names(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            (new RowMerger())
                ->merge(row(['id' => 1]), row(['name' => 'Alice']))
                ->toArray(),
        );
    }

    public function test_prefix_collision_with_left_name_throws(): void
    {
        $this->expectException(DuplicatedEntriesException::class);

        (new RowMerger('left_'))->merge(row(['left_name' => 'left']), row(['name' => 'right']));
    }

    public function test_prefix_renames_every_right_entry(): void
    {
        static::assertSame(
            ['id' => 1, 'right_id' => 1, 'right_name' => 'Alice'],
            (new RowMerger('right_'))
                ->merge(row(['id' => 1]), row(['id' => 1, 'name' => 'Alice']))
                ->toArray(),
        );
    }

    public function test_renamed_entries_carry_renamed_names(): void
    {
        $merged = (new RowMerger('r_'))->merge(row(['id' => 1]), row(['name' => 'Alice']));

        static::assertSame(['id', 'r_name'], $merged->names());
        static::assertSame('Alice', $merged->get('r_name'));
    }

    public function test_reused_plan_renames_rows_with_identical_name_sets(): void
    {
        $merger = new RowMerger('r_');

        static::assertSame(['id' => 1, 'r_x' => 'a'], $merger->merge(row(['id' => 1]), row(['x' => 'a']))->toArray());
        static::assertSame(['id' => 2, 'r_x' => 5], $merger->merge(row(['id' => 2]), row(['x' => 5]))->toArray());
    }

    public function test_reused_merger_handles_rows_with_different_entry_sets(): void
    {
        $merger = new RowMerger();

        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            $merger->merge(row(['id' => 1]), row(['name' => 'Alice']))->toArray(),
        );
        static::assertSame(
            ['id' => 2, 'name' => 'Bob', 'age' => 30],
            $merger->merge(row(['id' => 2]), row(['name' => 'Bob', 'age' => 30]))->toArray(),
        );
        static::assertSame(
            ['id' => 3, 'name' => 'Cid'],
            $merger->merge(row(['id' => 3]), row(['name' => 'Cid']))->toArray(),
        );
    }
}
