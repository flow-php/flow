<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\RowProjection;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;

final class RowProjectionTest extends FlowTestCase
{
    public function test_an_empty_rename_map_returns_the_row_untouched(): void
    {
        $row = row(['a' => 1]);

        static::assertSame($row, (new RowProjection())->rename($row, []));
    }

    public function test_keep_carries_the_schema_order_into_the_row(): void
    {
        // the caller passes names in Schema order, so a reorder on the Schema is not dropped here
        static::assertSame(
            ['c', 'a', 'b'],
            (new RowProjection())
                ->keep(row(['a' => 1, 'b' => 2, 'c' => 3]), ['c', 'a', 'b'])
                ->names(),
        );
    }

    public function test_keep_drops_columns_outside_the_name_list(): void
    {
        static::assertEquals(row(['a' => 1]), (new RowProjection())->keep(row(['a' => 1, 'b' => 2]), ['a']));
    }

    public function test_keep_ignores_a_name_the_row_does_not_carry(): void
    {
        static::assertEquals(row(['a' => 1]), (new RowProjection())->keep(row(['a' => 1]), ['a', 'missing']));
    }

    public function test_rename_keeps_positions_and_leaves_unlisted_columns_alone(): void
    {
        static::assertSame(
            ['x', 'b'],
            (new RowProjection())
                ->rename(row(['a' => 1, 'b' => 2]), ['a' => 'x'])
                ->names(),
        );
    }

    public function test_rename_maps_values_onto_their_new_names(): void
    {
        static::assertEquals(row(['x' => 1, 'y' => 2]), (new RowProjection())->rename(row(['a' => 1, 'b' => 2]), [
            'a' => 'x',
            'b' => 'y',
        ]));
    }
}
