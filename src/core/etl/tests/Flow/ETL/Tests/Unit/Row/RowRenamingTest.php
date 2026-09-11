<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\RowRenaming;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;

final class RowRenamingTest extends FlowTestCase
{
    public function test_an_empty_rename_map_returns_the_row_untouched(): void
    {
        $row = row(['a' => 1]);

        static::assertSame($row, RowRenaming::of([])->apply($row));
    }

    public function test_it_keeps_positions_and_leaves_unlisted_columns_alone(): void
    {
        static::assertSame(['x', 'b'], RowRenaming::of(['a' => 'x'])->apply(row(['a' => 1, 'b' => 2]))->names());
    }

    public function test_it_maps_values_onto_their_new_names(): void
    {
        static::assertEquals(
            row(['x' => 1, 'y' => 2]),
            RowRenaming::of(['a' => 'x', 'b' => 'y'])->apply(row(['a' => 1, 'b' => 2])),
        );
    }
}
