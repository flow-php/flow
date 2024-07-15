<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\Flattener;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn};
use PHPUnit\Framework\TestCase;

final class FlattenerTest extends TestCase
{
    public function test_flattening_flat_column() : void
    {
        $column = FlatColumn::int32('int32');
        $row = [
            'int32' => 1,
        ];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(
            [
                'int32' => 1,
            ],
            $flattener->flattenColumn($column, $row)
        );
    }

    public function test_flattening_when_column_is_not_present_in_row() : void
    {
        $column = FlatColumn::int32('int32');
        $row = [];

        $flattener = new Flattener(new DisabledValidator());
        self::assertSame(['int32' => null], $flattener->flattenColumn($column, $row));
    }
}
