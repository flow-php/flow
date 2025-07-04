<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\TestCase;

final class ReadFlatColumnValuesTest extends TestCase
{
    public function test_flat_column() : void
    {
        $data = new ReadFlatColumnValues(FlatColumn::int32('int32'), repetitionLevels: [0, 0, 0], definitionLevels: [0, 1, 1], values: [2, 3]);

        self::assertSame(3, $data->rowsCount());
        self::assertFalse($data->isEmpty());
    }

    public function test_flat_empty_column() : void
    {
        $data = new ReadFlatColumnValues(FlatColumn::int32('int32'), repetitionLevels: [], definitionLevels: [], values: []);

        self::assertSame(0, $data->rowsCount());
        self::assertTrue($data->isEmpty());
    }

    public function test_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new ReadFlatColumnValues($schema->columnsFlat()[0], repetitionLevels: [0, 1, 0], definitionLevels: [0, 3, 3], values: [2, 3]);

        self::assertSame(2, $data->rowsCount());
    }
}
