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

    public function test_skipping_rows_in_flat_column() : void
    {
        $data = new ReadFlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0, 0, 0, 0, 0],
            definitionLevels: [1, 1, 1, 1, 1, 1, 1],
            values: [1, 2, 3, 4, 5, 6, 7]
        );

        $skipped = $data->skipRows(2);

        self::assertSame(5, $skipped->rowsCount());
        self::assertSame([1, 1, 1, 1, 1], $skipped->definitionLevels());
        self::assertSame([0, 0, 0, 0, 0], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new ReadFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 0, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(4, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(2, $skipped->rowsCount());
        self::assertSame([2, 3, 3], $skipped->definitionLevels());
        self::assertSame([0, 0, 1], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list_with_multi_elements() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new ReadFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 1, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(3, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(1, $skipped->rowsCount());
        self::assertSame([3, 3], $skipped->definitionLevels());
        self::assertSame([0, 1], $skipped->repetitionLevels());
    }
}
