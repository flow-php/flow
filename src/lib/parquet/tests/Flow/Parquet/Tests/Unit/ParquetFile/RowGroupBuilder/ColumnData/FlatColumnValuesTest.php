<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\TestCase;

final class FlatColumnValuesTest extends TestCase
{
    public function test_flat_column() : void
    {
        $data = new FlatColumnValues(FlatColumn::int32('int32'), repetitionLevels: [0, 0, 0], definitionLevels: [0, 1, 1], values: [2, 3]);

        self::assertSame(1, $data->nullCount());
        self::assertSame(3, $data->rowsCount());
    }

    public function test_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new FlatColumnValues($schema->columnsFlat()[0], repetitionLevels: [0, 1, 0], definitionLevels: [0, 3, 3], values: [2, 3]);

        self::assertSame(1, $data->nullCount());
        self::assertSame(2, $data->rowsCount());
    }

    public function test_skipping_rows_in_flat_column() : void
    {
        $data = new FlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0, 0, 0, 0, 0],
            definitionLevels: [1, 1, 1, 1, 1, 1, 1],
            values: [1, 2, 3, 4, 5, 6, 7]
        );

        $skipped = $data->skipRows(2);

        self::assertSame(5, $skipped->rowsCount());
        self::assertSame([3, 4, 5, 6, 7], $skipped->values());
        self::assertSame([1, 1, 1, 1, 1], $skipped->definitionLevels());
        self::assertSame([0, 0, 0, 0, 0], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new FlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 0, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(4, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(2, $skipped->rowsCount());
        self::assertSame([4, 5], $skipped->values());
        self::assertSame([2, 3, 3], $skipped->definitionLevels());
        self::assertSame([0, 0, 1], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list_with_multi_elements() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new FlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 1, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(3, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(1, $skipped->rowsCount());
        self::assertSame([4, 5], $skipped->values());
        self::assertSame([3, 3], $skipped->definitionLevels());
        self::assertSame([0, 1], $skipped->repetitionLevels());
    }

    public function test_splitting_flat_columns_by_rows() : void
    {
        $data = new FlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0, 0, 0, 0, 0],
            definitionLevels: [1, 1, 1, 1, 1, 1, 1],
            values: [1, 2, 3, 4, 5, 6, 7]
        );

        $split = $data->splitByRows(2);

        self::assertCount(4, $split);
        self::assertSame([1, 2], $split[0]->values());
        self::assertSame([3, 4], $split[1]->values());
        self::assertSame([5, 6], $split[2]->values());
        self::assertSame([7], $split[3]->values());
    }

    public function test_splitting_list_by_rows() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new FlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 0, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(1, $data->nullCount());
        self::assertSame(4, $data->rowsCount());

        $split = $data->splitByRows(2);

        self::assertCount(3, $split);

        self::assertSame([0, 1], $split[0]->repetitionLevels());
        self::assertSame([3, 3], $split[0]->definitionLevels());
        self::assertSame([1, 2], $split[0]->values());

        self::assertSame([0, 0], $split[1]->repetitionLevels());
        self::assertSame([3, 2], $split[1]->definitionLevels());
        self::assertSame([3], $split[1]->values());

        self::assertSame([0, 1], $split[2]->repetitionLevels());
        self::assertSame([3, 3], $split[2]->definitionLevels());
        self::assertSame([4, 5], $split[2]->values());
    }
}
