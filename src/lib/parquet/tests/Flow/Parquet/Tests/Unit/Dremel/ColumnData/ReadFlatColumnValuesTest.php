<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\ColumnData;

use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\Adapter\Parquet\array_to_generator;
use function Flow\Parquet\empty_generator;

final class ReadFlatColumnValuesTest extends TestCase
{
    public function test_flat_column(): void
    {
        $data = new ReadFlatColumnValues(
            FlatColumn::int32('int32'),
            array_to_generator([2, 3]),
            repetitionLevels: [0, 0, 0],
            definitionLevels: [0, 1, 1],
        );

        static::assertSame(3, $data->rowsCount());
        static::assertFalse($data->isEmpty());
    }

    public function test_flat_empty_column(): void
    {
        $data = new ReadFlatColumnValues(
            FlatColumn::int32('int32'),
            empty_generator(),
            repetitionLevels: [],
            definitionLevels: [],
        );

        static::assertSame(0, $data->rowsCount());
        static::assertTrue($data->isEmpty());
    }

    public function test_list(): void
    {
        $schema = Schema::with(NestedColumn::list('list', ListElement::int32()));

        $data = new ReadFlatColumnValues(
            $schema->columnsFlat()[0],
            array_to_generator([2, 3]),
            repetitionLevels: [0, 1, 0],
            definitionLevels: [0, 3, 3],
        );

        static::assertSame(2, $data->rowsCount());
    }

    public function test_a_selection_keeps_the_levels_and_values_of_its_rows_only(): void
    {
        $leaf = Schema::with(NestedColumn::list('list', ListElement::int32()))->columnsFlat()[0];
        $page = new ReadFlatColumnValues(
            $leaf,
            array_to_generator([1, 2, 3, 4, 5]),
            repetitionLevels: [0, 1, 0, 0, 1, 1, 0],
            definitionLevels: [3, 3, 1, 3, 3, 3, 0],
        );

        $selected = $page->select(
            new RowSelection(new RowSelector(1, true), new RowSelector(2, false), new RowSelector(1, true)),
        );

        static::assertSame([0, 0, 1, 1], $selected->repetitionLevels());
        static::assertSame([1, 3, 3, 3], $selected->definitionLevels());
        static::assertSame([3, 4, 5], $selected->rawValues());
    }

    public function test_a_selection_over_another_row_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A selection of 2 rows cannot select from a page of 3 rows');

        (new ReadFlatColumnValues(
            FlatColumn::int32('int32'),
            array_to_generator([1, 2, 3]),
            repetitionLevels: [0, 0, 0],
            definitionLevels: [1, 1, 1],
        ))->select(RowSelection::all(2));
    }
}
