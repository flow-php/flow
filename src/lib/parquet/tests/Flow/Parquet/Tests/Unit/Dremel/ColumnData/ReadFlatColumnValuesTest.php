<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\ColumnData;

use function Flow\ETL\Adapter\Parquet\{array_to_generator, empty_generator};
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\TestCase;

final class ReadFlatColumnValuesTest extends TestCase
{
    public function test_flat_column() : void
    {
        $data = new ReadFlatColumnValues(FlatColumn::int32('int32'), array_to_generator([2, 3]), repetitionLevels: [0, 0, 0], definitionLevels: [0, 1, 1]);

        self::assertSame(3, $data->rowsCount());
        self::assertFalse($data->isEmpty());
    }

    public function test_flat_empty_column() : void
    {
        $data = new ReadFlatColumnValues(FlatColumn::int32('int32'), empty_generator(), repetitionLevels: [], definitionLevels: []);

        self::assertSame(0, $data->rowsCount());
        self::assertTrue($data->isEmpty());
    }

    public function test_list() : void
    {
        $schema = Schema::with(NestedColumn::list('list', ListElement::int32()));

        $data = new ReadFlatColumnValues($schema->columnsFlat()[0], array_to_generator([2, 3]), repetitionLevels: [0, 1, 0], definitionLevels: [0, 3, 3]);

        self::assertSame(2, $data->rowsCount());
    }
}
