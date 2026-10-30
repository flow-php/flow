<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\ColumnData;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use PHPUnit\Framework\TestCase;

final class WriteFlatColumnValuesTest extends TestCase
{
    public function test_flat_column(): void
    {
        $data = new WriteFlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0],
            definitionLevels: [0, 1, 1],
            values: [2, 3],
        );

        static::assertSame(1, $data->nullCount());
        static::assertSame(3, $data->rowsCount());
    }

    public function test_list(): void
    {
        $schema = Schema::with(NestedColumn::list('list', ListElement::int32()));

        $data = new WriteFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0],
            definitionLevels: [0, 3, 3],
            values: [2, 3],
        );

        static::assertSame(1, $data->nullCount());
        static::assertSame(2, $data->rowsCount());
    }
}
