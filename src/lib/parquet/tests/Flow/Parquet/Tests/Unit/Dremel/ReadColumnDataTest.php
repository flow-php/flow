<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel;

use Flow\Parquet\Dremel\ColumnData\{FlatValue, ReadFlatColumnValues};
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class ReadColumnDataTest extends TestCase
{
    public function test_create_flat_from_flat_data() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        $keyValuesGenerator = function () {
            yield 'a';
            yield 'b';
        };
        $valueValuesGenerator = function () {
            yield 1;
            yield 2;
        };

        $columnData = new ReadColumnData(
            $schema->get('m'),
            [
                $keyColumn->flatPath() => new ReadFlatColumnValues($keyColumn, $keyValuesGenerator(), [0, 2], [2, 2]),
                $valueColumn->flatPath() => new ReadFlatColumnValues($valueColumn, $valueValuesGenerator(), [0, 2], [3, 3]),
            ]
        );

        self::assertEquals(
            [
                new FlatValue($keyColumn, 0, 2, 'a'),
                new FlatValue($keyColumn, 2, 2, 'b'),
            ],
            \iterator_to_array($columnData->iterator($keyColumn))
        );
        self::assertEquals(
            [
                new FlatValue($valueColumn, 0, 3, 1),
                new FlatValue($valueColumn, 2, 3, 2),
            ],
            \iterator_to_array($columnData->iterator($valueColumn))
        );
    }

    public function test_iterating_over_flat_column_data() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $valuesGenerator = function () {
            yield 1;
            yield 2;
            yield 3;
        };

        $columnData = new ReadColumnData(
            $column,
            [
                $column->flatPath() => new ReadFlatColumnValues($column, $valuesGenerator(), [0, 0, 0], [1, 1, 1]),
            ]
        );

        self::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
                new FlatValue($column, 0, 1, 3),
            ],
            \iterator_to_array($columnData->iterator($column))
        );
    }

    public function test_iterating_over_map_column_data() : void
    {
        /** @var NestedColumn $column */
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        $keyValuesGenerator = function () {
            yield 'a';
            yield 'b';
        };
        $valueValuesGenerator = function () {
            yield 1;
            yield 2;
        };

        $columnData = new ReadColumnData(
            $schema->get('m'),
            [
                $keyColumn->flatPath() => new ReadFlatColumnValues($keyColumn, $keyValuesGenerator(), [0, 2], [2, 2]),
                $valueColumn->flatPath() => new ReadFlatColumnValues($valueColumn, $valueValuesGenerator(), [0, 2], [3, 3]),
            ]
        );

        self::assertEquals(
            [
                new FlatValue($keyColumn, 0, 2, 'a'),
                new FlatValue($keyColumn, 2, 2, 'b'),
            ],
            \iterator_to_array($columnData->iterator($keyColumn))
        );

        self::assertEquals(
            [
                new FlatValue($valueColumn, 0, 3, 1),
                new FlatValue($valueColumn, 2, 3, 2),
            ],
            \iterator_to_array($columnData->iterator($valueColumn))
        );
    }
}
