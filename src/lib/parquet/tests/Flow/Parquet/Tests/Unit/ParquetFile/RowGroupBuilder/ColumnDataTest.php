<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatValue};
use Flow\Parquet\ParquetFile\RowGroupBuilder\FlatColumnData;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class ColumnDataTest extends TestCase
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

        $columnData = FlatColumnData::initialize($schema->get('m'));
        $columnData->addValue(
            new FlatValue($keyColumn, 0, 2, 'a'),
            new FlatValue($valueColumn, 0, 3, 1),
            new FlatValue($keyColumn, 2, 2, 'b'),
            new FlatValue($valueColumn, 2, 3, 2)
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

        $columnData = FlatColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));
        $columnData->addValue(new FlatValue($column, 0, 1, 2));
        $columnData->addValue(new FlatValue($column, 0, 1, 3));

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

        $columnData = FlatColumnData::initialize($schema->get('m'));
        $columnData->addValue(new FlatValue($keyColumn, 0, 2, 'a'));
        $columnData->addValue(new FlatValue($keyColumn, 2, 2, 'b'));

        $columnData->addValue(new FlatValue($valueColumn, 0, 3, 1));
        $columnData->addValue(new FlatValue($valueColumn, 2, 3, 2));

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
