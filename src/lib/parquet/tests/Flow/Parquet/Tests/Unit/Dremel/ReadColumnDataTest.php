<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel;

use Flow\Parquet\Dremel\ColumnData\FlatValue;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;
use function iterator_to_array;

final class ReadColumnDataTest extends TestCase
{
    public function test_create_flat_from_flat_data(): void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        $keyColumn = type_instance_of(FlatColumn::class)->assert($schema->get('m.key_value.key'));
        $valueColumn = type_instance_of(FlatColumn::class)->assert($schema->get('m.key_value.value'));

        $keyValuesGenerator = static function () {
            yield 'a';
            yield 'b';
        };
        $valueValuesGenerator = static function () {
            yield 1;
            yield 2;
        };

        $columnData = new ReadColumnData($schema->get('m'), [
            $keyColumn->flatPath() => new ReadFlatColumnValues($keyColumn, $keyValuesGenerator(), [0, 2], [2, 2]),
            $valueColumn->flatPath() => new ReadFlatColumnValues($valueColumn, $valueValuesGenerator(), [0, 2], [3, 3]),
        ]);

        static::assertEquals(
            [
                new FlatValue($keyColumn, 0, 2, 'a'),
                new FlatValue($keyColumn, 2, 2, 'b'),
            ],
            iterator_to_array($columnData->iterator($keyColumn)),
        );
        static::assertEquals(
            [
                new FlatValue($valueColumn, 0, 3, 1),
                new FlatValue($valueColumn, 2, 3, 2),
            ],
            iterator_to_array($columnData->iterator($valueColumn)),
        );
    }

    public function test_iterating_over_flat_column_data(): void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $valuesGenerator = static function () {
            yield 1;
            yield 2;
            yield 3;
        };

        $columnData = new ReadColumnData($column, [
            $column->flatPath() => new ReadFlatColumnValues($column, $valuesGenerator(), [0, 0, 0], [1, 1, 1]),
        ]);

        static::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
                new FlatValue($column, 0, 1, 3),
            ],
            iterator_to_array($columnData->iterator($column)),
        );
    }

    public function test_iterating_over_map_column_data(): void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        $keyColumn = type_instance_of(FlatColumn::class)->assert($schema->get('m.key_value.key'));
        $valueColumn = type_instance_of(FlatColumn::class)->assert($schema->get('m.key_value.value'));

        $keyValuesGenerator = static function () {
            yield 'a';
            yield 'b';
        };
        $valueValuesGenerator = static function () {
            yield 1;
            yield 2;
        };

        $columnData = new ReadColumnData($schema->get('m'), [
            $keyColumn->flatPath() => new ReadFlatColumnValues($keyColumn, $keyValuesGenerator(), [0, 2], [2, 2]),
            $valueColumn->flatPath() => new ReadFlatColumnValues($valueColumn, $valueValuesGenerator(), [0, 2], [3, 3]),
        ]);

        static::assertEquals(
            [
                new FlatValue($keyColumn, 0, 2, 'a'),
                new FlatValue($keyColumn, 2, 2, 'b'),
            ],
            iterator_to_array($columnData->iterator($keyColumn)),
        );

        static::assertEquals(
            [
                new FlatValue($valueColumn, 0, 3, 1),
                new FlatValue($valueColumn, 2, 3, 2),
            ],
            iterator_to_array($columnData->iterator($valueColumn)),
        );
    }
}
