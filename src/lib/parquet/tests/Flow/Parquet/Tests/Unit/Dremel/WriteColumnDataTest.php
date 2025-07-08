<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel;

use Flow\Parquet\Dremel\ColumnData\{FlatValue, WriteFlatColumnValues};
use Flow\Parquet\Dremel\{ReadColumnData};
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class WriteColumnDataTest extends TestCase
{
    public static function initializeDataProvider() : \Generator
    {
        yield 'flat column' => [
            Schema::with(FlatColumn::int32('int32')),
            'int32',
            ['int32'],
        ];

        yield 'map column' => [
            Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32())),
            'm',
            ['m.key_value.key', 'm.key_value.value'],
        ];

        yield 'list column' => [
            Schema::with(NestedColumn::list('l', ListElement::int32())),
            'l',
            ['l.list.element'],
        ];

        yield 'struct column' => [
            Schema::with(NestedColumn::struct('s', [FlatColumn::int32('a'), FlatColumn::string('b')])),
            's',
            ['s.a', 's.b'],
        ];
    }

    public static function isEmptyDataProvider() : \Generator
    {
        yield 'empty column data' => [
            true,
            function (WriteColumnData $columnData, FlatColumn $column) : void {
                // No values added
            },
        ];

        yield 'column data with values' => [
            false,
            function (WriteColumnData $columnData, FlatColumn $column) : void {
                $columnData->addValue(new FlatValue($column, 0, 1, 1));
            },
        ];
    }

    public static function valueTypesDataProvider() : \Generator
    {
        yield 'string value' => ['test', 'string'];
        yield 'integer value' => ['123', 'string'];
        yield 'float value' => ['123.45', 'string'];
        yield 'boolean value' => ['true', 'string'];
        yield 'null value' => [null, 'null'];
    }

    public function test_add_value_multiple_values() : void
    {
        $schema = Schema::with(NestedColumn::struct('s', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]));

        /** @var NestedColumn $structColumn */
        $structColumn = $schema->get('s');
        /** @var FlatColumn $column1 */
        $column1 = $schema->get('s.int32');
        /** @var FlatColumn $column2 */
        $column2 = $schema->get('s.string');

        self::assertInstanceOf(FlatColumn::class, $column1);
        self::assertInstanceOf(FlatColumn::class, $column2);

        $maxDefLevel1 = $column1->repetitions()->maxDefinitionLevel();
        $maxDefLevel2 = $column2->repetitions()->maxDefinitionLevel();

        $columnData = WriteColumnData::initialize($structColumn);
        $columnData->addValue(
            new FlatValue($column1, 0, $maxDefLevel1, 100),
            new FlatValue($column2, 0, $maxDefLevel2, 'test')
        );

        self::assertEquals(
            [new FlatValue($column1, 0, $maxDefLevel1, 100)],
            \iterator_to_array($columnData->iterator($column1))
        );
        self::assertEquals(
            [new FlatValue($column2, 0, $maxDefLevel2, 'test')],
            \iterator_to_array($columnData->iterator($column2))
        );
    }

    /**
     * @dataProvider valueTypesDataProvider
     */
    public function test_add_value_with_different_value_types(int|float|string|bool|null $value, string $expectedType) : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::string('test'))->get('test');

        $columnData = WriteColumnData::initialize($column);

        if ($value === null) {
            $columnData->addValue(new FlatValue($column, 0, 0, $value));
        } else {
            $columnData->addValue(new FlatValue($column, 0, 1, $value));
        }

        $values = \iterator_to_array($columnData->iterator($column));
        self::assertCount(1, $values);
        self::assertEquals($value, $values[0]->value);

        if ($expectedType !== 'null') {
            self::assertIsString($values[0]->value);
        } else {
            self::assertNull($values[0]->value);
        }
    }

    public function test_add_values_merge_with_existing_data() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));

        $additionalValues = new WriteFlatColumnValues($column);
        $additionalValues->add(new FlatValue($column, 0, 1, 2));
        $additionalValues->add(new FlatValue($column, 0, 1, 3));

        $columnData->addValues($additionalValues);

        self::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
                new FlatValue($column, 0, 1, 3),
            ],
            \iterator_to_array($columnData->iterator($column))
        );
    }

    public function test_create_flat_from_flat_data() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        self::assertInstanceOf(FlatColumn::class, $keyColumn);
        self::assertInstanceOf(FlatColumn::class, $valueColumn);

        $columnData = WriteColumnData::initialize($schema->get('m'));
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

    public function test_empty_column_data_operations() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);

        self::assertTrue($columnData->isEmpty($column));
        self::assertEmpty(\iterator_to_array($columnData->iterator($column)));

        $normalized = $columnData->normalize();
        self::assertArrayHasKey('int32', $normalized);
        self::assertEmpty($normalized['int32']['repetition_levels']);
        self::assertEmpty($normalized['int32']['definition_levels']);
        self::assertEmpty($normalized['int32']['values']);
    }

    public function test_flat_values_returns_all_flat_values() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));
        $columnData = WriteColumnData::initialize($schema->get('m'));

        $flatValues = $columnData->flatValues();

        self::assertCount(2, $flatValues);
        self::assertArrayHasKey('m.key_value.key', $flatValues);
        self::assertArrayHasKey('m.key_value.value', $flatValues);
        self::assertContainsOnlyInstancesOf(WriteFlatColumnValues::class, $flatValues);
    }

    /**
     * @dataProvider initializeDataProvider
     *
     * @param array<string> $expectedFlatPaths
     */
    public function test_initialize_with_different_column_types(Schema $schema, string $columnPath, array $expectedFlatPaths) : void
    {
        $column = $schema->get($columnPath);
        $columnData = WriteColumnData::initialize($column);

        $flatValues = $columnData->flatValues();
        self::assertCount(\count($expectedFlatPaths), $flatValues);

        foreach ($expectedFlatPaths as $flatPath) {
            self::assertArrayHasKey($flatPath, $flatValues);
            self::assertInstanceOf(WriteFlatColumnValues::class, $flatValues[$flatPath]);
        }
    }

    /**
     * @dataProvider isEmptyDataProvider
     */
    public function test_is_empty(bool $isEmpty, \Closure $setupCallback) : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $setupCallback($columnData, $column);

        self::assertEquals($isEmpty, $columnData->isEmpty($column));
    }

    public function test_is_empty_throws_exception_for_unknown_column() : void
    {
        /** @var FlatColumn $column1 */
        $column1 = Schema::with(FlatColumn::int32('int32'))->get('int32');
        /** @var FlatColumn $column2 */
        $column2 = Schema::with(FlatColumn::string('string'))->get('string');

        $columnData = WriteColumnData::initialize($column1);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column string not found in FlatData');
        $columnData->isEmpty($column2);
    }

    public function test_iterating_over_flat_column_data() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
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
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        self::assertInstanceOf(FlatColumn::class, $keyColumn);
        self::assertInstanceOf(FlatColumn::class, $valueColumn);

        $columnData = WriteColumnData::initialize($schema->get('m'));
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

    public function test_iterator_returns_correct_iterator() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));
        $columnData->addValue(new FlatValue($column, 0, 1, 2));

        $iterator = $columnData->iterator($column);

        self::assertInstanceOf(\Iterator::class, $iterator);
        self::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
            ],
            \iterator_to_array($iterator)
        );
    }

    public function test_merge_combines_column_data() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData1 = WriteColumnData::initialize($column);
        $columnData1->addValue(new FlatValue($column, 0, 1, 1));

        $columnData2 = WriteColumnData::initialize($column);
        $columnData2->addValue(new FlatValue($column, 0, 1, 2));
        $columnData2->addValue(new FlatValue($column, 0, 1, 3));

        $result = $columnData1->merge($columnData2);

        self::assertSame($columnData1, $result);
        self::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
                new FlatValue($column, 0, 1, 3),
            ],
            \iterator_to_array($result->iterator($column))
        );
    }

    public function test_merge_with_map_column() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        self::assertInstanceOf(FlatColumn::class, $keyColumn);
        self::assertInstanceOf(FlatColumn::class, $valueColumn);

        $columnData1 = WriteColumnData::initialize($schema->get('m'));
        $columnData1->addValue(
            new FlatValue($keyColumn, 0, 2, 'a'),
            new FlatValue($valueColumn, 0, 3, 1)
        );

        $columnData2 = WriteColumnData::initialize($schema->get('m'));
        $columnData2->addValue(
            new FlatValue($keyColumn, 2, 2, 'b'),
            new FlatValue($valueColumn, 2, 3, 2)
        );

        $result = $columnData1->merge($columnData2);

        self::assertSame($columnData1, $result);
        self::assertEquals(
            [
                new FlatValue($keyColumn, 0, 2, 'a'),
                new FlatValue($keyColumn, 2, 2, 'b'),
            ],
            \iterator_to_array($result->iterator($keyColumn))
        );
        self::assertEquals(
            [
                new FlatValue($valueColumn, 0, 3, 1),
                new FlatValue($valueColumn, 2, 3, 2),
            ],
            \iterator_to_array($result->iterator($valueColumn))
        );
    }

    public function test_normalize_returns_correct_array_structure() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));
        $columnData->addValue(new FlatValue($column, 0, 1, 2));

        $normalized = $columnData->normalize();
        self::assertArrayHasKey('int32', $normalized);
        self::assertArrayHasKey('repetition_levels', $normalized['int32']);
        self::assertArrayHasKey('definition_levels', $normalized['int32']);
        self::assertArrayHasKey('values', $normalized['int32']);

        self::assertEquals([0, 0], $normalized['int32']['repetition_levels']);
        self::assertEquals([1, 1], $normalized['int32']['definition_levels']);
        self::assertEquals([1, 2], $normalized['int32']['values']);
    }

    public function test_normalize_with_map_column() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        self::assertInstanceOf(FlatColumn::class, $keyColumn);
        self::assertInstanceOf(FlatColumn::class, $valueColumn);

        $columnData = WriteColumnData::initialize($schema->get('m'));
        $columnData->addValue(
            new FlatValue($keyColumn, 0, 2, 'a'),
            new FlatValue($valueColumn, 0, 3, 1)
        );

        $normalized = $columnData->normalize();

        self::assertCount(2, $normalized);
        self::assertArrayHasKey('m.key_value.key', $normalized);
        self::assertArrayHasKey('m.key_value.value', $normalized);

        self::assertEquals([0], $normalized['m.key_value.key']['repetition_levels']);
        self::assertEquals([2], $normalized['m.key_value.key']['definition_levels']);
        self::assertEquals(['a'], $normalized['m.key_value.key']['values']);

        self::assertEquals([0], $normalized['m.key_value.value']['repetition_levels']);
        self::assertEquals([3], $normalized['m.key_value.value']['definition_levels']);
        self::assertEquals([1], $normalized['m.key_value.value']['values']);
    }

    public function test_to_read_column_data_creates_correct_structure() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));
        $columnData->addValue(new FlatValue($column, 0, 1, 2));

        $readColumnData = $columnData->toReadColumnData();

        self::assertInstanceOf(ReadColumnData::class, $readColumnData);
        self::assertSame($column, $readColumnData->column);
        self::assertArrayHasKey('int32', $readColumnData->flatValues);

        $values = \iterator_to_array($readColumnData->iterator($column));
        self::assertEquals(
            [
                new FlatValue($column, 0, 1, 1),
                new FlatValue($column, 0, 1, 2),
            ],
            $values
        );
    }

    public function test_to_read_column_data_with_map_column() : void
    {
        $schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int32()));

        /**
         * @var FlatColumn $keyColumn
         * @var FlatColumn $valueColumn
         */
        $keyColumn = $schema->get('m.key_value.key');
        $valueColumn = $schema->get('m.key_value.value');

        self::assertInstanceOf(FlatColumn::class, $keyColumn);
        self::assertInstanceOf(FlatColumn::class, $valueColumn);

        $columnData = WriteColumnData::initialize($schema->get('m'));
        $columnData->addValue(
            new FlatValue($keyColumn, 0, 2, 'a'),
            new FlatValue($valueColumn, 0, 3, 1)
        );

        $readColumnData = $columnData->toReadColumnData();

        self::assertInstanceOf(ReadColumnData::class, $readColumnData);
        self::assertCount(2, $readColumnData->flatValues);

        $keyValues = \iterator_to_array($readColumnData->iterator($keyColumn));
        self::assertEquals([new FlatValue($keyColumn, 0, 2, 'a')], $keyValues);

        $valueValues = \iterator_to_array($readColumnData->iterator($valueColumn));
        self::assertEquals([new FlatValue($valueColumn, 0, 3, 1)], $valueValues);
    }

    public function test_values_returns_correct_flat_column_values() : void
    {
        /** @var FlatColumn $column */
        $column = Schema::with(FlatColumn::int32('int32'))->get('int32');

        $columnData = WriteColumnData::initialize($column);
        $columnData->addValue(new FlatValue($column, 0, 1, 1));

        $values = $columnData->values('int32');

        self::assertInstanceOf(WriteFlatColumnValues::class, $values);
        self::assertSame($column, $values->column);
        self::assertEquals([1], $values->values());
    }
}
