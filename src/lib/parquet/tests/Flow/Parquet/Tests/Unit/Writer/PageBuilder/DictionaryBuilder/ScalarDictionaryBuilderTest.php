<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\PageBuilder\{Dictionary, DictionaryBuilder\ScalarDictionaryBuilder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ScalarDictionaryBuilderTest extends TestCase
{
    private ScalarDictionaryBuilder $builder;

    public static function scalar_value_types_provider() : \Generator
    {
        yield 'integers' => [
            [10, 20, 30, 20, 10],
            [10, 20, 30],
            [0, 1, 2, 1, 0],
        ];

        yield 'integers with nulls' => [
            [10, null, 20, null, 10],
            [10, 20],
            [0, 1, 0],
        ];

        yield 'single integer repeated' => [
            [99, 99, 99, 99],
            [99],
            [0, 0, 0, 0],
        ];

        yield 'mixed positive and negative integers' => [
            [1, -1, 2, -2, 1],
            [1, -1, 2, -2],
            [0, 1, 2, 3, 0],
        ];
    }

    protected function setUp() : void
    {
        $this->builder = new ScalarDictionaryBuilder();
    }

    public function test_all_null_values_returns_empty_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [null, null, null]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    public function test_alternating_pattern() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [1, 2, 1, 2, 1, 2, 1, 2]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2], $result->dictionary);
        self::assertSame([0, 1, 0, 1, 0, 1, 0, 1], $result->indices);
    }

    public function test_boolean_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BOOLEAN);
        $data = new WriteFlatColumnValues($column, values: [true, false, true, false, true]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([true, false], $result->dictionary);
        self::assertSame([0, 1, 0, 1, 0], $result->indices);
    }

    public function test_boolean_values_with_nulls() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BOOLEAN);
        $data = new WriteFlatColumnValues($column, values: [true, null, false, null, true]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([true, false], $result->dictionary);
        self::assertSame([0, 1, 0], $result->indices);
    }

    /**
     * @param array<null|scalar> $values
     * @param array<mixed> $expectedDictionary
     * @param array<int> $expectedIndices
     */
    #[DataProvider('scalar_value_types_provider')]
    public function test_different_scalar_types(array $values, array $expectedDictionary, array $expectedIndices) : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame($expectedDictionary, $result->dictionary);
        self::assertSame($expectedIndices, $result->indices);
    }

    public function test_duplicate_values_creates_indexed_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3, 2, 1, 4, 3]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2, 3, 4], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_edge_case_negative_values() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [-1, -2, -1, 0, -2]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([-1, -2, 0], $result->dictionary);
        self::assertSame([0, 1, 0, 2, 1], $result->indices);
    }

    public function test_edge_case_zero_values() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [0, 0, 0]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([0], $result->dictionary);
        self::assertSame([0, 0, 0], $result->indices);
    }

    public function test_empty_data_returns_empty_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: []);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    public function test_empty_strings() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);
        $data = new WriteFlatColumnValues($column, values: ['', 'hello', '', 'world', '']);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame(['', 'hello', 'world'], $result->dictionary);
        self::assertSame([0, 1, 0, 2, 0], $result->indices);
    }

    public function test_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.5, 2.5, 3.5, 2.5, 1.5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.5, 2.5, 3.5], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
    }

    public function test_large_number_of_duplicates() : void
    {
        $column = FlatColumn::int32('test_column');
        $values = array_fill(0, 1000, 42);
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([42], $result->dictionary);
        self::assertSame(array_fill(0, 1000, 0), $result->indices);
    }

    public function test_large_number_of_unique_values() : void
    {
        $column = FlatColumn::int32('test_column');
        $values = range(1, 1000);
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame($values, $result->dictionary);
        self::assertSame(array_keys($values), $result->indices);
    }

    public function test_maintains_first_occurrence_order() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [5, 2, 8, 1, 2, 5, 9]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([5, 2, 8, 1, 9], $result->dictionary);
        self::assertSame([0, 1, 2, 3, 1, 0, 4], $result->indices);
    }

    public function test_mixed_nulls_and_values() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [null, 1, null, 2, 1, null, 3, 2]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2, 3], $result->dictionary);
        self::assertSame([0, 1, 0, 2, 1], $result->indices);
    }

    public function test_multiple_unique_values_creates_ordered_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3, 4, 5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2, 3, 4, 5], $result->dictionary);
        self::assertSame([0, 1, 2, 3, 4], $result->indices);
    }

    public function test_single_boolean_value() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BOOLEAN);
        $data = new WriteFlatColumnValues($column, values: [true, true, null, true]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([true], $result->dictionary);
        self::assertSame([0, 0, 0], $result->indices);
    }

    public function test_single_non_null_value_creates_single_entry_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [42]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([42], $result->dictionary);
        self::assertSame([0], $result->indices);
    }

    public function test_single_value_with_nulls_creates_single_entry_dictionary() : void
    {
        $column = FlatColumn::int32('test_column');
        $data = new WriteFlatColumnValues($column, values: [null, 42, null, 42, null]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([42], $result->dictionary);
        self::assertSame([0, 0], $result->indices);
    }

    public function test_special_characters_in_strings() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);
        $data = new WriteFlatColumnValues($column, values: ['hello\nworld', 'hello\tworld', 'hello\nworld', 'hello world']);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame(['hello\nworld', 'hello\tworld', 'hello world'], $result->dictionary);
        self::assertSame([0, 1, 0, 2], $result->indices);
    }

    public function test_string_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);
        $data = new WriteFlatColumnValues($column, values: ['apple', 'banana', 'cherry', 'banana', 'apple']);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame(['apple', 'banana', 'cherry'], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
    }

    public function test_unicode_strings() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);
        $data = new WriteFlatColumnValues($column, values: ['héllo', 'wörld', 'héllo', '测试']);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame(['héllo', 'wörld', '测试'], $result->dictionary);
        self::assertSame([0, 1, 0, 2], $result->indices);
    }
}
