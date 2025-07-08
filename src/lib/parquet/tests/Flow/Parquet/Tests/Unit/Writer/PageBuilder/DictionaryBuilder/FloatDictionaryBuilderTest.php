<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\PageBuilder\{Dictionary, DictionaryBuilder\FloatDictionaryBuilder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatDictionaryBuilderTest extends TestCase
{
    private FloatDictionaryBuilder $builder;

    public static function float_value_types_provider() : \Generator
    {
        yield 'regular floats' => [
            [1.5, 2.5, 3.5, 2.5, 1.5],
            [1.5, 2.5, 3.5],
            [0, 1, 2, 1, 0],
        ];

        yield 'floats with nulls' => [
            [1.1, null, 2.2, null, 1.1],
            [1.1, 2.2],
            [0, 1, 0],
        ];

        yield 'single float repeated' => [
            [3.14, 3.14, 3.14, 3.14],
            [3.14],
            [0, 0, 0, 0],
        ];

        yield 'mixed precision floats' => [
            [1.0, 1.00, 1.000, 2.0, 1.0],
            [1.0, 2.0],
            [0, 0, 0, 1, 0],
        ];
    }

    protected function setUp() : void
    {
        $this->builder = new FloatDictionaryBuilder();
    }

    public function test_all_null_values_returns_empty_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [null, null, null]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    public function test_all_special_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.0, \INF, -\INF, \NAN, 1.0, \INF, null]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(4, $result->dictionary);
        self::assertSame([0, 1, 2, 3, 0, 1], $result->indices);
        self::assertSame(1.0, $result->dictionary[0]);
        self::assertSame(\INF, $result->dictionary[1]);
        self::assertSame(-\INF, $result->dictionary[2]);
        /** @phpstan-ignore-next-line */
        self::assertTrue(\is_nan($result->dictionary[3]));
    }

    public function test_alternating_float_pattern() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.1, 2.2, 1.1, 2.2, 1.1, 2.2]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.1, 2.2], $result->dictionary);
        self::assertSame([0, 1, 0, 1, 0, 1], $result->indices);
    }

    public function test_decimal_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.23, 4.56, 7.89, 4.56, 1.23, 9.99]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.23, 4.56, 7.89, 9.99], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3], $result->indices);
    }

    /**
     * @param array<null|float> $values
     * @param array<float> $expectedDictionary
     * @param array<int> $expectedIndices
     */
    #[DataProvider('float_value_types_provider')]
    public function test_different_float_types(array $values, array $expectedDictionary, array $expectedIndices) : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame($expectedDictionary, $result->dictionary);
        self::assertSame($expectedIndices, $result->indices);
    }

    public function test_double_precision_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::DOUBLE);
        $data = new WriteFlatColumnValues($column, values: [1.1234567890123456, 2.2345678901234567, 1.1234567890123456]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.1234567890123456, 2.2345678901234567], $result->dictionary);
        self::assertSame([0, 1, 0], $result->indices);
    }

    public function test_duplicate_float_values_creates_indexed_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.5, 2.5, 3.5, 2.5, 1.5, 4.5, 3.5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.5, 2.5, 3.5, 4.5], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_empty_data_returns_empty_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: []);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    public function test_integer_values_as_floats() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3, 2, 1]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2, 3], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
    }

    public function test_large_number_of_duplicates() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $values = array_fill(0, 1000, 3.14159);
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([3.14159], $result->dictionary);
        self::assertSame(array_fill(0, 1000, 0), $result->indices);
    }

    public function test_large_number_of_unique_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $values = [];

        for ($i = 1; $i <= 1000; $i++) {
            $values[] = $i * 0.1;
        }
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame($values, $result->dictionary);
        self::assertSame(array_keys($values), $result->indices);
    }

    public function test_maintains_first_occurrence_order() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [5.5, 2.2, 8.8, 1.1, 2.2, 5.5, 9.9]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([5.5, 2.2, 8.8, 1.1, 9.9], $result->dictionary);
        self::assertSame([0, 1, 2, 3, 1, 0, 4], $result->indices);
    }

    public function test_mixed_nulls_and_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [null, 1.1, null, 2.2, 1.1, null, 3.3, 2.2]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.1, 2.2, 3.3], $result->dictionary);
        self::assertSame([0, 1, 0, 2, 1], $result->indices);
    }

    public function test_mixed_positive_negative_zero() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.0, -1.0, 0.0, -0.0, 1.0, -1.0]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.0, -1.0, 0.0, -0.0], $result->dictionary);
        self::assertSame([0, 1, 2, 3, 0, 1], $result->indices);
    }

    public function test_multiple_unique_float_values_creates_ordered_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.1, 2.2, 3.3, 4.4, 5.5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.1, 2.2, 3.3, 4.4, 5.5], $result->dictionary);
        self::assertSame([0, 1, 2, 3, 4], $result->indices);
    }

    public function test_negative_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [-1.5, -2.5, -1.5, 0.0, -2.5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([-1.5, -2.5, 0.0], $result->dictionary);
        self::assertSame([0, 1, 0, 2, 1], $result->indices);
    }

    public function test_scientific_notation_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.23e5, 4.56e-3, 7.89e10, 4.56e-3, 1.23e5]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.23e5, 4.56e-3, 7.89e10], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
    }

    public function test_serialization_handles_float_precision() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $value1 = 1.0000000001;
        $value2 = 1.0000000002;
        $data = new WriteFlatColumnValues($column, values: [$value1, $value2, $value1]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(2, $result->dictionary);
        self::assertSame([0, 1, 0], $result->indices);
        self::assertSame($value1, $result->dictionary[0]);
        self::assertSame($value2, $result->dictionary[1]);
    }

    public function test_single_float_value_creates_single_entry_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [3.14]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([3.14], $result->dictionary);
        self::assertSame([0], $result->indices);
    }

    public function test_single_value_with_nulls_creates_single_entry_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [null, 2.5, null, 2.5, null]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([2.5], $result->dictionary);
        self::assertSame([0, 0], $result->indices);
    }

    public function test_special_float_values_infinity() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.0, \INF, -\INF, 1.0, \INF]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(3, $result->dictionary);
        self::assertSame([0, 1, 2, 0, 1], $result->indices);
        self::assertSame(1.0, $result->dictionary[0]);
        self::assertSame(\INF, $result->dictionary[1]);
        self::assertSame(-\INF, $result->dictionary[2]);
    }

    public function test_special_float_values_nan() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.0, \NAN, 2.0, \NAN, 1.0]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(3, $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
        self::assertSame(1.0, $result->dictionary[0]);
        /** @phpstan-ignore-next-line */
        self::assertTrue(\is_nan($result->dictionary[1]));
        self::assertSame(2.0, $result->dictionary[2]);
    }

    public function test_very_large_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1e10, 1e20, 1e30, 1e10, 1e20]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1e10, 1e20, 1e30], $result->dictionary);
        self::assertSame([0, 1, 2, 0, 1], $result->indices);
    }

    public function test_very_small_float_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1e-10, 1e-20, 1e-30, 1e-10, 1e-20]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1e-10, 1e-20, 1e-30], $result->dictionary);
        self::assertSame([0, 1, 2, 0, 1], $result->indices);
    }

    public function test_zero_and_negative_zero() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [0.0, -0.0, 0.0, -0.0]);

        $result = $this->builder->build($data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(2, $result->dictionary);
        self::assertSame([0, 1, 0, 1], $result->indices);
        self::assertSame(0.0, $result->dictionary[0]);
        self::assertSame(-0.0, $result->dictionary[1]);
    }
}
