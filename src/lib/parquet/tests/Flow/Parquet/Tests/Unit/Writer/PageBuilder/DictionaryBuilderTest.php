<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\PageBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\Writer\PageBuilder\{Dictionary, DictionaryBuilder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DictionaryBuilderTest extends TestCase
{
    private DictionaryBuilder $builder;

    public static function byte_array_date_time_logical_types_provider() : \Generator
    {
        yield 'BYTE_ARRAY with DATE logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::date()];
        yield 'BYTE_ARRAY with TIME logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::time()];
        yield 'BYTE_ARRAY with TIMESTAMP logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::timestamp()];
        yield 'FIXED_LEN_BYTE_ARRAY with DATE logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::date()];
        yield 'FIXED_LEN_BYTE_ARRAY with TIME logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::time()];
        yield 'FIXED_LEN_BYTE_ARRAY with TIMESTAMP logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::timestamp()];
    }

    public static function byte_array_string_logical_types_provider() : \Generator
    {
        yield 'BYTE_ARRAY with STRING logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::string()];
        yield 'BYTE_ARRAY with JSON logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::json()];
        yield 'BYTE_ARRAY with BSON logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::bson()];
        yield 'BYTE_ARRAY with UUID logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::uuid()];
        yield 'BYTE_ARRAY with ENUM logical type' => [PhysicalType::BYTE_ARRAY, LogicalType::enum()];
        yield 'FIXED_LEN_BYTE_ARRAY with STRING logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::string()];
        yield 'FIXED_LEN_BYTE_ARRAY with JSON logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::json()];
        yield 'FIXED_LEN_BYTE_ARRAY with BSON logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::bson()];
        yield 'FIXED_LEN_BYTE_ARRAY with UUID logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::uuid()];
        yield 'FIXED_LEN_BYTE_ARRAY with ENUM logical type' => [PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::enum()];
    }

    public static function float_double_physical_types_provider() : \Generator
    {
        yield 'FLOAT physical type' => [PhysicalType::FLOAT];
        yield 'DOUBLE physical type' => [PhysicalType::DOUBLE];
    }

    public static function int64_int32_physical_types_provider() : \Generator
    {
        yield 'INT64 with DATE logical type' => [PhysicalType::INT64, LogicalType::date()];
        yield 'INT64 with TIME logical type' => [PhysicalType::INT64, LogicalType::time()];
        yield 'INT64 with TIMESTAMP logical type' => [PhysicalType::INT64, LogicalType::timestamp()];
        yield 'INT64 with null logical type' => [PhysicalType::INT64, null];
        yield 'INT32 with DATE logical type' => [PhysicalType::INT32, LogicalType::date()];
        yield 'INT32 with TIME logical type' => [PhysicalType::INT32, LogicalType::time()];
        yield 'INT32 with TIMESTAMP logical type' => [PhysicalType::INT32, LogicalType::timestamp()];
        yield 'INT32 with null logical type' => [PhysicalType::INT32, null];
    }

    protected function setUp() : void
    {
        $this->builder = new DictionaryBuilder();
    }

    public function test_all_null_values_returns_empty_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT32);
        $data = new WriteFlatColumnValues($column, values: [null, null, null]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    public function test_boolean_physical_type() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BOOLEAN);
        $data = new WriteFlatColumnValues($column, values: [true, false, true, false, true, null, false]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([true, false], $result->dictionary);
        self::assertSame([0, 1, 0, 1, 0, 1], $result->indices);
    }

    public function test_boolean_physical_type_with_all_same_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BOOLEAN);
        $data = new WriteFlatColumnValues($column, values: [true, true, true, null, true]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([true], $result->dictionary);
        self::assertSame([0, 0, 0, 0], $result->indices);
    }

    #[DataProvider('byte_array_date_time_logical_types_provider')]
    public function test_byte_array_fixed_len_byte_array_with_date_time_logical_types(
        PhysicalType $physicalType,
        LogicalType $logicalType,
    ) : void {
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-03');
        $date4 = new \DateTimeImmutable('2023-01-04');

        $column = new FlatColumn('test_column', $physicalType, logicalType: $logicalType);
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date3, $date2, $date1, $date4, null, $date3]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(4, $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
        self::assertEquals($date1, $result->dictionary[0]);
        self::assertEquals($date2, $result->dictionary[1]);
        self::assertEquals($date3, $result->dictionary[2]);
        self::assertEquals($date4, $result->dictionary[3]);
    }

    #[DataProvider('byte_array_string_logical_types_provider')]
    public function test_byte_array_fixed_len_byte_array_with_string_logical_types(
        PhysicalType $physicalType,
        LogicalType $logicalType,
    ) : void {
        $column = new FlatColumn('test_column', $physicalType, logicalType: $logicalType);
        $data = new WriteFlatColumnValues($column, values: ['apple', 'banana', 'cherry', 'banana', 'apple', 'date', null, 'cherry']);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame(['apple', 'banana', 'cherry', 'date'], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_byte_array_with_decimal_logical_type() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::decimal(2, 10));
        $data = new WriteFlatColumnValues($column, values: [1.23, 4.56, 7.89, 4.56, 1.23, 9.99, null, 7.89]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.23, 4.56, 7.89, 9.99], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_empty_data_returns_empty_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT32);
        $data = new WriteFlatColumnValues($column, values: []);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([], $result->dictionary);
        self::assertSame([], $result->indices);
    }

    #[DataProvider('float_double_physical_types_provider')]
    public function test_float_double_physical_types(PhysicalType $physicalType) : void
    {
        $column = new FlatColumn('test_column', $physicalType);
        $data = new WriteFlatColumnValues($column, values: [1.5, 2.5, 3.5, 2.5, 1.5, 4.5, null, 3.5]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1.5, 2.5, 3.5, 4.5], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_float_with_special_values() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::FLOAT);
        $data = new WriteFlatColumnValues($column, values: [1.0, \INF, -\INF, \NAN, 1.0, \INF, null]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertCount(4, $result->dictionary);
        self::assertSame([0, 1, 2, 3, 0, 1], $result->indices);
        self::assertSame(1.0, $result->dictionary[0]);
        self::assertSame(\INF, $result->dictionary[1]);
        self::assertSame(-\INF, $result->dictionary[2]);
        /** @phpstan-ignore-next-line */
        self::assertTrue(\is_nan($result->dictionary[3]));
    }

    #[DataProvider('int64_int32_physical_types_provider')]
    public function test_int64_int32_physical_types_with_date_time_timestamp_logical_types(
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
    ) : void {
        $column = new FlatColumn('test_column', $physicalType, logicalType: $logicalType);
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3, 2, 1, 4, null, 3]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([1, 2, 3, 4], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    #[DataProvider('int64_int32_physical_types_provider')]
    public function test_int64_int32_physical_types_with_default_logical_types(
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
    ) : void {
        $column = new FlatColumn('test_column', $physicalType, logicalType: $logicalType);
        $data = new WriteFlatColumnValues($column, values: [10, 20, 30, 20, 10, 40, null, 30]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([10, 20, 30, 40], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 3, 2], $result->indices);
    }

    public function test_large_string_values() : void
    {
        $longString1 = \str_repeat('a', 1000);
        $longString2 = \str_repeat('b', 1000);
        $longString3 = \str_repeat('c', 1000);

        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $data = new WriteFlatColumnValues($column, values: [$longString1, $longString2, $longString3, $longString2, $longString1]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([$longString1, $longString2, $longString3], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0], $result->indices);
    }

    public function test_maintains_order_of_first_occurrence() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT32);
        $data = new WriteFlatColumnValues($column, values: [3, 1, 2, 1, 3, 2]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([3, 1, 2], $result->dictionary);
        self::assertSame([0, 1, 2, 1, 0, 2], $result->indices);
    }

    public function test_single_unique_value_creates_single_entry_dictionary() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT32);
        $data = new WriteFlatColumnValues($column, values: [42, 42, 42, null, 42]);

        $result = $this->builder->build($column, $data);

        self::assertInstanceOf(Dictionary::class, $result);
        self::assertSame([42], $result->dictionary);
        self::assertSame([0, 0, 0, 0], $result->indices);
    }

    public function test_unsupported_byte_array_logical_type_throws_exception() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::integer());
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Building dictionary for "INTEGER" is not supported');

        $this->builder->build($column, $data);
    }

    public function test_unsupported_physical_type_throws_exception() : void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT96);
        $data = new WriteFlatColumnValues($column, values: [1, 2, 3]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Building dictionary for "INT96" is not supported');

        $this->builder->build($column, $data);
    }
}
