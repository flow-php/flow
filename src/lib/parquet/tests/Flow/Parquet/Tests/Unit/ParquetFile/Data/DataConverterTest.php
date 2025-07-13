<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\{Converter, DataConverter};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use PHPUnit\Framework\TestCase;

final class DataConverterTest extends TestCase
{
    public function test_cache_behavior_between_from_and_to_parquet_type_methods() : void
    {
        $options = Options::default();
        $mockConverter = new MockConverter(true, 'converted_data');
        $dataConverter = new DataConverter([$mockConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result1 = $dataConverter->fromParquetType($column, 'data1');
        $result2 = $dataConverter->toParquetType($column, 'data2');

        self::assertSame('converted_data', $result1);
        self::assertSame('converted_data', $result2);
        self::assertSame(1, $mockConverter->isForCallCount); // Only called once for caching
        self::assertSame(1, $mockConverter->fromParquetTypeCallCount);
        self::assertSame(1, $mockConverter->toParquetTypeCallCount);
    }

    public function test_complex_data_types_handling() : void
    {
        $options = Options::default();
        $complexDataConverter = new ComplexDataMockConverter();
        $dataConverter = new DataConverter([$complexDataConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $arrayData = ['key' => 'value'];
        $result1 = $dataConverter->fromParquetType($column, $arrayData);
        self::assertSame('complex_from_parquet', $result1);

        $objectData = new \stdClass();
        $objectData->property = 'value';
        $result2 = $dataConverter->toParquetType($column, $objectData);
        self::assertSame('complex_to_parquet', $result2);
    }

    public function test_constructor_with_empty_converters_array() : void
    {
        $options = Options::default();
        $dataConverter = new DataConverter([], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->fromParquetType($column, 'data');

        self::assertSame('data', $result);
    }

    public function test_from_parquet_type_caches_converter_result() : void
    {
        $options = Options::default();
        $mockConverter = new MockConverter(true, 'converted_data');
        $dataConverter = new DataConverter([$mockConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result1 = $dataConverter->fromParquetType($column, 'data1');
        $result2 = $dataConverter->fromParquetType($column, 'data2');

        self::assertSame('converted_data', $result1);
        self::assertSame('converted_data', $result2);
        self::assertSame(1, $mockConverter->isForCallCount);
        self::assertSame(2, $mockConverter->fromParquetTypeCallCount);
    }

    public function test_from_parquet_type_caches_null_when_no_converter_matches() : void
    {
        $options = Options::default();
        $nonMatchingConverter = new MockConverter(false, 'not_used');
        $dataConverter = new DataConverter([$nonMatchingConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result1 = $dataConverter->fromParquetType($column, 'data1');
        $result2 = $dataConverter->fromParquetType($column, 'data2');

        self::assertSame('data1', $result1);
        self::assertSame('data2', $result2);
        self::assertSame(1, $nonMatchingConverter->isForCallCount);
        self::assertSame(0, $nonMatchingConverter->fromParquetTypeCallCount);
    }

    public function test_from_parquet_type_preserves_original_exception_as_previous() : void
    {
        $options = Options::default();
        $throwingConverter = new ThrowingMockConverter();
        $dataConverter = new DataConverter([$throwingConverter], $options);
        $column = new FlatColumn('test_column', PhysicalType::INT32);

        try {
            $dataConverter->fromParquetType($column, 'data');
            self::fail('Expected DataConversionException to be thrown');
        } catch (DataConversionException $e) {
            self::assertInstanceOf(\RuntimeException::class, $e->getPrevious());
            self::assertSame('Test exception from converter', $e->getPrevious()->getMessage());
        }
    }

    public function test_from_parquet_type_returns_null_for_null_data() : void
    {
        $options = Options::default();
        $dataConverter = new DataConverter([], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->fromParquetType($column, null);

        self::assertNull($result);
    }

    public function test_from_parquet_type_returns_original_data_when_no_converter_matches() : void
    {
        $options = Options::default();
        $dataConverter = new DataConverter([], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);
        $data = 'test_data';

        $result = $dataConverter->fromParquetType($column, $data);

        self::assertSame($data, $result);
    }

    public function test_from_parquet_type_uses_first_matching_converter() : void
    {
        $options = Options::default();
        $firstConverter = new MockConverter(true, 'first_converter_result');
        $secondConverter = new MockConverter(true, 'second_converter_result');
        $dataConverter = new DataConverter([$firstConverter, $secondConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->fromParquetType($column, 'data');

        self::assertSame('first_converter_result', $result);
        self::assertSame(1, $firstConverter->isForCallCount);
        self::assertSame(1, $firstConverter->fromParquetTypeCallCount);
        self::assertSame(0, $secondConverter->isForCallCount);
        self::assertSame(0, $secondConverter->fromParquetTypeCallCount);
    }

    public function test_from_parquet_type_uses_matching_converter() : void
    {
        $options = Options::default();
        $mockConverter = new MockConverter(true, 'converted_from_parquet');
        $dataConverter = new DataConverter([$mockConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);
        $data = 'original_data';

        $result = $dataConverter->fromParquetType($column, $data);

        self::assertSame('converted_from_parquet', $result);
    }

    public function test_from_parquet_type_with_different_column_paths_uses_separate_cache_entries() : void
    {
        $options = Options::default();
        $selectiveConverter = new SelectiveMockConverter('column1', 'converter_result');
        $dataConverter = new DataConverter([$selectiveConverter], $options);

        $column1 = new FlatColumn('column1', PhysicalType::INT32);
        $column2 = new FlatColumn('column2', PhysicalType::INT64);

        $result1 = $dataConverter->fromParquetType($column1, 'data');
        $result2 = $dataConverter->fromParquetType($column2, 'data');

        self::assertSame('converter_result', $result1);
        self::assertSame('data', $result2); // No converter matches column2
        self::assertSame(2, $selectiveConverter->isForCallCount); // Called for both columns
        self::assertSame(1, $selectiveConverter->fromParquetTypeCallCount); // Only called for column1
    }

    public function test_from_parquet_type_with_multiple_non_matching_converters() : void
    {
        $options = Options::default();
        $converter1 = new MockConverter(false, 'not_used1');
        $converter2 = new MockConverter(false, 'not_used2');
        $converter3 = new MockConverter(false, 'not_used3');
        $dataConverter = new DataConverter([$converter1, $converter2, $converter3], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->fromParquetType($column, 'original_data');

        self::assertSame('original_data', $result);
        self::assertSame(1, $converter1->isForCallCount);
        self::assertSame(1, $converter2->isForCallCount);
        self::assertSame(1, $converter3->isForCallCount);
        self::assertSame(0, $converter1->fromParquetTypeCallCount);
        self::assertSame(0, $converter2->fromParquetTypeCallCount);
        self::assertSame(0, $converter3->fromParquetTypeCallCount);
    }

    public function test_from_parquet_type_with_nested_column_path() : void
    {
        $options = Options::default();
        $selectiveConverter = new SelectiveMockConverter('nested.path.column', 'converted_result');
        $dataConverter = new DataConverter([$selectiveConverter], $options);

        $column = new FlatColumn('nested.path.column', PhysicalType::INT32);

        $result = $dataConverter->fromParquetType($column, 'data');

        self::assertSame('converted_result', $result);
    }

    public function test_from_parquet_type_wraps_converter_exceptions() : void
    {
        $options = Options::default();
        $throwingConverter = new ThrowingMockConverter();
        $dataConverter = new DataConverter([$throwingConverter], $options);
        $column = new FlatColumn('test_column', PhysicalType::INT32);

        $this->expectException(DataConversionException::class);
        $this->expectExceptionMessage("Failed to convert data from parquet type for column 'test_column'. Test exception from converter");

        $dataConverter->fromParquetType($column, 'data');
    }

    public function test_initialize_creates_data_converter_with_default_converters() : void
    {
        $options = Options::default();

        $dataConverter = DataConverter::initialize($options);

        self::assertInstanceOf(DataConverter::class, $dataConverter);
    }

    public function test_initialize_returns_new_instance_each_time() : void
    {
        $options = Options::default();

        $dataConverter1 = DataConverter::initialize($options);
        $dataConverter2 = DataConverter::initialize($options);

        self::assertNotSame($dataConverter1, $dataConverter2);
    }

    public function test_to_parquet_type_caches_converter_result() : void
    {
        $options = Options::default();
        $mockConverter = new MockConverter(true, 'converted_data');
        $dataConverter = new DataConverter([$mockConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result1 = $dataConverter->toParquetType($column, 'data1');
        $result2 = $dataConverter->toParquetType($column, 'data2');

        self::assertSame('converted_data', $result1);
        self::assertSame('converted_data', $result2);
        self::assertSame(1, $mockConverter->isForCallCount);
        self::assertSame(2, $mockConverter->toParquetTypeCallCount);
    }

    public function test_to_parquet_type_caches_null_when_no_converter_matches() : void
    {
        $options = Options::default();
        $nonMatchingConverter = new MockConverter(false, 'not_used');
        $dataConverter = new DataConverter([$nonMatchingConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result1 = $dataConverter->toParquetType($column, 'data1');
        $result2 = $dataConverter->toParquetType($column, 'data2');

        self::assertSame('data1', $result1);
        self::assertSame('data2', $result2);
        self::assertSame(1, $nonMatchingConverter->isForCallCount);
        self::assertSame(0, $nonMatchingConverter->toParquetTypeCallCount);
    }

    public function test_to_parquet_type_does_not_wrap_exceptions() : void
    {
        $options = Options::default();
        $throwingConverter = new ThrowingMockConverter();
        $dataConverter = new DataConverter([$throwingConverter], $options);
        $column = new FlatColumn('test_column', PhysicalType::INT32);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Test exception from converter');

        $dataConverter->toParquetType($column, 'data');
    }

    public function test_to_parquet_type_returns_null_for_null_data() : void
    {
        $options = Options::default();
        $dataConverter = new DataConverter([], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->toParquetType($column, null);

        self::assertNull($result);
    }

    public function test_to_parquet_type_returns_original_data_when_no_converter_matches() : void
    {
        $options = Options::default();
        $dataConverter = new DataConverter([], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);
        $data = 'test_data';

        $result = $dataConverter->toParquetType($column, $data);

        self::assertSame($data, $result);
    }

    public function test_to_parquet_type_uses_first_matching_converter() : void
    {
        $options = Options::default();
        $firstConverter = new MockConverter(true, 'first_converter_result');
        $secondConverter = new MockConverter(true, 'second_converter_result');
        $dataConverter = new DataConverter([$firstConverter, $secondConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);

        $result = $dataConverter->toParquetType($column, 'data');

        self::assertSame('first_converter_result', $result);
        self::assertSame(1, $firstConverter->isForCallCount);
        self::assertSame(1, $firstConverter->toParquetTypeCallCount);
        self::assertSame(0, $secondConverter->isForCallCount);
        self::assertSame(0, $secondConverter->toParquetTypeCallCount);
    }

    public function test_to_parquet_type_uses_matching_converter() : void
    {
        $options = Options::default();
        $mockConverter = new MockConverter(true, 'converted_to_parquet');
        $dataConverter = new DataConverter([$mockConverter], $options);
        $column = new FlatColumn('test', PhysicalType::INT32);
        $data = 'original_data';

        $result = $dataConverter->toParquetType($column, $data);

        self::assertSame('converted_to_parquet', $result);
    }

    public function test_to_parquet_type_with_different_column_paths_uses_separate_cache_entries() : void
    {
        $options = Options::default();
        $selectiveConverter = new SelectiveMockConverter('column1', 'converter_result');
        $dataConverter = new DataConverter([$selectiveConverter], $options);

        $column1 = new FlatColumn('column1', PhysicalType::INT32);
        $column2 = new FlatColumn('column2', PhysicalType::INT64);

        $result1 = $dataConverter->toParquetType($column1, 'data');
        $result2 = $dataConverter->toParquetType($column2, 'data');

        self::assertSame('converter_result', $result1);
        self::assertSame('data', $result2); // No converter matches column2
        self::assertSame(2, $selectiveConverter->isForCallCount); // Called for both columns
        self::assertSame(1, $selectiveConverter->toParquetTypeCallCount); // Only called for column1
    }

    public function test_to_parquet_type_with_nested_column_path() : void
    {
        $options = Options::default();
        $selectiveConverter = new SelectiveMockConverter('nested.path.column', 'converted_result');
        $dataConverter = new DataConverter([$selectiveConverter], $options);

        $column = new FlatColumn('nested.path.column', PhysicalType::INT32);

        $result = $dataConverter->toParquetType($column, 'data');

        self::assertSame('converted_result', $result);
    }
}

/**
 * Mock converter for testing purposes.
 */
final class MockConverter implements Converter
{
    public int $fromParquetTypeCallCount = 0;

    public int $isForCallCount = 0;

    public int $toParquetTypeCallCount = 0;

    public function __construct(
        private readonly bool $isForResult,
        private readonly mixed $conversionResult,
    ) {
    }

    public function fromParquetType(mixed $data) : mixed
    {
        $this->fromParquetTypeCallCount++;

        return $this->conversionResult;
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        $this->isForCallCount++;

        return $this->isForResult;
    }

    public function toParquetType(mixed $data) : mixed
    {
        $this->toParquetTypeCallCount++;

        return $this->conversionResult;
    }
}

/**
 * Mock converter that only matches specific column names.
 */
final class SelectiveMockConverter implements Converter
{
    public int $fromParquetTypeCallCount = 0;

    public int $isForCallCount = 0;

    public int $toParquetTypeCallCount = 0;

    public function __construct(
        private readonly string $matchingColumnName,
        private readonly mixed $conversionResult,
    ) {
    }

    public function fromParquetType(mixed $data) : mixed
    {
        $this->fromParquetTypeCallCount++;

        return $this->conversionResult;
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        $this->isForCallCount++;

        return $column->name() === $this->matchingColumnName;
    }

    public function toParquetType(mixed $data) : mixed
    {
        $this->toParquetTypeCallCount++;

        return $this->conversionResult;
    }
}

/**
 * Mock converter that handles complex data types.
 */
final class ComplexDataMockConverter implements Converter
{
    public function fromParquetType(mixed $data) : mixed
    {
        return 'complex_from_parquet';
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        return true;
    }

    public function toParquetType(mixed $data) : mixed
    {
        return 'complex_to_parquet';
    }
}

/**
 * Mock converter that throws exceptions for testing error handling.
 */
final class ThrowingMockConverter implements Converter
{
    public function fromParquetType(mixed $data) : mixed
    {
        throw new \RuntimeException('Test exception from converter');
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        return true;
    }

    public function toParquetType(mixed $data) : mixed
    {
        throw new \RuntimeException('Test exception from converter');
    }
}
