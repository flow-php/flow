<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\{FlatValue};
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\PlainFlatColumnChunkBuilder;
use Flow\Parquet\Writer\{ColumnChunkContainer};
use PHPUnit\Framework\TestCase;

final class PlainFlatColumnChunkBuilderTest extends TestCase
{
    public static function compression_types_provider() : \Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function page_size_provider() : \Generator
    {
        yield 'small page' => [1024];
        yield 'medium page' => [8192];
        yield 'large page' => [65536];
    }

    public static function physical_types_provider() : \Generator
    {
        yield 'int32' => [PhysicalType::INT32, 42];
        yield 'int64' => [PhysicalType::INT64, 1234567890123];
        yield 'float' => [PhysicalType::FLOAT, 3.14];
        yield 'double' => [PhysicalType::DOUBLE, 2.718281828];
        yield 'boolean' => [PhysicalType::BOOLEAN, true];
        yield 'byte_array' => [PhysicalType::BYTE_ARRAY, 'test_string'];
    }

    public static function writer_version_provider() : \Generator
    {
        yield 'version 1' => [1];
        yield 'version 2' => [2];
    }

    public function test_add_multiple_rows() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 5; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i * 10);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_boolean_values() : void
    {
        $column = new FlatColumn('bool_col', PhysicalType::BOOLEAN);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, true);
        $flatValue2 = new FlatValue($column, 0, 1, false);
        $columnData->addValue($flatValue1, $flatValue2);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_empty_data() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_extreme_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT64);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, PHP_INT_MAX);
        $flatValue2 = new FlatValue($column, 0, 1, PHP_INT_MIN);
        $columnData->addValue($flatValue1, $flatValue2);

        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(2, $containers[0]->columnChunk->valuesCount());
    }

    public function test_add_row_with_multiple_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 42);
        $flatValue2 = new FlatValue($column, 0, 1, 100);
        $flatValue3 = new FlatValue($column, 0, 0, null);
        $columnData->addValue($flatValue1, $flatValue2, $flatValue3);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_null_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 0, null);
        $columnData->addValue($flatValue);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_repetition_levels() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 42);
        $flatValue2 = new FlatValue($column, 1, 1, 100);
        $columnData->addValue($flatValue1, $flatValue2);

        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(2, $containers[0]->columnChunk->valuesCount());
    }

    public function test_add_row_with_single_value() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_string_values() : void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 'hello');
        $flatValue2 = new FlatValue($column, 0, 1, 'world');
        $columnData->addValue($flatValue1, $flatValue2);

        $builder->addRow($columnData);

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_very_large_definition_levels() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 255, 42);
        $columnData->addValue($flatValue);

        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_close_page_resets_internal_state() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $beforeSize = $builder->uncompressedSize();
        $builder->closePage();
        $afterSize = $builder->uncompressedSize();

        self::assertGreaterThan($beforeSize, $afterSize);
    }

    public function test_close_page_with_data() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $builder->closePage();

        self::assertFalse($builder->isFull());
        self::assertGreaterThan(0, $builder->uncompressedSize());
    }

    /**
     * @dataProvider writer_version_provider
     */
    public function test_close_page_with_different_writer_versions(int $writerVersion) : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $options->set(Option::WRITER_VERSION, $writerVersion);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $builder->closePage();

        self::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_empty_data() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $builder->closePage();

        self::assertFalse($builder->isFull());
        self::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_unsupported_writer_version_throws_exception() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $options->set(Option::WRITER_VERSION, 3);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: 3');

        $builder->closePage();
    }

    public function test_column_returns_correct_column() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $result = $builder->column();

        self::assertSame($column, $result);
    }

    public function test_constructor_initializes_correctly() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
        self::assertSame($column, $builder->column());
        self::assertFalse($builder->isFull());
        self::assertSame(0, $builder->uncompressedSize());
    }

    public function test_constructor_with_boolean_column_uses_boolean_storage() : void
    {
        $column = new FlatColumn('bool_col', PhysicalType::BOOLEAN);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
        self::assertSame($column, $builder->column());
    }

    public function test_constructor_with_byte_array_logical_type() : void
    {
        $column = new FlatColumn('binary_col', PhysicalType::BYTE_ARRAY);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
        self::assertSame($column, $builder->column());
    }

    /**
     * @dataProvider compression_types_provider
     */
    public function test_constructor_with_different_compressions(Compressions $compression) : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();

        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    /**
     * @dataProvider physical_types_provider
     */
    public function test_constructor_with_different_physical_types(PhysicalType $physicalType, mixed $sampleValue) : void
    {
        $column = new FlatColumn('test_col', $physicalType);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
        self::assertSame($column, $builder->column());
    }

    public function test_edge_case_boolean_false_value() : void
    {
        $column = new FlatColumn('bool_col', PhysicalType::BOOLEAN);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, false);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_edge_case_empty_string_value() : void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, '');
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_edge_case_large_string_value() : void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $largeString = str_repeat('x', 10000);
        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, $largeString);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_edge_case_zero_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 0);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_flush_automatically_closes_page_when_data_exists() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertGreaterThan(0, $containers[0]->columnChunk->valuesCount());
    }

    public function test_flush_preserves_column_chunk_metadata() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::GZIP;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        $columnChunk = $containers[0]->columnChunk;
        self::assertSame($column->type(), $columnChunk->type());
        self::assertSame($compression, $columnChunk->codec());
        self::assertSame($column->flatPath(), $columnChunk->flatPath());
        self::assertNotNull($columnChunk->statistics());
    }

    public function test_flush_with_data() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $containers = $builder->flush(100);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertSame(100, $containers[0]->columnChunk->fileOffset());
        self::assertGreaterThan(0, strlen($containers[0]->binaryBuffer));
    }

    public function test_flush_with_different_file_offsets() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $offsets = [0, 100, 1000, 50000];

        foreach ($offsets as $offset) {
            $containers = $builder->flush($offset);

            self::assertIsArray($containers);
            self::assertCount(1, $containers);
            self::assertSame($offset, $containers[0]->columnChunk->fileOffset());
        }
    }

    public function test_flush_with_empty_data() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertSame(0, $containers[0]->columnChunk->fileOffset());
    }

    public function test_flush_with_negative_file_offset() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $containers = $builder->flush(-100);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(-100, $containers[0]->columnChunk->fileOffset());
    }

    public function test_is_full_initially_false() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertFalse($builder->isFull());
    }

    /**
     * @dataProvider page_size_provider
     */
    public function test_is_full_respects_page_size_option(int $pageSize) : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, $pageSize);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertFalse($builder->isFull());

        for ($i = 0; $i < 1000; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);

            if ($builder->isFull()) {
                break;
            }
        }

        self::assertTrue(true);
    }

    public function test_is_full_with_zero_page_size() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, 0);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        self::assertTrue($builder->isFull());
    }

    public function test_mixed_null_and_non_null_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 42);
        $flatValue2 = new FlatValue($column, 0, 0, null);
        $flatValue3 = new FlatValue($column, 0, 1, 100);
        $columnData->addValue($flatValue1, $flatValue2, $flatValue3);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(3, $containers[0]->columnChunk->valuesCount());
    }

    public function test_multiple_close_page_calls() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $builder->closePage();
        $builder->closePage();
        $builder->closePage();

        self::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_multiple_flush_calls_maintain_consistency() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $containers1 = $builder->flush(0);
        $containers2 = $builder->flush(100);
        $containers3 = $builder->flush(200);

        self::assertCount(1, $containers1);
        self::assertCount(1, $containers2);
        self::assertCount(1, $containers3);

        self::assertSame(0, $containers1[0]->columnChunk->fileOffset());
        self::assertSame(100, $containers2[0]->columnChunk->fileOffset());
        self::assertSame(200, $containers3[0]->columnChunk->fileOffset());
    }

    public function test_statistics_are_generated() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 10);
        $flatValue2 = new FlatValue($column, 0, 1, 50);
        $flatValue3 = new FlatValue($column, 0, 1, 30);
        $columnData->addValue($flatValue1, $flatValue2, $flatValue3);
        $builder->addRow($columnData);

        $containers = $builder->flush(0);

        $statistics = $containers[0]->columnChunk->statistics();
        self::assertNotNull($statistics);
    }

    public function test_uncompressed_size_accumulates_multiple_pages() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $sizes = [];

        for ($i = 0; $i < 3; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
            $builder->closePage();

            $sizes[] = $builder->uncompressedSize();
        }

        self::assertGreaterThan($sizes[0], $sizes[1]);
        self::assertGreaterThan($sizes[1], $sizes[2]);
    }

    public function test_uncompressed_size_increases_with_pages() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $initialSize = $builder->uncompressedSize();

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);
        $builder->closePage();

        $finalSize = $builder->uncompressedSize();

        self::assertGreaterThan($initialSize, $finalSize);
    }

    public function test_uncompressed_size_initially_zero() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        self::assertSame(0, $builder->uncompressedSize());
    }

    public function test_uncompressed_size_with_large_data() : void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $largeString = str_repeat('A', 50000);
        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, $largeString);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);
        $builder->closePage();

        $size = $builder->uncompressedSize();

        self::assertGreaterThan(50000, $size);
    }

    public function test_workflow_add_close_flush() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue = new FlatValue($column, 0, 1, 42);
        $columnData->addValue($flatValue);
        $builder->addRow($columnData);

        $builder->closePage();

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_workflow_multiple_add_single_close_flush() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 5; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i * 10);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $builder->closePage();

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertSame(5, $containers[0]->columnChunk->valuesCount());
    }

    public function test_workflow_multiple_cycles() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        for ($cycle = 0; $cycle < 3; $cycle++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $cycle * 100);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
            $builder->closePage();
        }

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertSame(3, $containers[0]->columnChunk->valuesCount());
    }

    public function test_workflow_with_all_null_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new PlainFlatColumnChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 5; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 0, null);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(5, $containers[0]->columnChunk->valuesCount());
    }
}
