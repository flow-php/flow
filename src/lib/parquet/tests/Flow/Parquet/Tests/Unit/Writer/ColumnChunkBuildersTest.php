<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Dremel\{WriteColumnData};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, NestedColumn, PhysicalType};
use Flow\Parquet\Writer\{ColumnChunkBuilder, ColumnChunkBuilders, ColumnChunkContainer};
use PHPUnit\Framework\TestCase;

final class ColumnChunkBuildersTest extends TestCase
{
    public function test_add_multiple_times_with_same_column() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('test_col', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData1 = WriteColumnData::initialize($flatColumn);
        $columnData2 = WriteColumnData::initialize($flatColumn);

        $builders->add($columnData1);
        $builders->add($columnData2);

        self::assertTrue(true);
    }

    public function test_add_with_column_data_calls_correct_builder() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('test_col', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData = WriteColumnData::initialize($flatColumn);

        $builders->add($columnData);

        self::assertTrue(true);
    }

    public function test_add_with_flat_column() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData = WriteColumnData::initialize($flatColumn);
        $builders->add($columnData);

        self::assertTrue(true);
    }

    public function test_add_with_nested_column() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData = WriteColumnData::initialize($nestedColumn);
        $builders->add($columnData);

        self::assertTrue(true);
    }

    public function test_close_pages_before_flush() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();
        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_close_pages_with_empty_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $schema = Schema::with();

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();

        self::assertTrue(true);
    }

    public function test_close_pages_with_multiple_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();

        self::assertTrue(true);
    }

    public function test_close_pages_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();

        self::assertTrue(true);
    }

    public function test_close_pages_with_single_builder() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();

        self::assertTrue(true);
    }

    public function test_constructor_with_builders_array() : void
    {
        $mockBuilder = $this->createMock(ColumnChunkBuilder::class);
        $builders = new ColumnChunkBuilders(['test' => $mockBuilder]);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_constructor_with_empty_builders() : void
    {
        $builders = new ColumnChunkBuilders([]);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_different_compression_types() : void
    {
        $options = new Options();

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $compressions = [
            Compressions::UNCOMPRESSED,
            Compressions::GZIP,
            Compressions::SNAPPY,
        ];

        foreach ($compressions as $compression) {
            $builders = ColumnChunkBuilders::initialize($schema, $options, $compression);

            $containers = $builders->flush(0);

            self::assertIsArray($containers);
            self::assertCount(1, $containers);
            self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        }
    }

    public function test_different_physical_types() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $intColumn = new FlatColumn('int_col', PhysicalType::INT32);
        $longColumn = new FlatColumn('long_col', PhysicalType::INT64);
        $floatColumn = new FlatColumn('float_col', PhysicalType::FLOAT);
        $doubleColumn = new FlatColumn('double_col', PhysicalType::DOUBLE);
        $boolColumn = new FlatColumn('bool_col', PhysicalType::BOOLEAN);
        $stringColumn = new FlatColumn('string_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());

        $schema = Schema::with($intColumn, $longColumn, $floatColumn, $doubleColumn, $boolColumn, $stringColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(6, $containers);

        foreach ($containers as $container) {
            self::assertInstanceOf(ColumnChunkContainer::class, $container);
        }
    }

    public function test_edge_case_large_file_offset() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $largeOffset = 1000000;
        $containers = $builders->flush($largeOffset);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame($largeOffset, $containers[0]->columnChunk->fileOffset());
    }

    public function test_edge_case_zero_file_offset() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertSame(0, $containers[0]->columnChunk->fileOffset());
    }

    public function test_flush_calculates_offset_correctly() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(100);

        self::assertIsArray($containers);
        self::assertCount(2, $containers);

        $expectedOffset = 100 + strlen($containers[0]->binaryBuffer);
        self::assertGreaterThanOrEqual(100, $containers[0]->columnChunk->fileOffset());
        self::assertGreaterThanOrEqual($expectedOffset, $containers[1]->columnChunk->fileOffset());
    }

    public function test_flush_with_different_file_offset() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(1000);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_flush_with_empty_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $schema = Schema::with();

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertEmpty($containers);
    }

    public function test_flush_with_multiple_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(2, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
    }

    public function test_flush_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(2, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
    }

    public function test_flush_with_single_builder() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_initialize_with_empty_schema() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $schema = Schema::with();

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_initialize_with_flat_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());

        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_initialize_with_mixed_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('flat', PhysicalType::INT32);
        $childColumn = new FlatColumn('child', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);

        $schema = Schema::with($flatColumn, $nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_initialize_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);

        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        self::assertInstanceOf(ColumnChunkBuilders::class, $builders);
    }

    public function test_is_any_page_full_with_empty_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $schema = Schema::with();

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $result = $builders->isAnyPageFull();

        self::assertFalse($result);
    }

    public function test_is_any_page_full_with_multiple_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $result = $builders->isAnyPageFull();

        self::assertFalse($result);
    }

    public function test_is_any_page_full_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $result = $builders->isAnyPageFull();

        self::assertFalse($result);
    }

    public function test_is_any_page_full_with_single_builder() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $result = $builders->isAnyPageFull();

        self::assertFalse($result);
    }

    public function test_multiple_close_pages_calls() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $builders->closePages();
        $builders->closePages();
        $builders->closePages();

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_multiple_flush_calls() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $containers1 = $builders->flush(0);
        $containers2 = $builders->flush(100);

        self::assertIsArray($containers1);
        self::assertIsArray($containers2);
        self::assertCount(1, $containers1);
        self::assertCount(1, $containers2);
    }

    public function test_uncompressed_size_is_cumulative() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $totalSize = $builders->uncompressedSize();

        self::assertIsInt($totalSize);
        self::assertGreaterThanOrEqual(0, $totalSize);
    }

    public function test_uncompressed_size_with_empty_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $schema = Schema::with();

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $size = $builders->uncompressedSize();

        self::assertSame(0, $size);
    }

    public function test_uncompressed_size_with_multiple_builders() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $size = $builders->uncompressedSize();

        self::assertIsInt($size);
        self::assertGreaterThanOrEqual(0, $size);
    }

    public function test_uncompressed_size_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $size = $builders->uncompressedSize();

        self::assertIsInt($size);
        self::assertGreaterThanOrEqual(0, $size);
    }

    public function test_uncompressed_size_with_single_builder() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $size = $builders->uncompressedSize();

        self::assertIsInt($size);
        self::assertGreaterThanOrEqual(0, $size);
    }

    public function test_workflow_initialize_add_close_flush() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn = new FlatColumn('col1', PhysicalType::INT32);
        $schema = Schema::with($flatColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData = WriteColumnData::initialize($flatColumn);
        $builders->add($columnData);

        $builders->closePages();

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_workflow_with_multiple_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $flatColumn1 = new FlatColumn('col1', PhysicalType::INT32);
        $flatColumn2 = new FlatColumn('col2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $schema = Schema::with($flatColumn1, $flatColumn2);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData1 = WriteColumnData::initialize($flatColumn1);
        $columnData2 = WriteColumnData::initialize($flatColumn2);

        $builders->add($columnData1);
        $builders->add($columnData2);

        self::assertFalse($builders->isAnyPageFull());

        $builders->closePages();

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(2, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
    }

    public function test_workflow_with_nested_columns() : void
    {
        $options = new Options();
        $compressions = Compressions::UNCOMPRESSED;

        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $schema = Schema::with($nestedColumn);

        $builders = ColumnChunkBuilders::initialize($schema, $options, $compressions);

        $columnData = WriteColumnData::initialize($nestedColumn);
        $builders->add($columnData);

        self::assertFalse($builders->isAnyPageFull());

        $builders->closePages();

        $containers = $builders->flush(0);

        self::assertIsArray($containers);
        self::assertCount(2, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
    }
}
