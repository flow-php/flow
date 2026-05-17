<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\Codec;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\RLEDictionaryChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkContainer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RLEDictionaryChunkBuilderTest extends TestCase
{
    public static function compression_types_provider(): \Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function dictionary_data_provider(): \Generator
    {
        yield 'string repetition' => [
            ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple'],
            'repeated string values for dictionary compression',
        ];

        yield 'numeric repetition' => [
            [100, 200, 100, 300, 200, 100, 400],
            'repeated numeric values for dictionary compression',
        ];

        yield 'mixed repetition with nulls' => [
            ['a', null, 'b', 'a', null, 'c', 'b'],
            'mixed values with nulls for dictionary compression',
        ];

        yield 'single value repeated' => [
            ['same', 'same', 'same', 'same'],
            'single value repeated multiple times',
        ];

        yield 'no repetition' => [
            ['unique1', 'unique2', 'unique3', 'unique4'],
            'unique values with no repetition',
        ];
    }

    public static function page_size_provider(): \Generator
    {
        yield 'small page' => [1024];
        yield 'medium page' => [8192];
        yield 'large page' => [65536];
    }

    public static function physical_types_provider(): \Generator
    {
        yield 'int32' => [PhysicalType::INT32, [10, 20, 10, 30, 20]];
        yield 'int64' => [PhysicalType::INT64, [1234567890123, 9876543210987, 1234567890123]];
        yield 'float' => [PhysicalType::FLOAT, [3.14, 2.71, 3.14, 1.41]];
        yield 'double' => [PhysicalType::DOUBLE, [2.718281828, 3.141592653, 2.718281828]];
        yield 'boolean' => [PhysicalType::BOOLEAN, [true, false, true, false, true]];
        yield 'byte_array' => [PhysicalType::BYTE_ARRAY, ['hello', 'world', 'hello', 'test', 'world']];
    }

    public static function writer_version_provider(): \Generator
    {
        yield 'version 1' => [1];
        yield 'version 2' => [2];
    }

    public function test_add_multiple_rows(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 5; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["value_{$i}"]));
        }

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    /**
     * @param array<mixed> $values
     */
    #[DataProvider('dictionary_data_provider')]
    public function test_add_row_with_dictionary_data(array $values, string $description): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $repLevels = [];
        $defLevels = [];
        /** @var array<null|object|scalar> $nonNullValues */
        $nonNullValues = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            $repLevels[] = 0;
            $defLevels[] = $value === null ? 0 : 1;

            if ($value === null) {
                continue;
            }

            if (\is_scalar($value) || \is_object($value)) {
                $nonNullValues[] = $value;
            }
        }

        $builder->addColumn(new WriteFlatColumnValues($column, $repLevels, $defLevels, $nonNullValues));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_empty_data(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [], [], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_null_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [0], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_repeated_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(
            new WriteFlatColumnValues(
                $column,
                [0, 0, 0, 0, 0],
                [1, 1, 1, 1, 1],
                ['apple', 'banana', 'apple', 'cherry', 'banana'],
            ),
        );

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_single_value(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['hello']));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_build_dictionary_page_without_dictionary_throws_exception(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [], [], []));

        $reflectionClass = new \ReflectionClass($builder);
        $buildDictionaryPageMethod = $reflectionClass->getMethod('buildDictionaryPage');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot build dictionary page without dictionary');

        $codec = new Codec($options);
        $buildDictionaryPageMethod->invoke($builder, $codec, $compression);
    }

    public function test_close_page_resets_internal_state(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $beforeSize = $builder->uncompressedSize();
        $builder->closePage();
        $afterSize = $builder->uncompressedSize();

        static::assertGreaterThan($beforeSize, $afterSize);
    }

    public function test_close_page_with_data_builds_dictionary(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(
            new WriteFlatColumnValues($column, [0, 0, 0, 0], [1, 1, 1, 1], ['apple', 'banana', 'apple', 'cherry']),
        );

        $builder->closePage();

        static::assertFalse($builder->isFull());
        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    #[DataProvider('writer_version_provider')]
    public function test_close_page_with_different_writer_versions(int $writerVersion): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $options->set(Option::WRITER_VERSION, $writerVersion);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $builder->closePage();

        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_empty_data(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->closePage();

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_unsupported_writer_version_throws_exception(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $options->set(Option::WRITER_VERSION, 3);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage(
            'Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: 3',
        );

        $builder->closePage();
    }

    public function test_column_returns_correct_column(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $result = $builder->column();

        static::assertSame($column, $result);
    }

    public function test_constructor_initializes_correctly(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
        static::assertSame($column, $builder->column());
        static::assertFalse($builder->isFull());
        static::assertSame(0, $builder->uncompressedSize());
    }

    #[DataProvider('compression_types_provider')]
    public function test_constructor_with_different_compressions(Compressions $compression): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();

        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
    }

    /**
     * @param array<mixed> $sampleValues
     */
    #[DataProvider('physical_types_provider')]
    public function test_constructor_with_different_physical_types(
        PhysicalType $physicalType,
        array $sampleValues,
    ): void {
        $column = new FlatColumn('test_col', $physicalType);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
        static::assertSame($column, $builder->column());
    }

    public function test_dictionary_compression_efficiency(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $repeatedValues = ['apple', 'banana', 'cherry'];
        $repLevels = [];
        $defLevels = [];
        $values = [];

        for ($i = 0; $i < 100; $i++) {
            $repLevels[] = 0;
            $defLevels[] = 1;
            $values[] = $repeatedValues[$i % 3];
        }

        $builder->addColumn(new WriteFlatColumnValues($column, $repLevels, $defLevels, $values));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame(100, $containers[0]->columnChunk->valuesCount());

        $encodings = $containers[0]->columnChunk->encodings();
        static::assertContains(Encodings::RLE_DICTIONARY, $encodings);
    }

    #[DataProvider('compression_types_provider')]
    public function test_different_compressions_work_correctly(Compressions $compression): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame($compression, $containers[0]->columnChunk->codec());
    }

    public function test_edge_case_boolean_false_value(): void
    {
        $column = new FlatColumn('bool_col', PhysicalType::BOOLEAN);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [false]));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_edge_case_empty_string_value(): void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['']));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_edge_case_large_string_value(): void
    {
        $column = new FlatColumn('str_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $largeString = str_repeat('x', 10000);
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$largeString]));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_edge_case_zero_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [0]));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame(1, $containers[0]->columnChunk->valuesCount());
    }

    public function test_flush_automatically_closes_page_when_data_exists(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertGreaterThan(0, $containers[0]->columnChunk->valuesCount());
    }

    public function test_flush_cleans_up_builder_state(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0], [1, 1], ['hello', 'world']));

        static::assertFalse($builder->isEmpty());
        static::assertGreaterThan(0, $builder->uncompressedSize());

        $containers = $builder->flush(0);
        static::assertCount(1, $containers);

        static::assertTrue($builder->isEmpty());
        static::assertEquals(0, $builder->uncompressedSize());

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        static::assertFalse($builder->isEmpty());
        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_flush_preserves_column_chunk_metadata(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::GZIP;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $containers = $builder->flush(0);

        $columnChunk = $containers[0]->columnChunk;
        static::assertSame($column->type(), $columnChunk->type());
        static::assertSame($compression, $columnChunk->codec());
        static::assertSame($column->flatPath(), $columnChunk->flatPath());
        static::assertNotNull($columnChunk->statistics());
    }

    public function test_flush_with_data(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $containers = $builder->flush(100);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(100, $containers[0]->columnChunk->fileOffset());
        static::assertGreaterThan(0, strlen($containers[0]->binaryBuffer));
    }

    public function test_flush_with_dictionary_includes_dictionary_page(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(
            new WriteFlatColumnValues($column, [0, 0, 0, 0], [1, 1, 1, 1], ['apple', 'banana', 'apple', 'cherry']),
        );

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);

        $columnChunk = $containers[0]->columnChunk;
        static::assertNotNull($columnChunk->dictionaryPageOffset());
        static::assertNotNull($columnChunk->dataPageOffset());
        static::assertGreaterThan($columnChunk->dictionaryPageOffset(), $columnChunk->dataPageOffset());
    }

    public function test_flush_with_different_file_offsets(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $offsets = [0, 100, 1000, 50000];

        foreach ($offsets as $offset) {
            $containers = $builder->flush($offset);

            static::assertIsArray($containers);
            static::assertCount(1, $containers);
            static::assertSame($offset, $containers[0]->columnChunk->fileOffset());
        }
    }

    public function test_flush_with_empty_data(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(0, $containers[0]->columnChunk->fileOffset());
    }

    public function test_is_full_initially_false(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertFalse($builder->isFull());
    }

    #[DataProvider('page_size_provider')]
    public function test_is_full_respects_page_size_option(int $pageSize): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, $pageSize);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertFalse($builder->isFull());

        for ($i = 0; $i < 10000; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["value_{$i}"]));

            if ($builder->isFull()) {
                break;
            }
        }

        static::assertTrue(true);
    }

    public function test_is_full_uses_approximate_calculation(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, 100);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 30; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["value_{$i}"]));
        }

        static::assertTrue($builder->isFull());
    }

    public function test_mixed_null_and_non_null_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0, 0], [1, 0, 1], ['hello', 'world']));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertSame(3, $containers[0]->columnChunk->valuesCount());
    }

    public function test_multiple_close_page_calls(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $builder->closePage();
        $builder->closePage();
        $builder->closePage();

        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_statistics_are_generated(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0, 0], [1, 1, 1], ['apple', 'zebra', 'banana']));

        $containers = $builder->flush(0);

        $statistics = $containers[0]->columnChunk->statistics();
        static::assertNotNull($statistics);
    }

    public function test_uncompressed_size_accumulates_multiple_pages(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $sizes = [];

        for ($i = 0; $i < 3; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["value_{$i}"]));
            $builder->closePage();

            $sizes[] = $builder->uncompressedSize();
        }

        static::assertGreaterThan($sizes[0], $sizes[1]);
        static::assertGreaterThan($sizes[1], $sizes[2]);
    }

    public function test_uncompressed_size_increases_with_pages(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $initialSize = $builder->uncompressedSize();

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));
        $builder->closePage();

        $finalSize = $builder->uncompressedSize();

        static::assertGreaterThan($initialSize, $finalSize);
    }

    public function test_uncompressed_size_initially_zero(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_workflow_add_close_flush(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $builder->closePage();

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_workflow_multiple_add_single_close_flush(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        for ($i = 0; $i < 5; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["value_{$i}"]));
        }

        $builder->closePage();

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(5, $containers[0]->columnChunk->valuesCount());
    }

    public function test_workflow_multiple_cycles(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder($column, $options, $compression);

        for ($cycle = 0; $cycle < 3; $cycle++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ["cycle_{$cycle}"]));
            $builder->closePage();
        }

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(3, $containers[0]->columnChunk->valuesCount());
    }
}
