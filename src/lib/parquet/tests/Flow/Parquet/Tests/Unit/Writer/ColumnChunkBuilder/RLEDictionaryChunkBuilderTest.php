<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\RLEDictionaryChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkContainer;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use Flow\Parquet\Writer\StatisticsOptions;
use Flow\Parquet\Writer\ValueStorage\BufferValueStorage;
use Flow\Parquet\Writer\ValueStorage\DeltaByteArrayValueStorage;
use Flow\Parquet\Writer\ValueStorage\ValueStorage;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function array_map;
use function in_array;
use function is_object;
use function is_scalar;
use function pack;
use function strlen;

final class RLEDictionaryChunkBuilderTest extends TestCase
{
    public static function compression_types_provider(): Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function dictionary_data_provider(): Generator
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

    public static function overflow_provider(): Generator
    {
        yield 'PLAIN after an indexed page' => [new BufferValueStorage(), null, true];
        yield 'PLAIN from the first page' => [new BufferValueStorage(), 'a', false];
        yield 'DELTA_BYTE_ARRAY after an indexed page' => [new DeltaByteArrayValueStorage(), null, true];
        yield 'DELTA_BYTE_ARRAY from the first page' => [new DeltaByteArrayValueStorage(), 'a', false];
    }

    public static function page_size_provider(): Generator
    {
        yield 'small page' => [1024];
        yield 'medium page' => [8192];
        yield 'large page' => [65536];
    }

    public static function physical_types_provider(): Generator
    {
        yield 'int32' => [PhysicalType::INT32, [10, 20, 10, 30, 20]];
        yield 'int64' => [PhysicalType::INT64, [1234567890123, 9876543210987, 1234567890123]];
        yield 'float' => [PhysicalType::FLOAT, [3.14, 2.71, 3.14, 1.41]];
        yield 'double' => [PhysicalType::DOUBLE, [2.718281828, 3.141592653, 2.718281828]];
        yield 'boolean' => [PhysicalType::BOOLEAN, [true, false, true, false, true]];
        yield 'byte_array' => [PhysicalType::BYTE_ARRAY, ['hello', 'world', 'hello', 'test', 'world']];
    }

    public static function writer_version_provider(): Generator
    {
        yield 'version 1' => [1];
        yield 'version 2' => [2];
    }

    public function test_add_multiple_rows(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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

            if (is_scalar($value) || is_object($value)) {
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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [], [], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_null_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [0], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_repeated_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['hello']));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_a_chunk_of_nulls_writes_an_empty_dictionary_page(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            new Options(),
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions(new Options()),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [0], []));

        static::assertSame(4, $builder->flush(4)[0]->columnChunk->dictionaryPageOffset());
    }

    public function test_a_chunk_where_every_page_overflowed_writes_no_dictionary_page(): void
    {
        $column = FlatColumn::string('test_col');
        $options = (new Options())->set(Option::DICTIONARY_PAGE_SIZE, 1);
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));
        $builder->closePage();
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['b']));
        $chunk = $builder->flush(4)[0]->columnChunk;

        static::assertNull($chunk->dictionaryPageOffset());
        static::assertNotContains(Encodings::RLE_DICTIONARY, $chunk->encodings());
    }

    public function test_an_all_null_page_writes_the_bit_width_byte(): void
    {
        $column = FlatColumn::string('test_col');
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            new Options(),
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions(new Options()),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0], [0, 0], []));

        static::assertStringEndsWith(
            (new RLEBitPackedPacker(new RLEBitPackedHybrid()))->packWithLength(1, [0, 0]) . "\x00",
            $builder->flush(4)[0]->binaryBuffer,
        );
    }

    public function test_close_page_resets_internal_state(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $builder->closePage();

        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_empty_data(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['test']));

        $this->expectException(RuntimeException::class);
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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        $result = $builder->column();

        static::assertSame($column, $result);
    }

    public function test_constructor_initializes_correctly(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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

        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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

        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
        static::assertSame($column, $builder->column());
    }

    public function test_dictionary_compression_efficiency(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        static::assertFalse($builder->isFull());
    }

    #[DataProvider('page_size_provider')]
    public function test_is_full_respects_page_size_option(int $pageSize): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, $pageSize);
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_workflow_add_close_flush(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            $compression,
            StatisticsOptions::fromOptions($options),
            new BufferValueStorage(),
        );

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

    public function test_each_page_gets_a_column_index_entry_and_an_offset_index_location(): void
    {
        $column = FlatColumn::int64('id');
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            new Options(),
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions(new Options()),
            new BufferValueStorage(),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0], [1, 1], [1, 2]));
        $builder->closePage();
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [0], []));
        $container = $builder->flush(4)[0];
        $last = $container->offsetIndex->pages[1];

        static::assertEquals(
            new ColumnIndex([false, true], [pack('P', 1), ''], [pack('P', 2), ''], BoundaryOrder::ASCENDING, [0, 1]),
            $container->columnIndex,
        );
        static::assertSame(
            [0, 2],
            array_map(static fn(PageLocation $page): int => $page->firstRowIndex, $container->offsetIndex->pages),
        );
        static::assertSame($container->columnChunk->dataPageOffset(), $container->offsetIndex->pages[0]->offset);
        static::assertSame(4 + strlen($container->binaryBuffer), $last->offset + $last->compressedPageSize);
    }

    #[DataProvider('overflow_provider')]
    public function test_overflow_pages_use_the_fallback_storage_encoding(
        ValueStorage $fallback,
        ?string $firstPageValue,
        bool $indexed,
    ): void {
        $column = FlatColumn::string('test_col');
        $options = (new Options())->set(Option::DICTIONARY_PAGE_SIZE, 1);
        $builder = new RLEDictionaryChunkBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
            $fallback,
        );

        $builder->addColumn(
            $firstPageValue === null
                ? new WriteFlatColumnValues($column, [0], [0], [])
                : new WriteFlatColumnValues($column, [0], [1], [$firstPageValue]),
        );
        $builder->closePage();
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['b']));
        $encodings = $builder->flush(4)[0]->columnChunk->encodings();

        static::assertContains($fallback->encoding(), $encodings);
        static::assertSame($indexed, in_array(Encodings::RLE_DICTIONARY, $encodings, true));
    }
}
